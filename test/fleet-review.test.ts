import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {proposeFleet,scheduleFleet} from '../src/fleet-scheduler.ts';

function fixture(managerProvider:'fake'|'claude'='fake') {
  const root=mkdtempSync(join(tmpdir(),'fleet-independent-review-')),oldHome=process.env.AGENT_FLEET_HOME;
  process.env.AGENT_FLEET_HOME=join(root,'history');
  git(root,'init');writeFileSync(join(root,'seed.txt'),'isolated review fixture\n');git(root,'add','seed.txt');git(root,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m','fixture');
  const engine=Engine.init(root,'Independent fleet review');
  engine.config({version:1,maxOutputBytes:1024*1024,timeoutMs:5000,maxWorkers:1,maxReviews:1,instances:[
    {id:'manager',provider:managerProvider,role:'manager',model:'planner',capabilities:['planning']},
    {id:'worker',provider:'fake',role:'worker',model:'write',capabilities:['code']},
    {id:'reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']},
  ]});
  const poolPath=join(root,'pool.sqlite'),pool=new ResourcePool(poolPath),project={projectId:engine.root,engine};
  pool.upsertResource({id:'fake-manager',provider:'fake',model:'planner',endpointId:'local',endpointKind:'local',accountDomain:'fixture',capabilities:['planning'],roles:['manager']});
  pool.upsertResource({id:'a-worker-review-capable',provider:'fake',model:'write',endpointId:'local',endpointKind:'local',accountDomain:'fixture',capabilities:['code','review'],roles:['worker','reviewer']});
  pool.upsertResource({id:'b-independent-reviewer',provider:'fake',model:'review-approve',endpointId:'local',endpointKind:'local',accountDomain:'fixture',capabilities:['review'],roles:['reviewer']});
  pool.setLimits({global:1,projects:{[engine.root]:1},endpoints:{local:1},accounts:{fixture:1}});
  return {engine,pool,project,poolPath,close(){pool.close();engine.close();if(oldHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=oldHome;rmSync(root,{recursive:true,force:true});}};
}

test('dynamic fake manager saves a proposal without adopting or executing its plan',async()=>{
  const s=fixture();try {
    const result=await proposeFleet(s.pool,s.project,'Plan an isolated fixture');
    assert.equal(result.queued,false);assert.equal(result.run?.status,'exited');
    assert.equal((result.proposal as any)?.plan.goal,'Plan an isolated fixture');
    assert.equal(s.engine.store.getPlan(),null);assert.equal(s.engine.store.listRuns().length,1);
    const lease=s.pool.status().leases[0];assert.equal(lease.state,'completed');assert.equal(lease.runId,result.run!.id);
    assert.equal(s.engine.store.instances().find(i=>i.id===result.run!.instanceId)?.role,'manager');
    assert.equal(s.engine.store.getMeta('fleetManagerPending'),null);
    await assert.rejects(s.engine.chat('bypass'),/fleet resource arbitration/);
  }finally{s.close();}
});

test('queued manager retains message/request identity across reopen and rejects another message',async()=>{
  const s=fixture();try {
    s.pool.enqueue({id:'capacity-blocker',projectId:'other-project',sessionId:'other-session',taskId:'other-task',attemptId:'other-attempt',role:'manager',capabilities:['planning']});
    const blocker=s.pool.claimNext()!;assert(blocker);
    const first=await proposeFleet(s.pool,s.project,'Keep this exact message');assert.equal(first.queued,true);
    assert.equal(s.engine.store.listRuns().length,0);
    await assert.rejects(proposeFleet(s.pool,s.project,'A different message'),/original message/);
    assert.equal(s.pool.status().requests.length,2);
    assert.equal((s.engine.store.getMeta('fleetManagerPending') as any)?.message,'Keep this exact message');
    s.pool.release(blocker.id,blocker.fence,'completed');
    const reopened=new ResourcePool(s.poolPath);try {
      const resumed=await proposeFleet(reopened,s.project,'Keep this exact message');
      assert.equal(resumed.requestId,first.requestId);assert.equal(resumed.run?.status,'exited');
      assert.equal((resumed.proposal as any)?.plan.goal,'Keep this exact message');
      assert.equal(reopened.status().requests.length,2);assert.equal(s.engine.store.listRuns().length,1);
      assert.equal(s.engine.store.getMeta('fleetManagerPending'),null);
    }finally{reopened.close();}
  }finally{s.close();}
});

test('required Claude manager does not fall back to a fake manager or launch a provider',async()=>{
  const s=fixture('claude');try {
    const result=await proposeFleet(s.pool,s.project,'Must use configured Claude');
    assert.equal(result.queued,false);assert.match(result.blocked!,/required provider/);
    assert.equal(s.engine.store.listRuns().length,0);assert.equal(s.pool.status().leases.length,0);assert.equal(s.pool.status().requests.length,0);
  }finally{s.close();}
});

test('verified worker releases capacity before an independent resource reviews its exact commit',async()=>{
  const s=fixture();try {
    s.engine.plan({version:1,goal:'Task boundary',tasks:[{id:'work',title:'Write artifact',prompt:'fixture only',dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e',"if(require('node:fs').readFileSync('artifact.txt','utf8')!=='AgentFleet fixture artifact\\n')process.exit(1)"]]}]});
    const result=await scheduleFleet(s.pool,[s.project]);assert.deepEqual(result.errors,[]);assert.equal(result.projects[0].accepted,1);
    const leases=s.pool.status().leases,worker=leases.find(l=>l.request.role==='worker')!,reviewer=leases.find(l=>l.request.role==='reviewer')!;
    assert.equal(worker.resource.id,'a-worker-review-capable');assert.equal(reviewer.resource.id,'b-independent-reviewer');
    assert.equal(worker.state,'completed');assert.equal(reviewer.state,'completed');
    const events=s.pool.events(0,1000),released=events.find(e=>e.kind==='lease.completed'&&(e.data as any).id===worker.id)!,granted=events.find(e=>e.kind==='lease.granted'&&(e.data as any).id===reviewer.id)!;
    assert(released.event_id<granted.event_id,'worker completion must be committed before the review acquires global capacity one');
    const workerRun=s.engine.store.getRun(worker.runId!)!,reviewRun=s.engine.store.getRun(reviewer.runId!)!;
    assert.equal(workerRun.status,'accepted');assert.equal(reviewRun.baseSha,workerRun.commitSha);assert.notEqual(reviewRun.worktree,workerRun.worktree);
    assert.equal((s.engine.store.getMeta('reviewEvidence:'+workerRun.id) as any).reviewRunId,reviewRun.id);
  }finally{s.close();}
});
