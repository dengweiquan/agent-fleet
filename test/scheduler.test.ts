import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {schedule} from '../src/scheduler.ts';

function setup(options:{review?:string,worker?:string,timeout?:number,maxWorkers?:number}={}){
 const root=mkdtempSync(join(tmpdir(),'fleet-parallel-'));process.env.AGENT_FLEET_HOME=join(root,'history');git(root,'init');writeFileSync(join(root,'seed'),'fixed');git(root,'add','.');git(root,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m','fixed base');
 const e=Engine.init(root);e.config({version:1,timeoutMs:options.timeout??5000,maxOutputBytes:1024*1024,maxWorkers:options.maxWorkers??2,maxReviews:1,instances:[{id:'manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},...[1,2].map(n=>({id:'worker-'+n,provider:'fake',role:'worker',model:options.worker??'delay-write',capabilities:['code']})),{id:'reviewer',provider:'fake',role:'reviewer',model:options.review??'review-approve',capabilities:['review']}]});return e;
}
function events(e:Engine){let cursor=0;const all=[];while(true){const batch=e.store.events(cursor,1000);if(!batch.length)break;all.push(...batch);cursor=batch.at(-1)!.event_id;}return all;}
test('manager proposal → two overlapping workers → independent reviewer → accepted dependencies',{timeout:20000},async()=>{
 const e=setup();try{const manager=await e.propose('Create two files in parallel then join them');assert.equal(e.store.getPlan(),null);const proposal=e.store.getMeta('proposal:'+manager.id) as any;assert.equal(proposal.plan.tasks.length,3);e.adoptProposal(manager.id);
 const result=await e.exclusive(()=>schedule(e,'reviewer'));assert.deepEqual(result.pending,[]);assert.equal(result.accepted,3);
 const runs=e.store.listRuns(),workers=runs.filter(r=>r.instanceId.startsWith('worker-')),reviews=runs.filter(r=>r.instanceId==='reviewer');assert.equal(workers.length,3);assert.equal(reviews.length,3);assert.ok(workers.every(r=>r.status==='accepted'));assert.ok(reviews.every(r=>r.status==='exited'));
 const traces=events(e).filter(ev=>ev.kind==='stdout').flatMap(ev=>{try{const p=JSON.parse((ev.data as any).line);return p.type.startsWith('fixture.')?[{...p,runId:ev.run_id}]:[];}catch{return [];}});
 const start=(file:string)=>traces.find(t=>t.type==='fixture.started'&&t.filename===file).time,finish=(file:string)=>traces.find(t=>t.type==='fixture.finished'&&t.filename===file).time;
 assert.ok(Math.max(start('left.txt'),start('right.txt'))<Math.min(finish('left.txt'),finish('right.txt')),'worker process lifetimes really overlap');
 const left=workers.find(r=>r.taskId==='left')!,right=workers.find(r=>r.taskId==='right')!,joined=workers.find(r=>r.taskId==='join')!;
 git(joined.worktree,'merge-base','--is-ancestor',left.commitSha!,joined.commitSha!);git(joined.worktree,'merge-base','--is-ancestor',right.commitSha!,joined.commitSha!);
 for(const worker of workers){const evidence=e.store.getMeta('reviewEvidence:'+worker.id) as any;const review=e.store.getRun(evidence.reviewRunId)!;assert.equal(review.baseSha,worker.commitSha);assert.notEqual(review.worktree,worker.worktree);assert.notEqual(review.sessionId,worker.sessionId);assert.throws(()=>e.acceptReview(worker.id,review.id),/binding\/state/);}
 const count=runs.length;const again=await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(again.accepted,3);assert.equal(e.store.listRuns().length,count,'schedule resumes without replaying completed work');
 }finally{e.close();}
});
test('review rejection blocks dependency and is not auto-retried',async()=>{const e=setup({review:'review-reject'});try{const manager=await e.propose('reject fixture');e.adoptProposal(manager.id);const result=await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(result.accepted,0);assert.equal(e.store.listRuns().filter(r=>r.status==='rejected').length,2);assert.ok(!e.store.listRuns().some(r=>r.taskId==='join'));const count=e.store.listRuns().length;await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(e.store.listRuns().length,count);}finally{e.close();}});
test('unknown worker outcomes are persistent and never replayed by schedule',async()=>{const e=setup({worker:'hang',timeout:100});try{e.plan({version:1,goal:'Timeout fixture',tasks:['left','right'].map(id=>({id,title:id,prompt:'hang',dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e','process.exit(0)']]}))});await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(e.store.listRuns().filter(r=>r.status==='unknown').length,2);const count=e.store.listRuns().length;await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(e.store.listRuns().length,count);}finally{e.close();}});
test('invalid manager plan stays a rejected proposal and cannot execute',async()=>{const e=setup();try{const c=e.config()!;c.instances[0].model='planner-invalid';e.config(c);await assert.rejects(e.propose('bad plan'),/cycle/);assert.equal(e.store.getPlan(),null);await assert.rejects(e.exclusive(()=>schedule(e,'reviewer')),/validated plan/);}finally{e.close();}});
test('persisted interrupted verification becomes unknown and cannot rerun acceptance',async()=>{const e=setup();try{const manager=await e.propose('interrupted verification');e.adoptProposal(manager.id);const worker=await e.run('left','worker-1');e.store.setMeta('verification:'+worker.id,{verificationId:'interrupted',status:'running'});e.store.updateRun(worker.id,{status:'verifying'});e.reconcile();assert.equal(e.store.getRun(worker.id)?.status,'unknown');await assert.rejects(e.verify(worker.id),/exited/);assert.equal((e.store.getMeta('verification:'+worker.id) as any).status,'unknown');}finally{e.close();}});

test('unknown process quarantines its capacity and prevents third independent dispatch and resume replay',async()=>{
 const e=setup({worker:'hang',timeout:100,maxWorkers:1});try{
  e.plan({version:1,goal:'Unknown slot stays occupied',tasks:['a','b','c'].map(id=>({id,title:id,prompt:'hang',dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e','process.exit(0)']]}))});
  const first=await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(first.quarantined,true);assert.equal(e.store.listRuns().length,1);assert.equal(e.store.listRuns()[0].status,'unknown');
  const resumed=await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(resumed.quarantined,true);assert.equal(e.store.listRuns().length,1);assert.deepEqual(resumed.pending,['a','b','c']);
 }finally{e.close();}
});

test('verification completion cannot overwrite recovery uncertainty',async()=>{
 const e=setup();try{const manager=await e.propose('CAS verification');e.adoptProposal(manager.id);const run=await e.run('left','worker-1');
 const finish=e.store.finishVerification.bind(e.store);e.store.finishVerification=(id,phase,sha,tree)=>{e.store.updateRun(id,{status:'unknown'},'recovery.unknown',{});return finish(id,phase,sha,tree);};
 await assert.rejects(e.verify(run.id),/uncertainty/);assert.equal(e.store.getRun(run.id)?.status,'unknown');assert.equal((e.store.getMeta('verification:'+run.id) as any).status,'unknown');
 e.store.updateRun(run.id,{status:'exited'},'process.completed',{});assert.equal(e.store.getRun(run.id)?.status,'unknown');
 }finally{e.close();}
});

test('late recovery observation does not downgrade a completed verification phase',async()=>{
 const e=setup();try{const manager=await e.propose('CAS recovery');e.adoptProposal(manager.id);const run=await e.run('left','worker-1');await e.verify(run.id);
 const phaseBefore=e.store.getMeta('verification:'+run.id);assert.equal(e.store.recoverRun(run.id,{staleObservation:true}),'verified');assert.equal(e.store.getRun(run.id)?.status,'verified');assert.deepEqual(e.store.getMeta('verification:'+run.id),phaseBefore);
 }finally{e.close();}
});
