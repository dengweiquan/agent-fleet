import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,existsSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {scheduleFleet} from '../src/fleet-scheduler.ts';

function setup(options:{accountLimit?:number;tasks?:number;workerModel?:string;timeoutMs?:number;global?:number}={}) {
 const dir=mkdtempSync(join(tmpdir(),'fleet-two-projects-')),priorHome=process.env.AGENT_FLEET_HOME;
 process.env.AGENT_FLEET_HOME=join(dir,'history');
 const engines=['alpha','beta'].map(name=>{
  const root=join(dir,name);mkdirSync(root);git(root,'init');writeFileSync(join(root,'seed.txt'),`${name}: independent immutable base\n`);git(root,'add','.');git(root,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m',`${name} seed`);
  const e=Engine.init(root,`${name} fleet fixture`);
  e.config({version:1,maxOutputBytes:1024*1024,timeoutMs:options.timeoutMs??5000,maxWorkers:2,maxReviews:1,instances:[{id:'manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},{id:'legacy-worker',provider:'fake',role:'worker',model:'delay-write',capabilities:['code']},{id:'legacy-reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']}]});
  e.plan({version:1,goal:`${name} fixture only`,tasks:Array.from({length:options.tasks??2},(_,index)=>{
   const id=index===0?'shared':'shared-'+index,filename=`${name}-${id}.txt`,content=`${name}/${id}\n`;
   return {id,title:`${name} ${id}`,prompt:JSON.stringify({filename,content,delayMs:600}),dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e',`const fs=require('node:fs');if(fs.readFileSync(${JSON.stringify(filename)},'utf8')!==${JSON.stringify(content)})process.exit(1);if(fs.existsSync(${JSON.stringify(name==='alpha'?'beta-shared.txt':'alpha-shared.txt')}))process.exit(2)`]]};
  })});return e;
 });
 const poolPath=join(dir,'pool.sqlite'),pool=new ResourcePool(poolPath);
 for(const id of ['worker-a','worker-b'])pool.upsertResource({id,provider:'fake',model:options.workerModel??'delay-write',endpointId:'local',endpointKind:'local',accountDomain:'shared-worker-account',capabilities:['code'],roles:['worker'],capacity:1});
 pool.upsertResource({id:'reviewer-resource',provider:'fake',model:'review-approve',endpointId:'local',endpointKind:'local',accountDomain:'review-account',capabilities:['review'],roles:['reviewer'],capacity:1});
 pool.setLimits({global:options.global??3,projects:Object.fromEntries(engines.map(e=>[e.root,2])),endpoints:{local:3},accounts:{'shared-worker-account':options.accountLimit??1,'review-account':1}});
 return {dir,engines,pool,poolPath,projects:engines.map(engine=>({projectId:engine.root,engine})),close(){pool.close();for(const e of engines)e.close();if(priorHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=priorHome;rmSync(dir,{recursive:true,force:true});}};
}
function allEvents(e:Engine){const output=[];let after=0;for(;;){const page=e.store.events(after,1000);if(!page.length)return output;output.push(...page);after=page.at(-1)!.event_id;}}
function intervals(engines:Engine[]){return engines.flatMap(e=>{
 const traces=allEvents(e).filter(event=>event.kind==='stdout').flatMap(event=>{try{const data=JSON.parse((event.data as any).line);return data.type==='fixture.started'||data.type==='fixture.finished'?[{...data,runId:event.run_id}]:[];}catch{return [];}});
 return traces.filter(t=>t.type==='fixture.started').map(t=>({projectId:e.root,runId:t.runId,filename:t.filename,start:t.time,end:traces.find(other=>other.runId===t.runId&&other.type==='fixture.finished')?.time}));
});}
function assertAcceptedIsolation(s:ReturnType<typeof setup>,taskCount:number){
 assert.notEqual(s.engines[0].sessionId,s.engines[1].sessionId);assert.notEqual(s.engines[0].store.getMeta('baseSha'),s.engines[1].store.getMeta('baseSha'));
 const allRuns=s.engines.flatMap(e=>e.store.listRuns());assert.equal(new Set(allRuns.map(r=>r.id)).size,allRuns.length);assert.equal(new Set(allRuns.map(r=>r.sessionId)).size,allRuns.length);assert.equal(new Set(allRuns.map(r=>r.worktree)).size,allRuns.length);
 for(const [index,e] of s.engines.entries()){
  const name=index===0?'alpha':'beta',workers=e.store.listRuns().filter(r=>r.status==='accepted');assert.equal(workers.length,taskCount);
  for(const run of workers){assert.equal(run.baseSha,e.store.getMeta('baseSha'));assert.equal(readFileSync(join(run.worktree,`${name}-${run.taskId}.txt`),'utf8'),`${name}/${run.taskId}\n`);assert(!existsSync(join(run.worktree,`${index===0?'beta':'alpha'}-shared.txt`)));assert.equal(git(run.worktree,'rev-parse','HEAD'),run.commitSha);const evidence=e.store.getMeta('reviewEvidence:'+run.id) as any;assert(evidence?.reviewRunId);const review=e.store.getRun(evidence.reviewRunId)!;assert.equal(review.baseSha,run.commitSha);assert.notEqual(review.worktree,run.worktree);}
 }
 for(const e of s.engines)for(const run of e.store.listRuns()){const binding=e.store.getMeta('resourceLease:'+run.id) as any;assert(binding,'each invocation has a durable resource snapshot');assert.equal(binding.runId,run.id);assert.equal(binding.baseSha,run.baseSha);assert.equal(binding.worktree,run.worktree);assert.equal(binding.providerSessionId,run.sessionId);assert.equal(binding.request.projectId,e.root);assert.equal(binding.request.sessionId,e.sessionId);assert.equal(binding.resource.provider,'fake');assert.equal(binding.resource.endpointId,'local');assert(binding.resource.accountDomain);assert(e.store.instances().some(i=>i.id===run.instanceId&&i.role===binding.request.role));}
 const status=s.pool.status();assert(status.leases.every(l=>l.state==='completed'));assert(status.leases.every(l=>l.runId&&l.request.sessionId&&l.resource.provider==='fake'));assert(status.leases.every(l=>l.request.projectId===s.engines.find(e=>e.sessionId===l.request.sessionId)?.root));
 for(const e of s.engines){for(const worker of e.store.listRuns().filter(r=>r.status==='accepted')){const evidence=e.store.getMeta('reviewEvidence:'+worker.id) as any;const workerLease=status.leases.find(l=>l.runId===worker.id),reviewLease=status.leases.find(l=>l.runId===evidence.reviewRunId);assert(workerLease&&reviewLease);assert.notEqual(workerLease.resource.id,reviewLease.resource.id);assert.equal(workerLease.request.role,'worker');assert.equal(reviewLease.request.role,'reviewer');}}
}

test('two projects with the same task IDs share account capacity one fairly without mixing artifacts',{timeout:60000},async()=>{
 const s=setup({accountLimit:1});try{const result=await scheduleFleet(s.pool,s.projects);assert.deepEqual(result.projects.map(p=>p.accepted),[2,2]);assert(result.projects.every(p=>p.pending.length===0));assertAcceptedIsolation(s,2);
  const spans=intervals(s.engines).sort((a,b)=>a.start-b.start);assert.equal(spans.length,4);for(let i=1;i<spans.length;i++)assert(spans[i].start>=spans[i-1].end,'shared account cap=1 prevents real worker overlap');
  const workers=s.pool.status().leases.filter(l=>l.request.role==='worker');assert.equal(workers.length,4);assert.notEqual(workers[0].request.projectId,workers[1].request.projectId,'round robin advances the second project before the first gets another worker');assert(new Set(workers.map(l=>l.resource.id)).size<workers.length,'resource instances are reused across completed tasks');
 }finally{s.close();}
});

test('raising shared account capacity to two permits actual cross-project overlap with isolated reviews',{timeout:60000},async()=>{
 const s=setup({accountLimit:2});try{await scheduleFleet(s.pool,s.projects);assertAcceptedIsolation(s,2);const spans=intervals(s.engines);assert.equal(spans.length,4);assert(spans.some(a=>spans.some(b=>a.projectId!==b.projectId&&Math.max(a.start,b.start)<Math.min(a.end,b.end))),'independent project workers actually overlap');const workers=s.pool.status().leases.filter(l=>l.request.role==='worker');assert.equal(new Set(workers.map(l=>l.resource.id)).size,2);const before=s.engines.map(e=>e.store.listRuns().length);await scheduleFleet(s.pool,s.projects);assert.deepEqual(s.engines.map(e=>e.store.listRuns().length),before,'completed fleet resumes without replay');
 }finally{s.close();}
});

test('paused projects do not consume leases and resume creates each run only once',{timeout:60000},async()=>{
 const s=setup({accountLimit:2,tasks:1});try{for(const e of s.engines)e.pause();await scheduleFleet(s.pool,s.projects);assert.equal(s.pool.status().leases.length,0);assert(s.engines.every(e=>e.store.listRuns().length===0));s.engines[0].resume();await scheduleFleet(s.pool,s.projects);assert.equal(s.engines[0].store.listRuns().filter(r=>r.status==='accepted').length,1);assert.equal(s.engines[1].store.listRuns().length,0);const firstCount=s.engines[0].store.listRuns().length;s.engines[1].resume();await scheduleFleet(s.pool,s.projects);assertAcceptedIsolation(s,1);assert.equal(s.engines[0].store.listRuns().length,firstCount);const count=s.pool.status().leases.length;await scheduleFleet(s.pool,s.projects);assert.equal(s.pool.status().leases.length,count);
 }finally{s.close();}
});

test('an unknown worker retains global capacity across another project and pool reopen',{timeout:20000},async()=>{
 const s=setup({accountLimit:1,tasks:1,workerModel:'hang',timeoutMs:100,global:1});try{await scheduleFleet(s.pool,s.projects);assert.equal(s.engines.flatMap(e=>e.store.listRuns()).length,1);assert.equal(s.engines.flatMap(e=>e.store.listRuns())[0].status,'unknown');assert.equal(s.engines[1].store.listRuns().length,0);const leases=s.pool.status().leases;assert.equal(leases.length,1);assert.equal(leases[0].state,'unknown');const reopened=new ResourcePool(s.poolPath);try{await scheduleFleet(reopened,s.projects);assert.equal(reopened.status().leases.length,1);assert.equal(reopened.status().leases[0].id,leases[0].id);assert.equal(reopened.status().leases[0].state,'unknown');assert.equal(s.engines.flatMap(e=>e.store.listRuns()).length,1);}finally{reopened.close();}
 }finally{s.close();}
});

test('project aliases cannot manufacture a second capacity account',{timeout:20000},async()=>{const s=setup({tasks:1});try{await assert.rejects(scheduleFleet(s.pool,[{projectId:'alias-account',engine:s.engines[0]}]),/project|canonical|root/i);assert.equal(s.engines[0].store.listRuns().length,0);assert.equal(s.pool.status().leases.length,0);}finally{s.close();}});
