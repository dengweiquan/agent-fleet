import {mkdtempSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {schedule} from '../src/scheduler.ts';
import assert from 'node:assert/strict';
const root=mkdtempSync(join(tmpdir(),'agent-fleet-parallel-demo-'));process.env.AGENT_FLEET_HOME=join(root,'history');
git(root,'init');writeFileSync(join(root,'README.md'),'Independent parallel synthetic fixture.\n');git(root,'add','.');git(root,'-c','user.name=AgentFleet Demo','-c','user.email=demo@localhost','-c','commit.gpgsign=false','commit','-m','fixed synthetic base');
const e=Engine.init(root,'Manager → concurrent workers → independent reviewer');
try{
 e.config({version:1,maxWorkers:2,maxReviews:1,timeoutMs:5000,maxOutputBytes:1024*1024,instances:[{id:'manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},...[1,2].map(n=>({id:'worker-'+n,provider:'fake',role:'worker',model:'delay-write',capabilities:['code']})),{id:'reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']}]});
 const manager=await e.exclusive(()=>e.propose('Build left and right in parallel; join only accepted commits.'));
 e.adoptProposal(manager.id);const result=await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(result.accepted,3);
 const runs=e.store.listRuns(),workers=runs.filter(r=>r.instanceId.startsWith('worker-')),reviews=runs.filter(r=>r.instanceId==='reviewer');
 const records=e.store.events(0,1000).filter(ev=>ev.kind==='stdout').flatMap(ev=>{try{const p=JSON.parse((ev.data as any).line);return p.type?.startsWith('fixture.')?[p]:[];}catch{return [];}});
 const time=(file:string,kind:string)=>records.find(r=>r.filename===file&&r.type===kind).time;
 const overlapMs=Math.min(time('left.txt','fixture.finished'),time('right.txt','fixture.finished'))-Math.max(time('left.txt','fixture.started'),time('right.txt','fixture.started'));assert.ok(overlapMs>0);
 const count=runs.length;await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(e.store.listRuns().length,count);
 console.log(JSON.stringify({ok:true,modelsInvoked:false,project:root,history:process.env.AGENT_FLEET_HOME,sessionId:e.sessionId,managerRunId:manager.id,proposalHash:(e.store.getMeta('proposal:'+manager.id) as any).hash,maxWorkers:2,maxReviews:1,workerOverlapMs:overlapMs,result,workers:workers.map(r=>({id:r.id,task:r.taskId,commit:r.commitSha,status:r.status})),reviews:reviews.map(r=>({id:r.id,candidate:r.baseSha,status:r.status})),repeatScheduleAddedRuns:0,resumeCommand:`AGENT_FLEET_HOME='${process.env.AGENT_FLEET_HOME}' agent-fleet resume ${e.sessionId}`},null,2));
}finally{e.close();}
