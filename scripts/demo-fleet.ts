import {mkdtempSync,mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {scheduleFleet} from '../src/fleet-scheduler.ts';

// Synthetic repositories only. No provider authentication, model calls or installations.
const output=process.argv[2]?resolve(process.argv[2]):mkdtempSync(join(tmpdir(),'agent-fleet-demo-'));
mkdirSync(output,{recursive:true});
const priorHome=process.env.AGENT_FLEET_HOME;
const scenarios=[];
try{
 for(const accountLimit of [1,2]){
  const dir=join(output,'account-cap-'+accountLimit);mkdirSync(dir);process.env.AGENT_FLEET_HOME=join(dir,'history');
  const engines=['alpha','beta'].map(name=>{const root=join(dir,name);mkdirSync(root);git(root,'init');writeFileSync(join(root,'seed.txt'),name+' independent base\n');git(root,'add','.');git(root,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m',name+' demo seed');const e=Engine.init(root,name+' fleet demo');e.config({version:1,maxOutputBytes:1024*1024,timeoutMs:5000,maxWorkers:2,maxReviews:1,instances:[{id:'manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},{id:'worker',provider:'fake',role:'worker',model:'delay-write',capabilities:['code']},{id:'reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']}]});e.plan({version:1,goal:name+' synthetic artifacts',tasks:['shared','second'].map(id=>{const filename=name+'-'+id+'.txt',content=name+'/'+id+'\n';return {id,title:filename,prompt:JSON.stringify({filename,content,delayMs:600}),dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e',`if(require('node:fs').readFileSync(${JSON.stringify(filename)},'utf8')!==${JSON.stringify(content)})process.exit(1)`]]};})});return e;});
  const pool=new ResourcePool(join(dir,'pool.sqlite'));
  try{
   for(const id of ['worker-a','worker-b'])pool.upsertResource({id,provider:'fake',model:'delay-write',endpointId:'local',endpointKind:'local',accountDomain:'shared-worker-account',capabilities:['code'],roles:['worker'],capacity:1});
   pool.upsertResource({id:'reviewer-resource',provider:'fake',model:'review-approve',endpointId:'local',endpointKind:'local',accountDomain:'review-account',capabilities:['review'],roles:['reviewer'],capacity:1});
   pool.setLimits({global:3,projects:Object.fromEntries(engines.map(e=>[e.root,2])),endpoints:{local:3},accounts:{'shared-worker-account':accountLimit,'review-account':1}});
   const result=await scheduleFleet(pool,engines.map(engine=>({projectId:engine.root,engine})));
   const projects=engines.map((e,index)=>{let after=0;const events=[];for(;;){const page=e.store.events(after,1000);if(!page.length)break;events.push(...page);after=page.at(-1)!.event_id;}const eventsPath=join(dir,(index===0?'alpha':'beta')+'-events.json');writeFileSync(eventsPath,JSON.stringify(events,null,2)+'\n');return {projectId:e.root,sessionId:e.sessionId,baseSha:e.store.getMeta('baseSha'),eventsPath,runs:e.store.listRuns(),acceptedArtifacts:e.store.listRuns().filter(r=>r.status==='accepted').map(r=>({taskId:r.taskId,commitSha:r.commitSha,worktree:r.worktree,content:readFileSync(join(r.worktree,(index===0?'alpha':'beta')+'-'+r.taskId+'.txt'),'utf8')})),fixtureTiming:events.filter(ev=>ev.kind==='stdout').flatMap(ev=>{try{const v=JSON.parse((ev.data as any).line);return v.type==='fixture.started'||v.type==='fixture.finished'?[{runId:ev.run_id,...v}]:[];}catch{return [];}})};});
   const scenario={accountLimit,result,projects,pool:pool.status(),modelInvoked:false,realQuotaConsumed:false};const path=join(dir,'result.json');writeFileSync(path,JSON.stringify(scenario,null,2)+'\n');scenarios.push({accountLimit,path,accepted:result.projects.reduce((n,p)=>n+p.accepted,0),sessions:projects.map(p=>p.sessionId),worktrees:projects.flatMap(p=>p.runs.map(r=>r.worktree))});
  }finally{pool.close();for(const e of engines)e.close();}
 }
 const summary={artifactDirectory:output,modelInvoked:false,realQuotaConsumed:false,scenarios};writeFileSync(join(output,'summary.json'),JSON.stringify(summary,null,2)+'\n');process.stdout.write(JSON.stringify(summary,null,2)+'\n');
}finally{if(priorHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=priorHome;}
