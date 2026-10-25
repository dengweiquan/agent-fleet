import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {existsSync,mkdirSync,mkdtempSync,readFileSync,realpathSync,rmSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import type {Resource} from '../src/resource-pool.ts';

const cli=fileURLToPath(new URL('../src/cli.ts',import.meta.url));
const delay=(ms:number)=>new Promise(resolve=>setTimeout(resolve,ms));
function fixture(workerCount=2){
  const temp=realpathSync(mkdtempSync(join(tmpdir(),'fleet-cli-pool-'))),pool=join(temp,'pool.sqlite');
  const env:NodeJS.ProcessEnv={...process.env,AGENT_FLEET_HOME:join(temp,'history')};
  function raw(cwd:string,...args:string[]){return spawnSync(process.execPath,['--experimental-strip-types',cli,'--pool',pool,'--json',...args],{cwd,env,encoding:'utf8',timeout:4000,maxBuffer:2*1024*1024});}
  function invoke(cwd:string,...args:string[]):any{const r=raw(cwd,...args);assert.equal(r.status,0,`${args.join(' ')}\n${r.stderr}`);const result=JSON.parse(r.stdout);assert.equal(result.ok,true);return result.data;}
  function jsonFile(name:string,value:unknown){const path=join(temp,name);writeFileSync(path,JSON.stringify(value));return path;}
  const projects=['alpha','beta'].map(name=>{
    const root=join(temp,name);mkdirSync(root);const git=(...args:string[])=>{const r=spawnSync('git',args,{cwd:root,env,encoding:'utf8',timeout:4000});assert.equal(r.status,0,r.stderr);};
    git('init');writeFileSync(join(root,'seed.txt'),name+' seed\n');git('add','.');git('-c','user.name=Fixture','-c','user.email=fixture@local','-c','commit.gpgsign=false','commit','-m',name+' seed');
    const sessionId=invoke(root,'init',name+' shared-pool session').sessionId;
    invoke(root,'--session',sessionId,'config',jsonFile(name+'-config.json',{version:1,maxOutputBytes:131072,timeoutMs:2500,maxWorkers:2,maxReviews:1,instances:[{id:'manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},{id:'legacy-worker',provider:'fake',role:'worker',model:'delay-write',capabilities:['code']},{id:'legacy-reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']}]}));
    const filename=name+'.txt',content=name+' only\n';
    invoke(root,'--session',sessionId,'plan',jsonFile(name+'-plan.json',{version:1,goal:name+' isolated artifact',tasks:[{id:'same-task',title:name+' task',prompt:JSON.stringify({filename,content,delayMs:180}),dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e',`const fs=require('node:fs');if(fs.readFileSync(${JSON.stringify(filename)},'utf8')!==${JSON.stringify(content)})process.exit(1);if(fs.existsSync(${JSON.stringify(name==='alpha'?'beta.txt':'alpha.txt')}))process.exit(2)`]]}]}));
    return {root,sessionId,name};
  });
  const resources:Resource[]=Array.from({length:workerCount},(_,i)=>({id:'worker-'+(i+1),provider:'fake',model:'delay-write',endpointId:'local',endpointKind:'local',accountDomain:'worker',roles:['worker'],capabilities:['code'],capacity:1}));
  resources.push({id:'reviewer',provider:'fake',model:'review-approve',endpointId:'local',endpointKind:'local',accountDomain:'review',roles:['reviewer'],capabilities:['review'],capacity:1});
  const poolConfig={version:1,resources,limits:{global:3,projects:Object.fromEntries(projects.map(p=>[p.root,2])),endpoints:{local:3},accounts:{worker:2,review:1}}};
  const poolFile=jsonFile('pool.json',poolConfig),fleetConfig={version:1,projects:projects.map(p=>({root:p.root,sessionId:p.sessionId,roles:{worker:{capabilities:['code'],preferredProvider:'fake',requiredProvider:'fake'},reviewer:{capabilities:['review'],requiredProvider:'fake'}}}))},fleetFile=jsonFile('fleet.json',fleetConfig);
  const status=(p:typeof projects[number])=>invoke(p.root,'--observe','--session',p.sessionId,'status');
  return {temp,pool,raw,invoke,jsonFile,projects,poolConfig,poolFile,fleetConfig,fleetFile,status,cleanup(){
    // Failed tests preserve evidence if a coordinator still owns either session.
    if(projects.some(p=>existsSync(join(p.root,'.agent-fleet','sessions',p.sessionId,'coordinator.lock'))))return;
    rmSync(temp,{recursive:true,force:true});
  }};
}

test('actual CLI pool configuration, read-only previews, role validation and project-scoped history',{timeout:20000},()=>{
  const f=fixture();try{
    const configured=f.invoke(f.temp,'pool','configure',f.poolFile);assert.equal(configured.modelsInvoked,false);assert.equal(configured.resources.length,3);
    const before=f.invoke(f.temp,'--observe','pool','status'),events=f.invoke(f.temp,'--observe','pool','events');
    const sessionEvents=f.projects.map(p=>f.invoke(p.root,'--session',p.sessionId,'events'));
    const index=readFileSync(join(f.temp,'history','sessions.json'),'utf8');
    const preview=f.invoke(f.temp,'--observe','fleet','schedule',f.fleetFile,'--dry-run');assert.equal(preview.executionStarted,false);assert.equal(preview.projects.length,2);assert.equal(preview.remoteExecutionImplemented,false);assert.ok(preview.projects.every((p:any)=>p.managerRequiredProvider==='fake'));
    assert.deepEqual(f.invoke(f.temp,'pool','status'),before);assert.deepEqual(f.invoke(f.temp,'pool','events'),events);assert.equal(readFileSync(join(f.temp,'history','sessions.json'),'utf8'),index);
    for(const [i,p] of f.projects.entries())assert.deepEqual(f.invoke(p.root,'--session',p.sessionId,'events'),sessionEvents[i]);
    for(const args of [['--observe','pool','configure',f.poolFile],['--observe','fleet','schedule',f.fleetFile]]){const r=f.raw(f.temp,...args);assert.equal(r.status,1);assert.match(r.stderr,/Observer/);}
    const badResource=f.jsonFile('bad-resource.json',{...f.poolConfig,resources:[{...f.poolConfig.resources[0],roles:['owner']}]});assert.equal(f.raw(f.temp,'pool','configure',badResource).status,1);
    const badRole=f.jsonFile('bad-role.json',{version:1,projects:[{...f.fleetConfig.projects[0],roles:{owner:{}}}]});assert.equal(f.raw(f.temp,'fleet','schedule',badRole,'--dry-run').status,1);
    const xai=f.jsonFile('xai-role.json',{version:1,projects:[{...f.fleetConfig.projects[0],roles:{worker:{requiredProvider:'xai'}}}]});const refused=f.raw(f.temp,'fleet','schedule',xai,'--dry-run');assert.equal(refused.status,1);assert.match(refused.stderr,/not implemented/);
    assert.deepEqual(f.invoke(f.temp,'pool','status'),before);assert.deepEqual(f.invoke(f.temp,'pool','events'),events);
    assert.equal(f.invoke(f.temp,'sessions','list').length,2);
    for(const p of f.projects){const nested=join(p.root,'nested');mkdirSync(nested);assert.deepEqual(f.invoke(f.temp,'--project',nested,'sessions','list').map((s:any)=>s.id),[p.sessionId]);assert.deepEqual(f.invoke(f.temp,'--observe','--project',p.root,'resume').map((s:any)=>s.id),[p.sessionId]);assert.equal(f.invoke(f.temp,'--observe','--project',p.root,'resume',p.sessionId).sessionId,p.sessionId);}
    const mismatch=f.raw(f.temp,'--project',f.projects[0].root,'resume',f.projects[1].sessionId);assert.equal(mismatch.status,1);assert.match(mismatch.stderr,/selected project scope/);
  }finally{f.cleanup();}
});

for(const workerCount of [1,2])test(`actual detached fleet CLI completes two projects using ${workerCount} worker resource(s) and one independent reviewer`,{timeout:20000},async()=>{
  const f=fixture(workerCount);try{
    f.invoke(f.temp,'pool','configure',f.poolFile);const launched=f.invoke(f.temp,'fleet','schedule',f.fleetFile);assert.equal(launched.launch.launched,true);assert.equal(typeof launched.launch.coordinatorPid,'number');
    // The spawning frontend has exited. Poll durable leases AND project outcomes;
    // launch, PID, heartbeat and worker exit zero are never treated as acceptance.
    const deadline=Date.now()+10000;let pool:any,states:any[]=[];
    do{
      pool=f.invoke(f.temp,'--observe','pool','status');states=f.projects.map(f.status);
      if(pool.leases.length===4&&pool.leases.every((l:any)=>l.state==='completed')&&states.every(s=>s.runs.some((r:any)=>r.status==='accepted'))&&!f.projects.some(p=>existsSync(join(p.root,'.agent-fleet','sessions',p.sessionId,'coordinator.lock'))))break;
      if(Date.now()>=deadline)assert.fail(`Fleet did not settle within 10 seconds: ${JSON.stringify({pool,states,temp:f.temp})}`);
      await delay(40);
    }while(true);
    assert.equal(pool.requests.length,4);assert.equal(pool.leases.filter((l:any)=>l.request.role==='worker').length,2);assert.equal(pool.leases.filter((l:any)=>l.request.role==='reviewer').length,2);
    assert.equal(new Set(pool.leases.filter((l:any)=>l.request.role==='worker').map((l:any)=>l.resource.id)).size,workerCount);
    const allRuns=states.flatMap(s=>s.runs);assert.equal(allRuns.length,4);assert.equal(new Set(allRuns.map((r:any)=>r.sessionId)).size,4);assert.equal(new Set(allRuns.map((r:any)=>r.worktree)).size,4);
    for(const [i,state] of states.entries()){
      const p=f.projects[i],worker=state.runs.find((r:any)=>r.status==='accepted'),review=state.runs.find((r:any)=>r.id!==worker.id);
      assert.equal(worker.taskId,'same-task');assert.equal(worker.baseSha,state.baseSha);assert.equal(review.baseSha,worker.commitSha);assert.equal(review.status,'exited');assert.notEqual(worker.instanceId,review.instanceId);
      assert.equal(readFileSync(join(worker.worktree,p.name+'.txt'),'utf8'),p.name+' only\n');assert.equal(existsSync(join(worker.worktree,p.name==='alpha'?'beta.txt':'alpha.txt')),false);
      assert.ok(pool.leases.filter((l:any)=>l.request.sessionId===p.sessionId).every((l:any)=>l.request.projectId===p.root));
      const events=f.invoke(p.root,'--session',p.sessionId,'events');assert.equal(events.filter((e:any)=>e.kind==='resource.run_bound').length,2);assert.equal(events.filter((e:any)=>e.kind==='provider.session').length,2);
    }
  }finally{f.cleanup();}
});
