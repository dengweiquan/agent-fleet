import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,realpathSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {Engine} from '../src/engine.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {git} from '../src/git.ts';
import type {Run} from '../src/types.ts';

const cli=fileURLToPath(new URL('../src/cli.ts',import.meta.url));
const permissions=['draft.create','note.append','pause','resume','stop'];
function fixture(){
  const temp=realpathSync(mkdtempSync(join(tmpdir(),'fleet-control-cli-'))),priorHome=process.env.AGENT_FLEET_HOME;
  const env:NodeJS.ProcessEnv={...process.env,AGENT_FLEET_HOME:join(temp,'history')};process.env.AGENT_FLEET_HOME=env.AGENT_FLEET_HOME;
  const projects=['alpha','beta'].map(name=>{
    const root=join(temp,name);mkdirSync(root);git(root,'init');writeFileSync(join(root,'seed.txt'),name+' fixture');git(root,'add','.');git(root,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m',name+' isolated seed');
    const e=Engine.init(root,name+' control fixture');e.config({version:1,timeoutMs:1000,maxOutputBytes:65536,maxWorkers:1,maxReviews:1,instances:[{id:name+'-manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},{id:name+'-worker',provider:'fake',role:'worker',model:'write',capabilities:['code']},{id:name+'-reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']}]});
    e.plan({version:1,goal:name+' immutable fixture plan',tasks:[{id:'task-a',title:name+' task',prompt:'Fixture only; never executed by this test',dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e','process.exit(0)']]}]});
    const sessionId=e.sessionId,dir=e.dir;e.close();return {root,sessionId,dir,name};
  });
  const poolPath=join(temp,'pool.sqlite'),pool=new ResourcePool(poolPath);
  for(const p of projects)pool.upsertResource({id:p.name+'-resource',provider:'fake',model:'write',endpointId:'local',endpointKind:'local',accountDomain:'fixture-account',roles:['worker'],capabilities:[p.name],capacity:1});
  pool.setLimits({global:2,projects:Object.fromEntries(projects.map(p=>[p.root,1])),endpoints:{local:2},accounts:{'fixture-account':2}});
  const leases=projects.map(p=>{const requestId=randomUUID();pool.enqueue({id:requestId,projectId:p.root,sessionId:p.sessionId,taskId:p.name+'-private-task',attemptId:randomUUID(),role:'worker',capabilities:[p.name]});const lease=pool.claimNext([requestId])!;assert.ok(lease);const e=new Engine(p.root,false,p.sessionId);e.store.setMeta('resourcePoolPath',poolPath);e.close();return lease;});pool.close();
  const alpha=projects[0],beta=projects[1];
  function scopeFor(project=alpha,allowed=permissions){const path=join(temp,'scope-'+randomUUID()+'.json');writeFileSync(path,JSON.stringify({version:1,project:project.root,sessionId:project.sessionId,permissions:allowed}),{mode:0o600});return path;}
  const scope=scopeFor();
  function raw(action:string,arg?:string,input?:string,options:{observer?:boolean;scope?:string}={}){return spawnSync(process.execPath,['--experimental-strip-types',cli,...(options.observer?['--observe']:[]),'control',options.scope??scope,action,...(arg===undefined?[]:[arg])],{cwd:temp,env,input,encoding:'utf8',timeout:5000,maxBuffer:2*1024*1024});}
  function call(action:string,arg?:string,body?:unknown,options:{observer?:boolean;scope?:string}={}):any{const r=raw(action,arg,body===undefined?undefined:JSON.stringify(body)+'\n',options);assert.equal(r.status,0,`${action}: ${r.stderr}`);const frame=JSON.parse(r.stdout);assert.equal(frame.ok,true,JSON.stringify(frame));return frame.data;}
  function denied(action:string,arg?:string,body?:unknown,options:{observer?:boolean;scope?:string}={},match=/observer|permission|scope|project|session|operation|unsupported|invalid|conflict/i){const r=raw(action,arg,body===undefined?undefined:JSON.stringify(body)+'\n',options);assert.notEqual(r.status,0,`${action} unexpectedly succeeded: ${r.stdout}`);assert.match(r.stdout+r.stderr,match);return r;}
  function request(operation:string,payload:unknown={},requestId:string=randomUUID()){return {version:1,requestId,projectId:alpha.root,sessionId:alpha.sessionId,operation,payload};}
  function inspect<T>(fn:(engine:Engine)=>T):T{const e=new Engine(alpha.root,true,alpha.sessionId);try{return fn(e);}finally{e.close();}}
  function syntheticRunning(status:'running'|'exited'='running'){const e=new Engine(alpha.root,false,alpha.sessionId);try{const id=randomUUID(),run:Run={id,taskId:'task-a',instanceId:'alpha-worker',provider:'fake',sessionId:randomUUID(),baseSha:e.store.getMeta('baseSha') as string,worktree:join(e.dir,'worktrees',id),status:'prepared',pid:null,processStart:null,exitCode:null,commitSha:null,reviewBy:null,createdAt:new Date().toISOString()};e.store.createRun(run);e.store.updateRun(id,{status,pid:999999,processStart:'fixture-unverified-process-start',...(status==='exited'?{exitCode:0}:{})},'fixture.state',{synthetic:true,noProcessSpawned:true});return e.store.getRun(id)!;}finally{e.close();}}
  return {temp,alpha,beta,leases,scope,scopeFor,raw,call,denied,request,inspect,syntheticRunning,cleanup(){if(priorHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=priorHome;rmSync(temp,{recursive:true,force:true});}};
}

test('control CLI persists submit/duplicate/conflict/apply receipts across separate processes without task execution',{timeout:20000},()=>{
  const f=fixture();try{
    const initial=f.call('status'),draft=f.request('draft.create',{title:'Draft only',text:'A proposed change, not an adopted plan.'});
    const accepted=f.call('submit',undefined,draft);assert.equal(accepted.requestId,draft.requestId);assert.equal(accepted.state,'accepted');assert.match(accepted.hash,/^[a-f0-9]{64}$/);assert.equal(accepted.received,true);assert.equal(accepted.completed,false);assert.equal(accepted.taskState,'unchanged');
    assert.deepEqual(f.call('receipt',draft.requestId),accepted);assert.deepEqual(f.call('submit',undefined,draft),accepted);
    f.denied('submit',undefined,{...draft,payload:{...draft.payload as object,text:'Conflicting instruction'}},{},/conflict/i);
    assert.equal(f.call('status').runs.length,0);assert.deepEqual(f.call('status').plan,initial.plan);
    const completed=f.call('apply',draft.requestId);assert.equal(completed.state,'completed');assert.equal(completed.hash,accepted.hash);assert.equal(completed.taskState,'unchanged');assert.equal(completed.result.executable,false);
    assert.deepEqual(f.call('receipt',draft.requestId),completed);assert.deepEqual(f.call('apply',draft.requestId),completed);assert.deepEqual(f.call('submit',undefined,draft),completed);
    const artifacts=f.call('artifacts');assert.equal(artifacts.length,1);assert.match(JSON.stringify(artifacts),/Draft only/);assert.match(JSON.stringify(artifacts),/A proposed change/);
    const note=f.request('note.append',{text:'Keep task instructions for the next supported boundary.',taskId:'task-a'});assert.equal(f.call('submit',undefined,note).state,'accepted');assert.equal(f.call('apply',note.requestId).state,'completed');assert.equal(f.call('artifacts').length,2);const nextArtifacts=f.call('artifacts',String(artifacts[0].artifactId));assert.equal(nextArtifacts.length,1);assert.equal(nextArtifacts[0].requestId,note.requestId);
    const status=f.call('status');assert.equal(status.runs.length,0);assert.deepEqual(status.plan,initial.plan);assert.equal(status.paused,false);
    assert.ok(f.call('events').length>=4);assert.deepEqual(f.call('events','9007199254740991'),[]);assert.deepEqual(f.call('artifacts','9007199254740991'),[]);
  }finally{f.cleanup();}
});

test('control pause only takes effect on explicit apply, and stop completion is not process termination',{timeout:20000},()=>{
  const f=fixture();try{
    const pause=f.request('pause');assert.equal(f.call('submit',undefined,pause).state,'accepted');assert.equal(f.call('status').paused,false);assert.equal(f.call('apply',pause.requestId).state,'completed');assert.equal(f.call('status').paused,true);
    const before=f.inspect(e=>e.store.events(0,1000).filter(event=>event.kind==='paused').length);assert.equal(before,1);f.call('apply',pause.requestId);assert.equal(f.inspect(e=>e.store.events(0,1000).filter(event=>event.kind==='paused').length),before);
    // Store-only identity fixture: no child process exists or is signalled by this test.
    const run=f.syntheticRunning(),stop=f.request('stop',{runId:run.id});assert.equal(f.call('submit',undefined,stop).state,'accepted');assert.equal(f.inspect(e=>e.store.getMeta('stop:'+run.id)),null);
    const stopped=f.call('apply',stop.requestId);assert.equal(stopped.state,'completed');assert.equal(f.inspect(e=>e.store.getMeta('stop:'+run.id)),true);assert.equal(f.inspect(e=>e.store.getRun(run.id)?.status),'running');assert.match(JSON.stringify(stopped),/processTerminationConfirmed[^a-z]*false/);
    const resume=f.request('resume');f.call('submit',undefined,resume);assert.equal(f.call('status').paused,true);assert.equal(f.call('apply',resume.requestId).state,'completed');
    const resumed=f.call('status');assert.equal(resumed.paused,false);assert.equal(resumed.runs.length,1);assert.equal(resumed.runs[0].id,run.id);assert.equal(resumed.runs[0].status,'unknown');assert.equal(f.inspect(e=>e.store.events(0,1000).filter(event=>event.kind==='process.started').length),0);
    const repeat=f.request('resume');f.call('submit',undefined,repeat);f.call('apply',repeat.requestId);assert.equal(f.call('status').runs.length,1);assert.equal(f.call('status').runs[0].status,'unknown');
  }finally{f.cleanup();}
});

test('control CLI enforces observer/operation/project scope and resources never expose another project',{timeout:20000},()=>{
  const f=fixture();try{
    const pause=f.request('pause');f.call('submit',undefined,pause);const events=f.call('events');
    f.denied('submit',undefined,f.request('note.append',{text:'denied'}),{observer:true},/Observer/i);f.denied('apply',pause.requestId,undefined,{observer:true},/Observer/i);assert.deepEqual(f.call('events'),events);assert.equal(f.call('status').paused,false);
    const readonlyScope=f.scopeFor(f.alpha,[]);f.denied('submit',undefined,f.request('pause'),{scope:readonlyScope},/permission|allow|authoriz/i);f.denied('apply',pause.requestId,undefined,{scope:readonlyScope},/permission|allow|authoriz/i);
    f.denied('submit',undefined,{...f.request('pause'),projectId:f.beta.root});f.denied('submit',undefined,{...f.request('pause'),sessionId:f.beta.sessionId});
    f.denied('submit',undefined,f.request('shell',{command:'touch forbidden'}));f.denied('submit',undefined,f.request('exec',{argv:['sh','-c','touch forbidden']}));f.denied('submit',undefined,{...f.request('pause'),permissions:['pause']});
    f.denied('submit',undefined,f.request('note.append',{text:'bad task',taskId:'not-in-plan'}),{},/Unknown task/i);
    const resources=f.call('resources',undefined,undefined,{observer:true}),serialized=JSON.stringify(resources);
    assert.match(serialized,/alpha-resource/);assert.doesNotMatch(serialized,/beta-resource|beta-private-task|beta-manager/);assert.equal(serialized.includes(f.beta.root),false);assert.equal(serialized.includes(f.beta.sessionId),false);assert.equal(serialized.includes(f.leases[1].id),false);
    assert.equal(resources.leases.length,1);assert.equal(resources.leases[0].request.projectId,f.alpha.root);assert.equal(resources.leases[0].request.sessionId,f.alpha.sessionId);assert.equal(f.call('status').runs.length,0);
  }finally{f.cleanup();}
});

test('control JSONL serve returns one scoped envelope per frame and survives rejected frames',{timeout:15000},()=>{
  const f=fixture();try{
    const note=f.request('note.append',{text:'Durable serve note'}),frames=[{action:'status'},{action:'submit',request:note},{action:'receipt',requestId:note.requestId},{action:'apply',requestId:note.requestId},{action:'resources'},{action:'shell',command:'forbidden'},{action:'status'}];
    const served=f.raw('serve',undefined,frames.map(frame=>JSON.stringify(frame)).join('\n')+'\n');assert.equal(served.status,0,served.stderr);const replies=served.stdout.trim().split('\n').map(line=>JSON.parse(line));assert.equal(replies.length,frames.length);assert.equal(replies[0].ok,true);assert.equal(replies[1].data.state,'accepted');assert.equal(replies[2].data.state,'accepted');assert.equal(replies[3].data.state,'completed');assert.equal(replies[5].ok,false);assert.match(replies[5].error.message,/Unsupported|Invalid/i);assert.equal(replies[6].ok,true);assert.equal(replies[6].data.runs.length,0);
    assert.equal(f.call('receipt',note.requestId).state,'completed');assert.equal(f.call('artifacts').length,1);
    const observed=f.raw('serve',undefined,JSON.stringify({action:'submit',request:f.request('pause')})+'\n'+JSON.stringify({action:'status'})+'\n',{observer:true});assert.equal(observed.status,0);const observerReplies=observed.stdout.trim().split('\n').map(line=>JSON.parse(line));assert.equal(observerReplies[0].ok,false);assert.match(observerReplies[0].error.message,/Observer/);assert.equal(observerReplies[1].ok,true);assert.equal(observerReplies[1].data.paused,false);
  }finally{f.cleanup();}
});

test('control request IDs retain colon/dot identity through submit, receipt and apply',{timeout:10000},()=>{
  const f=fixture();try{
    const request=f.request('note.append',{text:'Persist the original controller turn identity.'},'chat:turn.1');
    const accepted=f.call('submit',undefined,request);assert.equal(accepted.requestId,'chat:turn.1');assert.equal(accepted.state,'accepted');assert.deepEqual(f.call('receipt','chat:turn.1'),accepted);
    const completed=f.call('apply','chat:turn.1');assert.equal(completed.requestId,'chat:turn.1');assert.equal(completed.state,'completed');assert.deepEqual(f.call('receipt','chat:turn.1'),completed);assert.equal(f.call('artifacts')[0].requestId,'chat:turn.1');
  }finally{f.cleanup();}
});

test('stopping nonexistent or terminal work is a completed no-op without an unknown-command barrier',{timeout:15000},()=>{
  const f=fixture();try{
    const terminal=f.syntheticRunning('exited');
    for(const runId of ['missing-run',terminal.id]){
      const stop=f.request('stop',{runId});assert.equal(f.call('submit',undefined,stop).state,'accepted');const receipt=f.call('apply',stop.requestId);assert.equal(receipt.state,'completed');assert.equal(receipt.result.details.stopRequestRecorded,false);assert.equal(receipt.result.processTermination,'unverified');assert.notEqual(receipt.result.requestRecorded,true);assert.equal(f.inspect(e=>e.store.getMeta('stop:'+runId)),null);
      const pause=f.request('pause');f.call('submit',undefined,pause);assert.equal(f.call('apply',pause.requestId).state,'completed');assert.equal(f.call('status').paused,true);
    }
    assert.equal(f.call('status').runs.length,1);assert.equal(f.call('status').runs[0].status,'exited');assert.ok(f.call('events').every((event:any)=>!/unknown/.test(event.kind)));
  }finally{f.cleanup();}
});

test('resume with retained coordinator lock is rejected before apply and preserves accepted receipt',{timeout:10000},()=>{
  const f=fixture();try{
    const resume=f.request('resume');f.call('submit',undefined,resume);const accepted=f.call('receipt',resume.requestId);
    writeFileSync(join(f.alpha.dir,'coordinator.lock'),JSON.stringify({owner:'synthetic-unverified-owner',pid:999999,start:'fixture-only'}),{flag:'wx'});
    const before=f.inspect(e=>e.store.events(0,1000));f.denied('apply',resume.requestId,undefined,{},/lock|coordinator/i);
    assert.deepEqual(f.call('receipt',resume.requestId),accepted);assert.equal(f.call('receipt',resume.requestId).state,'accepted');assert.deepEqual(f.inspect(e=>e.store.events(0,1000)),before);assert.equal(f.call('status').runs.length,0);
  }finally{f.cleanup();}
});
