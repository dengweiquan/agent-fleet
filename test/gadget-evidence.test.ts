import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,realpathSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
import {Engine} from '../src/engine.ts';
import {Store} from '../src/store.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {PooledRemoteQueue,pooledProjectKey} from '../src/pooled-remote.ts';
import {getGadgetEvidence,recordGadgetEvidence} from '../src/gadget-evidence.ts';
import type {GadgetBinding,GadgetInput} from '../src/gadget-events.ts';
import type {Dispatch,Capabilities} from '../src/remote.ts';

function setup(){
  const dir=realpathSync(mkdtempSync(join(tmpdir(),'agent-fleet-gadget-evidence-'))),oldHome=process.env.AGENT_FLEET_HOME;
  process.env.AGENT_FLEET_HOME=join(dir,'index');const root=join(dir,'project');mkdirSync(root);
  execFileSync('git',['init','-q','-b','main',root]);execFileSync('git',['-c','core.hooksPath=/dev/null','-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-qm','Synthetic fixture'],{cwd:root});
  const engine=Engine.init(root,'Offline gadget evidence'),pool=new ResourcePool(join(dir,'pool.sqlite'));
  pool.upsertResource({id:'external-fixture',provider:'external-agent',model:'offline-fixture',endpointId:'gadget-fixture',endpointKind:'external-agent',accountDomain:'fixture-account',capabilities:['code'],roles:['worker'],capacity:4});
  pool.setLimits({global:4,projects:{[engine.root]:4},endpoints:{'gadget-fixture':4},accounts:{'fixture-account':4}});
  engine.store.setMeta('resourcePoolPath',pool.path);const queue=new PooledRemoteQueue(pool),projectKey=pooledProjectKey(engine.root);
  const scope={endpointId:'gadget-fixture',resourceIds:['external-fixture'],projects:[{projectKey,sessionId:engine.sessionId}]};
  const caps:Capabilities={protocolVersion:1,provider:'external-agent',capabilities:['code'],quota:'unknown',auth:'available'};
  function offer(tag:string):GadgetBinding{
    const dispatch:Dispatch={version:1,messageId:`message-${tag}`,taskId:`task-${tag}`,runId:`run-${tag}`,attemptId:`attempt-${tag}`,endpointId:'gadget-fixture',fence:1,repositoryId:projectKey,baseSha:engine.store.getMeta('baseSha') as string,provider:'external-agent',requiredCapabilities:['code'],instructions:'Synthetic offline evidence only',dependencies:[]};
    queue.offer({projectKey,dispatch,request:{id:`pool-${tag}`,projectId:engine.root,sessionId:engine.sessionId,taskId:dispatch.taskId,attemptId:dispatch.attemptId,role:'worker',capabilities:['code'],requiredProvider:'external-agent',eligibleResourceIds:['external-fixture']}});
    return {projectId:engine.root,sessionId:engine.sessionId,taskId:dispatch.taskId,runId:dispatch.runId,attemptId:dispatch.attemptId,endpointId:dispatch.endpointId,requestId:`gadget-request-${tag}`,sidechatId:`sidechat-${tag}`};
  }
  function close(){engine.close();pool.close();if(oldHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=oldHome;rmSync(dir,{recursive:true,force:true});}
  return {dir,root,engine,pool,queue,scope,caps,offer,close};
}
function quiet(sourceId:string):GadgetInput{return {sourceId,kind:'quiet',data:{}};}

test('gadget reducer snapshots and global audit events commit together and duplicate ingress stays idempotent across reopen',()=>{
  const f=setup();try{
    const binding=f.offer('a'),reader=new Store(join(f.engine.dir,'state.sqlite'),{readOnly:true});let notifications=0;
    f.engine.store.subscribe(event=>{if(!event.kind.startsWith('gadget.'))return;assert.equal(reader.gadgetEvidenceSnapshot().revision,1);assert.ok(reader.events(event.event_id-1,1).length);notifications++;});
    const first=recordGadgetEvidence(f.engine,binding,quiet('quiet-one'));assert.equal(first.revision,1);assert.equal(first.state.accepted,false);assert.equal(first.state.transport,'disabled-unpaired');
    assert.ok(first.events.every(e=>e.run_id===null&&e.run_seq===null));assert.ok(first.events.every(e=>(e.data as any).binding.runId===binding.runId));assert.equal(notifications,first.events.length);
    const before=f.engine.store.events(0,1000),duplicate=recordGadgetEvidence(f.engine,binding,quiet('quiet-one'));assert.equal(duplicate.revision,first.revision);assert.deepEqual(duplicate.events,[]);assert.deepEqual(f.engine.store.events(0,1000),before);
    const reopened=new Engine(f.root,true,f.engine.sessionId);assert.deepEqual(getGadgetEvidence(reopened,binding.requestId)?.state,first.state);assert.throws(()=>recordGadgetEvidence(reopened,binding,quiet('denied')),/Observer/);reopened.close();reader.close();
  }finally{f.close();}
});

test('remote offer tuple and trusted session identities are mandatory before persisting any evidence',()=>{
  const f=setup();try{
    const binding=f.offer('a'),before=f.engine.store.events(0,1000);
    for(const field of ['projectId','sessionId','taskId','runId','attemptId','endpointId'] as const){const wrong={...binding,[field]:field==='projectId'?join(f.dir,'other'): `wrong-${field}`};assert.throws(()=>recordGadgetEvidence(f.engine,wrong,quiet(`wrong-${field}`)));}
    assert.deepEqual(f.engine.store.events(0,1000),before);assert.equal(getGadgetEvidence(f.engine,binding.requestId),null);
    const pending={...binding,taskId:'task-pending',runId:'run-pending',attemptId:'attempt-pending',requestId:'request-pending',sidechatId:'sidechat-pending'};
    f.engine.store.setMeta('pooledRemoteOffers',[{dispatch:{version:1,messageId:'message-pending',taskId:pending.taskId,runId:pending.runId,attemptId:pending.attemptId,endpointId:pending.endpointId,provider:'external-agent',repositoryId:pooledProjectKey(f.engine.root),baseSha:f.engine.store.getMeta('baseSha'),fence:1,requiredCapabilities:['code'],instructions:'Offline intent only',dependencies:[]}}]);
    assert.ok(f.engine.remoteStatus().some(row=>row.runId===pending.runId&&row.state==='offer-pending'));assert.throws(()=>recordGadgetEvidence(f.engine,pending,quiet('unoffered')),/existing remote offer/);
    f.engine.store.setMeta('sessionId','tampered-session');assert.throws(()=>recordGadgetEvidence(f.engine,binding,quiet('tampered')),/fixed engine/);assert.throws(()=>getGadgetEvidence(f.engine,binding.requestId),/fixed engine/);f.engine.store.setMeta('sessionId',f.engine.sessionId);
  }finally{f.close();}
});

test('one immutable gadget request binds each run/attempt and sidechats cannot cross attempts within a session',()=>{
  const f=setup();try{
    const a=f.offer('a'),b=f.offer('b');recordGadgetEvidence(f.engine,a,quiet('initial'));
    const before=f.engine.store.gadgetEvidenceSnapshot();
    assert.throws(()=>recordGadgetEvidence(f.engine,{...a,sidechatId:'changed-sidechat'},quiet('change')),/immutable/);
    assert.throws(()=>recordGadgetEvidence(f.engine,{...a,requestId:'replacement-request'},quiet('replace')),/run\/attempt/);
    assert.throws(()=>recordGadgetEvidence(f.engine,{...b,sidechatId:a.sidechatId},quiet('reuse')),/sidechat/);
    assert.deepEqual(f.engine.store.gadgetEvidenceSnapshot(),before);
    assert.equal(recordGadgetEvidence(f.engine,b,quiet('new-attempt')).revision,2);
  }finally{f.close();}
});

test('gadget CAS conflicts and inserted-event failures cannot partially overwrite snapshot or publish events',()=>{
  const f=setup();try{
    const binding=f.offer('a');recordGadgetEvidence(f.engine,binding,quiet('initial'));
    const original=f.engine.store.gadgetEvidenceSnapshot(),other=new Store(join(f.engine.dir,'state.sqlite'));
    other.compareAndSwapGadgetEvidence(original.revision,original.records,[{kind:'gadget.cas_fixture',data:{binding}}]);other.close();
    const snapshot=f.engine.store.gadgetEvidenceSnapshot(),before=f.engine.store.events(0,1000);let calls=0;f.engine.store.subscribe(()=>calls++);
    assert.throws(()=>f.engine.store.compareAndSwapGadgetEvidence(original.revision,original.records,[{kind:'gadget.stale',data:{}}]),/revision conflict/);
    assert.deepEqual(f.engine.store.gadgetEvidenceSnapshot(),snapshot);assert.deepEqual(f.engine.store.events(0,1000),before);assert.equal(calls,0);
    const db=new DatabaseSync(join(f.engine.dir,'state.sqlite'));db.exec("CREATE TRIGGER gadget_fixture_failure BEFORE INSERT ON events WHEN NEW.kind='gadget.fixture_failure' BEGIN SELECT RAISE(ABORT,'fixture audit failure'); END");
    assert.throws(()=>f.engine.store.compareAndSwapGadgetEvidence(snapshot.revision,snapshot.records,[{kind:'gadget.before_failure',data:{}},{kind:'gadget.fixture_failure',data:{}}]),/fixture audit failure/);
    assert.deepEqual(f.engine.store.gadgetEvidenceSnapshot(),snapshot);assert.deepEqual(f.engine.store.events(0,1000),before);assert.equal(calls,0);db.close();
    assert.throws(()=>f.engine.store.compareAndSwapGadgetEvidence(snapshot.revision,snapshot.records,[{kind:'run.accepted',data:{}}]),/Invalid bounded gadget/);
    assert.throws(()=>f.engine.store.setMeta('gadget:evidence:v1',{}),/compare-and-swap/);
    const observer=new Store(join(f.engine.dir,'state.sqlite'),{readOnly:true});assert.throws(()=>observer.compareAndSwapGadgetEvidence(snapshot.revision,snapshot.records,[]),/read-only/);observer.close();
  }finally{f.close();}
});

test('offline delivery uncertainty never acknowledges remote queue, releases held capacity, or accepts a task',()=>{
  const f=setup();try{
    const binding=f.offer('a'),claim=f.queue.claim(f.scope,{claimId:'claim-a',capabilities:f.caps});assert.ok(claim);
    const queueBefore=f.queue.list(f.scope),poolBefore=f.pool.status(),runsBefore=f.engine.store.listRuns();
    recordGadgetEvidence(f.engine,binding,{sourceId:'disconnect-one',kind:'disconnect',data:{}});
    const state=getGadgetEvidence(f.engine,binding.requestId)!.state;assert.equal(state.accepted,false);assert.equal(state.transport,'disabled-unpaired');
    assert.deepEqual(f.queue.list(f.scope),queueBefore);assert.deepEqual(f.pool.status(),poolBefore);assert.deepEqual(f.engine.store.listRuns(),runsBefore);assert.equal(f.pool.status().leases[0].state,'held');assert.equal(f.queue.list(f.scope)[0].acknowledged,false);
  }finally{f.close();}
});

test('provider ACK and completed/failed task observations cannot ACK the coordinator or release a held lease',()=>{
  for(const status of ['completed','failed']){
    const f=setup();try{
      const binding=f.offer('a');assert.ok(f.queue.claim(f.scope,{claimId:'claim-a',capabilities:f.caps}));
      const queueBefore=f.queue.list(f.scope),poolBefore=f.pool.status(),runsBefore=f.engine.store.listRuns();
      const ack=recordGadgetEvidence(f.engine,binding,{sourceId:'provider-ack',kind:'ack',data:{result:{session_id:binding.sidechatId,is_thread:true,message_id:'user-synthetic'}}});
      assert.equal(ack.state.delivery,'delivered');
      const result=recordGadgetEvidence(f.engine,binding,{sourceId:`provider-${status}`,kind:'event',data:{type:'event',event:'task.status',payload:{session_id:binding.sidechatId,message_id:'user-synthetic',task_id:'provider-task',status}}});
      assert.equal(result.state.tasks[0].lastObservedStatus,status);assert.equal(result.state.accepted,false);
      assert.deepEqual(f.queue.list(f.scope),queueBefore);assert.deepEqual(f.pool.status(),poolBefore);assert.deepEqual(f.engine.store.listRuns(),runsBefore);
      assert.equal(f.queue.list(f.scope)[0].acknowledged,false);assert.equal(f.pool.status().leases[0].state,'held');
      const observed=getGadgetEvidence(f.engine,binding.requestId)!;assert.equal(observed.state.transport,'disabled-unpaired');assert.equal(observed.state.accepted,false);
    }finally{f.close();}
  }
});
