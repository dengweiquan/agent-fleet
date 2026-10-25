import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Connector,ConnectorError,ConnectorJournal,ConnectorWatchdog,SshStdioTransport} from '../src/connector.ts';
import type {ConnectorErrorCode,ConnectorFrame,ConnectorReply,ConnectorTransport,Identity,TerminationEvidence} from '../src/connector.ts';

const owner=(id='owner-a'):Identity=>({ownerId:id,pid:id==='owner-a'?1001:1002,processStart:'fixture-start-'+id,bootId:'fixture-boot'});
function proof(identity:Identity):TerminationEvidence{return {identity,positivelyVerified:true,method:'vm-lifecycle',record:'Trusted fixture supervisor observed VM instance termination',verifiedBy:'fixture-supervisor'};}
class FakeTransport implements ConnectorTransport {
  readonly connectionKey:string;
  constructor(connectionKey='a'.repeat(64)){this.connectionKey=connectionKey;}
  connects:number[]=[];requests:ConnectorFrame[]=[];closes=0;
  connectError:ConnectorError|null=null;
  replyTo:(frame:ConnectorFrame)=>Promise<ConnectorReply>=async frame=>({id:frame.id,generation:frame.generation,ok:true,data:{events:[{eventId:7}],serverCursor:7}});
  async connect(generation:number){this.connects.push(generation);if(this.connectError)throw this.connectError;}
  async request(frame:ConnectorFrame){this.requests.push(structuredClone(frame));return this.replyTo(frame);}
  async close(){this.closes++;}
}
function fixture(){const dir=mkdtempSync(join(tmpdir(),'fleet-connector-')),path=join(dir,'connector.sqlite');return {path,cleanup:()=>rmSync(dir,{recursive:true,force:true})};}

test('connector persists immutable outbox, committed inbox/cursor and read-only observations across reopen',async()=>{
  const f=fixture();let journal=new ConnectorJournal(f.path);let connector:Connector|undefined;
  try{
    const request={version:1 as const,op:'hello'};journal.enqueue('read-1',request,{readOnly:true});journal.enqueue('read-1',request,{readOnly:true});
    assert.equal(journal.status().outbox.length,1);assert.throws(()=>journal.enqueue('read-1',{version:1,op:'events'},{readOnly:true}),/immutable/);
    journal.close();journal=new ConnectorJournal(f.path);const transport=new FakeTransport();connector=new Connector(journal,transport,owner());
    assert.equal((await connector.step()).state,'replied');const saved=journal.status();assert.equal(saved.outbox[0].state,'replied');assert.equal(saved.cursor,1);assert.equal(saved.inbox[0].reply.data!==undefined,true);
    assert.equal(journal.recordReply(saved.inbox[0].reply).cursor,1);assert.equal(journal.status().cursor,1,'Identical reply does not advance cursor twice');
    await connector.close();connector=undefined;journal.close();journal=new ConnectorJournal(f.path,{readOnly:true});assert.equal(journal.status().cursor,1);assert.equal(journal.status().outbox[0].state,'replied');assert.throws(()=>journal.enqueue('denied',{version:1,op:'hello'}),/read-only/);
  }finally{if(connector)await connector.close();journal.close();f.cleanup();}
});

test('lost reply persists unknown across reopen and never automatically repeats task effect',async()=>{
  const f=fixture();let journal=new ConnectorJournal(f.path);let connector:Connector|undefined;let effects=0;
  try{
    const transport=new FakeTransport();transport.replyTo=async()=>{effects++;throw new ConnectorError('DISCONNECTED','reply lost after fixture effect');};
    journal.enqueue('effect-1',{version:1,op:'claim',taskId:'fixture-task'});connector=new Connector(journal,transport,owner());
    assert.equal((await connector.ensureConnected()).state,'idle');
    journal.markSynchronized(journal.status().generation,{helloConfirmed:true,replayCursor:0});
    assert.equal((await connector.step()).state,'disconnected');assert.equal(effects,1);assert.equal(journal.status().outbox[0].state,'unknown');assert.equal(journal.status().cursor,0);
    await connector.close();connector=undefined;journal.close();journal=new ConnectorJournal(f.path);const reopenedTransport=new FakeTransport();connector=new Connector(journal,reopenedTransport,owner());
    await connector.step();assert.equal(reopenedTransport.requests.length,0);assert.equal(journal.status().outbox[0].state,'unknown');assert.equal(effects,1);
    const unresolved=journal.status().outbox[0];journal.reconcile('effect-1',{resolution:'replied',reply:{id:'effect-1',generation:unresolved.generation!,ok:true,data:{claimId:'original-claim',executed:true}}});
    assert.equal(journal.status().outbox[0].state,'replied');assert.equal(journal.status().cursor,1);await connector.step();assert.equal(reopenedTransport.requests.length,0);assert.equal(effects,1);
  }finally{if(connector)await connector.close();journal.close();f.cleanup();}
});

test('stale-generation response cannot acknowledge a request or advance the committed cursor',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());
  try{journal.enqueue('read-stale',{version:1,op:'hello'},{readOnly:true});transport.replyTo=async frame=>({id:frame.id,generation:frame.generation-1,ok:true,data:{serverCursor:999}});
    assert.equal((await connector.step()).state,'blocked');assert.equal(journal.status().outbox[0].state,'unknown');assert.equal(journal.status().cursor,0);assert.equal(journal.status().inbox.length,0);
  }finally{await connector.close();journal.close();}
});

test('durable owner lock prevents two active connectors and requires exact verified termination evidence',async()=>{
  const f=fixture(),first=new ConnectorJournal(f.path),second=new ConnectorJournal(f.path),a=new FakeTransport(),b=new FakeTransport(),one=new Connector(first,a,owner()),two=new Connector(second,b,owner('owner-b'));
  try{
    assert.equal((await one.ensureConnected()).state,'idle');assert.equal((await two.ensureConnected()).state,'blocked');assert.equal(a.connects.length,1);assert.equal(b.connects.length,0);
    assert.throws(()=>second.acquire(owner('owner-b')),/termination evidence/);
    assert.throws(()=>second.acquire(owner('owner-b'),{...proof(owner()),method:'pid-absent'} as any),/termination evidence/);
    assert.throws(()=>second.acquire(owner('owner-b'),proof({...owner(),processStart:'wrong-start'})),/termination evidence/);
    await one.close();assert.equal(first.status().owner?.ownerId,'owner-a','Close alone cannot clear ownership proof');
    const oldGeneration=first.status().generation;const newGeneration=second.acquire(owner('owner-b'),proof(owner()));assert.ok(newGeneration>oldGeneration);assert.equal(second.status().owner?.ownerId,'owner-b');assert.equal((await two.ensureConnected()).state,'idle');assert.equal(b.connects.length,1);
  }finally{await one.close();await two.close();first.close();second.close();f.cleanup();}
});

test('disabled has priority over connection, reconnection and watchdog restart without erasing work',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());let factoryCalls=0;
  try{journal.enqueue('queued',{version:1,op:'claim'});journal.setDisabled(true);assert.equal((await connector.ensureConnected()).state,'disabled');assert.equal((await connector.step()).state,'disabled');assert.equal(transport.connects.length,0);assert.equal(transport.requests.length,0);
    const watchdog=new ConnectorWatchdog(journal);assert.equal((await watchdog.tick({nowMs:1000,owned:connector,termination:proof(owner()),approvedFactory:true,factory:()=>{factoryCalls++;return connector;}})).state,'disabled');assert.equal(factoryCalls,0);assert.equal(journal.status().outbox[0].state,'queued');
    assert.throws(()=>journal.acquire(owner()),/disabled/);
  }finally{await connector.close();journal.close();}
});

test('disabled while request is in flight refuses the late response and preserves unknown',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());
  let respond!:(reply:ConnectorReply)=>void;let started!:()=>void;const sent=new Promise<void>(resolve=>{started=resolve;});
  transport.replyTo=frame=>new Promise(resolve=>{respond=resolve;started();});
  try{journal.enqueue('inflight',{version:1,op:'hello'},{readOnly:true});const step=connector.step();await sent;journal.setDisabled(true);const frame=transport.requests[0];respond({id:frame.id,generation:frame.generation,ok:true});assert.equal((await step).state,'disabled');assert.equal(journal.status().outbox[0].state,'unknown');assert.equal(journal.status().cursor,0);assert.equal((await connector.step()).state,'disabled');assert.equal(transport.requests.length,1);
  }finally{await connector.close();journal.close();}
});

test('watchdog bounded backoff and circuit survive reopen and never claim queued tasks',async()=>{
  const f=fixture();let journal=new ConnectorJournal(f.path),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());
  try{
    journal.acquire(owner());journal.enqueue('pending-task',{version:1,op:'claim'});transport.connectError=new ConnectorError('DISCONNECTED','transient fixture disconnect');
    let watchdog=new ConnectorWatchdog(journal,{maxAttempts:3,baseDelayMs:50,maxDelayMs:80});
    assert.equal((await watchdog.tick({nowMs:1000,owned:connector})).state,'backoff');assert.equal(journal.status().watchdog.nextAttemptAt,1050);assert.equal((await watchdog.tick({nowMs:1049,owned:connector})).state,'backoff');assert.equal(transport.connects.length,1);assert.equal(transport.requests.length,0);
    await connector.close();journal.close();journal=new ConnectorJournal(f.path);transport=new FakeTransport();transport.connectError=new ConnectorError('DISCONNECTED','still disconnected');connector=new Connector(journal,transport,owner());watchdog=new ConnectorWatchdog(journal,{maxAttempts:3,baseDelayMs:50,maxDelayMs:80});
    assert.equal((await watchdog.tick({nowMs:1050,owned:connector})).state,'backoff');assert.equal(journal.status().watchdog.nextAttemptAt,1130);assert.equal((await watchdog.tick({nowMs:1130,owned:connector})).state,'circuit-open');assert.equal(journal.status().watchdog.attempts,3);assert.equal((await watchdog.tick({nowMs:999999,owned:connector})).state,'circuit-open');assert.equal((await connector.ensureConnected()).state,'blocked','Primary cannot bypass the persisted circuit');assert.equal(transport.connects.length,2);assert.equal(transport.requests.length,0);assert.equal(journal.status().outbox[0].state,'queued');
    journal.setDisabled(true);assert.equal((await watchdog.tick({nowMs:1000000,owned:connector})).state,'disabled');
  }finally{await connector.close();journal.close();f.cleanup();}
});

test('auth/config/fingerprint/network/dependency blocks persist with no watchdog repair',async()=>{
  for(const code of ['AUTH_EXPIRED','MISSING_SSH_CONFIG','HOST_KEY_MISMATCH','FIREWALL','DEPENDENCY'] as ConnectorErrorCode[]){
    const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());
    try{journal.acquire(owner());transport.connectError=new ConnectorError(code,'fixture block token=private-test-token');const watchdog=new ConnectorWatchdog(journal);
      const result=await watchdog.tick({nowMs:1000,owned:connector});assert.equal(result.state,'blocked',code);assert.ok(journal.status().watchdog.blockedReason?.startsWith(code));assert.doesNotMatch(result.error??'',/private-test-token/);transport.connectError=null;
      assert.equal((await watchdog.tick({nowMs:999999,owned:connector})).state,'blocked');assert.equal((await connector.ensureConnected()).state,'blocked','Primary cannot bypass the persisted block');assert.equal(transport.connects.length,1);assert.equal(transport.requests.length,0);
    }finally{await connector.close();journal.close();}
  }
});

test('watchdog requires verified owned-process termination plus approved factory and never pumps tasks',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner('owner-b'),{reconcileConnection:async(link,generation,snapshot)=>{const response=await link.request({id:'watchdog-hello-'+generation,generation,payload:{version:1,op:'hello'}});assert.equal(response.ok,true);return {helloConfirmed:true,replayCursor:snapshot.serverCursor};}});let calls=0;
  try{journal.acquire(owner());journal.enqueue('task',{version:1,op:'claim'});const watchdog=new ConnectorWatchdog(journal),factory=()=>{calls++;return connector;};
    assert.equal((await watchdog.tick({nowMs:1000,factory,approvedFactory:true})).state,'restart-required');assert.equal(calls,0);
    assert.equal((await watchdog.tick({nowMs:1001,termination:proof(owner()),factory})).state,'restart-required');assert.equal(calls,0);
    assert.equal((await watchdog.tick({nowMs:1002,termination:proof(owner()),factory,approvedFactory:true})).state,'restarted');assert.equal(calls,1);assert.equal(transport.connects.length,1);assert.deepEqual(transport.requests.map(frame=>frame.payload.op),['hello']);assert.equal(journal.status().outbox[0].state,'queued');
  }finally{await connector.close();journal.close();}
});

test('SSH transport construction is opt-in, uses strict existing auth and opens no connection',()=>{
  assert.throws(()=>new SshStdioTransport({alias:'macmini',command:['agent-fleet','connector-serve'],approved:false} as any),/approval/);
  assert.throws(()=>new SshStdioTransport({alias:'host; arbitrary-command',command:['agent-fleet'],approved:true}),/existing SSH config alias/);
  const transport=new SshStdioTransport({alias:'macmini',command:['/fixture/agent-fleet','connector-serve'],approved:true});assert.ok(transport.args.includes('BatchMode=yes'));assert.ok(transport.args.includes('StrictHostKeyChecking=yes'));assert.ok(transport.args.includes('ConnectionAttempts=1'));assert.ok(!transport.args.includes('-L')&&!transport.args.includes('-R'));
});

test('fresh generation reconciles hello/cursor before mutations; default connector cannot assume synchronization',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),identity=owner();
  let connector=new Connector(journal,transport,identity);
  try{
    journal.enqueue('claim-after-reconcile',{version:1,op:'claim',taskId:'fixture'});
    assert.equal((await connector.step()).state,'blocked');assert.equal(transport.requests.length,0);assert.equal(journal.status().outbox[0].state,'queued');
    await connector.close();
    let reconciliations=0;
    connector=new Connector(journal,transport,identity,{reconcileConnection:async(link,generation,snapshot)=>{
      reconciliations++;assert.equal(snapshot.outbox[0].state,'queued');
      const response=await link.request({id:'hello-'+generation,generation,payload:{version:1,op:'hello'}});assert.equal(response.ok,true);
      return {helloConfirmed:true,replayCursor:snapshot.serverCursor};
    }});
    assert.equal((await connector.step()).state,'replied');assert.equal(reconciliations,1);assert.deepEqual(transport.requests.map(frame=>frame.payload.op),['hello','claim']);assert.equal(journal.status().synchronizedGeneration,journal.status().generation);
  }finally{await connector.close();journal.close();}
});

test('unresolved effects block a different queued mutation until explicit reconciliation and resynchronization',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());
  try{
    journal.enqueue('first',{version:1,op:'claim',taskId:'one'});journal.enqueue('next',{version:1,op:'claim',taskId:'two'});
    assert.equal((await connector.ensureConnected()).state,'idle');journal.markSynchronized(journal.status().generation,{helloConfirmed:true,replayCursor:0});
    transport.replyTo=async()=>{throw new ConnectorError('DISCONNECTED','effect may have happened');};assert.equal((await connector.step()).state,'disconnected');
    assert.equal((await connector.ensureConnected()).state,'idle');assert.throws(()=>journal.markSynchronized(journal.status().generation,{helloConfirmed:true,replayCursor:0}),/unknown|unresolved|uncertain|sent/i);
    await connector.step();assert.equal(transport.requests.length,1);assert.deepEqual(journal.status().outbox.map(item=>item.state),['unknown','queued']);
    const original=journal.status().outbox[0];journal.reconcile('first',{resolution:'replied',reply:{id:'first',generation:original.generation!,ok:true,data:{claimId:'original'}}});
    transport.replyTo=async frame=>({id:frame.id,generation:frame.generation,ok:true});
    assert.equal((await connector.ensureConnected()).state,'idle');journal.markSynchronized(journal.status().generation,{helloConfirmed:true,replayCursor:0});assert.equal((await connector.step()).state,'replied');assert.deepEqual(transport.requests.map(frame=>frame.id),['first','next']);
  }finally{await connector.close();journal.close();}
});

test('server replay cursor is durable, distinct from inbox sequence, and rejects stale/conflicting events',async()=>{
  const f=fixture();let journal=new ConnectorJournal(f.path);
  try{
    const generation=journal.acquire(owner());
    journal.recordServerEvents(generation,[{event_id:4,kind:'fixture.first'},{event_id:9,kind:'fixture.second'}]);assert.equal(journal.status().serverCursor,9);assert.equal(journal.status().cursor,0);
    journal.recordServerEvents(generation,[{event_id:4,kind:'fixture.first'},{event_id:9,kind:'fixture.second'}]);assert.equal(journal.status().serverCursor,9);
    assert.throws(()=>journal.recordServerEvents(generation,[{event_id:9,kind:'changed'}]),/conflict|event/i);
    assert.throws(()=>journal.recordServerEvents(generation,[{event_id:8,kind:'unseen-old'}]),/cursor|regress|backward|event/i);
    assert.throws(()=>journal.markSynchronized(generation,{helloConfirmed:true,replayCursor:10}),/cursor|replay/i);
    journal.markSynchronized(generation,{helloConfirmed:true,replayCursor:9});assert.equal(journal.status().synchronizedGeneration,generation);
    const newer=journal.nextGeneration(owner());assert.throws(()=>journal.recordServerEvents(generation,[{event_id:10,kind:'late'}]),/generation|stale/i);assert.notEqual(journal.status().synchronizedGeneration,newer);
    journal.close();journal=new ConnectorJournal(f.path);assert.equal(journal.status().serverCursor,9);assert.equal(journal.status().cursor,0);
  }finally{journal.close();f.cleanup();}
});

test('read-only labels cannot smuggle task mutations through the synchronization barrier',()=>{
  const journal=new ConnectorJournal(':memory:');try{
    for(const op of ['claim','ack','report','exec','shell'])assert.throws(()=>journal.enqueue('pretend-read-'+op,{version:1,op},{readOnly:true}),/read.only|read-only|allow|operation/i);
    assert.equal(journal.status().outbox.length,0);
  }finally{journal.close();}
});

test('primary respects persisted backoff and immutable connection target',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());
  const other=new FakeTransport('b'.repeat(64)),changed=new Connector(journal,other,owner());
  try{
    journal.setWatchdog({attempts:1,nextAttemptAt:2000,circuitOpen:false,blockedReason:null});assert.equal((await connector.ensureConnected(1000)).state,'blocked');assert.equal(transport.connects.length,0);
    assert.equal((await connector.ensureConnected(2000)).state,'idle');assert.equal(transport.connects.length,1);assert.equal(journal.status().connectionKey,'a'.repeat(64));
    await connector.close();assert.equal((await changed.ensureConnected(2001)).state,'blocked');assert.equal(other.connects.length,0);assert.equal(journal.status().connectionKey,'a'.repeat(64));
  }finally{await connector.close();await changed.close();journal.close();}
});

test('watchdog does not call an unverified SSH spawn healthy and rejects mutation in health callback',async()=>{
  const journal=new ConnectorJournal(':memory:'),transport=new FakeTransport(),connector=new Connector(journal,transport,owner());
  try{
    journal.acquire(owner());journal.enqueue('work',{version:1,op:'claim'});const watchdog=new ConnectorWatchdog(journal);
    assert.equal((await watchdog.tick({nowMs:1000,owned:connector})).state,'reconciliation-required');assert.equal(transport.requests.length,0);assert.equal(journal.status().outbox[0].state,'queued');
    await connector.close();
    const attemptedMutation=new Connector(journal,transport,owner(),{reconcileConnection:async(link,generation)=>{await link.request({id:'forbidden',generation,payload:{version:1,op:'claim'}});return {helloConfirmed:true,replayCursor:0};}});
    try{assert.equal((await watchdog.tick({nowMs:1001,owned:attemptedMutation})).state,'blocked');assert.equal(transport.requests.length,0);assert.equal(journal.status().outbox[0].state,'queued');}finally{await attemptedMutation.close();}
  }finally{await connector.close();journal.close();}
});

test('hung fake transport connect and request are bounded without replay',{timeout:3000},async()=>{
  const first=new ConnectorJournal(':memory:'),hungConnect=new FakeTransport();hungConnect.connect=async()=>new Promise(()=>{});
  const connecting=new Connector(first,hungConnect,owner(),{operationTimeoutMs:20});
  try{assert.equal((await connecting.ensureConnected()).state,'disconnected');assert.equal(hungConnect.requests.length,0);assert.ok(hungConnect.closes>=1);}finally{await connecting.close();first.close();}
  const second=new ConnectorJournal(':memory:'),hungRequest=new FakeTransport();hungRequest.replyTo=async()=>new Promise(()=>{});
  const requesting=new Connector(second,hungRequest,owner(),{operationTimeoutMs:20});
  try{second.enqueue('timed-read',{version:1,op:'hello'},{readOnly:true});assert.equal((await requesting.step()).state,'disconnected');assert.equal(second.status().outbox[0].state,'unknown');await requesting.step();assert.equal(hungRequest.requests.length,1);}finally{await requesting.close();second.close();}
});
