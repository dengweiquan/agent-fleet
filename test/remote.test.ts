import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {RunnerJournal,RemoteCoordinator,FakeTransport,DisabledSshTransport,validateDispatch,negotiate} from '../src/remote.ts';
import type {Dispatch} from '../src/remote.ts';
const caps={protocolVersion:1 as const,provider:'fake',capabilities:['code'],quota:'unknown' as const,auth:'available' as const};
const message=(overrides:Partial<Dispatch>={}):Dispatch=>({version:1,messageId:'msg-1',taskId:'task-1',runId:'run-1',attemptId:'attempt-1',endpointId:'remote-1',fence:1,repositoryId:'fixture',baseSha:'a'.repeat(40),provider:'fake',requiredCapabilities:['code'],instructions:'offline fixture',dependencies:[],...overrides});
function fixture(t:any){const dir=mkdtempSync(join(tmpdir(),'agent-fleet-remote-'));const runner=new RunnerJournal(join(dir,'runner.sqlite'),'remote-1',caps);t.after(()=>{try{runner.close();}catch{}rmSync(dir,{recursive:true,force:true});});return {dir,runner};}
test('lost ACK after side effect reconciles across coordinator restart without replay',async t=>{
  const {dir,runner}=fixture(t);let effects=0;const transport=new FakeTransport(runner,async()=>({exitCode:0,effects:++effects}));
  let coordinator=new RemoteCoordinator(join(dir,'coordinator.sqlite'),transport,'remote-1');transport.loseNextAck=true;
  await assert.rejects(()=>coordinator.dispatch(message()),/lost ACK/);
  assert.equal(coordinator.status('run-1'),'unknown');assert.equal(effects,1);assert.equal(runner.query('run-1')?.state,'completed');
  coordinator.close();coordinator=new RemoteCoordinator(join(dir,'coordinator.sqlite'),transport,'remote-1');t.after(()=>coordinator.close());
  transport.connected=true;const result=await coordinator.reconcile('run-1');assert.equal(result.state,'completed');assert.equal(effects,1);
  assert.equal(result.events.length,3);assert.equal(result.cursor,3);assert.equal((await coordinator.reconcile('run-1')).events.length,0);
  await assert.rejects(()=>coordinator.dispatch(message()),/UNIQUE/);assert.equal(effects,1);
});
test('durable duplicate inbox returns original ACK; changed payload or reused attempt rejected',async t=>{
  const {runner}=fixture(t);const first=runner.receive(message());assert.deepEqual(runner.receive(message()),first);
  assert.equal(runner.events().length,1);assert.throws(()=>runner.receive(message({instructions:'changed'})),/content conflict/);
  assert.throws(()=>runner.receive(message({messageId:'msg-2'})),/already exists/);
  let effects=0;await runner.execute('run-1',async()=>++effects);await runner.execute('run-1',async()=>++effects);assert.equal(effects,1);
  assert.equal(runner.query('run-1')?.state,'completed');assert.equal(runner.events().at(-1)?.kind,'execution.completed');
});
test('new fence blocks stale dispatch and fences already queued ownership without effects',async t=>{
  const {runner}=fixture(t);runner.receive(message());runner.receive(message({messageId:'msg-2',runId:'run-2',attemptId:'attempt-2',fence:2}));
  assert.throws(()=>runner.receive(message({messageId:'msg-3',runId:'run-3',attemptId:'attempt-3'})),/Stale/);
  let effects=0;await runner.execute('run-1',async()=>++effects);assert.equal(effects,0);assert.equal(runner.query('run-1')?.state,'unknown');
  assert.deepEqual(runner.receive(message()).meaning,'durably-received-only');
});
test('fence cannot undo an in-flight external side effect; completion becomes unknown',async t=>{
  const {runner}=fixture(t);runner.receive(message());let effects=0;
  await runner.execute('run-1',async()=>{effects++;runner.receive(message({messageId:'msg-2',runId:'run-2',attemptId:'attempt-2',fence:2}));return {exitCode:0};});
  assert.equal(effects,1);assert.equal(runner.query('run-1')?.state,'unknown');assert.equal(runner.events().at(-1)?.kind,'execution.fenced');
});
test('disconnect before delivery is unknown; reconnect absence never redispatches',async t=>{
  const {dir,runner}=fixture(t);let effects=0;const transport=new FakeTransport(runner,async()=>++effects),coordinator=new RemoteCoordinator(join(dir,'c.sqlite'),transport,'remote-1');t.after(()=>coordinator.close());
  transport.connected=false;await assert.rejects(()=>coordinator.dispatch(message()),/disconnection/);transport.connected=true;
  assert.equal((await coordinator.reconcile('run-1')).state,'unknown');assert.equal(runner.query('run-1'),null);assert.equal(effects,0);
});
test('handler interruption and runner restart preserve unknown; event IDs and run sequences persist',async t=>{
  const {dir,runner}=fixture(t);runner.receive(message());let effects=0;
  await assert.rejects(()=>runner.execute('run-1',async()=>{effects++;throw new Error('interrupted');}));await runner.execute('run-1',async()=>effects++);assert.equal(effects,1);
  runner.receive(message({messageId:'msg-2',runId:'run-2',attemptId:'attempt-2'}));
  runner.db.prepare("UPDATE remote_runs SET state='executing' WHERE run_id='run-2'").run();runner.close();
  const reopened=new RunnerJournal(join(dir,'runner.sqlite'),'remote-1',caps);t.after(()=>reopened.close());assert.deepEqual(reopened.recover(),['run-2']);assert.equal(reopened.query('run-2')?.state,'unknown');
  const events=reopened.events();assert.deepEqual(events.map(e=>e.eventId),[1,2,3,4,5]);assert.deepEqual(events.filter(e=>e.runId==='run-1').map(e=>e.runSeq),[1,2,3]);
});
test('protocol refuses local paths, floating refs, unknown provider/auth, and unapproved snapshots',async()=>{
  assert.throws(()=>validateDispatch(message({repositoryId:'/Users/example/repo'})),/repositoryId/);
  assert.throws(()=>validateDispatch(message({baseSha:'main'})),/SHA/);
  assert.throws(()=>validateDispatch({...message(),cwd:'/tmp'} as any),/Unknown/);
  assert.throws(()=>validateDispatch(message({snapshot:{kind:'explicit-commit',commitSha:'b'.repeat(40),approvalId:''}})),/snapshot/);
  assert.throws(()=>negotiate(caps,'Muse',['code']),/negotiation/);assert.throws(()=>negotiate({...caps,auth:'unknown'},'fake',[]),/negotiation/);
  await assert.rejects(()=>new DisabledSshTransport().send(message()),/disabled/);
});
test('coordinator binds endpoint identity and rejects corrupted query/events without cursor advance',async t=>{
  const {dir,runner}=fixture(t),transport=new FakeTransport(runner,async()=>({exitCode:0})),path=join(dir,'bound.sqlite');
  const coordinator=new RemoteCoordinator(path,transport,'remote-1');t.after(()=>coordinator.close());
  assert.throws(()=>new RemoteCoordinator(path,transport,'remote-2'),/different endpoint/);
  await coordinator.dispatch(message());
  const query=transport.query.bind(transport),events=transport.events.bind(transport);
  transport.query=async id=>({...(await query(id))!,runId:'wrong-run'});
  await assert.rejects(()=>coordinator.reconcile('run-1'),/identity mismatch/);assert.equal(coordinator.cursor(),0);
  transport.query=query;transport.events=async after=>(await events(after)).slice(1);
  await assert.rejects(()=>coordinator.reconcile('run-1'),/cursor/);assert.equal(coordinator.cursor(),0);
  transport.events=async after=>(await events(after)).map((e,i)=>i===0?{...e,attemptId:'wrong-attempt'}:e);
  await assert.rejects(()=>coordinator.reconcile('run-1'),/identity mismatch/);assert.equal(coordinator.cursor(),0);
  transport.events=events;await coordinator.reconcile('run-1');assert.equal(coordinator.cursor(),3);
  transport.events=async()=>runner.events(0).map((e,i)=>i===0?{...e,data:{changed:true}}:e);
  await assert.rejects(()=>coordinator.reconcile('run-1'),/content conflict/);assert.equal(coordinator.cursor(),3);
});
