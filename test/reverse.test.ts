import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {ReverseCoordinator,remotePing} from '../src/remote.ts';
import type {Dispatch} from '../src/remote.ts';
const caps={protocolVersion:1 as const,provider:'external-agent',capabilities:['fixture'],quota:'unknown' as const,auth:'available' as const};
const message=(overrides:Partial<Dispatch>={}):Dispatch=>({version:1,messageId:'msg-1',taskId:'task-1',runId:'run-1',attemptId:'attempt-1',endpointId:'muse-fixture',fence:1,repositoryId:'fixture',baseSha:'a'.repeat(40),provider:'external-agent',requiredCapabilities:['fixture'],instructions:'offline fixture',dependencies:[],...overrides});
test('Muse reverse pull/claim ACK loss reopens same claim; result and event cursor persist',t=>{
  const dir=mkdtempSync(join(tmpdir(),'fleet-reverse-')),file=join(dir,'queue.sqlite');let q=new ReverseCoordinator(file);t.after(()=>{q.close();rmSync(dir,{recursive:true,force:true});});
  q.offer(message());assert.equal(q.pull('muse-fixture',caps).length,1);
  const claim=q.claim('msg-1','muse-fixture','claim-1',caps);assert.equal(q.pull('muse-fixture',caps).length,0);
  const ack=q.ack('msg-1','claim-1');q.close();q=new ReverseCoordinator(file); // ACK response lost.
  assert.deepEqual(q.ack('msg-1','claim-1'),ack);assert.deepEqual(q.claim('msg-1','muse-fixture','claim-1',caps).dispatch,claim.dispatch);
  q.disconnected('msg-1');assert.equal(q.reconcile('msg-1').state,'unknown');assert.equal(q.pull('muse-fixture',caps).length,0);
  assert.throws(()=>q.claim('msg-1','muse-fixture','claim-new',caps),/Already claimed/);
  q.report('msg-1','claim-1','evt-1','heartbeat',{});assert.equal(q.reconcile('msg-1').state,'unknown');
  const result=q.report('msg-1','claim-1','evt-2','result',{exitCode:0,commitSha:'b'.repeat(40),providerSessionId:'own-session'});
  assert.equal(result.state,'execution-completed');assert.notEqual(result.state,'accepted');assert.deepEqual(q.report('msg-1','claim-1','evt-2','result',{exitCode:0,commitSha:'b'.repeat(40),providerSessionId:'own-session'}),result);
  const events=q.events('muse-fixture');assert.equal(events.length,6);assert.deepEqual(events.map(e=>e.runSeq),[1,2,3,4,5,6]);assert.equal(q.events('muse-fixture',events.at(-1)!.eventId).length,0);
  assert.equal(q.events('unrelated-endpoint').length,0);
});
test('reverse queue rejects identity conflicts, missing ACK, capability mismatch, and stale fence',t=>{
  const q=new ReverseCoordinator(':memory:');t.after(()=>q.close());q.offer(message());
  assert.equal(q.pull('muse-fixture',{...caps,provider:'invented-muse-cli'}).length,0);
  assert.throws(()=>q.claim('msg-1','wrong-endpoint','c1',caps),/Wrong/);
  q.claim('msg-1','muse-fixture','c1',caps);assert.throws(()=>q.report('msg-1','c1','e1','result',{exitCode:0}),/ACKed/);q.ack('msg-1','c1');
  q.offer(message({messageId:'msg-2',taskId:'task-2',runId:'run-2',attemptId:'attempt-2',fence:2}));
  assert.equal(q.report('msg-1','c1','e1','result',{exitCode:0}).state,'unknown');
  assert.throws(()=>q.report('msg-1','c1','e1','result',{exitCode:1}),/content conflict/);
  assert.throws(()=>q.offer(message({messageId:'msg-3',taskId:'task-3',runId:'run-3',attemptId:'attempt-3',fence:1})),/Stale/);
});
test('ping is side-effect-free and does not claim connectivity or invoke a provider',()=>{
  assert.deepEqual(remotePing(),{product:'AgentFleet',protocolVersion:1,direction:'runner-initiated-pull',transport:'ssh-stdio',diagnosticOnly:true,modelInvoked:false,quota:'unknown',ssh:'unknown',loopbackTunnel:'unknown',directPort:'unknown'});
});
test('reverse reports are bounded and credential patterns are redacted before persistence',t=>{
  const q=new ReverseCoordinator(':memory:');t.after(()=>q.close());q.offer(message());q.claim('msg-1','muse-fixture','c1',caps);q.ack('msg-1','c1');
  q.report('msg-1','c1','e1','progress',{summary:'Authorization: Bearer abcdef123456',token:'raw-secret-value',nested:{password:'hidden'}});
  const serialized=JSON.stringify(q.events('muse-fixture'));assert.ok(!serialized.includes('abcdef123456'));assert.ok(!serialized.includes('raw-secret-value'));assert.ok(!serialized.includes('hidden'));assert.ok(serialized.includes('[REDACTED]'));
  assert.throws(()=>q.report('msg-1','c1','e2','progress',{summary:'x'.repeat(17000)}),/size limit/);
});
test('read-only listing never creates a missing DB and rejects queue mutations',t=>{
  const dir=mkdtempSync(join(tmpdir(),'fleet-observer-')),file=join(dir,'queue.sqlite');t.after(()=>rmSync(dir,{recursive:true,force:true}));
  assert.throws(()=>new ReverseCoordinator(file,{readOnly:true}));
  const writer=new ReverseCoordinator(file);writer.offer(message());const observer=new ReverseCoordinator(file,{readOnly:true});
  t.after(()=>{observer.close();writer.close();});assert.equal(observer.list().length,1);assert.equal(observer.list('other').length,0);assert.equal(observer.list()[0].taskId,'task-1');
  assert.ok(!JSON.stringify(observer.list()).includes('offline fixture'));
  assert.throws(()=>observer.offer(message()),/Observer/);assert.throws(()=>observer.claim('msg-1','muse-fixture','c1',caps),/Observer/);
  assert.throws(()=>observer.ack('msg-1','c1'),/Observer/);assert.throws(()=>observer.disconnected('msg-1'),/Observer/);
});
test('same task cannot bypass claimed/unknown state with new run or endpoint',t=>{
  const q=new ReverseCoordinator(':memory:');t.after(()=>q.close());q.offer(message());q.claim('msg-1','muse-fixture','c1',caps);q.ack('msg-1','c1');q.disconnected('msg-1');
  assert.throws(()=>q.offer(message({messageId:'msg-retry',runId:'run-retry',attemptId:'attempt-retry',endpointId:'other-endpoint'})),/Task already/);
  assert.equal(q.list()[0].state,'unknown');assert.equal(q.list().length,1);
});
