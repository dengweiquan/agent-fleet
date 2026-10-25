import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Store } from '../src/store.ts';
import { defaultConfig } from '../src/plan.ts';
import type { Run } from '../src/types.ts';

function run(id: string): Run { return { id, taskId:'task', instanceId:'worker', provider:'fake', sessionId:`session-${id}`, baseSha:'a'.repeat(40), worktree:'/tmp/fixture-worktree', status:'prepared', pid:null, processStart:null, exitCode:null, commitSha:null, reviewBy:null, createdAt:new Date().toISOString() }; }
test('run state and event are atomic; publication sees committed data; sequence spans handles and restarts', () => {
  const dir = mkdtempSync(join(tmpdir(), 'agent-fleet-store-'));
  try {
    const path = join(dir, 'state.sqlite'), store = new Store(path), reader = new Store(path, {readOnly:true});
    const seen: number[] = [];
    store.subscribe(event => { assert.equal(reader.events(event.event_id - 1)[0].event_id, event.event_id); seen.push(event.event_id); });
    store.createRun(run('one'));
    store.updateRun('one', {status:'running', pid:123, processStart:'start-marker'});
    const before = store.events();
    assert.throws(() => store.updateRun('one', {status:'exited', exitCode:0}, 'invalid kind'));
    assert.equal(store.getRun('one')?.status, 'running');
    assert.deepEqual(store.events(), before);
    store.updateRun('one', {status:'exited', exitCode:0});
    assert.equal(store.getRun('one')?.status, 'exited');
    assert.equal(store.getRun('one')?.commitSha, null);
    const second = new Store(path);
    second.event('one', 'observation', {});
    store.event('one', 'observation', {});
    assert.deepEqual(store.events().map(e => e.event_id), [1,2,3,4,5]);
    assert.deepEqual(store.events().map(e => e.run_seq), [1,2,3,4,5]);
    assert.deepEqual(seen, [1,2,3,5]);
    second.close(); store.close(); reader.close();
    const reopened = new Store(path);
    assert.equal(reopened.getRun('one')?.status, 'exited');
    assert.equal(reopened.event('one', 'after.restart', {}).run_seq, 6);
    reopened.close();
  } finally { rmSync(dir, {recursive:true, force:true}); }
});
test('read-only mode cannot create a database or mutate existing state', () => {
  const dir = mkdtempSync(join(tmpdir(), 'agent-fleet-observer-'));
  try {
    const missing = join(dir, 'missing.sqlite');
    assert.throws(() => new Store(missing, {readOnly:true})); assert.equal(existsSync(missing), false);
    const db = join(dir, 'db.sqlite'); const owner = new Store(db); owner.setMeta('paused', true); owner.close();
    const reader = new Store(db, {readOnly:true});
    assert.equal(reader.getMeta('paused'), true);
    assert.throws(() => reader.setMeta('paused', false), /read-only/);
    assert.throws(() => reader.event(null, 'test', {}), /read-only/);
    reader.close();
  } finally { rmSync(dir, {recursive:true, force:true}); }
});
test('immutable run identity, validated config, bounded pagination, credential filtering and subscriber isolation', () => {
  const store = new Store(':memory:');
  store.setConfig(defaultConfig()); assert.equal(store.getConfig()?.instances.length, 5);
  assert.throws(() => store.setMeta('config', {}));
  store.createRun(run('r'));
  assert.throws(() => store.updateRun('r', {sessionId:'replacement'}), /immutable/);
  assert.throws(() => store.createRun({...run('new'), status:'accepted'}));
  assert.throws(() => store.event('missing', 'output', {}));
  assert.throws(() => store.events(0, 1001));
  store.subscribe(() => { throw new Error('subscriber failed'); });
  const e = store.event('r', 'output', {token:'secret', text:'Bearer abc sk-abcdefghijk password=hello'});
  assert.deepEqual(e.data, {token:'[REDACTED]', text:'Bearer [REDACTED] [REDACTED] password=[REDACTED]'});
  assert.equal(store.events().at(-1)?.event_id, e.event_id);
  store.close();
});
