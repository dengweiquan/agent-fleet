import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { ResourcePool } from '../src/resource-pool.ts';
import type { Resource, PoolRequest, PoolLimits } from '../src/resource-pool.ts';

const resource=(id='worker',patch:Partial<Resource>={}):Resource => ({id,provider:'fake',model:'write',endpointId:'local',endpointKind:'local',accountDomain:'fake-account',capabilities:['code'],roles:['worker'],capacity:8,...patch});
const request=(id:string,projectId='project-a',patch:Partial<PoolRequest>={}):PoolRequest => ({id,projectId,sessionId:`session-${projectId}`,taskId:id,attemptId:`attempt-${id}`,role:'worker',capabilities:['code'],...patch});
const limits=(patch:Partial<PoolLimits>={}):PoolLimits => ({global:8,projects:{'project-a':8,'project-b':8},endpoints:{local:8},accounts:{'fake-account':8},...patch});
const setup=() => {const dir=mkdtempSync(join(tmpdir(),'agent-fleet-pool-'));return {dir,path:join(dir,'pool.sqlite')};};

test('all capacity dimensions are enforced independently and release frees capacity', () => {
  for (const dimension of ['global','project','endpoint','account','resource']) {
    const pool=new ResourcePool(':memory:');
    pool.upsertResource(resource('worker',{capacity:dimension === 'resource' ? 1 : 8}));
    pool.setLimits(limits(dimension === 'global' ? {global:1} : dimension === 'project' ? {projects:{'project-a':1}} : dimension === 'endpoint' ? {endpoints:{local:1}} : dimension === 'account' ? {accounts:{'fake-account':1}} : {}));
    pool.enqueue(request('a')); pool.enqueue(request('b'));
    const first=pool.claimNext()!; assert.ok(first,dimension); assert.equal(pool.claimNext(),null,dimension);
    pool.release(first.id,first.fence,'completed'); assert.equal(pool.claimNext()?.request.id,'b',dimension); pool.close();
  }
});
test('unspecified project, endpoint and account capacities default to one across resources', () => {
  for (const dimension of ['project','endpoint','account']) {
    const pool=new ResourcePool(':memory:');
    pool.upsertResource(resource('one')); pool.upsertResource(resource('two'));
    pool.setLimits(limits(dimension === 'project' ? {projects:{}} : dimension === 'endpoint' ? {endpoints:{}} : {accounts:{}}));
    pool.enqueue(request('a')); pool.enqueue(request('b'));
    assert.ok(pool.claimNext()); assert.equal(pool.claimNext(),null); pool.close();
  }
});
test('persistent round-robin and per-project FIFO cover all participating coordinator scopes', () => {
  const {dir,path}=setup();
  try {
    let pool=new ResourcePool(path); pool.upsertResource(resource()); pool.setLimits(limits({global:1}));
    for (const [id,project] of [['a1','project-a'],['a2','project-a'],['b1','project-b'],['b2','project-b']]) pool.enqueue(request(id,project));
    const owned=['a1','a2','b1','b2'];
    const first=pool.claimNext(owned)!; assert.equal(first.request.id,'a1'); pool.release(first.id,first.fence,'completed'); pool.close();
    pool=new ResourcePool(path);
    const sequence=[];
    for (let i=0;i<3;i++) {const lease=pool.claimNext(owned)!;sequence.push(lease.request.id);pool.release(lease.id,lease.fence,'completed');}
    assert.deepEqual(sequence,['b1','a2','b2']);
    assert.deepEqual(pool.events().map(e => e.event_id),Array.from({length:pool.events().length},(_,i)=>i+1)); pool.close();
  } finally {rmSync(dir,{recursive:true,force:true});}
});
test('unowned head cannot block active owner or be stolen, while capacity stays global', () => {
  const pool=new ResourcePool(':memory:');pool.upsertResource(resource());pool.setLimits(limits({global:1}));
  pool.enqueue(request('paused-head','project-a'));pool.enqueue(request('active','project-b'));
  assert.equal(pool.claimNext([]),null);
  const lease=pool.claimNext(['active'])!;assert.equal(lease.request.id,'active');
  assert.equal(pool.status().requests.find(q=>q.request.id==='paused-head')?.state,'queued');
  assert.equal(pool.claimNext(['paused-head']),null);
  pool.release(lease.id,lease.fence,'completed');
  assert.equal(pool.claimNext(['active']),null);
  assert.equal(pool.claimNext(['paused-head'])?.request.id,'paused-head');pool.close();
});
test('local endpoint/account aliases cannot bypass host or real-provider capacity domains', () => {
  const pool=new ResourcePool(':memory:');
  pool.upsertResource(resource('real-a',{provider:'codex',accountDomain:'codex-auth'}));
  pool.upsertResource(resource('real-b',{provider:'codex',accountDomain:'codex-auth'}));
  assert.throws(()=>pool.upsertResource(resource('host-alias',{endpointId:'local-alias'})),/share one endpoint/);
  assert.throws(()=>pool.upsertResource(resource('auth-alias',{provider:'codex',accountDomain:'another-label'})),/share one account/);
  assert.throws(()=>pool.upsertResource(resource('real-b',{provider:'codex',accountDomain:'changed-label'})),/share one account/);
  pool.upsertResource(resource('claude',{provider:'claude',accountDomain:'claude-auth'}));
  pool.upsertResource(resource('fake-a',{accountDomain:'synthetic-a'}));
  pool.upsertResource(resource('fake-b',{accountDomain:'synthetic-b'}));
  pool.upsertResource(resource('remote-a',{provider:'codex',endpointKind:'ssh-stdio',endpointId:'remote-a',accountDomain:'remote-auth'}));
  pool.upsertResource(resource('remote-b',{provider:'codex',endpointKind:'ssh-stdio',endpointId:'remote-b',accountDomain:'other-remote-auth'}));
  assert.equal(pool.status().resources.length,7);pool.close();
});
test('incapable head does not block capable project; role, capabilities, required provider and resource allowlists are hard constraints', () => {
  const pool=new ResourcePool(':memory:');
  pool.upsertResource(resource()); pool.upsertResource(resource('reviewer',{roles:['reviewer'],capabilities:['review'],model:'review'}));
  pool.setLimits(limits());
  pool.enqueue(request('impossible','project-a',{requiredProvider:'external-agent'}));
  pool.enqueue(request('excluded','project-a',{excludedResourceIds:['worker']}));
  pool.enqueue(request('empty-list','project-a',{eligibleResourceIds:[]}));
  pool.enqueue(request('unsupported','project-a',{capabilities:['gpu']}));
  pool.enqueue(request('review','project-b',{role:'reviewer',capabilities:['review'],eligibleResourceIds:['reviewer'],preferredProvider:'claude'}));
  const lease=pool.claimNext()!;assert.equal(lease.request.id,'review');assert.equal(lease.resource.id,'reviewer');assert.equal(pool.claimNext(),null);pool.close();
});
test('request ids are payload-idempotent, attempt identities are unique and active definitions cannot drift', () => {
  const pool=new ResourcePool(':memory:');const registered=pool.upsertResource(resource());pool.setLimits(limits());
  const q=request('a');assert.deepEqual(pool.enqueue(q),pool.enqueue(q));
  assert.throws(()=>pool.enqueue({...q,role:'reviewer'}),/different payload/);
  assert.throws(()=>pool.enqueue({...q,id:'new-id'}),/UNIQUE/);
  assert.deepEqual(pool.upsertResource(resource()),registered);pool.setLimits(limits());
  assert.throws(()=>pool.upsertResource(resource('new')),/pending or active/);
  assert.throws(()=>pool.upsertResource(resource('worker',{model:'other'})),/pending or active/);
  assert.throws(()=>pool.setLimits(limits({global:9})),/pending or active/);
  const lease=pool.claimNext()!;pool.release(lease.id,lease.fence,'completed');
  pool.upsertResource(resource('new'));pool.setLimits(limits({global:9}));pool.close();
});
test('unknown survives restart and holds capacity until explicitly fenced, bound reconciliation', () => {
  const {dir,path}=setup();
  try {
    let pool=new ResourcePool(path);pool.upsertResource(resource());pool.setLimits(limits({global:1}));pool.enqueue(request('a'));pool.enqueue(request('b'));
    const lease=pool.claimNext()!;
    assert.throws(()=>pool.bindRun(lease.id,lease.fence+1,'run-a'),/fence/);
    pool.bindRun(lease.id,lease.fence,'run-a');pool.bindRun(lease.id,lease.fence,'run-a');
    assert.throws(()=>pool.bindRun(lease.id,lease.fence,'run-b'),/immutable/);
    pool.release(lease.id,lease.fence,'unknown');pool.close();pool=new ResourcePool(path);
    assert.equal(pool.status().leases[0].state,'unknown');assert.equal(pool.claimNext(),null);
    assert.throws(()=>pool.release(lease.id,lease.fence,'completed'),/explicit reconciliation/);
    assert.throws(()=>pool.reconcile(lease.id,lease.fence,'failed',{executionEnded:true,verifiedBy:'operator',reason:'verified child completion record',runId:'wrong-run'}),/exactly/);
    assert.throws(()=>pool.reconcile(lease.id,lease.fence,'failed',{executionEnded:false,verifiedBy:'operator',reason:'old heartbeat',runId:'run-a'} as never),/execution end/);
    pool.reconcile(lease.id,lease.fence,'failed',{executionEnded:true,verifiedBy:'operator',reason:'verified child completion record',runId:'run-a'});
    assert.throws(()=>pool.release(lease.id,lease.fence,'failed'));
    const next=pool.claimNext()!;assert.equal(next.request.id,'b');assert.ok(next.fence>lease.fence);pool.close();
  } finally {rmSync(dir,{recursive:true,force:true});}
});
test('observers cannot create or mutate; input JSON bounds and cost honesty are enforced', () => {
  const {dir,path}=setup();
  try {
    assert.throws(()=>new ResourcePool(path,{readOnly:true}));assert.equal(existsSync(path),false);
    const owner=new ResourcePool(path);assert.equal(owner.upsertResource(resource()).costKnown,false);owner.close();
    const reader=new ResourcePool(path,{readOnly:true});assert.equal(reader.status().resources.length,1);
    assert.throws(()=>reader.enqueue(request('x')),/read-only/);assert.throws(()=>reader.claimNext(),/read-only/);reader.close();
    const pool=new ResourcePool(path);
    assert.throws(()=>pool.upsertResource(resource('known',{costKnown:true})),/rates/);
    assert.throws(()=>pool.upsertResource(resource('transport-drift',{endpointKind:'ssh-stdio'})),/conflicting transport/);
    assert.throws(()=>pool.upsertResource(resource('unknown-provider',{provider:'muse'} as never)),/unsupported provider/);
    assert.throws(()=>pool.enqueue(request('x',{length:1} as never)));
    assert.throws(()=>pool.enqueue(request('x','a',{capabilities:Array.from({length:65},(_,i)=>`c${i}`)})));
    assert.throws(()=>pool.setLimits(limits({global:0})));
    assert.throws(()=>pool.events(0,1001));pool.close();
  } finally {rmSync(dir,{recursive:true,force:true});}
});
test('separate processes cannot over-claim the shared global capacity', async () => {
  const {dir,path}=setup();
  try {
    const pool=new ResourcePool(path);pool.upsertResource(resource());pool.setLimits(limits({global:1}));
    for(let i=0;i<4;i++)pool.enqueue(request(`task-${i}`));pool.close();
    const moduleUrl=new URL('../src/resource-pool.ts',import.meta.url).href;
    const outputs=await Promise.all(Array.from({length:4},()=>new Promise<string>((resolve,reject)=>{
      const child=spawn(process.execPath,['--experimental-strip-types','--input-type=module','-e',`import {ResourcePool} from ${JSON.stringify(moduleUrl)};const p=new ResourcePool(${JSON.stringify(path)});console.log(JSON.stringify(p.claimNext()));p.close();`],{stdio:['ignore','pipe','pipe']});
      let output='',error='';child.stdout.on('data',c=>output+=c);child.stderr.on('data',c=>error+=c);child.on('error',reject);child.on('close',code=>code===0?resolve(output):reject(new Error(error)));
    })));
    assert.equal(outputs.map(v=>JSON.parse(v)).filter(Boolean).length,1);
    const observer=new ResourcePool(path,{readOnly:true});assert.equal(observer.status().leases.filter(l=>l.state==='held').length,1);observer.close();
  } finally {rmSync(dir,{recursive:true,force:true});}
});
