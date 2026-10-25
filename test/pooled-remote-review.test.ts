import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {PooledRemoteQueue,pooledProjectKey} from '../src/pooled-remote.ts';
import type {PooledBinding,PooledClaim,RemoteScope} from '../src/pooled-remote.ts';
import type {Capabilities} from '../src/remote.ts';

const caps:Capabilities={protocolVersion:1,provider:'external-agent',capabilities:['code'],quota:'unknown',auth:'available'};
const binding=(claim:PooledClaim):PooledBinding=>Object.fromEntries(['projectKey','sessionId','taskId','messageId','runId','attemptId','endpointId','resourceId','accountDomain','leaseId','fence','claimId'].map(key=>[key,claim[key as keyof PooledClaim]])) as unknown as PooledBinding;
function fixture(){
  const pool=new ResourcePool(':memory:');
  pool.upsertResource({id:'remote',provider:'external-agent',model:'offline-fixture',endpointId:'muse',endpointKind:'external-agent',accountDomain:'muse-account',capabilities:['code'],roles:['worker'],capacity:1});
  const projectId='/synthetic/reviewer-project',projectKey=pooledProjectKey(projectId),sessionId='session';
  pool.setLimits({global:1,projects:{[projectId]:1},endpoints:{muse:1},accounts:{'muse-account':1}});
  const queue=new PooledRemoteQueue(pool),scope:RemoteScope={endpointId:'muse',resourceIds:['remote'],projects:[{projectKey,sessionId}]};
  function offer(n:number,instructions='Offline protocol fixture only'){return queue.offer({projectKey,request:{id:`request-${n}`,projectId,sessionId,taskId:`task-${n}`,attemptId:`attempt-${n}`,role:'worker',capabilities:['code'],requiredProvider:'external-agent',eligibleResourceIds:['remote']},dispatch:{version:1,messageId:`message-${n}`,runId:`run-${n}`,taskId:`task-${n}`,attemptId:`attempt-${n}`,endpointId:'muse',fence:1,repositoryId:'fixture-repository',baseSha:'a'.repeat(40),provider:'external-agent',requiredCapabilities:['code'],instructions,dependencies:[]}});}
  return {pool,queue,scope,offer};
}

test('completed history cannot hide the next offered remote task',{timeout:60000},()=>{
  const s=fixture();try{
    // Exercise real state transitions, not fabricated terminal database rows.
    for(let n=0;n<1000;n++){
      s.offer(n);const claim=s.queue.claim(s.scope,{claimId:`claim-${n}`,capabilities:caps})!;assert(claim,`history claim ${n}`);
      s.queue.ack(s.scope,binding(claim));s.queue.report(s.scope,binding(claim),{sourceEventId:`result-${n}`,kind:'result',data:{exitCode:0}});
    }
    s.offer(1000);
    assert.equal(s.queue.pull(s.scope,caps).length,1,'terminal rows must not consume the active queue window');
    const next=s.queue.claim(s.scope,{claimId:'claim-next',capabilities:caps});assert.equal(next?.taskId,'task-1000');
    assert.equal(s.pool.status().leases.filter(lease=>lease.state==='held').length,1);
    const firstPage=s.queue.list(s.scope,0,1000),lastPage=s.queue.list(s.scope,firstPage.at(-1)!.rowCursor!,1000);
    assert.equal(firstPage.length,1000);assert.equal(lastPage.length,1);assert.equal(lastPage[0].taskId,'task-1000');
  }finally{s.pool.close();}
});

test('scope fixes project/session pairs and endpoint resources rather than accepting cross-products',()=>{
  const s=fixture();try{
    s.offer(1);
    const wrongPair={...s.scope,projects:[{projectKey:s.scope.projects[0].projectKey,sessionId:'other-session'},{projectKey:pooledProjectKey('/other'),sessionId:'session'}]};
    assert.deepEqual(s.queue.pull(wrongPair,caps),[]);assert.equal(s.queue.claim(wrongPair,{claimId:'cross-pair',capabilities:caps}),null);
    assert.throws(()=>s.queue.claim({...s.scope,endpointId:'different-endpoint'},{claimId:'wrong-endpoint',capabilities:caps}));
    assert.equal(s.queue.claim(s.scope,{claimId:'false-provider',capabilities:{...caps,provider:'codex'}}),null);
    assert.equal(s.pool.status().leases.length,0);
    const claim=s.queue.claim(s.scope,{claimId:'right',capabilities:caps})!;assert(claim);
    assert.throws(()=>s.queue.report(s.scope,binding(claim),{sourceEventId:'pre-ack',kind:'result',data:{exitCode:0}}),/ACK/);
    assert.equal(s.pool.status().leases[0].state,'held');
  }finally{s.pool.close();}
});

test('pause and resume must not recreate a missing shared capacity authority',()=>{
  const root=mkdtempSync(join(tmpdir(),'fleet-pool-loss-review-')),prior=process.env.AGENT_FLEET_HOME;
  process.env.AGENT_FLEET_HOME=join(root,'history');let engine:Engine|undefined;
  try{
    git(root,'init');writeFileSync(join(root,'seed'),'fixture');git(root,'add','seed');git(root,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m','fixture');
    engine=Engine.init(root);const path=join(root,'authority.sqlite'),pool=new ResourcePool(path);pool.close();
    engine.store.setMeta('resourcePoolPath',path);rmSync(path);
    assert.throws(()=>engine!.pause(),/missing|unavailable|open|exist|authority/i);
    assert.equal(existsSync(path),false,'pause must not manufacture an empty authoritative database');
    assert.throws(()=>engine!.resume(),/missing|unavailable|open|exist|authority/i);
    assert.equal(existsSync(path),false,'resume must not manufacture an empty authoritative database');
  }finally{engine?.close();if(prior===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=prior;rmSync(root,{recursive:true,force:true});}
});

test('a dispatch too large for the connector reply cannot commit a remote claim',()=>{
  const s=fixture();try{
    try{s.offer(2,'x'.repeat(65000));}catch(error){assert.match(String(error),/capacity|size|bound|exceed|large/i);assert.equal(s.pool.status().leases.length,0);return;}
    assert.throws(()=>s.queue.claim(s.scope,{claimId:'oversized-reply',capabilities:caps}),/capacity|size|bound|exceed|large/i);
    assert.equal(s.pool.status().leases.length,0,'oversized claim response must roll back the capacity grant');
  }finally{s.pool.close();}
});
