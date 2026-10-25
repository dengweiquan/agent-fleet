import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync,realpathSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Engine} from '../src/engine.ts';
import {git,head} from '../src/git.ts';
import {runIndependentReview} from '../src/reviewer.ts';

async function setup(model='review-approve',timeoutMs=1000){
  const root=realpathSync(mkdtempSync(join(tmpdir(),'fleet-review-')));process.env.AGENT_FLEET_HOME=join(root,'index');
  git(root,'init');writeFileSync(join(root,'seed.txt'),'seed');git(root,'add','.');git(root,'-c','user.name=Fixture','-c','user.email=fixture@local','-c','commit.gpgsign=false','commit','-m','fixture');
  const engine=Engine.init(root,'Independent reviewer fixture'),cfg=engine.config()!;
  cfg.timeoutMs=timeoutMs;cfg.instances.find(instance=>instance.id==='fake-reviewer')!.model=model;engine.config(cfg);
  engine.plan({version:1,goal:'Review fixture',tasks:[{id:'task',title:'Fixture',prompt:'Write artifact',capabilities:['code'],dependencies:[],acceptance:[[process.execPath,'-e',"if(require('node:fs').readFileSync('artifact.txt','utf8')!=='AgentFleet fixture artifact\\n')process.exit(2)"]]}]});
  const worker=await engine.run('task','fake-worker');await engine.verify(worker.id);
  return {root,engine,worker:engine.store.getRun(worker.id)!,cleanup:()=>{engine.close();rmSync(root,{recursive:true,force:true});delete process.env.AGENT_FLEET_HOME;}};
}
test('independent reviewer executes at exact candidate and persists bound verdict without accepting worker',{timeout:15000},async()=>{
  const f=await setup();try{
    const review=await f.engine.exclusive(()=>runIndependentReview(f.engine,f.worker.id,'fake-reviewer'));
    assert.equal(review.status,'exited');assert.notEqual(review.id,f.worker.id);assert.notEqual(review.sessionId,f.worker.sessionId);assert.notEqual(review.worktree,f.worker.worktree);
    assert.equal(review.baseSha,f.worker.commitSha);assert.equal(head(review.worktree),f.worker.commitSha);assert.equal(f.engine.store.getRun(f.worker.id)?.status,'verified');
    assert.deepEqual(f.engine.store.getMeta(`reviewBinding:${review.id}`),{workerRunId:f.worker.id,commitSha:f.worker.commitSha,reviewerId:'fake-reviewer'});
    const evidence=f.engine.store.getMeta(`reviewEvidence:${f.worker.id}`) as any;assert.equal(evidence.reviewRunId,review.id);assert.equal(evidence.verdict,'approve');assert.equal(evidence.workerRunId,f.worker.id);assert.equal(evidence.commitSha,f.worker.commitSha);
    await assert.rejects(runIndependentReview(f.engine,f.worker.id,'fake-reviewer'),/already exists|Existing review/);
  }finally{f.cleanup();}
});
test('reject verdict is evidence, while malformed verdict never becomes acceptance',{timeout:20000},async()=>{
  for(const model of ['review-reject','review-invalid']){const f=await setup(model);try{const review=await runIndependentReview(f.engine,f.worker.id,'fake-reviewer');assert.equal(f.engine.store.getRun(f.worker.id)?.status,'verified');if(model==='review-reject'){assert.equal(review.status,'exited');assert.equal((f.engine.store.getMeta(`reviewEvidence:${f.worker.id}`) as any).verdict,'reject');}else{assert.equal(review.status,'rejected');assert.equal(f.engine.store.getMeta(`reviewEvidence:${f.worker.id}`),null);}await assert.rejects(runIndependentReview(f.engine,f.worker.id,'fake-reviewer'),/Existing review|already exists/);}finally{f.cleanup();}}
});
test('reviewer timeout remains unknown and cannot be replayed',{timeout:10000},async()=>{
  const f=await setup('hang',150);try{const review=await runIndependentReview(f.engine,f.worker.id,'fake-reviewer');assert.equal(review.status,'unknown');assert.equal(f.engine.store.getRun(f.worker.id)?.status,'verified');assert.equal(f.engine.store.getMeta(`reviewEvidence:${f.worker.id}`),null);await assert.rejects(runIndependentReview(f.engine,f.worker.id,'fake-reviewer'),/Existing review/);}finally{f.cleanup();}
});
test('dirty worker and worker-as-reviewer are refused before creating review attempts',{timeout:10000},async()=>{
  const f=await setup();try{await assert.rejects(runIndependentReview(f.engine,f.worker.id,'fake-worker'),/reviewer/);writeFileSync(join(f.worker.worktree,'tampered.txt'),'changed');await assert.rejects(runIndependentReview(f.engine,f.worker.id,'fake-reviewer'),/candidate changed/);assert.equal(f.engine.store.listRuns().length,1);}finally{f.cleanup();}
});
test('reviewer credentials/live authorization and remote endpoint fail closed before launch',{timeout:10000},async()=>{
  const f=await setup();try{await assert.rejects(runIndependentReview(f.engine,f.worker.id,'claude-reviewer'),/explicit approval/);assert.equal(f.engine.store.listRuns().length,1);}finally{f.cleanup();}
});
