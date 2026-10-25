import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {listSessions} from '../src/sessions.ts';

function setup(){const dir=mkdtempSync(join(tmpdir(),'fleet-engine-'));process.env.AGENT_FLEET_HOME=join(dir,'index');git(dir,'init');writeFileSync(join(dir,'seed.txt'),'seed');git(dir,'add','.');git(dir,'-c','user.name=Fixture','-c','user.email=fixture@local','-c','commit.gpgsign=false','commit','-m','fixture');const e=Engine.init(dir,'test session');return {dir,e};}
const acceptance=[[process.execPath,'-e',"if(require('node:fs').readFileSync('artifact.txt','utf8')!=='AgentFleet fixture artifact\\n')process.exit(2)"]];
const task=(id:string,dependencies:string[]=[])=>({id,title:id,prompt:'Write fixture artifact',dependencies,capabilities:['code'],acceptance});
test('vertical flow: accepted commits alone unblock dependencies, with independent verification and review',async()=>{
  const {dir,e}=setup();e.plan({version:1,goal:'Fixture DAG',tasks:[task('a'),task('b',['a'])]});
  await assert.rejects(e.run('b','fake-worker'),/accepted/);
  const a=await e.exclusive(()=>e.run('a','fake-worker'));assert.equal(a.status,'exited');assert.equal(a.commitSha,null);
  await assert.rejects(e.run('b','fake-worker'),/accepted/);
  const verified=await e.verify(a.id);assert.equal(verified?.status,'verified');
  assert.throws(()=>e.review(a.id,'fake-worker',{}),/reviewer/);
  assert.throws(()=>e.review(a.id,'fake-reviewer',{runId:a.id,commitSha:'bad'}),/evidence/);
  e.review(a.id,'fake-reviewer',{runId:a.id,commitSha:verified!.commitSha,reviewerId:'fake-reviewer',verdict:'approve',notes:'Independent fixture reviewer checked exact commit and passing acceptance.'});
  const b=await e.run('b','fake-worker');assert.equal(b.status,'exited');assert.notEqual(a.worktree,b.worktree);assert.notEqual(a.sessionId,b.sessionId);
  git(b.worktree,'merge-base','--is-ancestor',verified!.commitSha!,'HEAD');
  assert.equal(git(dir,'log','--format=%s','-1'),'fixture');
  const sessionId=e.sessionId;e.close();assert.equal(listSessions()[0].id,sessionId);
  const restored=new Engine(dir,true,sessionId);assert.equal(restored.store.getRun(a.id)?.status,'accepted');assert.throws(()=>restored.pause(),/Observer/);restored.close();
});
test('manager chat persists and still permits roadmap import; configuration snapshot then freezes',async()=>{
  const {e}=setup();const cfg=e.config()!;cfg.instances[0]={...cfg.instances[0],provider:'fake',model:'success'};e.config(cfg);
  const chat=await e.chat('Plan a small fixture');assert.equal(chat?.status,'exited');e.plan({version:1,goal:'after conversation',tasks:[task('a')]});assert.equal(e.chatHistory().length,2);e.close();
});
test('faults, pause and unknown do not replay; retry has a new run and session identity',async()=>{
  const {e}=setup();const cfg=e.config()!;cfg.instances.push({id:'broken',provider:'fake',role:'worker',model:'malformed',capabilities:['code']});e.config(cfg);e.plan({version:1,goal:'faults',tasks:[task('a')]});
  e.pause();await assert.rejects(e.run('a','broken'),/paused/);e.resume();
  const a=await e.run('a','broken');assert.equal(a.status,'failed');
  const retry=await e.run('a','fake-worker',undefined,a.id);assert.notEqual(retry.id,a.id);assert.notEqual(retry.sessionId,a.sessionId);
  e.store.updateRun(retry.id,{status:'running',pid:999999,processStart:'unproven'});const result=e.reconcile();assert.equal(result.length,1);assert.equal(e.store.getRun(retry.id)?.status,'unknown');await assert.rejects(e.run('a','fake-worker',undefined,retry.id),/unknown/);await assert.rejects(e.run('a','fake-worker',undefined,a.id),/latest/);e.close();
});
test('remote endpoint manager cannot silently execute on local machine',async()=>{
  const {e}=setup();const cfg=e.config()!;cfg.endpoints!.push({id:'muse',kind:'external-agent',target:'unverified'});cfg.instances[0].endpointId='muse';e.config(cfg);await assert.rejects(e.chat('hello'),/Remote execution/);assert.equal(e.store.listRuns().length,0);e.close();
});
test('timeout stays unknown, and stop is only a request until child completion',async()=>{
  const {e}=setup();const cfg=e.config()!;cfg.timeoutMs=100;cfg.instances.push({id:'hang',provider:'fake',role:'worker',model:'hang',capabilities:['code']});e.config(cfg);e.plan({version:1,goal:'hang',tasks:[task('a')]});
  const a=await e.run('a','hang');assert.equal(a.status,'unknown');await assert.rejects(e.run('a','hang',undefined,a.id),/unknown/);e.close();
});

test('stop request is not terminal until owned child completes',async()=>{
  const {e}=setup();const cfg=e.config()!;cfg.instances.push({id:'hang',provider:'fake',role:'worker',model:'hang',capabilities:['code']});e.config(cfg);e.plan({version:1,goal:'stop',tasks:[task('a')]});
  const pending=e.run('a','hang');let r=e.store.listRuns()[0];
  while(!e.controls.has(r.id)){await new Promise(resolve=>setTimeout(resolve,10));r=e.store.getRun(r.id)!;}
  e.stop(r.id);assert.equal(e.store.getRun(r.id)?.status,'running');
  const finished=await pending;assert.equal(finished.status,'stopped');e.close();
});
