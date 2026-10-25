import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {existsSync,mkdtempSync,mkdirSync,readFileSync,rmSync,writeFileSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';

const cli=fileURLToPath(new URL('../src/cli.ts',import.meta.url));
const wait=(ms:number)=>new Promise(resolve=>setTimeout(resolve,ms));
function fixture(extraEnv:Record<string,string>={}) {
  const temp=realpathSync(mkdtempSync(join(tmpdir(),'fleet-cli-'))),root=join(temp,'project');mkdirSync(root);
  const env:NodeJS.ProcessEnv={...process.env,AGENT_FLEET_HOME:join(temp,'index'),...extraEnv};
  const git=(...args:string[])=>{const r=spawnSync('git',args,{cwd:root,env,encoding:'utf8',timeout:5000});assert.equal(r.status,0,r.stderr);return r.stdout.trim();};
  git('init');writeFileSync(join(root,'seed.txt'),'fixture\n');git('add','.');git('-c','user.name=Fixture','-c','user.email=fixture@local','-c','commit.gpgsign=false','commit','-m','fixture');
  function raw(...args:string[]){return spawnSync(process.execPath,['--experimental-strip-types',cli,'--json',...args],{cwd:root,env,encoding:'utf8',timeout:5000,maxBuffer:2*1024*1024});}
  function invoke(...args:string[]){const r=raw(...args);assert.equal(r.status,0,`${args.join(' ')}\n${r.stderr}`);const result=JSON.parse(r.stdout);assert.equal(result.ok,true);return result.data;}
  function jsonFile(name:string,data:unknown){const path=join(temp,name);writeFileSync(path,JSON.stringify(data));return path;}
  function configure(session:string,model='write',timeoutMs=3000){return invoke('--session',session,'config',jsonFile('config.json',{version:1,timeoutMs,maxOutputBytes:65536,instances:[{id:'manager',provider:'fake',role:'manager',model:'success',capabilities:['planning','conversation']},{id:'worker',provider:'fake',role:'worker',model,capabilities:['code']},{id:'reviewer',provider:'fake',role:'reviewer',model:'review',capabilities:['review']}]}));}
  function plan(session:string){return invoke('--session',session,'plan',jsonFile('plan.json',{version:1,goal:'CLI isolation fixture',tasks:[{id:'task-a',title:'Write fixture',prompt:'Write the fixture artifact',dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e',"if(require('node:fs').readFileSync('artifact.txt','utf8')!=='AgentFleet fixture artifact\\n')process.exit(3)"]]}]}));}
  const dir=(id:string)=>join(root,'.agent-fleet','sessions',id);
  async function settled(id:string,predicate:(runs:any[])=>boolean,timeoutMs=7000){const deadline=Date.now()+timeoutMs;let state:any;do{state=invoke('--session',id,'status');if(predicate(state.runs))return state;await wait(60);}while(Date.now()<deadline);assert.fail(`Timed out waiting for session: ${JSON.stringify(state)}`);}
  async function unlocked(id:string){const deadline=Date.now()+5000;while(existsSync(join(dir(id),'coordinator.lock'))){if(Date.now()>deadline)assert.fail('Coordinator lock did not release');await wait(40);}}
  return {temp,root,env,raw,invoke,jsonFile,configure,plan,dir,settled,unlocked,cleanup:()=>rmSync(temp,{recursive:true,force:true})};
}

test('CLI detached run, history preview, independent review and session isolation survive frontend exits',{timeout:20000},async()=>{
  const f=fixture();try{
    const initial=f.invoke('init','First history session'),session=initial.sessionId;f.configure(session);
    const chat=f.invoke('--session',session,'chat','Propose a small roadmap');assert.equal(chat.launched,true);assert.equal(typeof chat.coordinatorPid,'number');
    await f.settled(session,runs=>runs.some(r=>r.taskId==='conversation'&&r.status==='exited'));await f.unlocked(session);
    f.plan(session);
    // invoke uses spawnSync: the launching frontend is already gone when this returns.
    const detached=f.invoke('--session',session,'run','task-a','worker');assert.equal(detached.launched,true);
    const completed=await f.settled(session,runs=>runs.some(r=>r.taskId==='task-a'&&r.status==='exited'));await f.unlocked(session);
    const run=completed.runs.find((r:any)=>r.taskId==='task-a');assert.equal(run.commitSha,null);assert.equal(existsSync(join(run.worktree,'artifact.txt')),true);
    const events=f.invoke('--session',session,'events');const sessions=events.filter((e:any)=>e.kind==='provider.session');
    assert.equal(sessions.length,2);assert.equal(new Set(sessions.map((e:any)=>e.data.sessionId)).size,2);assert.equal(sessions.find((e:any)=>e.run_id===run.id).data.sessionId,run.sessionId);
    const verified=f.invoke('--session',session,'verify',run.id);assert.equal(verified.status,'verified');
    const evidence=f.jsonFile('review.json',{runId:run.id,commitSha:verified.commitSha,reviewerId:'reviewer',verdict:'approve',notes:'Fixture reviewer verified exact candidate and passing command evidence.'});
    assert.equal(f.invoke('--session',session,'review',run.id,'reviewer',evidence).status,'accepted');
    const history=f.invoke('sessions','list');assert.equal(history[0].title,'First history session');assert.equal(history[0].root,f.root);assert.equal(history[0].summary.manager,'manager');assert.equal(history[0].summary.taskStates.accepted,1);
    const second=f.invoke('init','Second independent session');assert.notEqual(second.sessionId,session);f.configure(second.sessionId);assert.equal(f.invoke('status').sessionId,second.sessionId);
    const before=f.invoke('--session',session,'events');
    const preview=f.invoke('resume',session);assert.equal(preview.sessionId,session);assert.equal(preview.runs.find((r:any)=>r.id===run.id).status,'accepted');
    assert.equal(f.invoke('status').sessionId,second.sessionId,'Non-TTY preview must not change current session');
    assert.deepEqual(f.invoke('--session',session,'events'),before,'Preview must not append events or replay');
    assert.equal(f.invoke('--session',second.sessionId,'status').runs.length,0);assert.equal(f.invoke('sessions','list').length,2);
    assert.equal(f.invoke('resume').length,2,'Non-TTY resume without ID lists history');
    for(const command of [['run','task-a'],['stop',run.id],['retry',run.id],['pause'],['continue'],['config',evidence],['plan',evidence],['propose','hello'],['schedule'],['review-run',run.id,'reviewer'],['accept-review',run.id,'some-review']]){
      const denied=f.raw('--observe','--session',session,...command);assert.equal(denied.status,1,command.join(' '));assert.match(denied.stderr,/Observer/);
    }
    assert.equal(f.invoke('--observe','--session',session,'status').runs.find((r:any)=>r.id===run.id).status,'accepted');
    assert.deepEqual(f.invoke('--session',session,'events'),before,'Denied observer actions must not write state');
  }finally{f.cleanup();}
});

test('CLI fake hang timeout remains unknown and explicit retry does not replay',{timeout:15000},async()=>{
  const f=fixture();try{const session=f.invoke('init','Hang timeout').sessionId;f.configure(session,'hang',150);f.plan(session);
    f.invoke('--session',session,'run','task-a','worker');const state=await f.settled(session,runs=>runs.length===1&&runs[0].status==='unknown');await f.unlocked(session);
    const run=state.runs[0];assert.equal(f.invoke('resume',session).runs[0].status,'unknown');
    f.invoke('--session',session,'retry',run.id);const retryDeadline=Date.now()+5000;
    while(!f.invoke('--session',session,'events').some((e:any)=>e.kind==='command.error'&&/unknown/.test(e.data.message))){if(Date.now()>retryDeadline)assert.fail('Retry did not report its rejection');await wait(60);}await f.unlocked(session);
    const after=f.invoke('--session',session,'status');assert.equal(after.runs.length,1);assert.equal(after.runs[0].id,run.id);assert.equal(after.runs[0].status,'unknown');
    assert.ok(f.invoke('--session',session,'events').some((e:any)=>e.kind==='command.error'&&/unknown/.test(e.data.message)));
  }finally{f.cleanup();}
});

test('CLI coordinator SIGKILL retains lock; recovery marks unknown without re-execution',{timeout:18000},async()=>{
  const f=fixture();let childDeadline=0;try{
    // This test-only hook makes the orphan fake hang child exit by itself. It never
    // modifies product code, official CLIs, parent process environment or other PIDs.
    const hook=join(f.temp,'finite-fake.mjs');writeFileSync(hook,"if(process.argv[1]?.endsWith('/test/fake-cli.ts')&&process.argv[2]==='hang')setTimeout(()=>process.exit(0),2200);\n");
    f.env.NODE_OPTIONS=`--import=${pathToFileURL(hook).href}`;
    const session=f.invoke('init','Crash recovery').sessionId;f.configure(session,'hang',5000);f.plan(session);
    const launch=f.invoke('--session',session,'run','task-a','worker');childDeadline=Date.now()+2700;
    const running=await f.settled(session,runs=>runs.length===1&&runs[0].status==='running');const run=running.runs[0];childDeadline=Date.now()+2700;
    const lock=join(f.dir(session),'coordinator.lock'),owner=JSON.parse(readFileSync(lock,'utf8'));
    assert.equal(owner.pid,launch.coordinatorPid);assert.equal(owner.sessionId,session);assert.ok(owner.start===null||typeof owner.start==='string','Unavailable process-start inspection must remain null');
    process.kill(launch.coordinatorPid,'SIGKILL'); // Only the PID returned by this test's detached launch.
    await wait(80);const resumed=f.raw('--session',session,'continue');assert.equal(resumed.status,1);assert.match(resumed.stderr,/lock retained/);
    assert.equal(existsSync(lock),true);assert.equal(f.invoke('resume',session).runs[0].status,'unknown');
    const after=f.invoke('--session',session,'status');assert.equal(after.runs.length,1);assert.equal(after.runs[0].id,run.id);
    const events=f.invoke('--session',session,'events');assert.ok(events.some((e:any)=>e.kind==='recovery.inspected'&&e.data.replay===false));assert.ok(events.some((e:any)=>e.kind==='recovery.unknown'));
    assert.equal(events.filter((e:any)=>e.kind==='process.started').length,1);assert.equal(JSON.parse(readFileSync(lock,'utf8')).pid,launch.coordinatorPid);
  }finally{if(childDeadline>Date.now())await wait(childDeadline-Date.now());f.cleanup();}
});

test('CLI manager plan proposal and detached concurrent reviewer pipeline complete without frontend lifetime',{timeout:20000},async()=>{
  const f=fixture();try{
    const session=f.invoke('init','Concurrent planning').sessionId;
    f.invoke('config',f.jsonFile('parallel-config.json',{version:1,maxWorkers:2,maxReviews:1,timeoutMs:5000,maxOutputBytes:1024*1024,instances:[{id:'manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},...[1,2].map(n=>({id:'worker-'+n,provider:'fake',role:'worker',model:'delay-write',capabilities:['code']})),{id:'reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']}]}));
    f.invoke('propose','Prepare two independent files then join accepted results');
    const proposed=await f.settled(session,runs=>runs.some(r=>r.instanceId==='manager'&&r.status==='exited'));await f.unlocked(session);
    const manager=proposed.runs[0];assert.equal(f.invoke('plan'),null);assert.equal(f.invoke('proposal',manager.id).plan.tasks.length,3);f.invoke('plan','accept',manager.id);
    const launch=f.invoke('schedule','reviewer');assert.equal(launch.launched,true);
    const completed=await f.settled(session,runs=>runs.filter(r=>r.status==='accepted').length===3,12000);await f.unlocked(session);
    assert.equal(completed.runs.filter((r:any)=>r.instanceId==='reviewer').length,3);
    const before=f.invoke('events'),count=completed.runs.length;assert.equal(f.invoke('resume',session).runs.length,count);assert.deepEqual(f.invoke('events'),before);
    f.invoke('schedule','reviewer');const deadline=Date.now()+4000;
    while(f.invoke('events').filter((ev:any)=>ev.kind==='schedule.finished').length<2){if(Date.now()>deadline)assert.fail('Second schedule did not settle');await wait(40);}await f.unlocked(session);
    assert.equal(f.invoke('status').runs.length,count,'Finished pipeline is never replayed');
  }finally{f.cleanup();}
});
