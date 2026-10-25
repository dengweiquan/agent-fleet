import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,statSync,rmSync,symlinkSync,renameSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {createSession,listSessions,findSession,selectSession,touchSession,setCurrentSession} from '../src/sessions.ts';

function setup(){const temp=mkdtempSync(join(tmpdir(),'agent-fleet-sessions-'));process.env.AGENT_FLEET_HOME=join(temp,'index');const root=join(temp,'project');mkdirSync(root);return {temp,root,cleanup:()=>{rmSync(temp,{recursive:true,force:true});delete process.env.AGENT_FLEET_HOME;}};}
test('multiple sessions persist metadata, select and activity without provider credentials',()=>{
  const f=setup();try{
    const first=createSession(f.root,'First task'),second=createSession(f.root,'Second task');
    assert.equal(selectSession(f.root).id,second.id);assert.equal(selectSession(f.root,first.id).id,first.id);
    writeFileSync(join(first.dir,'state.sqlite'),'fixture');
    touchSession(first.id,{manager:'claude-manager',taskStates:{accepted:1,unknown:1}});
    assert.equal(findSession(first.id).availability,'available');assert.deepEqual(findSession(first.id).summary?.taskStates,{accepted:1,unknown:1});
    setCurrentSession(first);assert.equal(selectSession(f.root).id,first.id);
    assert.equal(listSessions().length,2);assert.equal(statSync(join(f.temp,'index','sessions.json')).mode&0o777,0o600);
    assert.equal(statSync(first.dir).mode&0o777,0o700);
    const restarted=spawnSync(process.execPath,['--experimental-strip-types','--input-type=module','-e',`import {listSessions} from ${JSON.stringify(new URL('../src/sessions.ts',import.meta.url).href)};console.log(JSON.stringify(listSessions()))`],{env:process.env,encoding:'utf8'});
    assert.equal(restarted.status,0);assert.equal(JSON.parse(restarted.stdout).length,2);
  }finally{f.cleanup();}
});
test('missing project is retained as unavailable, not removed from history',()=>{const f=setup();try{const r=createSession(f.root,'Move test');renameSync(f.root,f.root+'-moved');assert.equal(findSession(r.id).availability,'unavailable');assert.equal(listSessions().length,1);}finally{f.cleanup();}});
test('wrong project, traversal id and tampered paths cannot select a session',()=>{const f=setup();try{const r=createSession(f.root,'Identity');const other=join(f.temp,'other');mkdirSync(other);assert.throws(()=>selectSession(other,r.id),/another project/);assert.throws(()=>findSession('../project'),/Invalid session id/);const index=join(f.temp,'index','sessions.json');const raw=JSON.parse(readFileSync(index,'utf8'));raw.sessions[0].dir=f.temp;writeFileSync(index,JSON.stringify(raw));assert.throws(()=>listSessions(),/path mismatch/);}finally{f.cleanup();}});
test('symlink identity changes are unavailable and selection fails closed',()=>{const f=setup();try{const r=createSession(f.root,'Symlink');renameSync(r.dir,r.dir+'-old');symlinkSync(r.dir+'-old',r.dir);assert.equal(findSession(r.id).availability,'unavailable');assert.throws(()=>selectSession(f.root,r.id),/Unsafe filesystem/);}finally{f.cleanup();}});
test('SIGKILL writer lock blocks mutation without deletion or guessed PID recovery',()=>{const f=setup();try{createSession(f.root,'Existing');const lock=join(f.temp,'index','sessions.lock');const crashed=spawnSync(process.execPath,['--input-type=module','-e',`import {writeFileSync} from 'node:fs';writeFileSync(${JSON.stringify(lock)},'{"owner":"crashed-writer"}',{flag:'wx',mode:0o600});process.kill(process.pid,'SIGKILL');`],{encoding:'utf8'});assert.equal(crashed.signal,'SIGKILL');assert.throws(()=>createSession(f.root,'Next'),/index locked/);assert.equal(readFileSync(lock,'utf8'),'{"owner":"crashed-writer"}');assert.equal(listSessions().length,1);}finally{f.cleanup();}});
test('invalid titles and metadata are rejected without accepting credentials fields',()=>{const f=setup();try{assert.throws(()=>createSession(f.root,'\x1b[31m'),/Invalid session title/);const r=createSession(f.root,'Valid');assert.throws(()=>touchSession(r.id,{token:'secret'} as any),/Unknown summary/);assert.throws(()=>touchSession(r.id,{taskStates:{running:-1}}),/Invalid task state/);}finally{f.cleanup();}});
