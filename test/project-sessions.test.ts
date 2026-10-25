import test from 'node:test';
import assert from 'node:assert/strict';
import {existsSync,mkdirSync,mkdtempSync,readFileSync,realpathSync,renameSync,rmSync,symlinkSync,writeFileSync} from 'node:fs';
import {join,relative} from 'node:path';
import {tmpdir} from 'node:os';
import {createSession,listSessions} from '../src/sessions.ts';

function fixture(){
  const temp=realpathSync(mkdtempSync(join(tmpdir(),'fleet-project-sessions-'))),before=process.env.AGENT_FLEET_HOME;
  process.env.AGENT_FLEET_HOME=join(temp,'history');
  const first=join(temp,'project'),second=join(temp,'project-sibling');mkdirSync(first);mkdirSync(second);
  return {temp,first,second,cleanup(){if(before===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=before;rmSync(temp,{recursive:true,force:true});}};
}

test('project filter isolates two project histories and matches roots rather than prefixes',()=>{
  const f=fixture();try{
    const a=createSession(f.first,'Shared title'),a2=createSession(f.first,'Second session'),b=createSession(f.second,'Shared title');
    for(const r of [a,a2,b])writeFileSync(join(r.dir,'state.sqlite'),'fixture');
    const first=listSessions(f.first),second=listSessions(f.second);
    assert.deepEqual(new Set(first.map(r=>r.id)),new Set([a.id,a2.id]));assert.deepEqual(second.map(r=>r.id),[b.id]);
    assert.ok(first.every(r=>r.root===f.first&&r.availability==='available'));assert.equal(listSessions().length,3);
    assert.deepEqual(listSessions(f.temp),[],'Parent directory is not a project-wide prefix filter');
  }finally{f.cleanup();}
});

test('relative paths, trailing separators and directory aliases resolve to the actual project',()=>{
  const f=fixture();try{
    const a=createSession(f.first,'Canonical project');createSession(f.second,'Unrelated project');
    const alias=join(f.temp,'project-alias');symlinkSync(f.first,alias,'dir');
    for(const input of [relative(process.cwd(),f.first),f.first+'/',join(f.first,'.'),alias])assert.deepEqual(listSessions(input).map(r=>r.id),[a.id]);
  }finally{f.cleanup();}
});

test('filtered history is read-only and retains unavailable sessions for the selected project',()=>{
  const f=fixture();try{
    const a=createSession(f.first,'Missing database'),b=createSession(f.second,'Other project');writeFileSync(join(b.dir,'state.sqlite'),'fixture');
    const index=join(f.temp,'history','sessions.json'),pointer=join(f.first,'.agent-fleet','current.json');
    const savedIndex=readFileSync(index,'utf8'),savedPointer=readFileSync(pointer,'utf8');
    const filtered=listSessions(f.first);assert.equal(filtered.length,1);assert.equal(filtered[0].id,a.id);assert.equal(filtered[0].availability,'unavailable');
    assert.equal(readFileSync(index,'utf8'),savedIndex);assert.equal(readFileSync(pointer,'utf8'),savedPointer);assert.equal(existsSync(join(f.temp,'history','sessions.lock')),false);
    renameSync(f.first,f.first+'-moved');assert.throws(()=>listSessions(f.first),/ENOENT/);assert.equal(listSessions().find(r=>r.id===a.id)?.availability,'unavailable');
  }finally{f.cleanup();}
});

test('invalid project filters fail without creating history or treating files as projects',()=>{
  const f=fixture();try{
    assert.throws(()=>listSessions(''),/Invalid project filter/);assert.throws(()=>listSessions('\0'),/Invalid project filter/);assert.throws(()=>listSessions(join(f.temp,'absent')),/ENOENT/);
    const file=join(f.temp,'not-a-directory');writeFileSync(file,'fixture');assert.throws(()=>listSessions(file),/Unsafe filesystem identity/);
    assert.equal(existsSync(join(f.temp,'history')),false);assert.deepEqual(listSessions(f.first),[]);assert.equal(existsSync(join(f.temp,'history')),false);
  }finally{f.cleanup();}
});
