import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync,execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {DatabaseSync} from 'node:sqlite';
import {Engine} from '../src/engine.ts';

test('legacy format fixture remains read-only and unchanged while new sessions use the current format',{timeout:15000},()=>{
  const temp=mkdtempSync(join(tmpdir(),'fleet-compat-')),repo=fileURLToPath(new URL('../',import.meta.url)),priorHome=process.env.AGENT_FLEET_HOME;
  process.env.AGENT_FLEET_HOME=join(temp,'history');
  try{
    const env={...process.env},git=(...args:string[])=>execFileSync('git',['-c','core.hooksPath=/dev/null',...args],{cwd:temp,env,stdio:'pipe'});
    git('init');writeFileSync(join(temp,'seed'),'synthetic format fixture');git('add','seed');git('-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m','fixture');
    const fixture=Engine.init(temp,'Synthetic legacy format'),sessionId=fixture.sessionId,dbPath=join(fixture.dir,'state.sqlite');fixture.close();
    // Frozen M1 storage contract: no logical-format marker and no concurrency config fields.
    // Construct only this synthetic database; do not require private historical Git commits.
    const legacy=new DatabaseSync(dbPath),config=JSON.parse(legacy.prepare("SELECT value FROM meta WHERE key='config'").get()!.value as string);
    delete config.maxWorkers;delete config.maxReviews;
    legacy.prepare("DELETE FROM meta WHERE key='logicalFormatVersion'").run();legacy.prepare("UPDATE meta SET value=? WHERE key='config'").run(JSON.stringify(config));
    const snapshot=()=>JSON.stringify({meta:legacy.prepare('SELECT * FROM meta ORDER BY key').all(),runs:legacy.prepare('SELECT * FROM runs ORDER BY rowid').all(),events:legacy.prepare('SELECT * FROM events ORDER BY event_id').all()});
    const before=snapshot(),indexPath=join(process.env.AGENT_FLEET_HOME!,'sessions.json'),index=readFileSync(indexPath,'utf8');
    const invoke=(args:string[])=>spawnSync(process.execPath,['--experimental-strip-types',join(repo,'src/cli.ts'),'--json',...args],{cwd:temp,env,encoding:'utf8',timeout:5000});
    const data=(args:string[])=>{const r=invoke(args);assert.equal(r.status,0,r.stderr);return JSON.parse(r.stdout).data;};
    assert.equal(data(['resume',sessionId]).sessionId,sessionId);assert.equal(data(['status']).logicalFormatVersion,1);assert.equal(data(['config']).maxWorkers,2);
    assert.equal(snapshot(),before,'read-only compatibility must preserve exact stored legacy values/events');assert.equal(readFileSync(indexPath,'utf8'),index);
    const configFile=join(temp,'new-config.json');writeFileSync(configFile,JSON.stringify({...config,maxWorkers:2,maxReviews:1}));const blocked=invoke(['config',configFile]);assert.equal(blocked.status,1);assert.match(blocked.stderr,/Legacy session is read-only/);assert.equal(snapshot(),before);
    const next=data(['init','Separate current-format session']);assert.equal(next.logicalFormatVersion,2);assert.notEqual(next.sessionId,sessionId);assert.equal(snapshot(),before);
    assert.equal(data(['--session',sessionId,'status']).logicalFormatVersion,1);legacy.close();
  }finally{if(priorHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=priorHome;rmSync(temp,{recursive:true,force:true});}
});
