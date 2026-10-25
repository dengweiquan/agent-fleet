import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,realpathSync,existsSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
import {ControlLedger} from '../src/control.ts';
import type {ControlOperation,ControlRequest,ControlScope} from '../src/control.ts';

const permissions=()=>new Set<ControlOperation>(['draft.create','note.append','pause','resume','stop']);
function fixture(){const dir=realpathSync(mkdtempSync(join(tmpdir(),'agent-fleet-control-')));return {dir,path:join(dir,'control.sqlite'),scope:{projectId:dir,sessionId:'session-one'} as ControlScope,cleanup:()=>rmSync(dir,{recursive:true,force:true})};}
function request(scope:ControlScope,requestId:string,operation:ControlOperation='pause',payload:unknown={}):ControlRequest{return {version:1,requestId,...scope,operation,payload} as ControlRequest;}

test('receipt commits acceptance before any effect, hashes sorted content and rechecks permission on duplicates',async()=>{
  const f=fixture(),allowed=permissions(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:()=>allowed});
  try{
    const req=request(f.scope,'request-a','note.append',{text:'context',taskId:'task-a'}),receipt=ledger.submit(req);
    assert.equal(receipt.state,'accepted');assert.equal(receipt.received,true);assert.equal(receipt.completed,false);assert.equal(receipt.taskState,'unchanged');assert.equal(ledger.artifacts().length,0);
    const observer=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions(),readOnly:true});assert.equal(observer.receipt(req.requestId)?.hash,receipt.hash);observer.close();
    const reordered={payload:{taskId:'task-a',text:'context'},operation:'note.append',sessionId:f.scope.sessionId,projectId:f.scope.projectId,requestId:'request-a',version:1} as ControlRequest;
    assert.deepEqual(ledger.submit(reordered),receipt);assert.equal(ledger.events().length,1);
    assert.throws(()=>ledger.submit({...req,payload:{text:'different'}} as ControlRequest),/conflicts/);
    allowed.delete('note.append');assert.throws(()=>ledger.submit(req),/permission denied/);await assert.rejects(()=>ledger.apply(req.requestId),/permission denied/);assert.equal(ledger.receipt(req.requestId)?.state,'accepted');
  }finally{ledger.close();f.cleanup();}
});

test('strict bounded request schema rejects executable fields, scope mixing, unsafe IDs and multibyte overflow',()=>{
  const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});
  try{
    const valid=request(f.scope,'safe');
    for(const bad of [
      {...valid,version:2},{...valid,requestId:'../escape'},{...valid,projectId:'/untrusted/project'},{...valid,sessionId:'other'},
      {...valid,operation:'exec',payload:{command:'echo bad'}},{...valid,permissions:['pause']},{...valid,payload:{shell:'echo bad'}},
      request(f.scope,'n','note.append',{text:'note',taskId:'../task'}),request(f.scope,'s','stop',{runId:'../run'}),
      request(f.scope,'n','note.append',{text:'中'.repeat(11000)}),request(f.scope,'n','note.append',{text:'bad\u001b[31m'}),
      request(f.scope,'d','draft.create',{title:'title',text:'body',execute:true}),request(f.scope,'s','stop',{runId:'run',force:true}),
    ])assert.throws(()=>ledger.submit(bad as never));
    assert.equal(ledger.receipts().length,0);assert.equal(ledger.events().length,0);
  }finally{ledger.close();f.cleanup();}
});

test('drafts and notes apply atomically as inert artifacts and never invoke a supplied handler',async()=>{
  const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});let calls=0;
  try{
    ledger.submit(request(f.scope,'draft','draft.create',{title:'Roadmap idea',text:'A literal shell example: echo not-executed'}));
    ledger.submit(request(f.scope,'note','note.append',{text:'Evidence note',taskId:'task-a'}));
    for(const id of ['draft','note']){const receipt=await ledger.apply(id,()=>{calls++;throw new Error('must not run');});assert.equal(receipt.state,'completed');assert.equal(receipt.taskState,'unchanged');assert.equal((receipt.result as {executable:boolean}).executable,false);}
    assert.equal(calls,0);assert.equal(ledger.artifacts().length,2);assert.equal(ledger.artifacts()[1].taskId,'task-a');
    await ledger.apply('draft');assert.equal(ledger.artifacts().length,2);
    assert.deepEqual(ledger.events().map(e=>e.event_id),[1,2,3,4,5,6]);
  }finally{ledger.close();f.cleanup();}
});

test('effect reservation is committed before callback, results are sanitized and completion does not assert task state',async()=>{
  const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});
  try{
    ledger.submit(request(f.scope,'pause'));
    const receipt=await ledger.apply('pause',async req=>{
      assert.equal(req.operation,'pause');assert.equal(ledger.receipt('pause')?.state,'applying');
      const observer=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions(),readOnly:true});assert.equal(observer.receipt('pause')?.state,'unknown');assert.equal(observer.events().at(-1)?.kind,'request.applying');observer.close();
      return {paused:true,token:'do-not-store',nested:{message:'Bearer abc123',password:'secret'}};
    });
    assert.equal(receipt.state,'completed');assert.equal(receipt.completed,true);assert.equal(receipt.taskState,'unchanged');
    assert.deepEqual(receipt.result,{paused:true,token:'[REDACTED]',nested:{message:'Bearer [REDACTED]',password:'[REDACTED]'}});
    assert.doesNotMatch(JSON.stringify(ledger.receipts()),/do-not-store|abc123/);
    let again=0;assert.equal((await ledger.apply('pause',()=>again++)).state,'completed');assert.equal(again,0);
    ledger.submit(request(f.scope,'stop','stop',{runId:'run-one'}));
    const stopped=await ledger.apply('stop',()=>({runId:'run-one',stopRequestRecorded:true}));
    assert.deepEqual(stopped.result,{processTermination:'unverified',details:{runId:'run-one',stopRequestRecorded:true}});assert.equal(stopped.taskState,'unchanged');
  }finally{ledger.close();f.cleanup();}
});

test('trusted no-effect stop result never claims a request was recorded or creates an unknown barrier',async()=>{
  const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});
  try{
    ledger.submit(request(f.scope,'terminal-stop','stop',{runId:'already-ended'}));
    const receipt=await ledger.apply('terminal-stop',()=>({stopRequestRecorded:false,reason:'attempt-not-running',observedRunState:'exited'}));
    assert.equal(receipt.state,'completed');assert.deepEqual(receipt.result,{processTermination:'unverified',details:{stopRequestRecorded:false,reason:'attempt-not-running',observedRunState:'exited'}});
    assert.doesNotMatch(JSON.stringify(ledger.events()),/stop-request-recorded/);
    ledger.submit(request(f.scope,'resume','resume'));assert.equal((await ledger.apply('resume',()=>({resumed:true}))).state,'completed');
  }finally{ledger.close();f.cleanup();}
});

test('handler errors or oversized completion evidence stay unknown and cannot be replayed',async()=>{
  for(const outcome of ['throw','oversized','escaped-error']){
    const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});let calls=0;
    try{
      ledger.submit(request(f.scope,'effect'));
      const receipt=await ledger.apply('effect',()=>{calls++;if(outcome==='oversized')return 'x'.repeat(20000);if(outcome==='escaped-error')throw new Error('\ud800'.repeat(2000));throw new Error('external effect may have happened; token=private-token');});
      assert.equal(receipt.state,'unknown');assert.equal(receipt.completed,false);assert.equal(receipt.result,null);assert.doesNotMatch(receipt.error??'',/private-token/);
      assert.equal((await ledger.apply('effect',()=>calls++)).state,'unknown');assert.equal(calls,1);
      ledger.submit(request(f.scope,'next','resume'));await assert.rejects(()=>ledger.apply('next',()=>calls++),/unresolved effect/);assert.equal(calls,1);
      ledger.submit(request(f.scope,'note','note.append',{text:'Operator evidence only'}));assert.equal((await ledger.apply('note')).state,'completed');
    }finally{ledger.close();f.cleanup();}
  }
});

test('concurrent requests cannot bypass the session applying barrier; duplicate owner cannot invoke callback twice',async()=>{
  const f=fixture(),one=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()}),two=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});let finish!:(value:unknown)=>void,called=0;
  try{
    one.submit(request(f.scope,'pause'));one.submit(request(f.scope,'resume','resume'));
    const ongoing=one.apply('pause',()=>{called++;return new Promise(resolve=>{finish=resolve;});});
    assert.equal((await two.apply('pause',()=>called++)).state,'unknown');await assert.rejects(()=>two.apply('resume',()=>called++),/unresolved effect/);assert.equal(called,1);
    finish({paused:true});assert.equal((await ongoing).state,'completed');assert.equal((await two.apply('resume',()=>({paused:false}))).state,'completed');
  }finally{one.close();two.close();f.cleanup();}
});

test('read-only observer never creates files or changes schema/state; wrong bound scope cannot reuse ledger',async()=>{
  const f=fixture();
  try{
    const missing=join(f.dir,'missing','control.sqlite');assert.throws(()=>new ControlLedger(missing,{scope:f.scope,allowedOperations:permissions(),readOnly:true}));assert.equal(existsSync(join(f.dir,'missing')),false);
    const owner=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});owner.submit(request(f.scope,'pause'));owner.close();
    assert.throws(()=>new ControlLedger(f.path,{scope:{...f.scope,sessionId:'different-session'},allowedOperations:permissions()}),/different trusted scope/);
    const observer=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions(),readOnly:true});assert.equal(observer.receipt('pause')?.state,'accepted');assert.throws(()=>observer.submit(request(f.scope,'pause')),/read-only/);await assert.rejects(()=>observer.apply('pause',()=>{}),/read-only/);assert.equal(observer.events().length,1);observer.close();
  }finally{f.cleanup();}
});

test('48 KiB pages remain within framed-output bounds and cursors continue without skipping records',async()=>{
  const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});
  try{
    for(let i=0;i<5;i++){ledger.submit(request(f.scope,`draft-${i}`,'draft.create',{title:'Large inert document',text:'a'.repeat(32768)}));await ledger.apply(`draft-${i}`);}
    const first=ledger.artifacts(0,100);assert.equal(first.length,1);assert.ok(Buffer.byteLength(JSON.stringify(first))<=49152);
    const ids:number[]=[];let after=0;
    while(true){const batch=ledger.artifacts(after,100);if(!batch.length)break;ids.push(...batch.map(a=>a.artifactId));after=batch.at(-1)!.artifactId;}
    assert.deepEqual(ids,[1,2,3,4,5]);assert.ok(Buffer.byteLength(JSON.stringify(ledger.receipts()))<=49152);assert.ok(Buffer.byteLength(JSON.stringify(ledger.events()))<=49152);
    assert.throws(()=>ledger.events(0,1001));assert.throws(()=>ledger.artifacts(-1));assert.throws(()=>ledger.receipts(0,0));
  }finally{ledger.close();f.cleanup();}
});

test('process interruption after applying commit reopens unknown without automatic reexecution',async()=>{
  const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});
  ledger.submit(request(f.scope,'interrupted'));ledger.close();
  const moduleUrl=new URL('../src/control.ts',import.meta.url).href;
  const source=`import {ControlLedger} from ${JSON.stringify(moduleUrl)};const ledger=new ControlLedger(${JSON.stringify(f.path)},{scope:${JSON.stringify(f.scope)},allowedOperations:new Set(['pause'])});await ledger.apply('interrupted',()=>{console.log('HANDLER_ENTERED');return new Promise(()=>{setInterval(()=>{},1000);});});`;
  const child=spawn(process.execPath,['--experimental-strip-types','--input-type=module','-e',source],{stdio:['ignore','pipe','pipe']});
  let stderr='';child.stderr.on('data',chunk=>stderr+=chunk);
  try{
    await new Promise<void>((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error(`fixture startup timeout: ${stderr}`)),5000);child.stdout.on('data',chunk=>{if(String(chunk).includes('HANDLER_ENTERED')){clearTimeout(timer);resolve();}});child.once('error',error=>{clearTimeout(timer);reject(error);});});
    await new Promise<void>((resolve,reject)=>{child.once('close',()=>resolve());child.once('error',reject);child.kill('SIGKILL');});
    const reopened=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});let calls=0;
    assert.equal(reopened.receipt('interrupted')?.state,'unknown');assert.equal((await reopened.apply('interrupted',()=>calls++)).state,'unknown');assert.equal(calls,0);assert.equal(reopened.events().at(-1)?.kind,'request.applying');reopened.close();
  }finally{if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');f.cleanup();}
});

test('two independent processes applying one receipt execute the local fixture handler exactly once',async()=>{
  const f=fixture(),ledger=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions()});ledger.submit(request(f.scope,'shared'));ledger.close();
  const moduleUrl=new URL('../src/control.ts',import.meta.url).href,marker=join(f.dir,'effects.txt');
  const source=`import {ControlLedger} from ${JSON.stringify(moduleUrl)};import {appendFileSync} from 'node:fs';const ledger=new ControlLedger(${JSON.stringify(f.path)},{scope:${JSON.stringify(f.scope)},allowedOperations:new Set(['pause'])});const r=await ledger.apply('shared',async()=>{appendFileSync(${JSON.stringify(marker)},'effect\\n');await new Promise(r=>setTimeout(r,80));return {paused:true};});console.log(JSON.stringify(r));ledger.close();`;
  try{
    const receipts=await Promise.all([0,1].map(()=>new Promise<string>((resolve,reject)=>{const child=spawn(process.execPath,['--experimental-strip-types','--input-type=module','-e',source],{stdio:['ignore','pipe','pipe']});let out='',err='';child.stdout.on('data',c=>out+=c);child.stderr.on('data',c=>err+=c);child.once('error',reject);child.once('close',code=>code===0?resolve(out):reject(new Error(err)));})));
    assert.equal(readFileSync(marker,'utf8'),'effect\n');assert.ok(receipts.map(r=>JSON.parse(r).state).includes('completed'));
    const observed=new ControlLedger(f.path,{scope:f.scope,allowedOperations:permissions(),readOnly:true});assert.equal(observed.receipt('shared')?.state,'completed');assert.deepEqual(observed.events().map(e=>e.kind),['request.received','request.applying','request.completed']);observed.close();
  }finally{f.cleanup();}
});
