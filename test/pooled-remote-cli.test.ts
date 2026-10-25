import test,{mock} from 'node:test';
import childProcess from 'node:child_process';
import {syncBuiltinESMExports} from 'node:module';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,realpathSync,writeFileSync,readFileSync,existsSync,renameSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PassThrough} from 'node:stream';
import {DatabaseSync} from 'node:sqlite';
import {fileURLToPath} from 'node:url';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {pooledRemoteCommand} from '../src/pooled-remote-cli.ts';
import {serveConnector} from '../src/connector-server.ts';
import {runObserver} from '../src/connector-client.ts';
import type {ConnectorConfig} from '../src/connector-client.ts';
import {ConnectorJournal} from '../src/connector.ts';
import type {ConnectorTransport,ConnectorFrame,ConnectorReply} from '../src/connector.ts';
import {remoteCommand} from '../src/remote-cli.ts';
import {schedule} from '../src/scheduler.ts';
import {scheduleFleet} from '../src/fleet-scheduler.ts';

const caps={protocolVersion:1,provider:'external-agent',capabilities:['code'],quota:'unknown',auth:'available'};
function fixture(){
 const dir=realpathSync(mkdtempSync(join(tmpdir(),'fleet-pooled-cli-'))),priorHome=process.env.AGENT_FLEET_HOME;process.env.AGENT_FLEET_HOME=join(dir,'history');
 const engines=['alpha','beta'].map(name=>{const root=join(dir,name);mkdirSync(root);git(root,'init');writeFileSync(join(root,'seed.txt'),name+' isolated seed\n');git(root,'add','.');git(root,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m',name+' synthetic seed');const e=Engine.init(root,name+' pooled CLI');e.config({version:1,maxOutputBytes:131072,timeoutMs:1500,maxWorkers:1,maxReviews:1,instances:[{id:'manager',provider:'fake',role:'manager',model:'planner',capabilities:['planning']},{id:'worker',provider:'fake',role:'worker',model:'delay-write',capabilities:['code']},{id:'reviewer',provider:'fake',role:'reviewer',model:'review-approve',capabilities:['review']}]});e.plan({version:1,goal:name+' only',tasks:[{id:'same-task',title:name+' task',prompt:JSON.stringify({filename:name+'.txt',content:name,delayMs:10}),dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e','process.exit(0)']]}]});return e;});
 const poolPath=join(dir,'pool.sqlite'),pool=new ResourcePool(poolPath);
 pool.upsertResource({id:'muse-worker',provider:'external-agent',model:'synthetic-existing-session',endpointId:'muse',endpointKind:'external-agent',accountDomain:'muse-account',roles:['worker'],capabilities:['code'],capacity:1});
 pool.upsertResource({id:'local-worker',provider:'fake',model:'delay-write',endpointId:'local',endpointKind:'local',accountDomain:'fake-account',roles:['worker'],capabilities:['code'],capacity:2});
 pool.upsertResource({id:'local-reviewer',provider:'fake',model:'review-approve',endpointId:'local',endpointKind:'local',accountDomain:'fake-account',roles:['reviewer'],capabilities:['review'],capacity:1});
 pool.setLimits({global:4,projects:Object.fromEntries(engines.map(e=>[e.root,2])),endpoints:{muse:1,local:3},accounts:{'muse-account':1,'fake-account':3}});pool.close();
 const fleet=join(dir,'fleet.json');
 const authorize=(flags:boolean[]=[false,false])=>writeFileSync(fleet,JSON.stringify({version:1,projects:engines.map((e,i)=>({root:e.root,sessionId:e.sessionId,...(flags[i]?{remoteClaimsAuthorized:true}:{})}))}));authorize();
 const call=(payload:unknown,observer=false)=>pooledRemoteCommand(['request',fleet,'muse','muse-worker'],{poolPath,observer},payload);
 const offer=(i:number)=>pooledRemoteCommand(['offer',fleet,engines[i].sessionId,'same-task','muse','synthetic-'+i],{poolPath,observer:false});
 return {dir,poolPath,fleet,engines,authorize,call,offer,close(){for(const e of engines)e.close();if(priorHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=priorHome;rmSync(dir,{recursive:true,force:true});}};
}
function logical(path:string){const db=new DatabaseSync(path,{readOnly:true});try{const tables=db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all() as {name:string}[];return Object.fromEntries(tables.map(t=>[t.name,db.prepare('SELECT * FROM "'+t.name.replaceAll('"','""')+'"').all()]));}finally{db.close();}}
function onlyBinding(claim:any){const {dispatch,...binding}=claim;return binding;}
async function serve(f:ReturnType<typeof fixture>,data:string|Buffer,observer=true){const input=new PassThrough(),output=new PassThrough();let text='';output.on('data',chunk=>text+=chunk);const done=serveConnector([f.fleet,'muse','muse-worker'],{poolPath:f.poolPath,observer},input,output);input.end(data);try{await done;return text.trim().split('\n').filter(Boolean).map(line=>JSON.parse(line));}finally{input.destroy();output.destroy();}}
const frame=(id:string,payload:Record<string,unknown>,generation=1)=>JSON.stringify({id,generation,payload:{version:1,...payload}})+'\n';

test('actual pooled offer/request binds two sessions; self-reported capability cannot enable default-disabled claims',{timeout:20000},async()=>{const f=fixture();try{const a=await f.offer(0) as any,b=await f.offer(1) as any;assert.notEqual(a.messageId,b.messageId);assert.equal(a.taskId,b.taskId);assert.notEqual(a.sessionId,b.sessionId);assert.notEqual(a.baseSha,b.baseSha);const listed=await f.call({version:1,op:'list'}) as any[];assert.equal(listed.length,2);assert.equal((await f.call({version:1,op:'pull',capabilities:caps}) as any[]).length,2);await assert.rejects(f.call({version:1,op:'claim',claimId:'denied-by-manifest',capabilities:caps}),/disabled|authorization/);const pool=new ResourcePool(f.poolPath,{readOnly:true});try{assert.equal(pool.status().leases.length,0);}finally{pool.close();}
 f.authorize([false,true]);const claim=await f.call({version:1,op:'claim',claimId:'authorized-beta',capabilities:caps}) as any;assert.equal(claim.sessionId,f.engines[1].sessionId);assert.equal(claim.messageId,b.messageId);const binding=onlyBinding(claim);const ack=await f.call({version:1,op:'ack',binding}) as any;assert.equal(ack.meaning,'durably-received-only');const result=await f.call({version:1,op:'report',binding,sourceEventId:'beta-result',kind:'result',data:{exitCode:0}}) as any;assert.equal(result.accepted,false);assert.equal(result.state,'execution-completed');assert(f.engines.every(e=>e.store.listRuns().length===0),'protocol result is never local acceptance');await assert.rejects(f.call({version:1,op:'claim',claimId:'observer-denied',capabilities:caps},true),/Observer/);
 }finally{f.close();}});

test('pooled offered work blocks local replay and all legacy remote mutation paths',{timeout:20000},async()=>{const f=fixture();try{await f.offer(0);await f.offer(1);for(const e of f.engines){assert.equal(e.remoteStatus().length,1);await assert.rejects(e.run('same-task','worker'),/remote|replay|arbitration/i);await assert.rejects(remoteCommand(e,['offer','same-task','muse','legacy'],false),/Pooled|pool|legacy/);await assert.rejects(remoteCommand(e,['request','muse'],false),/Pooled|pool|legacy/);const result=await e.exclusive(()=>schedule(e,'reviewer'));assert.equal(result.accepted,0);assert.equal(e.store.listRuns().length,0);assert(!existsSync(join(e.dir,'remote.sqlite')));}
 const pool=new ResourcePool(f.poolPath);try{const result=await scheduleFleet(pool,f.engines.map(engine=>({projectId:engine.root,engine})));assert(result.projects.every(p=>p.accepted===0));assert.equal(pool.status().leases.length,0);assert(f.engines.every(e=>e.store.listRuns().length===0));}finally{pool.close();}
 }finally{f.close();}});

test('missing shared authority fails closed without creating another pool or permitting replay',{timeout:20000},async()=>{const f=fixture();const backup=f.poolPath+'.saved';try{await f.offer(0);renameSync(f.poolPath,backup);assert.throws(()=>f.engines[0].remoteStatus());await assert.rejects(f.engines[0].run('same-task','worker'));await assert.rejects(f.call({version:1,op:'hello'},true));await assert.rejects(f.call({version:1,op:'claim',claimId:'no-pool',capabilities:caps}));assert(!existsSync(f.poolPath));assert.equal(f.engines[0].store.listRuns().length,0);}finally{if(existsSync(backup))renameSync(backup,f.poolPath);f.close();}});

test('one observed JSONL connection serves hello/events and rejects claim with no logical changes',{timeout:20000},async()=>{const f=fixture();try{await f.offer(0);await f.offer(1);f.authorize([true,true]);const before=logical(f.poolPath),sessionBefore=f.engines.map(e=>logical(join(e.dir,'state.sqlite'))),history=readFileSync(join(f.dir,'history','sessions.json'),'utf8');const responses=await serve(f,frame('hello',{op:'hello'})+frame('events',{op:'events',after:0,limit:100})+frame('claim',{op:'claim',claimId:'observer-cannot-claim',capabilities:caps}));assert.equal(responses.length,3);assert.deepEqual(responses.map(r=>r.id),['hello','events','claim']);assert(responses.every(r=>r.generation===1));assert.equal(responses[0].data.endpointId,'muse');assert.equal(responses[0].data.modelInvoked,false);assert.equal(responses[1].ok,true);assert.equal(responses[2].ok,false);assert.match(responses[2].error,/Observer/);const events=responses[1].data;assert(events.length>=2);assert(events.every((e:any)=>Number.isSafeInteger(e.event_id)));const pool=new ResourcePool(f.poolPath,{readOnly:true});try{const all=pool.events();assert(events.every((event:any)=>all.some(e=>e.event_id===event.event_id)));}finally{pool.close();}assert.deepEqual(logical(f.poolPath),before);assert.deepEqual(f.engines.map(e=>logical(join(e.dir,'state.sqlite'))),sessionBefore);assert.equal(readFileSync(join(f.dir,'history','sessions.json'),'utf8'),history);}finally{f.close();}});

test('malformed/oversized/incomplete JSONL frames stay bounded and cannot mutate scope',{timeout:20000},async()=>{const f=fixture();try{await f.offer(0);const before=logical(f.poolPath);const response=await serve(f,'{broken\n'+frame('valid-after-error',{op:'hello'}));assert.equal(response.length,2);assert.equal(response[0].ok,false);assert.equal(response[1].ok,true);const extra=JSON.stringify({id:'wrong-scope',generation:1,payload:{version:1,op:'hello'},path:'/arbitrary'})+'\n';assert.equal((await serve(f,extra))[0].ok,false);await assert.rejects(serve(f,'x'.repeat(65537)+'\n'),/64KiB/);await assert.rejects(serve(f,'x'.repeat(65537)),/64KiB/);await assert.rejects(serve(f,JSON.stringify({id:'partial',generation:1,payload:{version:1,op:'hello'}})),/incomplete/);assert.deepEqual(logical(f.poolPath),before);}finally{f.close();}});

class InMemoryServerTransport implements ConnectorTransport {
 readonly connectionKey='f'.repeat(64);requests:ConnectorFrame[]=[];connections:number[]=[];
 private input=new PassThrough();private output=new PassThrough();private done:Promise<void>|null=null;private remainder='';private pending=new Map<string,{resolve:(reply:ConnectorReply)=>void,reject:(error:Error)=>void}>();
 private f:ReturnType<typeof fixture>;
 constructor(f:ReturnType<typeof fixture>){this.f=f;this.output.on('data',chunk=>{this.remainder+=chunk.toString();let newline:number;while((newline=this.remainder.indexOf('\n'))>=0){const line=this.remainder.slice(0,newline);this.remainder=this.remainder.slice(newline+1);const reply=JSON.parse(line) as ConnectorReply;const wait=this.pending.get(reply.id);this.pending.delete(reply.id);wait?.resolve(reply);}});}
 async connect(generation:number){this.connections.push(generation);this.done=serveConnector([this.f.fleet,'muse','muse-worker'],{poolPath:this.f.poolPath,observer:true},this.input,this.output);void this.done.catch(error=>{for(const wait of this.pending.values())wait.reject(error);this.pending.clear();});}
 async request(frame:ConnectorFrame):Promise<ConnectorReply>{this.requests.push(structuredClone(frame));return new Promise((resolve,reject)=>{this.pending.set(frame.id,{resolve,reject});this.input.write(JSON.stringify(frame)+'\n');});}
 async close(){if(this.done){this.input.end();await this.done;this.done=null;}this.input.destroy();this.output.destroy();}
}

// OS ownership evidence is synthetic too: sandbox-denied ps/sysctl must not weaken production fail-closed checks.
test('finite observer performs hello/event replay over the real in-memory server and never claims or wakes',{timeout:20000},async()=>{const f=fixture();const originalExec=childProcess.execFileSync;const osProbe=mock.method(childProcess,'execFileSync',((command:any,...args:any[])=>command==='/usr/sbin/sysctl'?'synthetic-test-boot-identity':command==='/bin/ps'?String('Sun Oct 25 00:00:00 2026 synthetic-test-node-process'):(originalExec as any)(command,...args)) as any);syncBuiltinESMExports();try{await f.offer(0);await f.offer(1);const config:ConnectorConfig={version:1,journalPath:join(f.dir,'connector.sqlite'),sshAlias:'synthetic-never-used',connectionApproved:true,server:{node:process.execPath,cli:fileURLToPath(new URL('../src/cli.ts',import.meta.url)),pool:f.poolPath,fleet:f.fleet,endpointId:'muse',resourceId:'muse-worker'}};const before=logical(f.poolPath),firstTransport=new InMemoryServerTransport(f);const first=await runObserver(config,1,{transport:firstTransport});assert.equal(first.modelInvoked,false);assert.equal(first.museWakeInterface,'unverified');assert(first.serverCursor>0);assert(firstTransport.requests.some(r=>r.payload.op==='hello'));assert(firstTransport.requests.some(r=>r.payload.op==='events'));assert(firstTransport.requests.every(r=>['hello','events'].includes(r.payload.op)));const journal=new ConnectorJournal(config.journalPath,{readOnly:true});let prior:any;try{prior=journal.status();assert.equal(prior.serverCursor,first.serverCursor);assert.equal(prior.owner,null);assert(journal.serverEvents().length>=2);}finally{journal.close();}const secondTransport=new InMemoryServerTransport(f);const second=await runObserver(config,1,{transport:secondTransport});assert.equal(second.serverCursor,first.serverCursor);assert(secondTransport.connections[0]>firstTransport.connections[0]);assert(secondTransport.requests.every(r=>['hello','events'].includes(r.payload.op)));const again=new ConnectorJournal(config.journalPath,{readOnly:true});try{assert.equal(again.status().serverCursor,prior.serverCursor);assert.equal(again.serverEvents().length,2,'replay keeps the same global events rather than duplicating');}finally{again.close();}assert.deepEqual(logical(f.poolPath),before);}finally{osProbe.mock.restore();syncBuiltinESMExports();f.close();}});

test('observer replays multiple individually valid large events within the JSONL response bound',{timeout:20000},async()=>{
 const f=fixture(),originalExec=childProcess.execFileSync;
 const osProbe=mock.method(childProcess,'execFileSync',((command:any,...args:any[])=>command==='/usr/sbin/sysctl'?'synthetic-test-boot-identity':command==='/bin/ps'?'Sun Oct 25 00:00:00 2026 synthetic-test-node-process':(originalExec as any)(command,...args)) as any);syncBuiltinESMExports();
 try{
  await f.offer(0);await f.offer(1);f.authorize([true,false]);
  const claim=await f.call({version:1,op:'claim',claimId:'large-events-worker',capabilities:caps}) as any,binding=onlyBinding(claim);
  await f.call({version:1,op:'ack',binding});
  for(let i=0;i<5;i++)await f.call({version:1,op:'report',binding,sourceEventId:'large-progress-'+i,kind:'progress',data:{text:'x'.repeat(15000),sequence:i}});
  const events=await f.call({version:1,op:'events',after:0,limit:100}) as any[],expectedCursor=events.at(-1)!.event_id;
  assert(Buffer.byteLength(JSON.stringify(events))>65536,'fixture really exceeds one response frame');
  const config:ConnectorConfig={version:1,journalPath:join(f.dir,'large-event-connector.sqlite'),sshAlias:'synthetic-never-used',connectionApproved:true,server:{node:process.execPath,cli:fileURLToPath(new URL('../src/cli.ts',import.meta.url)),pool:f.poolPath,fleet:f.fleet,endpointId:'muse',resourceId:'muse-worker'}};
  const before=logical(f.poolPath),transport=new InMemoryServerTransport(f),result=await runObserver(config,1,{transport});
  assert.equal(result.serverCursor,expectedCursor,'bounded replay must reach every committed event');assert.equal(result.modelInvoked,false);assert(transport.requests.every(r=>['hello','events'].includes(r.payload.op)));
  const journal=new ConnectorJournal(config.journalPath,{readOnly:true});try{assert.equal(journal.serverEvents(0,100).length,events.length);}finally{journal.close();}
  assert.deepEqual(logical(f.poolPath),before);
 }finally{osProbe.mock.restore();syncBuiltinESMExports();f.close();}
});
