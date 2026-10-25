import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';

// Provider protocol, role, and execution location are deliberately independent.
export type ExecutionEndpoint =
  | { id:string; kind:'local'; repositoryMap:Record<string,string> }
  | { id:string; kind:'ssh-stdio'; hostAlias:string; repositoryMap:Record<string,string>; enabled:false }
  | { id:string; kind:'external-agent'; protocol:'unverified'; enabled:false };
export interface AgentBinding { instanceId:string; provider:string; role:'manager'|'worker'|'reviewer'; endpointId:string }
export interface Capabilities { protocolVersion:1; provider:string; capabilities:string[]; quota:'unknown'; auth:'available'|'unavailable'|'unknown' }
export interface DependencyArtifact { taskId:string; commitSha:string; acceptanceId:string; reviewId:string }
export interface Dispatch {
  version:1; messageId:string; taskId:string; runId:string; attemptId:string;
  endpointId:string; fence:number; repositoryId:string; baseSha:string;
  provider:string; requiredCapabilities:string[]; instructions:string;
  dependencies:DependencyArtifact[];
  snapshot?:{ kind:'explicit-commit'; commitSha:string; approvalId:string };
}
export interface Ack { messageId:string; runId:string; attemptId:string; accepted:true; meaning:'durably-received-only' }
export interface RemoteRun { runId:string; attemptId:string; taskId:string; state:'queued'|'executing'|'completed'|'failed'|'unknown'; fence:number; result:unknown }
export interface RemoteEvent { eventId:number; runId:string; attemptId:string; runSeq:number; kind:string; data:unknown }
const identifier = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const sha = /^[a-f0-9]{40}$|^[a-f0-9]{64}$/;
function redactProtocolData(value:unknown):unknown {
  if(typeof value==='string')return value.replace(/\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,})\b/g,'[REDACTED]').replace(/(bearer\s+)[^\s"',;]+/gi,'$1[REDACTED]').replace(/\b(token|password|secret|api[_-]?key)\s*[:=]\s*[^\s,;]+/gi,'$1=[REDACTED]');
  if(Array.isArray(value))return value.map(redactProtocolData);
  if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,/^(?:authorization|cookie|password|secret|token|api[_-]?key|access[_-]?token|refresh[_-]?token)$/i.test(k)?'[REDACTED]':redactProtocolData(v)]));
  return value;
}
function id(value:unknown,name:string) { if(typeof value!=='string'||!identifier.test(value))throw new Error(`Invalid ${name}`); }
function digest(value:unknown):string {
  const canonical=(v:any):string=>Array.isArray(v)?`[${v.map(canonical).join(',')}]`:v&&typeof v==='object'?`{${Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')}}`:JSON.stringify(v);
  return createHash('sha256').update(canonical(value)).digest('hex');
}
export function validateDispatch(d:Dispatch):void {
  if(!d||d.version!==1)throw new Error('Unsupported protocol');
  for(const k of ['messageId','taskId','runId','attemptId','endpointId','repositoryId','provider'] as const)id(d[k],k);
  if(!Number.isSafeInteger(d.fence)||d.fence<1)throw new Error('Invalid fence');
  if(!sha.test(d.baseSha))throw new Error('Fixed base SHA required');
  if(typeof d.instructions!=='string'||d.instructions.length>65536)throw new Error('Invalid bounded instructions');
  if(!Array.isArray(d.requiredCapabilities)||d.requiredCapabilities.length>32)throw new Error('Invalid capability request');
  d.requiredCapabilities.forEach(c=>id(c,'capability'));
  if(!Array.isArray(d.dependencies)||d.dependencies.length>128)throw new Error('Invalid dependency artifacts');
  for(const a of d.dependencies){id(a.taskId,'dependency task');id(a.acceptanceId,'acceptance evidence');id(a.reviewId,'review evidence');if(!sha.test(a.commitSha))throw new Error('Accepted dependency commit required');}
  if(d.snapshot&&(d.snapshot.kind!=='explicit-commit'||!sha.test(d.snapshot.commitSha)||!identifier.test(d.snapshot.approvalId)))throw new Error('Explicit approved snapshot commit required');
  const allowed=new Set(['version','messageId','taskId','runId','attemptId','endpointId','fence','repositoryId','baseSha','provider','requiredCapabilities','instructions','dependencies','snapshot']);
  if(Object.keys(d).some(k=>!allowed.has(k)))throw new Error('Unknown dispatch field (paths/credentials are not accepted)');
}
export function negotiate(remote:Capabilities, provider:string, required:string[]):void {
  if(remote.protocolVersion!==1||remote.provider!==provider||remote.auth!=='available'||required.some(c=>!remote.capabilities.includes(c)))throw new Error('Endpoint capability/auth negotiation failed');
}
function open(path:string){const db=new DatabaseSync(path);db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=1000;');return db;}
export class RunnerJournal {
  db:DatabaseSync; endpointId:string; capabilities:Capabilities;
  constructor(path:string,endpointId:string,capabilities:Capabilities){
    id(endpointId,'endpoint');this.endpointId=endpointId;this.capabilities=capabilities;this.db=open(path);
    this.db.exec(`CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value INTEGER NOT NULL);
      INSERT OR IGNORE INTO metadata VALUES('fence',0);
      CREATE TABLE IF NOT EXISTS inbox(message_id TEXT PRIMARY KEY,digest TEXT NOT NULL,ack TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS remote_runs(run_id TEXT PRIMARY KEY,attempt_id TEXT NOT NULL UNIQUE,task_id TEXT NOT NULL,state TEXT NOT NULL,fence INTEGER NOT NULL,message TEXT NOT NULL,result TEXT);
      CREATE TABLE IF NOT EXISTS remote_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,attempt_id TEXT NOT NULL,run_seq INTEGER NOT NULL,kind TEXT NOT NULL,data TEXT NOT NULL,UNIQUE(run_id,run_seq));`);
    this.db.exec('CREATE TABLE IF NOT EXISTS runner_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),endpoint_id TEXT NOT NULL)');
    this.db.prepare('INSERT OR IGNORE INTO runner_identity VALUES(1,?)').run(endpointId);
    if((this.db.prepare('SELECT endpoint_id FROM runner_identity').get() as any).endpoint_id!==endpointId){this.db.close();throw new Error('Runner journal belongs to a different endpoint');}
  }
  close(){this.db.close();}
  private transaction<T>(fn:()=>T):T {this.db.exec('BEGIN IMMEDIATE');try{const r=fn();this.db.exec('COMMIT');return r;}catch(e){this.db.exec('ROLLBACK');throw e;}}
  private event(runId:string,attemptId:string,kind:string,data:unknown){
    this.db.prepare('INSERT INTO remote_events(run_id,attempt_id,run_seq,kind,data) SELECT ?,?,COALESCE(MAX(run_seq),0)+1,?,? FROM remote_events WHERE run_id=?').run(runId,attemptId,kind,JSON.stringify(redactProtocolData(data)),runId);
  }
  receive(d:Dispatch):Ack {
    validateDispatch(d);if(d.endpointId!==this.endpointId)throw new Error('Wrong execution endpoint');
    negotiate(this.capabilities,d.provider,d.requiredCapabilities);
    return this.transaction(()=>{
      const fingerprint=digest(d),old=this.db.prepare('SELECT digest,ack FROM inbox WHERE message_id=?').get(d.messageId) as any;
      if(old){if(old.digest!==fingerprint)throw new Error('Message ID content conflict');return JSON.parse(old.ack);}
      const fence=(this.db.prepare("SELECT value FROM metadata WHERE key='fence'").get() as any).value;
      if(d.fence<fence)throw new Error('Stale fencing token');
      if(this.db.prepare('SELECT run_id FROM remote_runs WHERE run_id=? OR attempt_id=?').get(d.runId,d.attemptId))throw new Error('Run/attempt already exists; query instead of redispatch');
      this.db.prepare("UPDATE metadata SET value=? WHERE key='fence'").run(d.fence);
      const ack:Ack={messageId:d.messageId,runId:d.runId,attemptId:d.attemptId,accepted:true,meaning:'durably-received-only'};
      this.db.prepare('INSERT INTO remote_runs VALUES(?,?,?,?,?,?,NULL)').run(d.runId,d.attemptId,d.taskId,'queued',d.fence,JSON.stringify(d));
      this.db.prepare('INSERT INTO inbox VALUES(?,?,?)').run(d.messageId,fingerprint,JSON.stringify(ack));
      this.event(d.runId,d.attemptId,'received',{messageId:d.messageId,fence:d.fence});return ack;
    });
  }
  query(runId:string):RemoteRun|null {
    const r=this.db.prepare('SELECT * FROM remote_runs WHERE run_id=?').get(runId) as any;
    return r?{runId:r.run_id,attemptId:r.attempt_id,taskId:r.task_id,state:r.state,fence:r.fence,result:r.result?JSON.parse(r.result):null}:null;
  }
  events(after=0,limit=100):RemoteEvent[]{
    if(!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>1000)throw new Error('Invalid event cursor/limit');
    return (this.db.prepare('SELECT * FROM remote_events WHERE event_id>? ORDER BY event_id LIMIT ?').all(after,limit) as any[]).map(e=>({eventId:e.event_id,runId:e.run_id,attemptId:e.attempt_id,runSeq:e.run_seq,kind:e.kind,data:JSON.parse(e.data)}));
  }
  async execute(runId:string,handler:(d:Dispatch)=>Promise<unknown>):Promise<void>{
    const d=this.transaction(()=>{
      const r=this.db.prepare('SELECT * FROM remote_runs WHERE run_id=?').get(runId) as any;
      if(!r||r.state!=='queued')return null; // Never replay executing/unknown/completed.
      const fence=(this.db.prepare("SELECT value FROM metadata WHERE key='fence'").get() as any).value;
      if(r.fence<fence){this.db.prepare("UPDATE remote_runs SET state='unknown' WHERE run_id=?").run(runId);this.event(runId,r.attempt_id,'fenced',{reason:'Dispatch ownership superseded; investigate'});return null;}
      this.db.prepare("UPDATE remote_runs SET state='executing' WHERE run_id=?").run(runId);this.event(runId,r.attempt_id,'execution.started',{});return JSON.parse(r.message) as Dispatch;
    });
    if(!d)return;
    try{
      const rawResult=await handler(d);if(JSON.stringify(rawResult??null).length>32768)throw new Error('Execution result exceeds protocol limit');const result=redactProtocolData(rawResult);
      this.transaction(()=>{
        const fence=(this.db.prepare("SELECT value FROM metadata WHERE key='fence'").get() as any).value;
        const stale=d.fence<fence;
        this.db.prepare('UPDATE remote_runs SET state=?,result=? WHERE run_id=?').run(stale?'unknown':'completed',JSON.stringify(result??null),runId);
        this.event(runId,d.attemptId,stale?'execution.fenced':'execution.completed',{meaning:'Execution evidence only; never acceptance',result:result??null});
      });
    }catch(error){
      // A thrown handler may already have performed effects: fail closed to unknown.
      this.transaction(()=>{this.db.prepare("UPDATE remote_runs SET state='unknown' WHERE run_id=?").run(runId);this.event(runId,d.attemptId,'execution.unknown',{reason:'Handler interrupted; side effects may exist'});});
      throw error;
    }
  }
  recover(){return this.transaction(()=>{const rows=this.db.prepare("SELECT run_id,attempt_id FROM remote_runs WHERE state='executing'").all() as any[];for(const r of rows){this.db.prepare("UPDATE remote_runs SET state='unknown' WHERE run_id=?").run(r.run_id);this.event(r.run_id,r.attempt_id,'recovery.unknown',{reason:'Runner restart; no death inference from heartbeat or PID'});}return rows.map(r=>r.run_id);});}
}
export interface RunnerTransport {
  send(message:Dispatch):Promise<Ack>;
  query(runId:string):Promise<RemoteRun|null>;
  events(after:number):Promise<RemoteEvent[]>;
}
export class DisabledSshTransport implements RunnerTransport {
  async send(_message:Dispatch):Promise<Ack>{throw new Error('SSH integration disabled: explicit target, auth, repository mapping and approval required');}
  async query(_id:string):Promise<RemoteRun|null>{throw new Error('SSH integration disabled');}
  async events(_after:number):Promise<RemoteEvent[]>{throw new Error('SSH integration disabled');}
}
export class FakeTransport implements RunnerTransport {
  connected=true; loseNextAck=false; runner:RunnerJournal; handler:(d:Dispatch)=>Promise<unknown>;
  constructor(runner:RunnerJournal,handler:(d:Dispatch)=>Promise<unknown>){this.runner=runner;this.handler=handler;}
  private requireConnection(){if(!this.connected)throw new Error('Simulated disconnection');}
  async send(d:Dispatch){this.requireConnection();const ack=this.runner.receive(d);await this.runner.execute(d.runId,this.handler);if(this.loseNextAck){this.loseNextAck=false;this.connected=false;throw new Error('Simulated lost ACK after execution');}return ack;}
  async query(id:string){this.requireConnection();return this.runner.query(id);}
  async events(after:number){this.requireConnection();return this.runner.events(after);}
}
export class RemoteCoordinator {
  db:DatabaseSync; transport:RunnerTransport; endpointId:string;
  constructor(path:string,transport:RunnerTransport,endpointId:string){id(endpointId,'endpoint');this.endpointId=endpointId;this.db=open(path);this.transport=transport;this.db.exec(`CREATE TABLE IF NOT EXISTS dispatches(run_id TEXT PRIMARY KEY,message TEXT NOT NULL,state TEXT NOT NULL,ack TEXT);
    CREATE TABLE IF NOT EXISTS cursor(singleton INTEGER PRIMARY KEY CHECK(singleton=1),event_id INTEGER NOT NULL);INSERT OR IGNORE INTO cursor VALUES(1,0);
    CREATE TABLE IF NOT EXISTS observations(event_id INTEGER PRIMARY KEY,data TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS endpoint_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),endpoint_id TEXT NOT NULL);`);
    this.db.prepare('INSERT OR IGNORE INTO endpoint_identity VALUES(1,?)').run(endpointId);
    if((this.db.prepare('SELECT endpoint_id FROM endpoint_identity').get() as any).endpoint_id!==endpointId){this.db.close();throw new Error('Coordinator journal belongs to a different endpoint');}
  }
  close(){this.db.close();}
  async dispatch(d:Dispatch){
    validateDispatch(d);if(d.endpointId!==this.endpointId)throw new Error('Coordinator endpoint mismatch');this.db.prepare('INSERT INTO dispatches VALUES(?,?,?,NULL)').run(d.runId,JSON.stringify(d),'sending');
    try{const ack=await this.transport.send(d);if(ack.accepted!==true||ack.messageId!==d.messageId||ack.runId!==d.runId||ack.attemptId!==d.attemptId||ack.meaning!=='durably-received-only')throw new Error('ACK identity mismatch');this.db.prepare('UPDATE dispatches SET state=?,ack=? WHERE run_id=?').run('received',JSON.stringify(ack),d.runId);return ack;}
    catch(e){this.db.prepare("UPDATE dispatches SET state='unknown' WHERE run_id=?").run(d.runId);throw e;}
  }
  status(runId:string):string|null{return (this.db.prepare('SELECT state FROM dispatches WHERE run_id=?').get(runId) as any)?.state??null;}
  cursor():number{return (this.db.prepare('SELECT event_id FROM cursor WHERE singleton=1').get() as any).event_id;}
  async reconcile(runId:string){
    const stored=this.db.prepare('SELECT message FROM dispatches WHERE run_id=?').get(runId) as any;if(!stored)throw new Error('Unknown coordinator run');const message=JSON.parse(stored.message) as Dispatch;
    // Reconnection performs observations only; absence never authorizes resend.
    try{
      const run=await this.transport.query(runId);
      if(run&&(run.runId!==runId||run.attemptId!==message.attemptId||run.taskId!==message.taskId||!['queued','executing','completed','failed','unknown'].includes(run.state)||!Number.isSafeInteger(run.fence)||run.fence<1))throw new Error('Remote run identity mismatch');
      const events=await this.transport.events(this.cursor());
      this.db.exec('BEGIN IMMEDIATE');try{
        if(!Array.isArray(events)||events.length>1000)throw new Error('Invalid event page');
        let expected=this.cursor()+1;const runSequences=new Map<string,number>();
        for(const original of events){
          const e=original?{...original,data:redactProtocolData(original.data)}:original;
          if(!e||!Number.isSafeInteger(e.eventId)||e.eventId<1||!Number.isSafeInteger(e.runSeq)||e.runSeq<1)throw new Error('Invalid event sequence');
          id(e.runId,'event run');id(e.attemptId,'event attempt');id(e.kind,'event kind');
          const encoded=JSON.stringify(e);if(encoded.length>65536)throw new Error('Event payload too large');
          const prior=this.db.prepare('SELECT data FROM observations WHERE event_id=?').get(e.eventId) as any;
          if(prior){if(digest(JSON.parse(prior.data))!==digest(e))throw new Error('Event identity content conflict');continue;}
          if(e.eventId!==expected)throw new Error('Non-contiguous event cursor');expected++;
          const known=this.db.prepare('SELECT message FROM dispatches WHERE run_id=?').get(e.runId) as any;
          if(known&&JSON.parse(known.message).attemptId!==e.attemptId)throw new Error('Event attempt identity mismatch');
          const previous=this.db.prepare("SELECT data FROM observations WHERE json_extract(data,'$.runId')=? ORDER BY event_id DESC LIMIT 1").get(e.runId) as any;
          if(previous&&JSON.parse(previous.data).attemptId!==e.attemptId)throw new Error('Event run identity conflict');
          const seq=runSequences.get(e.runId)??(previous?JSON.parse(previous.data).runSeq:0);
          if(e.runSeq!==seq+1)throw new Error('Non-contiguous run sequence');runSequences.set(e.runId,e.runSeq);
          this.db.prepare('INSERT INTO observations VALUES(?,?)').run(e.eventId,encoded);
        }
        if(events.length)this.db.prepare('UPDATE cursor SET event_id=? WHERE singleton=1').run(Math.max(this.cursor(),...events.map(e=>e.eventId)));
        this.db.prepare('UPDATE dispatches SET state=? WHERE run_id=?').run(run?.state??'unknown',runId);this.db.exec('COMMIT');
      }catch(e){this.db.exec('ROLLBACK');throw e;}
      return {run:run?{...run,result:redactProtocolData(run.result)}:null,state:run?.state??'unknown',cursor:this.cursor(),events:events.map(e=>({...e,data:redactProtocolData(e.data)}))};
    }catch(e){this.db.prepare("UPDATE dispatches SET state='unknown' WHERE run_id=?").run(runId);throw e;}
  }
}

export function remotePing(){return {product:'AgentFleet',protocolVersion:1,direction:'runner-initiated-pull',transport:'ssh-stdio',diagnosticOnly:true,modelInvoked:false,quota:'unknown',ssh:'unknown',loopbackTunnel:'unknown',directPort:'unknown'};}
export type ReportKind='heartbeat'|'progress'|'result'|'unknown';
export interface ReverseClaim { messageId:string; claimId:string; state:string; dispatch:Dispatch }
// This queue lives on the Mac coordinator. Muse initiates every operation.
export class ReverseCoordinator {
  db:DatabaseSync; readonly:boolean;
  constructor(path:string,options:{readOnly?:boolean}={}){this.readonly=options.readOnly??false;this.db=this.readonly?new DatabaseSync(path,{readOnly:true}):open(path);if(this.readonly)return;this.db.exec(`
    CREATE TABLE IF NOT EXISTS offers(message_id TEXT PRIMARY KEY,digest TEXT NOT NULL,dispatch TEXT NOT NULL,endpoint_id TEXT NOT NULL,run_id TEXT NOT NULL UNIQUE,attempt_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,claim_id TEXT UNIQUE,ack TEXT);
    CREATE TABLE IF NOT EXISTS reverse_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT NOT NULL,run_id TEXT NOT NULL,run_seq INTEGER NOT NULL,kind TEXT NOT NULL,data TEXT NOT NULL,UNIQUE(run_id,run_seq));
    CREATE TABLE IF NOT EXISTS reports(source_event_id TEXT PRIMARY KEY,message_id TEXT NOT NULL,digest TEXT NOT NULL,event_id INTEGER NOT NULL);`);}
  close(){this.db.close();}
  private transaction<T>(fn:()=>T):T{if(this.readonly)throw new Error('Observer access cannot mutate reverse queue');this.db.exec('BEGIN IMMEDIATE');try{const result=fn();this.db.exec('COMMIT');return result;}catch(e){this.db.exec('ROLLBACK');throw e;}}
  private row(messageId:string):any {id(messageId,'message');const row=this.db.prepare('SELECT * FROM offers WHERE message_id=?').get(messageId);if(!row)throw new Error('Unknown offer');return row;}
  private fence(endpointId:string):number{return Number((this.db.prepare("SELECT COALESCE(MAX(json_extract(dispatch,'$.fence')),0) AS fence FROM offers WHERE endpoint_id=?").get(endpointId) as any).fence);}
  private append(row:any,kind:string,data:unknown):number {
    this.db.prepare('INSERT INTO reverse_events(message_id,run_id,run_seq,kind,data) SELECT ?,?,COALESCE(MAX(run_seq),0)+1,?,? FROM reverse_events WHERE run_id=?').run(row.message_id,row.run_id,kind,JSON.stringify(redactProtocolData(data)),row.run_id);
    return Number((this.db.prepare('SELECT last_insert_rowid() AS id').get() as any).id);
  }
  offer(d:Dispatch){validateDispatch(d);return this.transaction(()=>{
    const old=this.db.prepare('SELECT digest FROM offers WHERE message_id=?').get(d.messageId) as any;
    if(old){if(old.digest!==digest(d))throw new Error('Message ID content conflict');return this.reconcile(d.messageId);}
    const pending=this.db.prepare("SELECT message_id FROM offers WHERE json_extract(dispatch,'$.taskId')=? AND state!='execution-failed' LIMIT 1").get(d.taskId) as any;
    if(pending)throw new Error('Task already has unresolved or completed execution; reconcile original offer before another attempt');
    if(d.fence<this.fence(d.endpointId))throw new Error('Stale fencing token');
    this.db.prepare('INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,NULL)').run(d.messageId,digest(d),JSON.stringify(d),d.endpointId,d.runId,d.attemptId,'offered',null);
    this.append(this.row(d.messageId),'offered',{endpointId:d.endpointId});return this.reconcile(d.messageId);
  });}
  pull(endpointId:string,capabilities:Capabilities,limit=10):Dispatch[]{
    id(endpointId,'endpoint');if(!Number.isInteger(limit)||limit<1||limit>100)throw new Error('Invalid pull limit');
    const rows=this.db.prepare("SELECT dispatch FROM offers WHERE endpoint_id=? AND state='offered' ORDER BY rowid LIMIT ?").all(endpointId,limit) as any[];
    return rows.map(r=>JSON.parse(r.dispatch) as Dispatch).filter(d=>{try{negotiate(capabilities,d.provider,d.requiredCapabilities);return true;}catch{return false;}});
  }
  claim(messageId:string,endpointId:string,claimId:string,capabilities:Capabilities):ReverseClaim {
    id(claimId,'claim');id(endpointId,'endpoint');return this.transaction(()=>{
      const row=this.row(messageId);if(row.endpoint_id!==endpointId)throw new Error('Wrong execution endpoint');
      const dispatch=JSON.parse(row.dispatch) as Dispatch;negotiate(capabilities,dispatch.provider,dispatch.requiredCapabilities);
      if(row.claim_id){if(row.claim_id!==claimId)throw new Error('Already claimed; reconcile original claim, never reassign unknown effects');return {messageId,claimId,state:row.state,dispatch};}
      if(dispatch.fence<this.fence(endpointId))throw new Error('Stale fencing token; do not claim');
      if(row.state!=='offered')throw new Error('Offer cannot be claimed');
      this.db.prepare("UPDATE offers SET state='claimed',claim_id=? WHERE message_id=?").run(claimId,messageId);this.append(row,'claimed',{claimId,endpointId});
      return {messageId,claimId,state:'claimed',dispatch};
    });
  }
  ack(messageId:string,claimId:string):Ack {return this.transaction(()=>{
    const row=this.row(messageId);if(!claimId||row.claim_id!==claimId)throw new Error('Claim identity mismatch');
    if(row.ack)return JSON.parse(row.ack);
    const ack:Ack={messageId,runId:row.run_id,attemptId:row.attempt_id,accepted:true,meaning:'durably-received-only'};
    this.db.prepare("UPDATE offers SET state='received',ack=? WHERE message_id=?").run(JSON.stringify(ack),messageId);this.append(row,'runner.ack',{claimId,meaning:ack.meaning});return ack;
  });}
  report(messageId:string,claimId:string,sourceEventId:string,kind:ReportKind,data:unknown):{eventId:number;state:string} {
    id(sourceEventId,'source event');if(!['heartbeat','progress','result','unknown'].includes(kind))throw new Error('Unsupported report kind');
    const encoded=JSON.stringify(data);if(encoded===undefined||encoded.length>16384)throw new Error('Report exceeds protocol size limit');
    if(kind==='result'){
      const r=data as any;if(!r||!Number.isInteger(r.exitCode)||r.exitCode<0||r.exitCode>255)throw new Error('Result requires exitCode');
      if(r.commitSha!==undefined&&!sha.test(r.commitSha))throw new Error('Result commit must be immutable SHA');
    }
    return this.transaction(()=>{
      const row=this.row(messageId);if(!claimId||row.claim_id!==claimId||!row.ack)throw new Error('ACKed claim required');
      const fingerprint=digest({messageId,claimId,kind,data}),old=this.db.prepare('SELECT * FROM reports WHERE source_event_id=?').get(sourceEventId) as any;
      if(old){if(old.message_id!==messageId||old.digest!==fingerprint)throw new Error('Source event ID content conflict');return {eventId:old.event_id,state:row.state};}
      if(['execution-completed','execution-failed'].includes(row.state)&&kind!=='heartbeat')throw new Error('Terminal execution report already recorded');
      const stale=JSON.parse(row.dispatch).fence<this.fence(row.endpoint_id);
      const state=kind==='result'?(stale?'unknown':(data as any).exitCode===0?'execution-completed':'execution-failed'):kind==='unknown'?'unknown':row.state;
      // Heartbeat does not transition execution or authorize reallocation.
      const eventId=this.append(row,`runner.${kind}`,data);this.db.prepare('INSERT INTO reports VALUES(?,?,?,?)').run(sourceEventId,messageId,fingerprint,eventId);
      this.db.prepare('UPDATE offers SET state=? WHERE message_id=?').run(state,messageId);return {eventId,state};
    });
  }
  disconnected(messageId:string){return this.transaction(()=>{const row=this.row(messageId);if(row.claim_id&&!['execution-completed','execution-failed'].includes(row.state)){this.db.prepare("UPDATE offers SET state='unknown' WHERE message_id=?").run(messageId);this.append(row,'connection.unknown',{reason:'Disconnected; original claim retained, no requeue'});}return this.reconcile(messageId);});}
  reconcile(messageId:string,endpointId?:string){const row=this.row(messageId);if(endpointId&&row.endpoint_id!==endpointId)throw new Error('Wrong execution endpoint');return {messageId,runId:row.run_id,attemptId:row.attempt_id,endpointId:row.endpoint_id,claimId:row.claim_id,state:row.state,ack:row.ack?JSON.parse(row.ack):null};}
  list(endpointId?:string){if(endpointId!==undefined)id(endpointId,'endpoint');const rows=(endpointId===undefined?this.db.prepare('SELECT * FROM offers ORDER BY rowid').all():this.db.prepare('SELECT * FROM offers WHERE endpoint_id=? ORDER BY rowid').all(endpointId)) as any[];return rows.map(row=>{const d=JSON.parse(row.dispatch) as Dispatch;return {messageId:row.message_id,taskId:d.taskId,runId:row.run_id,attemptId:row.attempt_id,endpointId:row.endpoint_id,claimId:row.claim_id,state:row.state,provider:d.provider,repositoryId:d.repositoryId,baseSha:d.baseSha,fence:d.fence,acknowledged:row.ack!==null};});}
  events(endpointId:string,after=0,limit=100){id(endpointId,'endpoint');if(!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>1000)throw new Error('Invalid cursor/limit');return (this.db.prepare('SELECT e.* FROM reverse_events e JOIN offers o USING(message_id) WHERE o.endpoint_id=? AND e.event_id>? ORDER BY e.event_id LIMIT ?').all(endpointId,after,limit) as any[]).map(e=>({eventId:e.event_id,messageId:e.message_id,runId:e.run_id,runSeq:e.run_seq,kind:e.kind,data:JSON.parse(e.data)}));}
}
