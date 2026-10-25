import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, chmodSync, realpathSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { randomUUID,createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import type { ChildProcessWithoutNullStreams } from 'node:child_process';

export interface ConnectorIdentity { ownerId:string; pid:number; processStart:string; bootId:string }
export type Identity = ConnectorIdentity;
export interface TerminationEvidence { identity:ConnectorIdentity; positivelyVerified:true; method:'supervisor-exit'|'vm-lifecycle'; record:string; verifiedBy:string }
export interface ConnectorPayload { version:1; op:string; [key:string]:unknown }
export interface ConnectorFrame { id:string; generation:number; payload:ConnectorPayload }
export type Frame = ConnectorFrame;
export interface ConnectorReply { id:string; generation:number; ok:boolean; data?:unknown; error?:unknown }
export type Reply = ConnectorReply;
export interface ConnectorTransport { readonly connectionKey?:string;connect(generation:number):Promise<void>; request(frame:ConnectorFrame):Promise<ConnectorReply>; close():Promise<void> }
export type Transport = ConnectorTransport;
export type OutboxState = 'queued'|'sent'|'unknown'|'replied';
export interface OutboxEntry { seq:number; id:string; payload:ConnectorPayload; readOnly:boolean; state:OutboxState; generation:number|null }
export interface InboxEntry { cursor:number; id:string; reply:ConnectorReply }
export interface WatchdogState { attempts:number; nextAttemptAt:number; circuitOpen:boolean; blockedReason:string|null }
export interface SynchronizationEvidence {helloConfirmed:true;replayCursor:number}
export interface ServerEvent {event_id:number;[key:string]:unknown}
export interface ConnectorStatus { disabled:boolean; connectionKey:string|null;owner:ConnectorIdentity|null; generation:number; synchronizedGeneration:number|null;serverCursor:number;outbox:OutboxEntry[]; inbox:InboxEntry[]; cursor:number; watchdog:WatchdogState }
export type ConnectorState = {state:'disabled'|'idle'|'replied'|'blocked'|'disconnected';id?:string;error?:string;code?:ConnectorErrorCode};
export type ConnectorErrorCode = 'DISABLED'|'LOCKED'|'AUTH_EXPIRED'|'MISSING_SSH_CONFIG'|'HOST_KEY_MISMATCH'|'FIREWALL'|'DEPENDENCY'|'DISCONNECTED'|'PROTOCOL'|'SYNCHRONIZATION_REQUIRED';
export class ConnectorError extends Error {
  readonly code:ConnectorErrorCode;
  constructor(code:ConnectorErrorCode,message:string) { super(redact(message));this.name='ConnectorError';this.code=code; }
}
const blockedCodes = new Set<ConnectorErrorCode>(['AUTH_EXPIRED','MISSING_SSH_CONFIG','HOST_KEY_MISMATCH','FIREWALL','DEPENDENCY','LOCKED','PROTOCOL']);
const defaultWatchdog:WatchdogState = {attempts:0,nextAttemptAt:0,circuitOpen:false,blockedReason:null};
const readonlyOps=new Set(['hello','events','reconcile','reconcile-claim']);
function redact(text:string):string {
  return text.replace(/\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,})\b/g,'[REDACTED]').replace(/(Bearer\s+)\S+/gi,'$1[REDACTED]').replace(/((?:password|token|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+/gi,'$1[REDACTED]');
}
function redactData(value:unknown):unknown {
  if(typeof value==='string')return redact(value);
  if(Array.isArray(value))return value.map(redactData);
  if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,/^(?:token|password|secret|authorization|cookie|api[_-]?key)$/i.test(key)?'[REDACTED]':redactData(item)]));
  return value;
}
function text(value:unknown,max=512):string {
  if(typeof value!=='string'||!value.trim()||value.length>max||/[\x00-\x1f\x7f]/.test(value))throw new Error('connector: invalid bounded string');return value;
}
function integer(value:unknown,min=0,max=Number.MAX_SAFE_INTEGER):number {
  if(!Number.isSafeInteger(value)||(value as number)<min||(value as number)>max)throw new Error('connector: invalid bounded integer');return value as number;
}
function encode(value:unknown,max=65536):string {
  const result=JSON.stringify(value);if(result===undefined||Buffer.byteLength(result)>max)throw new Error('connector: JSON frame exceeds capacity');return result;
}
function jsonObject(value:unknown):Record<string,unknown> {
  if(!value||typeof value!=='object'||Array.isArray(value))throw new Error('connector: expected object');return value as Record<string,unknown>;
}
function identity(value:ConnectorIdentity):ConnectorIdentity {
  const v=jsonObject(value);if(Object.keys(v).some(k=>!['ownerId','pid','processStart','bootId'].includes(k)))throw new Error('connector: unexpected owner identity field');
  return {ownerId:text(v.ownerId),pid:integer(v.pid,1),processStart:text(v.processStart),bootId:text(v.bootId)};
}
function sameIdentity(a:ConnectorIdentity|null,b:ConnectorIdentity):boolean { return a!==null&&encode(a)===encode(identity(b)); }
function evidence(value:TerminationEvidence,owner:ConnectorIdentity|null):TerminationEvidence {
  if(!owner||!value||value.positivelyVerified!==true||!['supervisor-exit','vm-lifecycle'].includes(value.method)||!sameIdentity(owner,value.identity))throw new ConnectorError('LOCKED','connector: exact positive owner termination evidence required; missing PID or heartbeat is insufficient');
  return {identity:identity(value.identity),positivelyVerified:true,method:value.method,record:text(value.record,2048),verifiedBy:text(value.verifiedBy)};
}
function payload(value:ConnectorPayload):ConnectorPayload {
  const p=jsonObject(value);if(p.version!==1)throw new Error('connector: unsupported payload version');text(p.op,100);encode(p,49152);return JSON.parse(encode(p)) as ConnectorPayload;
}
function reply(value:ConnectorReply):ConnectorReply {
  const r=jsonObject(value);if(Object.keys(r).some(k=>!['id','generation','ok','data','error'].includes(k))||typeof r.ok!=='boolean')throw new ConnectorError('PROTOCOL','connector: malformed reply');
  text(r.id);integer(r.generation,1);if(r.ok&&Object.hasOwn(r,'error'))throw new ConnectorError('PROTOCOL','connector: contradictory reply');
  encode(r);return JSON.parse(encode(r)) as ConnectorReply;
}
function classified(error:unknown):ConnectorError {
  if(error instanceof ConnectorError)return error;
  return new ConnectorError('DISCONNECTED',error instanceof Error?error.message:String(error));
}
async function bounded<T>(operation:Promise<T>,timeoutMs:number,label:string):Promise<T>{
  let timer:ReturnType<typeof setTimeout>|undefined;
  try{return await Promise.race([operation,new Promise<T>((_,reject)=>{timer=setTimeout(()=>reject(new ConnectorError('DISCONNECTED',`${label} deadline exceeded`)),timeoutMs);})]);}finally{if(timer)clearTimeout(timer);}
}
export function sshConnectionKey(alias:string,command:string[]):string{return createHash('sha256').update(JSON.stringify({alias,command,protocolVersion:1})).digest('hex');}

/** No credential material is read or copied. Proof objects must come from a trusted supervisor. */
export class ConnectorJournal {
  readonly path:string;
  private db:DatabaseSync;
  private readOnly:boolean;
  constructor(path:string,options:{readOnly?:boolean}={}) {
    this.readOnly=options.readOnly??false;
    if(!this.readOnly&&path!==':memory:')mkdirSync(dirname(path),{recursive:true,mode:0o700});
    this.db=new DatabaseSync(path,{readOnly:this.readOnly});this.path=path===':memory:'?`:memory:${randomUUID()}`:realpathSync(resolve(path));
    this.db.exec('PRAGMA busy_timeout=5000;');
    if(!this.readOnly){
      this.db.exec(`PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS connector_state(id INTEGER PRIMARY KEY CHECK(id=1),disabled INTEGER NOT NULL,owner TEXT,generation INTEGER NOT NULL,watchdog TEXT NOT NULL,server_cursor INTEGER NOT NULL DEFAULT 0,synchronized_generation INTEGER,connection_key TEXT);
        CREATE TABLE IF NOT EXISTS connector_outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,payload TEXT NOT NULL,read_only INTEGER NOT NULL,state TEXT NOT NULL,generation INTEGER);
        CREATE TABLE IF NOT EXISTS connector_inbox(cursor INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,reply TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS connector_audit(event_id INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS connector_server_events(event_id INTEGER PRIMARY KEY,value TEXT NOT NULL);`);
      const columns=new Set(this.db.prepare('PRAGMA table_info(connector_state)').all().map(row=>row.name));
      if(!columns.has('server_cursor'))this.db.exec('ALTER TABLE connector_state ADD COLUMN server_cursor INTEGER NOT NULL DEFAULT 0');
      if(!columns.has('synchronized_generation'))this.db.exec('ALTER TABLE connector_state ADD COLUMN synchronized_generation INTEGER');
      if(!columns.has('connection_key'))this.db.exec('ALTER TABLE connector_state ADD COLUMN connection_key TEXT');
      this.db.prepare('INSERT OR IGNORE INTO connector_state(id,disabled,owner,generation,watchdog) VALUES(1,0,NULL,0,?)').run(encode(defaultWatchdog));
      if(path!==':memory:')chmodSync(path,0o600);
    }
  }
  close():void {this.db.close();}
  private tx<T>(fn:()=>T):T {if(this.readOnly)throw new Error('connector: read-only observer');this.db.exec('BEGIN IMMEDIATE');try{const result=fn();this.db.exec('COMMIT');return result;}catch(error){this.db.exec('ROLLBACK');throw error;}}
  private state():{disabled:boolean;connectionKey:string|null;owner:ConnectorIdentity|null;generation:number;synchronizedGeneration:number|null;serverCursor:number;watchdog:WatchdogState} {
    const row=this.db.prepare('SELECT * FROM connector_state WHERE id=1').get()!;
    return {disabled:!!row.disabled,connectionKey:(row.connection_key as string|null)??null,owner:row.owner?JSON.parse(row.owner as string):null,generation:Number(row.generation),synchronizedGeneration:row.synchronized_generation===null?null:Number(row.synchronized_generation),serverCursor:Number(row.server_cursor),watchdog:JSON.parse(row.watchdog as string)};
  }
  private audit(kind:string,data:unknown):void {this.db.prepare('INSERT INTO connector_audit(kind,data) VALUES(?,?)').run(kind,encode(redactData(data)));}
  private demandOwner(owner:ConnectorIdentity):void {const state=this.state();if(state.disabled)throw new ConnectorError('DISABLED','connector is disabled');if(!sameIdentity(state.owner,owner))throw new ConnectorError('LOCKED','connector owner identity mismatch');}
  isDisabled():boolean {return this.state().disabled;}
  isOwner(owner:ConnectorIdentity):boolean {return sameIdentity(this.state().owner,owner);}
  bindConnectionKey(key:string):void{
    if(!/^[a-f0-9]{64}$/.test(key))throw new ConnectorError('PROTOCOL','immutable SHA256 connectionKey is required');
    this.tx(()=>{const s=this.state();if(s.disabled)throw new ConnectorError('DISABLED','connector is disabled');if(s.connectionKey&&s.connectionKey!==key)throw new ConnectorError('LOCKED','journal belongs to another fixed remote target; new journal or reviewed migration required');if(!s.connectionKey){if(s.serverCursor||this.db.prepare('SELECT 1 FROM connector_inbox LIMIT 1').get()||this.db.prepare("SELECT 1 FROM connector_outbox WHERE state<>'queued' LIMIT 1").get())throw new ConnectorError('LOCKED','existing unbound history requires explicit reviewed migration');this.db.prepare('UPDATE connector_state SET connection_key=? WHERE id=1').run(key);}});
  }
  blockConnection(owner:ConnectorIdentity,reason:string):void{this.tx(()=>{const s=this.state();if(!sameIdentity(s.owner,owner))throw new ConnectorError('LOCKED','connector owner identity mismatch');this.db.prepare('UPDATE connector_state SET watchdog=? WHERE id=1').run(encode({...s.watchdog,blockedReason:redact(reason).slice(0,2048)}));});}
  setDisabled(disabled:boolean):void {
    if(typeof disabled!=='boolean')throw new Error('connector: disabled must be boolean');
    this.tx(()=>{this.db.prepare('UPDATE connector_state SET disabled=? WHERE id=1').run(disabled?1:0);if(disabled){this.db.exec("UPDATE connector_outbox SET state='unknown' WHERE state='sent'");this.db.exec('UPDATE connector_state SET synchronized_generation=NULL WHERE id=1');}this.audit('disabled.changed',{disabled});});
  }
  acquire(value:ConnectorIdentity,proof?:TerminationEvidence):number {
    const owner=identity(value);
    return this.tx(()=>{
      const state=this.state();if(state.disabled)throw new ConnectorError('DISABLED','connector is disabled');
      if(sameIdentity(state.owner,owner))return state.generation;
      if(state.owner){const verified=evidence(proof!,state.owner);this.audit('owner.terminated',verified);}
      this.db.exec("UPDATE connector_outbox SET state='unknown' WHERE state='sent'");
      const generation=state.generation+1;this.db.prepare('UPDATE connector_state SET owner=?,generation=?,synchronized_generation=NULL WHERE id=1').run(encode(owner),generation);this.audit('owner.acquired',{owner,generation});return generation;
    });
  }
  releaseOwner(owner:ConnectorIdentity):void {
    this.tx(()=>{if(!sameIdentity(this.state().owner,owner))throw new ConnectorError('LOCKED','connector owner identity mismatch');if(this.db.prepare("SELECT 1 FROM connector_outbox WHERE state='sent' LIMIT 1").get())throw new ConnectorError('LOCKED','connector has an in-flight request');this.db.exec('UPDATE connector_state SET owner=NULL WHERE id=1');this.audit('owner.released',{ownerId:owner.ownerId});});
  }
  reconcileOwnerTermination(proof:TerminationEvidence):void {
    this.tx(()=>{if(this.state().disabled)throw new ConnectorError('DISABLED','connector is disabled');const verified=evidence(proof,this.state().owner);this.db.exec("UPDATE connector_outbox SET state='unknown' WHERE state='sent'");this.db.exec('UPDATE connector_state SET owner=NULL WHERE id=1');this.audit('owner.terminated',verified);});
  }
  nextGeneration(owner:ConnectorIdentity):number {
    return this.tx(()=>{this.demandOwner(owner);if(this.db.prepare("SELECT 1 FROM connector_outbox WHERE state='sent' LIMIT 1").get())throw new ConnectorError('LOCKED','connector has an unresolved in-flight request');const next=this.state().generation+1;this.db.prepare('UPDATE connector_state SET generation=?,synchronized_generation=NULL WHERE id=1').run(next);return next;});
  }
  enqueue(id:string,value:ConnectorPayload,options:{readOnly?:boolean}={}):OutboxEntry {
    text(id);const p=payload(value),serialized=encode(p);if(options.readOnly!==undefined&&typeof options.readOnly!=='boolean')throw new Error('connector: readOnly must be boolean');
    if(options.readOnly===true&&!readonlyOps.has(p.op))throw new Error('connector: operation is not in the confirmed read-only protocol');
    const readOnly=options.readOnly??readonlyOps.has(p.op);
    return this.tx(()=>{
      const old=this.getOutbox(id);if(old){if(encode(old.payload)!==serialized||old.readOnly!==readOnly)throw new Error('connector: immutable request id/payload conflict');return old;}
      if(Number(this.db.prepare('SELECT COUNT(*) AS n FROM connector_outbox').get()!.n)>=4096)throw new Error('connector: outbox history capacity reached');
      const row=this.db.prepare("INSERT INTO connector_outbox(id,payload,read_only,state,generation) VALUES(?,?,?,'queued',NULL)").run(id,serialized,readOnly?1:0);this.audit('request.queued',{id});return {seq:Number(row.lastInsertRowid),id,payload:p,readOnly,state:'queued',generation:null};
    });
  }
  private getOutbox(id:string):OutboxEntry|null {const r=this.db.prepare('SELECT * FROM connector_outbox WHERE id=?').get(id);return r?this.outboxRow(r):null;}
  private outboxRow(r:Record<string,unknown>):OutboxEntry {return {seq:Number(r.seq),id:r.id as string,payload:JSON.parse(r.payload as string),readOnly:!!r.read_only,state:r.state as OutboxState,generation:r.generation===null?null:Number(r.generation)};}
  claimQueued(owner:ConnectorIdentity,generation:number):ConnectorFrame|null {
    return this.tx(()=>{
      this.demandOwner(owner);if(this.state().generation!==generation)throw new ConnectorError('LOCKED','stale connector generation');
      if(this.db.prepare("SELECT 1 FROM connector_outbox WHERE state='sent' LIMIT 1").get())throw new ConnectorError('LOCKED','at most one outstanding request is allowed');
      const uncertain=!!this.db.prepare("SELECT 1 FROM connector_outbox WHERE state='unknown' LIMIT 1").get();
      const mutationAllowed=!uncertain&&this.state().synchronizedGeneration===generation;
      const row=this.db.prepare(`SELECT * FROM connector_outbox WHERE state='queued' ${mutationAllowed?'':'AND read_only=1'} ORDER BY seq LIMIT 1`).get();
      if(!row){if(!mutationAllowed&&this.db.prepare("SELECT 1 FROM connector_outbox WHERE state='queued' AND read_only=0 LIMIT 1").get())throw new ConnectorError('SYNCHRONIZATION_REQUIRED','startup/generation reconciliation and zero unknown outcomes are required before mutation');return null;}
      const q=this.outboxRow(row);this.db.prepare("UPDATE connector_outbox SET state='sent',generation=? WHERE id=? AND state='queued'").run(generation,q.id);this.audit('request.sent',{id:q.id,generation});return {id:q.id,generation,payload:q.payload};
    });
  }
  recordServerEvents(generation:number,events:ServerEvent[]):number {
    integer(generation,1);if(!Array.isArray(events)||events.length>1000)throw new Error('connector: event replay batch must have at most 1000 items');encode(events,65536);
    return this.tx(()=>{
      const s=this.state();if(s.disabled)throw new ConnectorError('DISABLED','connector is disabled');if(s.generation!==generation)throw new ConnectorError('PROTOCOL','stale event replay generation');
      let cursor=s.serverCursor;
      for(const value of events){const event=jsonObject(value),id=integer(event.event_id,1),serialized=encode(event);const old=this.db.prepare('SELECT value FROM connector_server_events WHERE event_id=?').get(id);
        if(old){if(old.value!==serialized)throw new ConnectorError('PROTOCOL','conflicting duplicate server event');continue;}
        if(id<=cursor)throw new ConnectorError('PROTOCOL','server event cursor regressed');
        this.db.prepare('INSERT INTO connector_server_events(event_id,value) VALUES(?,?)').run(id,serialized);cursor=id;
      }
      this.db.prepare('UPDATE connector_state SET server_cursor=? WHERE id=1').run(cursor);return cursor;
    });
  }
  markSynchronized(generation:number,proof:SynchronizationEvidence):void {
    integer(generation,1);if(!proof||proof.helloConfirmed!==true)throw new ConnectorError('PROTOCOL','validated hello confirmation required');integer(proof.replayCursor);
    this.tx(()=>{
      const s=this.state();if(s.disabled)throw new ConnectorError('DISABLED','connector is disabled');
      if(s.generation!==generation||proof.replayCursor!==s.serverCursor)throw new ConnectorError('PROTOCOL','synchronization generation/cursor mismatch');
      if(this.db.prepare("SELECT 1 FROM connector_outbox WHERE state IN ('sent','unknown') LIMIT 1").get())throw new ConnectorError('SYNCHRONIZATION_REQUIRED','unknown/sent requests require explicit outcome reconciliation');
      this.db.prepare('UPDATE connector_state SET synchronized_generation=? WHERE id=1').run(generation);this.audit('connection.synchronized',{generation,serverCursor:s.serverCursor});
    });
  }
  serverEvents(after=0,limit=100):ServerEvent[]{integer(after);integer(limit,1,1000);return this.db.prepare('SELECT value FROM connector_server_events WHERE event_id>? ORDER BY event_id LIMIT ?').all(after,limit).map(row=>JSON.parse(row.value as string));}
  recordReply(value:ConnectorReply):InboxEntry {
    const r=reply(value),serialized=encode(r);
    return this.tx(()=>{
      const old=this.db.prepare('SELECT * FROM connector_inbox WHERE id=?').get(r.id);
      if(old){if(old.reply!==serialized)throw new ConnectorError('PROTOCOL','conflicting duplicate reply');return {cursor:Number(old.cursor),id:r.id,reply:r};}
      const q=this.getOutbox(r.id);if(!q||q.state!=='sent'||q.generation!==r.generation)throw new ConnectorError('PROTOCOL','reply does not identify current sent request');
      const row=this.db.prepare('INSERT INTO connector_inbox(id,reply) VALUES(?,?)').run(r.id,serialized);this.db.prepare("UPDATE connector_outbox SET state='replied' WHERE id=?").run(r.id);this.audit('reply.stored',{id:r.id,generation:r.generation,cursor:Number(row.lastInsertRowid)});return {cursor:Number(row.lastInsertRowid),id:r.id,reply:r};
    });
  }
  disconnected(owner:ConnectorIdentity,generation:number,reason:string):void {
    this.tx(()=>{if(!sameIdentity(this.state().owner,owner))throw new ConnectorError('LOCKED','connector owner identity mismatch');this.db.prepare("UPDATE connector_outbox SET state='unknown' WHERE state='sent' AND generation=?").run(generation);this.audit('transport.disconnected',{generation,reason:redact(reason).slice(0,2048)});});
  }
  reconcile(id:string,resolution:{resolution:'replied';reply:ConnectorReply}|{resolution:'not-executed';verifiedBy:string;record:string}):void {
    text(id);
    this.tx(()=>{
      const q=this.getOutbox(id);if(!q||q.state!=='unknown')throw new Error('connector: only unknown requests can be reconciled');
      if(resolution.resolution==='replied'){
        const r=reply(resolution.reply);if(r.id!==id||r.generation!==q.generation)throw new ConnectorError('PROTOCOL','reconciliation reply identity mismatch');
        this.db.prepare('INSERT INTO connector_inbox(id,reply) VALUES(?,?)').run(id,encode(r));this.db.prepare("UPDATE connector_outbox SET state='replied' WHERE id=?").run(id);this.audit('request.reconciled',{id,resolution:'replied'});
      }else if(resolution.resolution==='not-executed'){
        const verifiedBy=text(resolution.verifiedBy),record=text(resolution.record,2048);this.db.prepare("UPDATE connector_outbox SET state='queued',generation=NULL WHERE id=?").run(id);this.audit('request.reconciled',{id,resolution:'not-executed',verifiedBy,record});
      }else throw new Error('connector: invalid reconciliation');
    });
  }
  setWatchdog(value:WatchdogState):void {
    integer(value.attempts,0,100);integer(value.nextAttemptAt);if(typeof value.circuitOpen!=='boolean'||(value.blockedReason!==null&&typeof value.blockedReason!=='string'))throw new Error('connector: invalid watchdog state');
    this.tx(()=>{if(this.state().disabled)throw new ConnectorError('DISABLED','connector is disabled');this.db.prepare('UPDATE connector_state SET watchdog=? WHERE id=1').run(encode(value));});
  }
  status():ConnectorStatus {
    this.db.exec('BEGIN');try{
      const s=this.state(),outbox=this.db.prepare('SELECT * FROM connector_outbox ORDER BY seq').all().map(r=>this.outboxRow(r));
      const inbox=this.db.prepare('SELECT * FROM connector_inbox ORDER BY cursor').all().map(r=>({cursor:Number(r.cursor),id:r.id as string,reply:JSON.parse(r.reply as string)}));
      this.db.exec('COMMIT');return {...s,outbox,inbox,cursor:inbox.at(-1)?.cursor??0};
    }catch(error){this.db.exec('ROLLBACK');throw error;}
  }
}

/** Bounded pump. It carries control frames only; no Muse task/wake API is invented here. */
export class Connector {
  readonly identity:ConnectorIdentity;
  readonly journal:ConnectorJournal;
  private transport:ConnectorTransport;
  private connected=false;
  private busy=false;
  private connecting:Promise<ConnectorState>|null=null;
  private generation=0;
  private connectionKey:string|undefined;
  private operationTimeoutMs:number;
  private verifying:Promise<ConnectorState>|null=null;
  private reconcileConnection?: (transport:ConnectorTransport,generation:number,snapshot:ConnectorStatus)=>Promise<SynchronizationEvidence>;
  constructor(journal:ConnectorJournal,transport:ConnectorTransport,owner:ConnectorIdentity,options:{connectionKey?:string;operationTimeoutMs?:number;reconcileConnection?:(transport:ConnectorTransport,generation:number,snapshot:ConnectorStatus)=>Promise<SynchronizationEvidence>}={}){this.journal=journal;this.transport=transport;this.identity=identity(owner);this.reconcileConnection=options.reconcileConnection;this.connectionKey=options.connectionKey??transport.connectionKey;this.operationTimeoutMs=integer(options.operationTimeoutMs??30000,10,300000);}
  async ensureConnected(nowMs=Date.now()):Promise<ConnectorState>{
    integer(nowMs);
    if(this.journal.isDisabled()){await this.close().catch(()=>{});return {state:'disabled'};}
    if(this.connecting)return this.connecting;
    const policy=this.journal.status().watchdog;
    if(policy.blockedReason)return {state:'blocked',error:policy.blockedReason};
    if(policy.circuitOpen)return {state:'blocked',error:'connector reconnect circuit is open; explicit operator reconciliation required'};
    if(policy.nextAttemptAt>nowMs)return {state:'blocked',error:`connector reconnect backoff until ${policy.nextAttemptAt}`};
    this.connecting=this.connectOwned();
    try{return await this.connecting;}finally{this.connecting=null;}
  }
  private async connectOwned():Promise<ConnectorState>{
    try{
      if(!this.connectionKey)throw new ConnectorError('PROTOCOL','immutable SHA256 connectionKey is required before connecting');
      this.journal.bindConnectionKey(this.connectionKey);
      this.journal.acquire(this.identity);
      if(this.connected)return {state:'idle'};
      this.generation=this.journal.nextGeneration(this.identity);await bounded(this.transport.connect(this.generation),this.operationTimeoutMs,'connect');
      if(this.journal.isDisabled()){await this.transport.close();return {state:'disabled'};}
      this.connected=true;return {state:'idle'};
    }catch(error){return await this.handleError(error);}
  }
  private async handleError(error:unknown):Promise<ConnectorState>{
    const err=classified(error);this.connected=false;
    if(this.journal.isOwner(this.identity))this.journal.disconnected(this.identity,this.generation,err.message);
    try{await bounded(this.transport.close(),this.operationTimeoutMs,'transport close');}catch(closeError){if(this.journal.isOwner(this.identity))this.journal.blockConnection(this.identity,`LOCKED: transport termination unverified: ${classified(closeError).message}`);}
    if(this.journal.isDisabled()||err.code==='DISABLED')return {state:'disabled'};
    if(blockedCodes.has(err.code)&&this.journal.isOwner(this.identity))this.journal.setWatchdog({...this.journal.status().watchdog,blockedReason:`${err.code}: ${err.message}`});
    return {state:blockedCodes.has(err.code)?'blocked':'disconnected',error:`${err.code}: ${err.message}`,code:err.code};
  }
  async verifyConnection(nowMs=Date.now(),force=false):Promise<ConnectorState>{
    const connection=await this.ensureConnected(nowMs);if(connection.state!=='idle')return connection;
    if(this.verifying)return this.verifying;
    this.verifying=this.verifyOwned(force);try{return await this.verifying;}finally{this.verifying=null;}
  }
  private async verifyOwned(force:boolean):Promise<ConnectorState>{
    const snapshot=this.journal.status();
    if(snapshot.outbox.some(q=>q.state==='unknown'||q.state==='sent'))return {state:'blocked',code:'SYNCHRONIZATION_REQUIRED',error:'unknown/sent outcomes require explicit reconciliation'};
    if(!force&&snapshot.synchronizedGeneration===this.generation)return {state:'idle'};
    if(!this.reconcileConnection)return {state:'blocked',code:'SYNCHRONIZATION_REQUIRED',error:'validated hello and event replay callback is required'};
    let callbackActive=true;
    const readonlyTransport:ConnectorTransport={
      connect:async()=>{throw new ConnectorError('PROTOCOL','reconciliation cannot open another connection');},close:async()=>{},
      request:async frame=>{if(!callbackActive||frame.generation!==this.generation||!readonlyOps.has(frame.payload.op))throw new ConnectorError('PROTOCOL','reconciliation callback may send only current confirmed read-only frames');if(this.journal.isDisabled())throw new ConnectorError('DISABLED','connector is disabled');return this.transport.request(frame);},
    };
    try{const proof=await bounded(this.reconcileConnection(readonlyTransport,this.generation,snapshot),this.operationTimeoutMs,'hello/event reconciliation');this.journal.markSynchronized(this.generation,proof);return {state:'idle'};}
    catch(error){if(error instanceof ConnectorError&&error.code==='SYNCHRONIZATION_REQUIRED')return {state:'blocked',code:error.code,error:error.message};return await this.handleError(error);}finally{callbackActive=false;}
  }
  async step():Promise<ConnectorState>{
    if(this.journal.isDisabled()){await this.close().catch(()=>{});return {state:'disabled'};}
    if(this.busy)return {state:'blocked',error:'one connector step is already outstanding'};
    this.busy=true;
    try{
      const connection=await this.ensureConnected();if(connection.state!=='idle')return connection;
      if(this.verifying){const verification=await this.verifying;if(verification.state!=='idle'&&verification.code!=='SYNCHRONIZATION_REQUIRED')return verification;}
      const snapshot=this.journal.status();
      if(snapshot.synchronizedGeneration!==this.generation&&this.reconcileConnection&&!snapshot.outbox.some(q=>q.state==='unknown'||q.state==='sent')){
        const verified=await this.verifyConnection();if(verified.state!=='idle'&&verified.code!=='SYNCHRONIZATION_REQUIRED')return verified;
      }
      const frame=this.journal.claimQueued(this.identity,this.generation);if(!frame)return {state:'idle'};
      if(this.journal.isDisabled()){this.journal.disconnected(this.identity,this.generation,'disabled before send');return {state:'disabled'};}
      const result=await bounded(this.transport.request(frame),this.operationTimeoutMs,'request');this.journal.recordReply(result);return {state:'replied',id:frame.id};
    }catch(error){if(error instanceof ConnectorError&&error.code==='SYNCHRONIZATION_REQUIRED')return {state:'blocked',error:error.message};return await this.handleError(error);}finally{this.busy=false;}
  }
  async runOnce(maxSteps=1):Promise<ConnectorState[]>{integer(maxSteps,1,100);const results:ConnectorState[]=[];for(let i=0;i<maxSteps;i++){const result=await this.step();results.push(result);if(result.state!=='replied')break;}return results;}
  async close():Promise<void>{this.connected=false;if(this.journal.isOwner(this.identity))this.journal.disconnected(this.identity,this.generation,'owned transport closed');try{await bounded(this.transport.close(),this.operationTimeoutMs,'transport close');}catch(error){if(this.journal.isOwner(this.identity))this.journal.blockConnection(this.identity,'LOCKED: transport termination unverified; owner retained');throw error;}}
}

export interface WatchdogResult {state:'disabled'|'healthy'|'backoff'|'circuit-open'|'blocked'|'restart-required'|'restarted'|'reconciliation-required';error?:string;nextAttemptAt?:number;connector?:Connector}
export class ConnectorWatchdog {
  private journal:ConnectorJournal;
  private maxAttempts:number;
  private baseDelayMs:number;
  private maxDelayMs:number;
  constructor(journal:ConnectorJournal,options:{maxAttempts?:number;baseDelayMs?:number;maxDelayMs?:number}={}){this.journal=journal;this.maxAttempts=integer(options.maxAttempts??3,1,20);this.baseDelayMs=integer(options.baseDelayMs??1000,1,3600000);this.maxDelayMs=integer(options.maxDelayMs??30000,this.baseDelayMs,86400000);}
  async tick(options:{nowMs:number;owned?:Connector;termination?:TerminationEvidence;factory?:()=>Connector;approvedFactory?:boolean}):Promise<WatchdogResult>{
    integer(options.nowMs);
    const state=this.journal.status();if(state.disabled){if(options.owned&&options.owned.journal.path===this.journal.path&&this.journal.isOwner(options.owned.identity))await options.owned.close().catch(()=>{});return {state:'disabled'};}
    if(state.watchdog.blockedReason)return {state:'blocked',error:state.watchdog.blockedReason};
    if(state.watchdog.circuitOpen)return {state:'circuit-open'};
    if(options.nowMs<state.watchdog.nextAttemptAt)return {state:'backoff',nextAttemptAt:state.watchdog.nextAttemptAt};
    let connector=options.owned,restarted=false;
    try{
      if(connector){if(connector.journal.path!==this.journal.path||!this.journal.isOwner(connector.identity))return {state:'restart-required'};}
      else{
        if(!options.termination||!options.factory||options.approvedFactory!==true)return {state:'restart-required'};
        evidence(options.termination,state.owner);this.journal.reconcileOwnerTermination(options.termination);
        if(this.journal.isDisabled())return {state:'disabled'};
        connector=options.factory();if(connector.journal.path!==this.journal.path)throw new ConnectorError('LOCKED','approved factory returned another connector journal');restarted=true;
      }
      const result=await connector.verifyConnection(options.nowMs,true); // Fresh hello/replay only; never pumps task/outbox work.
      if(result.state==='disabled')return {state:'disabled'};
      if(result.state==='blocked'&&result.code==='SYNCHRONIZATION_REQUIRED')return {state:'reconciliation-required',error:result.error,connector};
      if(result.state==='blocked'){this.journal.setWatchdog({...state.watchdog,blockedReason:result.error??'blocked'});return {state:'blocked',error:result.error};}
      if(result.state==='disconnected')throw new ConnectorError('DISCONNECTED',result.error??'reconnect failed');
      this.journal.setWatchdog({...defaultWatchdog});return {state:restarted?'restarted':'healthy',connector};
    }catch(error){
      const err=classified(error);if(this.journal.isDisabled())return {state:'disabled'};
      const currentPolicy=this.journal.status().watchdog;if(currentPolicy.blockedReason)return {state:'blocked',error:currentPolicy.blockedReason};
      if(blockedCodes.has(err.code)){this.journal.setWatchdog({...state.watchdog,blockedReason:`${err.code}: ${err.message}`});return {state:'blocked',error:err.message};}
      const attempts=state.watchdog.attempts+1,nextAttemptAt=options.nowMs+Math.min(this.maxDelayMs,this.baseDelayMs*2**(attempts-1)),circuitOpen=attempts>=this.maxAttempts;
      this.journal.setWatchdog({attempts,nextAttemptAt,circuitOpen,blockedReason:null});return {state:circuitOpen?'circuit-open':'backoff',nextAttemptAt,error:err.message,connector};
    }
  }
}
export {ConnectorWatchdog as Watchdog};

/** Opt-in SSH transport: existing caller-approved alias, fixed argv, stdio only, no remote install. */
export class SshStdioTransport implements ConnectorTransport {
  readonly args:string[];
  readonly connectionKey:string;
  private child:ChildProcessWithoutNullStreams|null=null;
  private generation=0;
  private stdout=Buffer.alloc(0);
  private stderr='';
  private timeoutMs:number;
  private pending:{id:string;generation:number;resolve:(reply:ConnectorReply)=>void;reject:(error:Error)=>void;timer:ReturnType<typeof setTimeout>}|null=null;
  private failure:ConnectorError|null=null;
  private closing:Promise<void>|null=null;
  constructor(options:{alias:string;command:string[];approved:true;timeoutMs?:number}){
    if(options.approved!==true)throw new ConnectorError('MISSING_SSH_CONFIG','explicit existing SSH alias and fixed command approval required');
    if(!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/.test(options.alias))throw new ConnectorError('MISSING_SSH_CONFIG','only an existing SSH config alias is supported');
    if(!Array.isArray(options.command)||!options.command.length||options.command.length>32)throw new Error('connector: fixed command argv required');
    const command=options.command.map(arg=>{text(arg,4096);return `'${arg.replace(/'/g,"'\\''")}'`;}).join(' ');
    this.args=['-T','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10','-o','ConnectionAttempts=1','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=2','--',options.alias,command];
    this.connectionKey=sshConnectionKey(options.alias,options.command);
    this.timeoutMs=integer(options.timeoutMs??30000,100,300000);
  }
  async connect(generation:number):Promise<void>{
    integer(generation,1);if(this.child)throw new ConnectorError('LOCKED','SSH transport already connected');
    this.generation=generation;this.failure=null;this.stdout=Buffer.alloc(0);this.stderr='';
    await new Promise<void>((resolve,reject)=>{
      const child=spawn('/usr/bin/ssh',this.args,{stdio:['pipe','pipe','pipe']});this.child=child;
      child.once('spawn',resolve);child.once('error',error=>{const e=new ConnectorError('DEPENDENCY',error.message);if(this.child===child)this.fail(e);reject(e);});
      child.stdout.on('data',(chunk:Buffer)=>{if(this.child===child)this.consume(chunk);});
      child.stderr.on('data',(chunk:Buffer)=>{if(this.child===child)this.stderr=redact((this.stderr+chunk.toString('utf8')).slice(-16384));});
      child.once('close',()=>{if(this.child===child){this.child=null;this.fail(this.sshFailure());}});
    });
  }
  private sshFailure():ConnectorError{
    const s=this.stderr;
    if(/REMOTE HOST IDENTIFICATION HAS CHANGED|Host key verification failed/i.test(s))return new ConnectorError('HOST_KEY_MISMATCH','SSH host key verification failed; operator action required');
    if(/Permission denied|Authentication failed|expired/i.test(s))return new ConnectorError('AUTH_EXPIRED','SSH authentication failed or expired; operator action required');
    if(/Could not resolve hostname|Bad configuration option|no such identity|no configuration/i.test(s))return new ConnectorError('MISSING_SSH_CONFIG','SSH alias/configuration unavailable; operator action required');
    if(/Connection refused|Connection timed out|No route to host|Network is unreachable|Operation timed out/i.test(s))return new ConnectorError('FIREWALL','SSH network or firewall unavailable; operator action required');
    if(/command not found|No such file or directory/i.test(s))return new ConnectorError('DEPENDENCY','fixed remote command dependency is unavailable; operator action required');
    return new ConnectorError('DISCONNECTED','SSH stdio closed before a verified reply');
  }
  private fail(error:ConnectorError):void{this.failure=error;const pending=this.pending;this.pending=null;if(pending){clearTimeout(pending.timer);pending.reject(error);}}
  private consume(chunk:Buffer):void{
    this.stdout=Buffer.concat([this.stdout,chunk]);
    if(this.stdout.length>131072){this.fail(new ConnectorError('PROTOCOL','SSH stdout capacity exceeded'));void this.close().catch(()=>{});return;}
    let newline:number;
    while((newline=this.stdout.indexOf(10))>=0){
      const line=this.stdout.subarray(0,newline);this.stdout=this.stdout.subarray(newline+1);
      try{
        if(line.length>65536)throw new ConnectorError('PROTOCOL','reply frame exceeds 64 KiB');
        const result=reply(JSON.parse(line.toString('utf8'))),p=this.pending;
        if(!p||result.id!==p.id||result.generation!==p.generation)throw new ConnectorError('PROTOCOL','unsolicited or stale SSH reply');
        this.pending=null;clearTimeout(p.timer);p.resolve(result);
      }catch(error){this.fail(error instanceof ConnectorError?error:new ConnectorError('PROTOCOL','malformed SSH JSON-lines reply'));void this.close().catch(()=>{});return;}
    }
    if(this.stdout.length>65536){this.fail(new ConnectorError('PROTOCOL','unterminated reply exceeds 64 KiB'));void this.close().catch(()=>{});}
  }
  async request(frame:ConnectorFrame):Promise<ConnectorReply>{
    if(this.failure)throw this.failure;if(!this.child||frame.generation!==this.generation)throw new ConnectorError('DISCONNECTED','SSH transport unavailable or stale generation');
    if(this.pending)throw new ConnectorError('LOCKED','SSH transport permits one outstanding request');
    text(frame.id);payload(frame.payload);const encoded=encode(frame)+'\n',child=this.child;
    return new Promise<ConnectorReply>((resolve,reject)=>{
      const timer=setTimeout(()=>{this.fail(new ConnectorError('DISCONNECTED','bounded SSH request deadline exceeded'));void this.close().catch(()=>{});},this.timeoutMs);
      this.pending={id:frame.id,generation:frame.generation,resolve,reject,timer};
      // One bounded frame is the entire write queue; no producer can outrun pipe backpressure.
      child.stdin.write(encoded,error=>{if(error)this.fail(new ConnectorError('DISCONNECTED','SSH stdin write failed'));});
    });
  }
  async close():Promise<void>{
    if(this.closing)return this.closing;
    const child=this.child;if(this.pending)this.fail(new ConnectorError('DISCONNECTED','owned SSH transport closed'));
    if(!child)return;
    this.closing=new Promise<void>((resolve,reject)=>{
      const killTimer=setTimeout(()=>child.kill('SIGKILL'),1000);
      const deadline=setTimeout(()=>{clearTimeout(killTimer);reject(new ConnectorError('DISCONNECTED','owned SSH child termination is unverified; reconnect remains blocked'));},2000);
      child.once('close',()=>{clearTimeout(killTimer);clearTimeout(deadline);resolve();});
      child.stdin.end();child.kill('SIGTERM');
    });
    try{await this.closing;}finally{this.closing=null;}
  }
}
