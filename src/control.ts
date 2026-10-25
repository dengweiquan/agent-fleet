import { DatabaseSync } from 'node:sqlite';
import { createHash,randomUUID } from 'node:crypto';
import { mkdirSync,chmodSync,realpathSync } from 'node:fs';
import { dirname,isAbsolute } from 'node:path';

export type ControlOperation='draft.create'|'note.append'|'pause'|'resume'|'stop';
export interface ControlScope {projectId:string;sessionId:string}
type RequestBase={version:1;requestId:string;projectId:string;sessionId:string};
export type ControlRequest=RequestBase&(
  |{operation:'draft.create';payload:{title:string;text:string}}
  |{operation:'note.append';payload:{text:string;taskId?:string}}
  |{operation:'pause'|'resume';payload:Record<string,never>}
  |{operation:'stop';payload:{runId:string}}
);
export type ControlState='accepted'|'applying'|'completed'|'unknown';
export interface ControlReceipt {
  seq:number;requestId:string;projectId:string;sessionId:string;operation:ControlOperation;
  hash:string;state:ControlState;received:true;completed:boolean;taskState:'unchanged';
  result:unknown;error:string|null;receivedAt:string;updatedAt:string;
}
export interface ControlArtifact {
  artifactId:number;requestId:string;kind:'draft'|'note';title:string|null;text:string;taskId:string|null;createdAt:string;
}
export interface ControlEvent {event_id:number;requestId:string;kind:string;data:unknown;createdAt:string}
export interface ControlOptions {
  scope:ControlScope;
  allowedOperations:ReadonlySet<ControlOperation>|(()=>ReadonlySet<ControlOperation>);
  readOnly?:boolean;
}
export type ControlHandler=(request:ControlRequest)=>unknown|Promise<unknown>;
const operations:ControlOperation[]=['draft.create','note.append','pause','resume','stop'];

function obj(value:unknown,required:string[],optional:string[]=[]):Record<string,unknown>{
  if(!value||typeof value!=='object'||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value)))throw new Error('control: expected plain object');
  const result=value as Record<string,unknown>;
  for(const field of required)if(!Object.hasOwn(result,field))throw new Error(`control: missing ${field}`);
  for(const field of Object.keys(result))if(!required.includes(field)&&!optional.includes(field))throw new Error(`control: unexpected field ${field}`);
  return result;
}
function safeId(value:unknown):string{if(typeof value!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value))throw new Error('control: unsafe identifier');return value;}
function boundedText(value:unknown,maxBytes:number):string{
  if(typeof value!=='string'||!value.trim()||Buffer.byteLength(value)>maxBytes||/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(value))throw new Error('control: invalid bounded text');return value;
}
function canonical(value:unknown):string{
  if(value===null||typeof value!=='object')return JSON.stringify(value);
  if(Array.isArray(value))return `[${value.map(canonical).join(',')}]`;
  return `{${Object.keys(value).sort().map(key=>`${JSON.stringify(key)}:${canonical((value as Record<string,unknown>)[key])}`).join(',')}}`;
}
function boundedJson(value:unknown,maxBytes:number):string{const encoded=JSON.stringify(value);if(encoded===undefined||Buffer.byteLength(encoded)>maxBytes)throw new Error('control: JSON payload exceeds capacity');return encoded;}
function redact(value:string):string{return value.replace(/\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,})\b/g,'[REDACTED]').replace(/(Bearer\s+)\S+/gi,'$1[REDACTED]').replace(/((?:password|token|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+/gi,'$1[REDACTED]');}
function sanitize(value:unknown,depth=0):unknown{
  if(depth>8)throw new Error('control: handler result nesting exceeds capacity');
  if(value===undefined||value===null)return null;
  if(typeof value==='string')return redact(value);
  if(typeof value==='boolean')return value;
  if(typeof value==='number'){if(!Number.isFinite(value))throw new Error('control: handler result must be finite');return value;}
  if(Array.isArray(value)){if(value.length>100)throw new Error('control: handler result array exceeds capacity');return value.map(item=>sanitize(item,depth+1));}
  if(typeof value==='object'){
    if(![Object.prototype,null].includes(Object.getPrototypeOf(value)))throw new Error('control: handler result must be plain JSON');
    const entries=Object.entries(value);if(entries.length>100)throw new Error('control: handler result object exceeds capacity');
    return Object.fromEntries(entries.map(([key,item])=>[key,/^(?:authorization|cookie|password|secret|token|access[_-]?token|refresh[_-]?token|api[_-]?key)$/i.test(key)?'[REDACTED]':sanitize(item,depth+1)]));
  }
  throw new Error('control: handler result must be JSON');
}
function validateRequest(value:unknown,scope:ControlScope):ControlRequest{
  const r=obj(value,['version','requestId','projectId','sessionId','operation','payload']);
  if(r.version!==1)throw new Error('control: unsupported request version');
  if(r.projectId!==scope.projectId||r.sessionId!==scope.sessionId)throw new Error('control: request outside trusted project/session scope');
  const requestId=safeId(r.requestId),operation=r.operation as ControlOperation;
  if(!operations.includes(operation))throw new Error('control: unsupported operation');
  let payload:unknown;
  if(operation==='draft.create'){const p=obj(r.payload,['title','text']);payload={title:boundedText(p.title,512),text:boundedText(p.text,32768)};}
  else if(operation==='note.append'){const p=obj(r.payload,['text'],['taskId']);payload={text:boundedText(p.text,32768),...(Object.hasOwn(p,'taskId')?{taskId:safeId(p.taskId)}:{})};}
  else if(operation==='stop'){const p=obj(r.payload,['runId']);payload={runId:safeId(p.runId)};}
  else{obj(r.payload,[]);payload={};}
  const result={version:1,requestId,projectId:scope.projectId,sessionId:scope.sessionId,operation,payload};boundedJson(result,49152);return result as ControlRequest;
}
function page(after:number,limit:number):void{if(!Number.isSafeInteger(after)||after<0||!Number.isSafeInteger(limit)||limit<1||limit>1000)throw new Error('control: after >= 0 and limit 1..1000 required');}
function boundedPage<T>(items:T[]):T[]{const result:T[]=[];let bytes=2;for(const item of items){const size=Buffer.byteLength(JSON.stringify(item))+(result.length?1:0);if(bytes+size>49152)break;result.push(item);bytes+=size;}return result;}

/** Receipt acceptance means durable receipt only. This module never executes plans or model prompts. */
export class ControlLedger {
  readonly scope:Readonly<ControlScope>;
  private db:DatabaseSync;
  private readOnly:boolean;
  private allowed:ControlOptions['allowedOperations'];
  private active=new Set<string>();
  constructor(path:string,options:ControlOptions){
    if(!options||!options.scope)throw new Error('control: trusted scope is required');
    const s=obj(options.scope,['projectId','sessionId']);
    if(typeof s.projectId!=='string'||!isAbsolute(s.projectId)||Buffer.byteLength(s.projectId)>4096)throw new Error('control: canonical absolute project root required');
    this.scope=Object.freeze({projectId:realpathSync(s.projectId),sessionId:safeId(s.sessionId)});
    this.allowed=options.allowedOperations;this.permissions();this.readOnly=options.readOnly??false;
    if(!this.readOnly&&path!==':memory:')mkdirSync(dirname(path),{recursive:true,mode:0o700});
    this.db=new DatabaseSync(path,{readOnly:this.readOnly});this.db.exec('PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;');
    try{
      if(!this.readOnly){
        this.db.exec(`PRAGMA journal_mode=WAL;
          CREATE TABLE IF NOT EXISTS control_scope(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,project_id TEXT NOT NULL,session_id TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS control_requests(seq INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT UNIQUE NOT NULL,hash TEXT NOT NULL,request TEXT NOT NULL,state TEXT NOT NULL,execution_token TEXT,result TEXT,error TEXT,received_at TEXT NOT NULL,updated_at TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS control_artifacts(artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT UNIQUE NOT NULL REFERENCES control_requests(request_id),kind TEXT NOT NULL,title TEXT,text TEXT NOT NULL,task_id TEXT,created_at TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS control_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL REFERENCES control_requests(request_id),kind TEXT NOT NULL,data TEXT NOT NULL,created_at TEXT NOT NULL);`);
        this.db.prepare('INSERT OR IGNORE INTO control_scope(id,version,project_id,session_id) VALUES(1,1,?,?)').run(this.scope.projectId,this.scope.sessionId);
        if(path!==':memory:')chmodSync(path,0o600);
      }
      const stored=this.db.prepare('SELECT * FROM control_scope WHERE id=1').get();
      if(!stored||stored.version!==1||stored.project_id!==this.scope.projectId||stored.session_id!==this.scope.sessionId)throw new Error('control: ledger belongs to a different trusted scope or version');
    }catch(error){this.db.close();throw error;}
  }
  close():void{this.db.close();}
  private permissions():ReadonlySet<ControlOperation>{
    const allowed=typeof this.allowed==='function'?this.allowed():this.allowed;
    if(!allowed||typeof allowed.has!=='function'||typeof allowed[Symbol.iterator]!=='function')throw new Error('control: trusted permissions set required');
    for(const operation of allowed)if(!operations.includes(operation))throw new Error('control: unknown trusted permission');return allowed;
  }
  private authorize(operation:ControlOperation):void{if(!this.permissions().has(operation))throw new Error(`control: permission denied for ${operation}`);}
  private tx<T>(callback:()=>T):T{
    if(this.readOnly)throw new Error('control: observer is read-only');this.db.exec('BEGIN IMMEDIATE');
    try{const result=callback();this.db.exec('COMMIT');return result;}catch(error){this.db.exec('ROLLBACK');throw error;}
  }
  private event(id:string,kind:string,data:unknown):void{this.db.prepare('INSERT INTO control_events(request_id,kind,data,created_at) VALUES(?,?,?,?)').run(id,kind,boundedJson(sanitize(data),4096),new Date().toISOString());}
  private row(id:string):Record<string,unknown>|undefined{return this.db.prepare('SELECT * FROM control_requests WHERE request_id=?').get(id);}
  private projected(row:Record<string,unknown>):ControlReceipt{
    const request=JSON.parse(row.request as string) as ControlRequest;
    const state:ControlState=row.state==='applying'&&!this.active.has(request.requestId)?'unknown':row.state as ControlState;
    return {seq:Number(row.seq),requestId:request.requestId,projectId:request.projectId,sessionId:request.sessionId,operation:request.operation,hash:row.hash as string,state,received:true,completed:state==='completed',taskState:'unchanged',result:row.result===null?null:JSON.parse(row.result as string),error:row.error as string|null,receivedAt:row.received_at as string,updatedAt:row.updated_at as string};
  }
  submit(value:ControlRequest):ControlReceipt{
    const request=validateRequest(value,this.scope);this.authorize(request.operation);
    const serialized=canonical(request),hash=createHash('sha256').update(serialized).digest('hex');
    return this.tx(()=>{
      this.authorize(request.operation);const existing=this.row(request.requestId);
      if(existing){if(existing.hash!==hash)throw new Error('control: request ID conflicts with different content');return this.projected(existing);}
      if(Number(this.db.prepare('SELECT COUNT(*) AS n FROM control_requests').get()!.n)>=10000)throw new Error('control: receipt history capacity reached');
      const at=new Date().toISOString();this.db.prepare("INSERT INTO control_requests(request_id,hash,request,state,received_at,updated_at) VALUES(?,?,?,'accepted',?,?)").run(request.requestId,hash,serialized,at,at);
      this.event(request.requestId,'request.received',{hash,operation:request.operation,received:true,completed:false,taskState:'unchanged'});return this.projected(this.row(request.requestId)!);
    });
  }
  async apply(requestId:string,handler?:ControlHandler):Promise<ControlReceipt>{
    safeId(requestId);const initial=this.row(requestId);if(!initial)throw new Error('control: unknown request');
    const request=validateRequest(JSON.parse(initial.request as string),this.scope);this.authorize(request.operation);
    if(this.readOnly)throw new Error('control: observer is read-only');
    if(request.operation==='draft.create'||request.operation==='note.append')return this.applyArtifact(request);
    const token=randomUUID();
    const claimed=this.tx(()=>{
      this.authorize(request.operation);const current=this.row(requestId)!;
      if(current.state!=='accepted')return {execute:false,receipt:this.projected(current)};
      if(this.db.prepare("SELECT 1 FROM control_requests WHERE state IN ('applying','unknown') AND request_id<>? LIMIT 1").get(requestId))throw new Error('control: session has an unresolved effect; no automatic recovery or new side effect is allowed');
      if(typeof handler!=='function')throw new Error('control: explicit reviewed effect handler required');
      const changed=this.db.prepare("UPDATE control_requests SET state='applying',execution_token=?,updated_at=? WHERE request_id=? AND state='accepted'").run(token,new Date().toISOString(),requestId);
      if(changed.changes!==1)throw new Error('control: effect reservation conflict');this.event(requestId,'request.applying',{taskState:'unchanged'});return {execute:true,receipt:null};
    });
    if(!claimed.execute)return claimed.receipt!;
    this.active.add(requestId);
    try{
      // Reservation has committed before the handler can perform any external effect.
      this.authorize(request.operation);
      const returned=await handler!(structuredClone(request));
      const clean=sanitize(returned);
      const result=request.operation==='stop'?{processTermination:'unverified',details:clean}:clean;
      const encoded=boundedJson(result,16384);
      return this.tx(()=>{
        const changed=this.db.prepare("UPDATE control_requests SET state='completed',result=?,error=NULL,updated_at=? WHERE request_id=? AND state='applying' AND execution_token=?").run(encoded,new Date().toISOString(),requestId,token);
        if(changed.changes!==1)throw new Error('control: applying reservation changed; result uncertain');
        this.event(requestId,'request.completed',{taskState:'unchanged',effect:request.operation==='stop'?'stop-handler-completed':request.operation});return this.projected(this.row(requestId)!);
      });
    }catch(error){
      // 512 UTF-16 units remain below the 4 KiB event cap even when JSON must escape every unit.
      const message=redact(error instanceof Error?error.message:String(error)).slice(0,512);
      return this.tx(()=>{
        const changed=this.db.prepare("UPDATE control_requests SET state='unknown',error=?,result=NULL,updated_at=? WHERE request_id=? AND state='applying' AND execution_token=?").run(message,new Date().toISOString(),requestId,token);
        if(changed.changes===1)this.event(requestId,'request.unknown',{reason:message,taskState:'unchanged'});return this.projected(this.row(requestId)!);
      });
    }finally{this.active.delete(requestId);}
  }
  private applyArtifact(request:ControlRequest):ControlReceipt{
    return this.tx(()=>{
      this.authorize(request.operation);const current=this.row(request.requestId)!;if(current.state!=='accepted')return this.projected(current);
      const at=new Date().toISOString();let kind:'draft'|'note',title:string|null=null,body:string,taskId:string|null=null;
      if(request.operation==='draft.create'){kind='draft';title=request.payload.title;body=request.payload.text;}
      else if(request.operation==='note.append'){kind='note';body=request.payload.text;taskId=request.payload.taskId??null;}
      else throw new Error('control: operation is not an inert artifact');
      const inserted=this.db.prepare('INSERT INTO control_artifacts(request_id,kind,title,text,task_id,created_at) VALUES(?,?,?,?,?,?)').run(request.requestId,kind,title,body,taskId,at);
      this.db.prepare("UPDATE control_requests SET state='completed',result=?,updated_at=? WHERE request_id=? AND state='accepted'").run(boundedJson({artifactId:Number(inserted.lastInsertRowid),kind,executable:false},16384),at,request.requestId);
      this.event(request.requestId,'artifact.created',{artifactId:Number(inserted.lastInsertRowid),kind,executable:false,taskState:'unchanged'});this.event(request.requestId,'request.completed',{taskState:'unchanged'});return this.projected(this.row(request.requestId)!);
    });
  }
  receipt(requestId:string):ControlReceipt|null{safeId(requestId);const row=this.row(requestId);return row?this.projected(row):null;}
  receipts(after=0,limit=100):ControlReceipt[]{page(after,limit);return boundedPage(this.db.prepare('SELECT * FROM control_requests WHERE seq>? ORDER BY seq LIMIT ?').all(after,limit).map(row=>this.projected(row)));}
  artifacts(after=0,limit=100):ControlArtifact[]{page(after,limit);return boundedPage(this.db.prepare('SELECT * FROM control_artifacts WHERE artifact_id>? ORDER BY artifact_id LIMIT ?').all(after,limit).map(r=>({artifactId:Number(r.artifact_id),requestId:r.request_id as string,kind:r.kind as 'draft'|'note',title:r.title as string|null,text:r.text as string,taskId:r.task_id as string|null,createdAt:r.created_at as string})));}
  events(after=0,limit=100):ControlEvent[]{page(after,limit);return boundedPage(this.db.prepare('SELECT * FROM control_events WHERE event_id>? ORDER BY event_id LIMIT ?').all(after,limit).map(r=>({event_id:Number(r.event_id),requestId:r.request_id as string,kind:r.kind as string,data:JSON.parse(r.data as string),createdAt:r.created_at as string})));}
}
