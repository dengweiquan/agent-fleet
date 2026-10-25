import {createHash} from 'node:crypto';
import type {DatabaseSync} from 'node:sqlite';
import {ResourcePool} from './resource-pool.ts';
import type {PoolRequest,Resource,Lease} from './resource-pool.ts';
import {validateDispatch} from './remote.ts';
import type {Dispatch,Capabilities} from './remote.ts';

/** Scope is supplied by the trusted connection/manifest, never by the remote payload. */
export interface RemoteScope {endpointId:string;resourceIds:string[];projects:Array<{projectKey:string;sessionId:string}>}
export interface PooledBinding {
  projectKey:string;sessionId:string;taskId:string;messageId:string;runId:string;attemptId:string;
  endpointId:string;resourceId:string;accountDomain:string;leaseId:string;fence:number;claimId:string;
}
export interface PooledClaim extends PooledBinding {dispatch:Dispatch}
export interface PooledOffer {request:PoolRequest;dispatch:Dispatch;projectKey:string}
export interface PooledReport {sourceEventId:string;kind:'heartbeat'|'progress'|'result'|'unknown';data:unknown}
export type PooledState='offered'|'claimed'|'received'|'execution-completed'|'execution-failed'|'unknown';
const bindingKeys=['projectKey','sessionId','taskId','messageId','runId','attemptId','endpointId','resourceId','accountDomain','leaseId','fence','claimId'] as const;
const safe=/^[A-Za-z0-9][A-Za-z0-9:._@-]{0,255}$/;
function token(v:unknown,name:string):asserts v is string{if(typeof v!=='string'||!safe.test(v))throw new Error(`pooled remote: invalid ${name}`);}
function plain(v:unknown,keys:string[],optional:string[]=[]):asserts v is Record<string,any>{if(!v||typeof v!=='object'||Array.isArray(v)||![Object.prototype,null].includes(Object.getPrototypeOf(v)))throw new Error('pooled remote: plain object required');for(const key of keys)if(!Object.hasOwn(v,key))throw new Error(`pooled remote: missing ${key}`);if(Object.keys(v).some(k=>![...keys,...optional].includes(k)))throw new Error('pooled remote: unexpected field');}
function encode(v:unknown,max=65536):string{const s=JSON.stringify(v);if(s===undefined||Buffer.byteLength(s)>max)throw new Error('pooled remote: payload exceeds capacity');return s;}
function claimReplyFits(claim:PooledClaim):void{
  // Reserve the complete connector envelope, longest request ID/generation, recovery
  // fields, and terminating newline. This guard runs before the shared transaction commits.
  encode({id:'x'.repeat(128),generation:Number.MAX_SAFE_INTEGER,ok:true,data:{...claim,state:'execution-completed',replay:false}},65535);
}
function hash(v:unknown):string{const canonical=(x:any):string=>Array.isArray(x)?`[${x.map(canonical).join(',')}]`:x&&typeof x==='object'?`{${Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+canonical(x[k])).join(',')}}`:JSON.stringify(x);return createHash('sha256').update(canonical(v)).digest('hex');}
export function pooledProjectKey(projectId:string):string{if(typeof projectId!=='string'||!projectId.trim())throw new Error('pooled remote: project identity required');return createHash('sha256').update(projectId).digest('hex');}
function redact(v:unknown):unknown{
  if(typeof v==='string')return v.replace(/\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,})\b/g,'[REDACTED]').replace(/(bearer\s+)[^\s"',;]+/gi,'$1[REDACTED]').replace(/\b(token|password|secret|api[_-]?key)\s*[:=]\s*[^\s,;]+/gi,'$1=[REDACTED]').replace(/(^|[\s"'=])\/(?:Users|home|tmp|private|var|workspace|Volumes)\/[^\s"'<>]*/g,'$1[LOCAL_PATH]');
  if(Array.isArray(v))return v.map(redact);
  if(v&&typeof v==='object')return Object.fromEntries(Object.entries(v).map(([k,x])=>[k,/^(authorization|cookie|password|secret|token|api[_-]?key|access[_-]?token|refresh[_-]?token)$/i.test(k)?'[REDACTED]':/^(cwd|worktree|projectPath|repositoryPath|localPath)$/i.test(k)?'[LOCAL_PATH]':redact(x)]));return v;
}
function scopeValue(v:RemoteScope):RemoteScope{
  plain(v,['endpointId','resourceIds','projects']);token(v.endpointId,'endpoint');
  if(!Array.isArray(v.resourceIds)||!v.resourceIds.length||v.resourceIds.length>1000||new Set(v.resourceIds).size!==v.resourceIds.length)throw new Error('pooled remote: fixed resources required');v.resourceIds.forEach(x=>token(x,'resource'));
  if(!Array.isArray(v.projects)||!v.projects.length||v.projects.length>1000)throw new Error('pooled remote: explicit project/session scopes required');
  const projects=v.projects.map(p=>{plain(p,['projectKey','sessionId']);if(!/^[a-f0-9]{64}$/.test(p.projectKey))throw new Error('pooled remote: opaque project key required');token(p.sessionId,'session');return {...p};}).sort((a,b)=>(a.projectKey+':'+a.sessionId).localeCompare(b.projectKey+':'+b.sessionId));
  if(new Set(projects.map(p=>p.projectKey+':'+p.sessionId)).size!==projects.length)throw new Error('pooled remote: duplicate scope');
  return {endpointId:v.endpointId,resourceIds:[...v.resourceIds].sort(),projects};
}
function capabilitiesValue(v:Capabilities):Capabilities{plain(v,['protocolVersion','provider','capabilities','quota','auth']);if(v.protocolVersion!==1||v.quota!=='unknown'||!['available','unavailable','unknown'].includes(v.auth)||!Array.isArray(v.capabilities)||v.capabilities.length>32)throw new Error('pooled remote: invalid negotiated capabilities');token(v.provider,'provider');v.capabilities.forEach(c=>token(c,'capability'));return {...v,capabilities:[...new Set(v.capabilities)].sort()};}
function bindingValue(v:PooledBinding):PooledBinding{plain(v,[...bindingKeys]);for(const key of bindingKeys)if(key!=='fence')token(v[key],key);if(!Number.isSafeInteger(v.fence)||v.fence<1)throw new Error('pooled remote: invalid fence');return v;}
function authorized(scope:RemoteScope,row:any):boolean{return row.endpoint_id===scope.endpointId&&scope.projects.some(p=>p.projectKey===row.project_key&&p.sessionId===row.session_id);}
function bindingOnly(claim:PooledClaim):PooledBinding{return Object.fromEntries(bindingKeys.map(k=>[k,claim[k]])) as unknown as PooledBinding;}

/** Reverse runner messages and shared capacity mutate in the same SQLite transaction. */
export class PooledRemoteQueue {
  readonly pool:ResourcePool;readonly readOnly:boolean;
  constructor(pool:ResourcePool,options:{readOnly?:boolean}={}){
    this.pool=pool;this.readOnly=pool.isReadOnly||options.readOnly===true;
    if(!this.readOnly)pool.atomic(db=>db.exec(`
      CREATE TABLE IF NOT EXISTS pooled_remote_offers(message_id TEXT PRIMARY KEY,request_id TEXT UNIQUE NOT NULL REFERENCES pool_requests(id),project_key TEXT NOT NULL,session_id TEXT NOT NULL,task_id TEXT NOT NULL,run_id TEXT UNIQUE NOT NULL,attempt_id TEXT UNIQUE NOT NULL,endpoint_id TEXT NOT NULL,digest TEXT NOT NULL,dispatch TEXT NOT NULL,state TEXT NOT NULL,claim_id TEXT UNIQUE,ack TEXT);
      CREATE TABLE IF NOT EXISTS pooled_remote_claims(claim_id TEXT PRIMARY KEY,scope_hash TEXT NOT NULL,value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS pooled_remote_reports(source_event_id TEXT PRIMARY KEY,digest TEXT NOT NULL,event_id INTEGER NOT NULL REFERENCES pool_events(event_id),response TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS pooled_remote_events(event_id INTEGER PRIMARY KEY REFERENCES pool_events(event_id),project_key TEXT NOT NULL,session_id TEXT NOT NULL,endpoint_id TEXT NOT NULL,run_id TEXT NOT NULL,run_seq INTEGER NOT NULL,kind TEXT NOT NULL,data TEXT NOT NULL,UNIQUE(run_id,run_seq));
      CREATE TABLE IF NOT EXISTS pooled_remote_pauses(project_key TEXT NOT NULL,session_id TEXT NOT NULL,paused INTEGER NOT NULL CHECK(paused IN (0,1)),PRIMARY KEY(project_key,session_id));
    `));
  }
  private mutate(){if(this.readOnly)throw new Error('pooled remote: observer cannot mutate');}
  private exists(db:DatabaseSync):boolean{return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pooled_remote_offers'").get();}
  private resources(scope:RemoteScope):Resource[]{const resources=this.pool.status().resources;return scope.resourceIds.map(id=>{const r=resources.find(r=>r.id===id);if(!r||r.endpointId!==scope.endpointId||r.endpointKind==='local')throw new Error('pooled remote: resource outside fixed remote endpoint');token(r.accountDomain,'account domain');return r;});}
  private resourceClause(scope:RemoteScope):string{const values=scope.resourceIds.map(()=>'?').join(',');return `((o.claim_id IS NULL AND EXISTS(SELECT 1 FROM json_each(q.value,'$.eligibleResourceIds') AS eligible WHERE eligible.value IN (${values}))) OR (o.claim_id IS NOT NULL AND json_extract(c.value,'$.resourceId') IN (${values})))`;}
  private rows(db:DatabaseSync,scope:RemoteScope,after=0,limit=1000):any[]{const where=scope.projects.map(()=>'(o.project_key=? AND o.session_id=?)').join(' OR ');return db.prepare(`SELECT o.*,o.rowid AS row_cursor,c.value AS claim_value FROM pooled_remote_offers o JOIN pool_requests q ON q.id=o.request_id LEFT JOIN pooled_remote_claims c ON c.claim_id=o.claim_id WHERE o.endpoint_id=? AND (${where}) AND ${this.resourceClause(scope)} AND o.rowid>? ORDER BY o.rowid LIMIT ?`).all(scope.endpointId,...scope.projects.flatMap(p=>[p.projectKey,p.sessionId]),...scope.resourceIds,...scope.resourceIds,after,limit) as any[];}
  private candidates(db:DatabaseSync,scope:RemoteScope,caps:Capabilities,limit=10000):Array<{message_id:string;request_id:string}>{
    if(caps.auth!=='available')return [];
    const where=scope.projects.map(()=>'(o.project_key=? AND o.session_id=?)').join(' OR ');
    // Filter before limiting, and load only IDs; terminal history cannot starve new work.
    return db.prepare(`SELECT o.message_id,o.request_id FROM pooled_remote_offers o JOIN pool_requests q ON q.id=o.request_id LEFT JOIN pooled_remote_claims c ON c.claim_id=o.claim_id WHERE o.endpoint_id=? AND (${where}) AND ${this.resourceClause(scope)} AND o.state='offered' AND NOT EXISTS(SELECT 1 FROM pooled_remote_pauses p WHERE p.project_key=o.project_key AND p.session_id=o.session_id AND p.paused=1) AND json_extract(o.dispatch,'$.provider')=? AND NOT EXISTS(SELECT 1 FROM json_each(o.dispatch,'$.requiredCapabilities') required WHERE required.value NOT IN(SELECT value FROM json_each(?))) ORDER BY o.rowid LIMIT ?`).all(scope.endpointId,...scope.projects.flatMap(p=>[p.projectKey,p.sessionId]),...scope.resourceIds,...scope.resourceIds,caps.provider,JSON.stringify(caps.capabilities),limit) as Array<{message_id:string;request_id:string}>;
  }
  setPaused(projectKey:string,sessionId:string,paused:boolean):void{this.mutate();if(!/^[a-f0-9]{64}$/.test(projectKey)||typeof paused!=='boolean')throw new Error('pooled remote: invalid pause scope');token(sessionId,'session');this.pool.atomic(db=>{db.prepare('INSERT INTO pooled_remote_pauses VALUES(?,?,?) ON CONFLICT(project_key,session_id) DO UPDATE SET paused=excluded.paused').run(projectKey,sessionId,paused?1:0);this.pool.event('remote.scheduling_paused',{projectKey,sessionId,paused});});}
  private claimRow(db:DatabaseSync,scope:RemoteScope,binding:PooledBinding):{row:any;claim:PooledClaim;lease:Lease}{
    bindingValue(binding);const row=db.prepare('SELECT * FROM pooled_remote_offers WHERE message_id=?').get(binding.messageId) as any;
    if(!row||!authorized(scope,row)||!scope.resourceIds.includes(binding.resourceId))throw new Error('pooled remote: binding outside authorized scope');
    const saved=db.prepare('SELECT value FROM pooled_remote_claims WHERE claim_id=?').get(row.claim_id) as any;if(!saved)throw new Error('pooled remote: no bound claim');const claim=JSON.parse(saved.value) as PooledClaim;
    for(const key of bindingKeys)if(claim[key]!==binding[key])throw new Error(`pooled remote: identity mismatch ${key}`);
    const lease=this.pool.status().leases.find(l=>l.id===binding.leaseId&&l.fence===binding.fence);
    if(!lease||lease.runId!==binding.runId||lease.request.sessionId!==binding.sessionId||lease.request.taskId!==binding.taskId||lease.request.attemptId!==binding.attemptId||pooledProjectKey(lease.request.projectId)!==binding.projectKey||lease.resource.id!==binding.resourceId||lease.resource.endpointId!==binding.endpointId||lease.resource.accountDomain!==binding.accountDomain)throw new Error('pooled remote: lease binding/fence mismatch');
    return {row,claim,lease};
  }
  private append(db:DatabaseSync,row:any,kind:string,data:unknown):number{
    const safeData=redact(data);this.pool.event('remote.'+kind,{projectKey:row.project_key,sessionId:row.session_id,runId:row.run_id,data:safeData});
    const eventId=Number((db.prepare('SELECT last_insert_rowid() AS id').get() as any).id);
    db.prepare('INSERT INTO pooled_remote_events(event_id,project_key,session_id,endpoint_id,run_id,run_seq,kind,data) SELECT ?,?,?,?,?,COALESCE(MAX(run_seq),0)+1,?,? FROM pooled_remote_events WHERE run_id=?').run(eventId,row.project_key,row.session_id,row.endpoint_id,row.run_id,kind,encode(safeData),row.run_id);return eventId;
  }
  offer(value:PooledOffer){
    this.mutate();plain(value,['request','dispatch','projectKey']);validateDispatch(value.dispatch);encode(value.dispatch,40*1024);encode(value,131072);
    const {request,dispatch,projectKey}=value;
    if(projectKey!==pooledProjectKey(request.projectId))throw new Error('pooled remote: project key does not match server capacity identity');
    if(request.taskId!==dispatch.taskId||request.attemptId!==dispatch.attemptId||request.requiredProvider!==dispatch.provider||!Array.isArray(request.capabilities)||hash([...request.capabilities].sort())!==hash([...dispatch.requiredCapabilities].sort())||!request.eligibleResourceIds?.length)throw new Error('pooled remote: request/dispatch binding mismatch or missing explicit eligible resources');
    token(request.sessionId,'session');
    if(request.projectId.startsWith('/')&&encode(dispatch).includes(request.projectId))throw new Error('pooled remote: local project path cannot enter remote dispatch');
    for(const d of dispatch.dependencies)plain(d,['taskId','commitSha','acceptanceId','reviewId']);if(dispatch.snapshot)plain(dispatch.snapshot,['kind','commitSha','approvalId']);
    return this.pool.atomic(db=>{
      const old=db.prepare('SELECT * FROM pooled_remote_offers WHERE message_id=?').get(dispatch.messageId) as any;
      if(old){if(old.digest!==hash(value))throw new Error('pooled remote: offer message content conflict');return this.summary(old,db);}
      if(db.prepare("SELECT 1 FROM pooled_remote_offers WHERE project_key=? AND session_id=? AND task_id=? AND state!='execution-failed'").get(projectKey,request.sessionId,request.taskId))throw new Error('pooled remote: task has unresolved/completed attempt; no blind retry');
      const existing=this.pool.status();if(existing.requests.some(q=>q.request.id===request.id))throw new Error('pooled remote: pool request id already exists outside this offer');if(existing.requests.some(q=>q.request.projectId===request.projectId&&q.request.sessionId===request.sessionId&&q.request.taskId===request.taskId&&['queued','held','unknown'].includes(q.state)))throw new Error('pooled remote: task already reserved in shared pool');
      for(const id of request.eligibleResourceIds!){const r=existing.resources.find(r=>r.id===id);if(!r||r.endpointKind==='local'||r.endpointId!==dispatch.endpointId||r.provider!==dispatch.provider||!r.roles.includes(request.role)||request.capabilities.some(c=>!r.capabilities.includes(c)))throw new Error('pooled remote: offered resource/provider/role/capability mismatch');token(r.accountDomain,'account domain');}
      this.pool.enqueue(request);
      db.prepare('INSERT INTO pooled_remote_offers VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)').run(dispatch.messageId,request.id,projectKey,request.sessionId,request.taskId,dispatch.runId,dispatch.attemptId,dispatch.endpointId,hash(value),encode(dispatch,131072),'offered');
      const row=db.prepare('SELECT * FROM pooled_remote_offers WHERE message_id=?').get(dispatch.messageId) as any;this.append(db,row,'offered',{messageId:dispatch.messageId,taskId:dispatch.taskId});return this.summary(row);
    });
  }
  private summary(row:any,db?:DatabaseSync){const d=JSON.parse(row.dispatch) as Dispatch,rawClaim=row.claim_value??(row.claim_id&&db?(db.prepare('SELECT value FROM pooled_remote_claims WHERE claim_id=?').get(row.claim_id) as any)?.value:null),claim=rawClaim?JSON.parse(rawClaim) as PooledClaim:null;return {rowCursor:row.row_cursor as number|undefined,projectKey:row.project_key,sessionId:row.session_id,taskId:row.task_id,messageId:row.message_id,runId:row.run_id,attemptId:row.attempt_id,endpointId:row.endpoint_id,claimId:row.claim_id,state:row.state as PooledState,provider:d.provider,repositoryId:d.repositoryId,baseSha:d.baseSha,fence:claim?.fence??d.fence,resourceId:claim?.resourceId??null,acknowledged:row.ack!==null};}
  list(value:RemoteScope,after=0,limit=1000){const scope=scopeValue(value);if(!Number.isSafeInteger(after)||after<0||!Number.isSafeInteger(limit)||limit<1||limit>1000)throw new Error('pooled remote: invalid list cursor');return this.pool.inspect(db=>{if(!this.exists(db))return [];this.resources(scope);return this.rows(db,scope,after,limit).map(row=>this.summary(row));});}
  pull(value:RemoteScope,caps:Capabilities,limit=10){const scope=scopeValue(value),capabilities=capabilitiesValue(caps);if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('pooled remote: invalid pull limit');return this.pool.inspect(db=>{if(!this.exists(db))return [];this.resources(scope);return this.candidates(db,scope,capabilities,limit).map(candidate=>{const row=db.prepare('SELECT * FROM pooled_remote_offers WHERE message_id=?').get(candidate.message_id) as any;return {...this.summary(row),dispatch:redact(JSON.parse(row.dispatch)) as Dispatch};});});}
  claim(value:RemoteScope,input:{claimId:string;capabilities:Capabilities}):PooledClaim|null{
    this.mutate();const scope=scopeValue(value);plain(input,['claimId','capabilities']);token(input.claimId,'claim');const caps=capabilitiesValue(input.capabilities),scopeHash=hash({scope,capabilities:caps});
    return this.pool.atomic(db=>{
      this.resources(scope);const old=db.prepare('SELECT * FROM pooled_remote_claims WHERE claim_id=?').get(input.claimId) as any;
      if(old){if(old.scope_hash!==scopeHash)throw new Error('pooled remote: claim ID scope/content conflict');const claim=JSON.parse(old.value) as PooledClaim;claimReplyFits(claim);return claim;}
      const eligible=this.candidates(db,scope,caps);
      if(!eligible.length)return null;const lease=this.pool.claimNext(eligible.map(row=>row.request_id),scope.resourceIds);if(!lease)return null;
      const selected=eligible.find(row=>row.request_id===lease.request.id);if(!selected)throw new Error('pooled remote: arbitration selected unowned request');const row=db.prepare('SELECT * FROM pooled_remote_offers WHERE message_id=?').get(selected.message_id) as any;
      this.pool.bindRun(lease.id,lease.fence,row.run_id);
      const dispatch={...JSON.parse(row.dispatch),fence:lease.fence} as Dispatch;
      const claim:PooledClaim={projectKey:row.project_key,sessionId:row.session_id,taskId:row.task_id,messageId:row.message_id,runId:row.run_id,attemptId:row.attempt_id,endpointId:row.endpoint_id,resourceId:lease.resource.id,accountDomain:lease.resource.accountDomain,leaseId:lease.id,fence:lease.fence,claimId:input.claimId,dispatch:redact(dispatch) as Dispatch};
      claimReplyFits(claim);
      db.prepare('INSERT INTO pooled_remote_claims VALUES(?,?,?)').run(input.claimId,scopeHash,encode(claim,131072));
      db.prepare("UPDATE pooled_remote_offers SET state='claimed',claim_id=? WHERE message_id=? AND state='offered'").run(input.claimId,row.message_id);
      this.append(db,row,'claimed',bindingOnly(claim));return claim;
    });
  }
  ack(value:RemoteScope,binding:PooledBinding){this.mutate();const scope=scopeValue(value);return this.pool.atomic(db=>{this.resources(scope);const {row}=this.claimRow(db,scope,binding);if(row.ack)return JSON.parse(row.ack);if(row.state!=='claimed'&&row.state!=='unknown')throw new Error('pooled remote: ACK requires claim');const ack={...binding,accepted:true,meaning:'durably-received-only'};db.prepare('UPDATE pooled_remote_offers SET state=?,ack=? WHERE message_id=?').run(row.state==='unknown'?'unknown':'received',encode(ack),row.message_id);this.append(db,row,'ack',ack);return ack;});}
  report(value:RemoteScope,binding:PooledBinding,input:PooledReport){
    this.mutate();const scope=scopeValue(value);plain(input,['sourceEventId','kind','data']);token(input.sourceEventId,'source event');if(!['heartbeat','progress','result','unknown'].includes(input.kind))throw new Error('pooled remote: invalid report kind');encode(input.data,16384);
    if(input.kind==='result'){const r=input.data as any;if(!r||!Number.isInteger(r.exitCode)||r.exitCode<0||r.exitCode>255||(r.commitSha!==undefined&&!/^[a-f0-9]{40}$|^[a-f0-9]{64}$/.test(r.commitSha)))throw new Error('pooled remote: invalid execution result');}
    return this.pool.atomic(db=>{
      this.resources(scope);const {row,lease}=this.claimRow(db,scope,binding);if(!row.ack)throw new Error('pooled remote: ACK required');
      const fingerprint=hash({binding,input}),old=db.prepare('SELECT * FROM pooled_remote_reports WHERE source_event_id=?').get(input.sourceEventId) as any;
      if(old){if(old.digest!==fingerprint)throw new Error('pooled remote: source event content conflict');return JSON.parse(old.response);}
      if(['execution-completed','execution-failed'].includes(row.state))throw new Error('pooled remote: terminal report already recorded');
      const uncertain=row.state==='unknown'||lease.state==='unknown';
      if(!uncertain&&lease.state!=='held')throw new Error('pooled remote: lease no longer owned');
      let state:PooledState=uncertain?'unknown':row.state;
      if(input.kind==='unknown'){state='unknown';if(lease.state==='held')this.pool.release(lease.id,lease.fence,'unknown');}
      if(input.kind==='result'){
        state=uncertain?'unknown':(input.data as any).exitCode===0?'execution-completed':'execution-failed';
        if(!uncertain)this.pool.release(lease.id,lease.fence,state==='execution-completed'?'completed':'failed');
      }
      const eventId=this.append(db,row,uncertain&&input.kind==='result'?'late-result':input.kind,{...binding,sourceEventId:input.sourceEventId,evidence:input.data,meaning:'Execution evidence only; not task acceptance'});
      db.prepare('UPDATE pooled_remote_offers SET state=? WHERE message_id=?').run(state,row.message_id);
      const response={eventId,state,accepted:false};db.prepare('INSERT INTO pooled_remote_reports VALUES(?,?,?,?)').run(input.sourceEventId,fingerprint,eventId,encode(response));return response;
    });
  }
  disconnected(value:RemoteScope,binding:PooledBinding){this.mutate();const scope=scopeValue(value);return this.pool.atomic(db=>{this.resources(scope);const {row,lease}=this.claimRow(db,scope,binding);if(!['unknown','execution-completed','execution-failed'].includes(row.state)){if(lease.state==='held')this.pool.release(lease.id,lease.fence,'unknown');db.prepare("UPDATE pooled_remote_offers SET state='unknown' WHERE message_id=?").run(row.message_id);this.append(db,row,'disconnected',{...binding,replay:false});}return this.reconcile(scope,binding);});}
  reconcile(value:RemoteScope,binding:PooledBinding){const scope=scopeValue(value);return this.pool.inspect(db=>{if(!this.exists(db))throw new Error('pooled remote: no protocol journal');this.resources(scope);const {row,lease}=this.claimRow(db,scope,binding);return {...binding,state:row.state as PooledState,leaseState:lease.state,ack:row.ack?JSON.parse(row.ack):null,replay:false};});}
  reconcileClaim(value:RemoteScope,claimId:string):(PooledClaim&{state:PooledState;replay:false})|null{const scope=scopeValue(value);token(claimId,'claim');return this.pool.inspect(db=>{if(!this.exists(db))return null;this.resources(scope);const saved=db.prepare('SELECT value FROM pooled_remote_claims WHERE claim_id=?').get(claimId) as any;if(!saved)return null;const claim=JSON.parse(saved.value) as PooledClaim;claimReplyFits(claim);const {row}=this.claimRow(db,scope,bindingOnly(claim));return {...claim,state:row.state as PooledState,replay:false};});}
  events(value:RemoteScope,after=0,limit=100){const scope=scopeValue(value);if(!Number.isSafeInteger(after)||after<0||!Number.isSafeInteger(limit)||limit<1||limit>1000)throw new Error('pooled remote: invalid event cursor');return this.pool.inspect(db=>{if(!this.exists(db))return [];this.resources(scope);const where=scope.projects.map(()=>'(e.project_key=? AND e.session_id=?)').join(' OR ');return (db.prepare(`SELECT e.* FROM pooled_remote_events e JOIN pooled_remote_offers o ON o.run_id=e.run_id JOIN pool_requests q ON q.id=o.request_id LEFT JOIN pooled_remote_claims c ON c.claim_id=o.claim_id WHERE e.event_id>? AND e.endpoint_id=? AND (${where}) AND ${this.resourceClause(scope)} ORDER BY e.event_id LIMIT ?`).all(after,scope.endpointId,...scope.projects.flatMap(p=>[p.projectKey,p.sessionId]),...scope.resourceIds,...scope.resourceIds,limit) as any[]).map(e=>({event_id:e.event_id,run_id:e.run_id,run_seq:e.run_seq,projectKey:e.project_key,sessionId:e.session_id,endpointId:e.endpoint_id,kind:e.kind,data:JSON.parse(e.data)}));});}
}
