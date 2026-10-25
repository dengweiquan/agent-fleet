import { DatabaseSync } from 'node:sqlite';
import { randomUUID } from 'node:crypto';
import { mkdirSync, chmodSync, realpathSync, existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

export type PoolProvider = 'fake'|'claude'|'codex'|'external-agent';
export type PoolRole = 'manager'|'worker'|'reviewer';
export interface Resource {
  id:string; provider:PoolProvider; model:string; endpointId:string;
  endpointKind:'local'|'ssh-stdio'|'external-agent'; accountDomain:string;
  capabilities:string[]; roles:PoolRole[]; capacity?:number;
  costKnown?:boolean; cost?:{currency:string;inputPerMillion?:number;outputPerMillion?:number};
}
export interface PoolLimits { global:number; projects:Record<string,number>; endpoints:Record<string,number>; accounts:Record<string,number> }
export interface PoolRequest {
  id:string; projectId:string; sessionId:string; taskId:string; attemptId:string;
  role:PoolRole; capabilities:string[]; preferredProvider?:PoolProvider; requiredProvider?:PoolProvider;
  excludedResourceIds?:string[]; eligibleResourceIds?:string[];
}
export type LeaseState = 'held'|'unknown'|'completed'|'failed';
export interface Lease { id:string; fence:number; state:LeaseState; request:PoolRequest; resource:Resource; runId:string|null; createdAt:string; updatedAt:string }
export interface QueueEntry { seq:number; request:PoolRequest; state:'queued'|LeaseState }
export interface ReconciliationEvidence { executionEnded:true; verifiedBy:string; reason:string; runId:string|null }
export interface PoolEvent { event_id:number; kind:string; data:unknown; created_at:string }
export interface PoolStatus { resources:Resource[]; limits:PoolLimits; requests:QueueEntry[]; leases:Lease[] }
const providers = ['fake','claude','codex','external-agent'];
const roles = ['manager','worker','reviewer'];
const initialLimits: PoolLimits = { global:1, projects:{}, endpoints:{}, accounts:{} };
const active = (lease: Lease) => lease.state === 'held' || lease.state === 'unknown';

function object(value:unknown, required:string[], optional:string[] = []):Record<string,unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('pool: expected object');
  const obj = value as Record<string,unknown>;
  if (Object.getPrototypeOf(obj) !== Object.prototype && Object.getPrototypeOf(obj) !== null) throw new Error('pool: plain object required');
  for (const key of required) if (!Object.hasOwn(obj,key)) throw new Error(`pool: missing ${key}`);
  for (const key of Object.keys(obj)) if (![...required,...optional].includes(key)) throw new Error(`pool: unknown field ${key}`);
  return obj;
}
function string(value:unknown, max=256):string {
  if (typeof value !== 'string' || !value.trim() || value !== value.trim() || value.length > max || /[\x00-\x1f\x7f]/.test(value)) throw new Error('pool: invalid bounded string');
  return value;
}
function integer(value:unknown, min=1, max=1000):number {
  if (!Number.isSafeInteger(value) || (value as number) < min || (value as number) > max) throw new Error(`pool: integer ${min}..${max} required`);
  return value as number;
}
function strings(value:unknown):string[] {
  if (!Array.isArray(value) || value.length > 64) throw new Error('pool: at most 64 values required');
  const result = value.map(v => string(v));
  if (new Set(result).size !== result.length) throw new Error('pool: duplicate values');
  return result.sort();
}
function encode(value:unknown):string {
  const result = JSON.stringify(value);
  if (result === undefined || Buffer.byteLength(result) > 32768) throw new Error('pool: JSON payload exceeds 32 KiB');
  return result;
}
function provider(value:unknown):PoolProvider {
  if (!providers.includes(value as string)) throw new Error('pool: unsupported provider');
  return value as PoolProvider;
}
function role(value:unknown):PoolRole {
  if (!roles.includes(value as string)) throw new Error('pool: unsupported role');
  return value as PoolRole;
}
function resource(value:unknown):Resource {
  const r = object(value, ['id','provider','model','endpointId','endpointKind','accountDomain','capabilities','roles'], ['capacity','costKnown','cost']);
  if (!['local','ssh-stdio','external-agent'].includes(r.endpointKind as string)) throw new Error('pool: unsupported endpoint kind');
  const assignedRoles = strings(r.roles).map(role);
  if (!assignedRoles.length) throw new Error('pool: resource requires a role');
  const result:Resource = { id:string(r.id), provider:provider(r.provider), model:string(r.model), endpointId:string(r.endpointId), endpointKind:r.endpointKind as Resource['endpointKind'], accountDomain:string(r.accountDomain), capabilities:strings(r.capabilities), roles:assignedRoles, capacity:Object.hasOwn(r,'capacity') ? integer(r.capacity) : 1, costKnown:false };
  if (Object.hasOwn(r,'costKnown') && typeof r.costKnown !== 'boolean') throw new Error('pool: costKnown must be boolean');
  if (Object.hasOwn(r,'cost')) {
    const c = object(r.cost, ['currency'], ['inputPerMillion','outputPerMillion']);
    const cost:NonNullable<Resource['cost']> = {currency:string(c.currency,16)};
    for (const key of ['inputPerMillion','outputPerMillion'] as const) if (Object.hasOwn(c,key)) {
      if (typeof c[key] !== 'number' || !Number.isFinite(c[key]) || c[key] < 0 || c[key] > 1e9) throw new Error('pool: invalid cost');
      cost[key] = c[key];
    }
    if (cost.inputPerMillion === undefined && cost.outputPerMillion === undefined) throw new Error('pool: cost requires at least one rate');
    if (r.costKnown === false) throw new Error('pool: supplied cost contradicts costKnown=false');
    result.cost = cost; result.costKnown = true;
  } else if (r.costKnown === true) throw new Error('pool: known cost requires explicit rates');
  encode(result); return result;
}
function request(value:unknown):PoolRequest {
  const q = object(value, ['id','projectId','sessionId','taskId','attemptId','role','capabilities'], ['preferredProvider','requiredProvider','excludedResourceIds','eligibleResourceIds']);
  const result:PoolRequest = { id:string(q.id), projectId:string(q.projectId), sessionId:string(q.sessionId), taskId:string(q.taskId), attemptId:string(q.attemptId), role:role(q.role), capabilities:strings(q.capabilities) };
  for (const field of ['preferredProvider','requiredProvider'] as const) if (Object.hasOwn(q,field)) result[field] = provider(q[field]);
  for (const field of ['excludedResourceIds','eligibleResourceIds'] as const) if (Object.hasOwn(q,field)) result[field] = strings(q[field]);
  encode(result); return result;
}
function limits(value:unknown):PoolLimits {
  const l = object(value, ['global','projects','endpoints','accounts']);
  const result:PoolLimits = { global:integer(l.global), projects:{}, endpoints:{}, accounts:{} };
  for (const field of ['projects','endpoints','accounts'] as const) {
    const map = l[field];
    if (!map || typeof map !== 'object' || Array.isArray(map) || ![Object.prototype,null].includes(Object.getPrototypeOf(map)) || Object.keys(map).length > 1000) throw new Error('pool: invalid limit map');
    result[field] = Object.fromEntries(Object.keys(map).sort().map(key => [string(key),integer((map as Record<string,unknown>)[key])]));
  }
  encode(result); return result;
}
function compatible(q:PoolRequest,r:Resource):boolean {
  return r.roles.includes(q.role) && q.capabilities.every(c => r.capabilities.includes(c)) && (!q.requiredProvider || q.requiredProvider === r.provider) && !q.excludedResourceIds?.includes(r.id) && (!q.eligibleResourceIds || q.eligibleResourceIds.includes(r.id));
}
function validateRegistry(resources:Resource[]):void {
  const transports=new Map<string,Resource['endpointKind']>();
  const localAccounts=new Map<PoolProvider,string>();
  let localEndpoint:string|undefined;
  for (const r of resources) {
    const transport=transports.get(r.endpointId);
    if (transport !== undefined && transport !== r.endpointKind) throw new Error('pool: endpoint identity has conflicting transport definitions');
    transports.set(r.endpointId,r.endpointKind);
    if (r.endpointKind !== 'local') continue;
    if (localEndpoint !== undefined && localEndpoint !== r.endpointId) throw new Error('pool: local resources must share one endpoint identity for this host');
    localEndpoint=r.endpointId;
    if (r.provider === 'fake') continue;
    const account=localAccounts.get(r.provider);
    if (account !== undefined && account !== r.accountDomain) throw new Error('pool: local resources for one real provider must share one account domain; profile selection is not implemented');
    localAccounts.set(r.provider,r.accountDomain);
  }
}

/** Shared arbitration only: grants reserve capacity, never prove task acceptance or process death. */
export class ResourcePool {
  readonly path:string;
  private db:DatabaseSync;
  private readOnly:boolean;
  private transactionDepth=0;
  private readDepth=0;
  constructor(path:string, options:{readOnly?:boolean;mustExist?:boolean} = {}) {
    this.readOnly = options.readOnly ?? false;
    if(options.mustExist&&(path===':memory:'||!existsSync(path)))throw new Error('Shared resource authority is missing; refusing to create a replacement pool');
    if (!this.readOnly && path !== ':memory:') mkdirSync(dirname(path), {recursive:true,mode:0o700});
    this.db = new DatabaseSync(path, {readOnly:this.readOnly});
    if(options.mustExist){const expected=['pool_resources','pool_settings','pool_requests','pool_leases','pool_fairness','pool_events'];const present=this.db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(row=>row.name);if(expected.some(name=>!present.includes(name))){this.db.close();throw new Error('Shared resource authority schema is missing; explicit migration/reconciliation required');}}
    this.path = path === ':memory:' ? `:memory:${randomUUID()}` : realpathSync(resolve(path));
    this.db.exec('PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;');
    if (!this.readOnly) {
      this.db.exec(`PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS pool_resources(id TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pool_settings(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pool_requests(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,project_id TEXT NOT NULL,session_id TEXT NOT NULL,task_id TEXT NOT NULL,attempt_id TEXT NOT NULL,value TEXT NOT NULL,UNIQUE(project_id,session_id,task_id,attempt_id));
        CREATE TABLE IF NOT EXISTS pool_leases(fence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,request_id TEXT UNIQUE NOT NULL REFERENCES pool_requests(id),value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pool_fairness(project_id TEXT PRIMARY KEY,last_fence INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS pool_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,data TEXT NOT NULL,created_at TEXT NOT NULL);`);
      this.db.prepare('INSERT OR IGNORE INTO pool_settings(id,value) VALUES(1,?)').run(encode(initialLimits));
      if (path !== ':memory:') chmodSync(path,0o600);
    }
  }
  close():void { this.db.close(); }
  get isReadOnly():boolean{return this.readOnly;}
  private transaction<T>(fn:()=>T):T {
    if (this.readOnly) throw new Error('pool: observer is read-only');
    if(this.readDepth&&!this.transactionDepth)throw new Error('pool: cannot mutate within observer snapshot');
    const depth=this.transactionDepth,savepoint=`pool_nested_${depth}`;
    this.db.exec(depth?`SAVEPOINT ${savepoint}`:'BEGIN IMMEDIATE');this.transactionDepth++;
    try { const result=fn();if(result&&typeof (result as any).then==='function')throw new Error('pool: atomic callback must be synchronous');this.db.exec(depth?`RELEASE ${savepoint}`:'COMMIT');return result; }
    catch (error) { this.db.exec(depth?`ROLLBACK TO ${savepoint}`:'ROLLBACK');if(depth)this.db.exec(`RELEASE ${savepoint}`);throw error; }
    finally{this.transactionDepth--;}
  }
  /** Internal synchronous composition point: one transaction for arbitration plus protocol state. */
  atomic<T>(fn:(db:DatabaseSync)=>T):T{return this.transaction(()=>fn(this.db));}
  /** Internal consistent read snapshot; never creates tables or starts a nested BEGIN. */
  inspect<T>(fn:(db:DatabaseSync)=>T):T {
    if(this.transactionDepth||this.readDepth)return fn(this.db);
    this.db.exec('BEGIN');this.readDepth++;
    try{const result=fn(this.db);if(result&&typeof (result as any).then==='function')throw new Error('pool: inspect callback must be synchronous');this.db.exec('COMMIT');return result;}
    catch(error){this.db.exec('ROLLBACK');throw error;}finally{this.readDepth--;}
  }
  private audit(kind:string,data:unknown):void { this.db.prepare('INSERT INTO pool_events(kind,data,created_at) VALUES(?,?,?)').run(kind,encode(data),new Date().toISOString()); }
  private getResources():Resource[] { return this.db.prepare('SELECT value FROM pool_resources ORDER BY id').all().map(r => JSON.parse(r.value as string)); }
  private getLimits():PoolLimits { return JSON.parse(this.db.prepare('SELECT value FROM pool_settings WHERE id=1').get()!.value as string); }
  private getLeases():Lease[] { return this.db.prepare('SELECT value FROM pool_leases ORDER BY fence').all().map(r => JSON.parse(r.value as string)); }
  private getRequests():QueueEntry[] {
    return this.db.prepare('SELECT q.seq,q.value,l.value AS lease FROM pool_requests q LEFT JOIN pool_leases l ON l.request_id=q.id ORDER BY q.seq').all().map(r => ({seq:Number(r.seq), request:JSON.parse(r.value as string),state:r.lease ? JSON.parse(r.lease as string).state : 'queued'}));
  }
  private hasWork():boolean { return this.getRequests().some(q => q.state === 'queued' || q.state === 'held' || q.state === 'unknown'); }
  upsertResource(value:Resource):Resource {
    const normalized=resource(value), encoded=encode(normalized);
    return this.transaction(() => {
      const old=this.db.prepare('SELECT value FROM pool_resources WHERE id=?').get(normalized.id);
      const registered=this.getResources();
      validateRegistry([...registered.filter(r => r.id !== normalized.id),normalized]);
      if (old?.value === encoded) return normalized;
      if (this.hasWork()) throw new Error('pool: cannot change shared definitions while work is pending or active');
      if (!old && registered.length >= 1000) throw new Error('pool: registry capacity reached');
      this.db.prepare('INSERT INTO pool_resources(id,value) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value').run(normalized.id,encoded);
      this.audit('resource.updated',{id:normalized.id}); return normalized;
    });
  }
  setLimits(value:PoolLimits):void {
    const normalized=limits(value), encoded=encode(normalized);
    this.transaction(() => {
      if (encode(this.getLimits()) === encoded) return;
      if (this.hasWork()) throw new Error('pool: cannot change shared definitions while work is pending or active');
      this.db.prepare('UPDATE pool_settings SET value=? WHERE id=1').run(encoded); this.audit('limits.updated',normalized);
    });
  }
  enqueue(value:PoolRequest):QueueEntry {
    const normalized=request(value), encoded=encode(normalized);
    return this.transaction(() => {
      const entries=this.getRequests(), old=entries.find(q => q.request.id === normalized.id);
      if (old) { if (encode(old.request) !== encoded) throw new Error('pool: request id reused with different payload'); return old; }
      if (entries.length >= 10000) throw new Error('pool: queue history capacity reached');
      const r=this.db.prepare('INSERT INTO pool_requests(id,project_id,session_id,task_id,attempt_id,value) VALUES(?,?,?,?,?,?)').run(normalized.id,normalized.projectId,normalized.sessionId,normalized.taskId,normalized.attemptId,encoded);
      this.audit('request.queued',{id:normalized.id,projectId:normalized.projectId});
      return {seq:Number(r.lastInsertRowid),request:normalized,state:'queued'};
    });
  }
  claimNext(ownedRequestIds?:string[],allowedResourceIds?:string[]):Lease|null {
    if (ownedRequestIds !== undefined && (!Array.isArray(ownedRequestIds) || ownedRequestIds.length > 10000 || ownedRequestIds.some(x => typeof x !== 'string'))) throw new Error('pool: invalid owned request ids');
    if (allowedResourceIds !== undefined && (!Array.isArray(allowedResourceIds) || allowedResourceIds.length > 1000 || allowedResourceIds.some(x => typeof x !== 'string'))) throw new Error('pool: invalid allowed resource ids');
    return this.transaction(() => {
      const occupied=this.getLeases().filter(active), lim=this.getLimits();
      if (occupied.length >= lim.global) return null;
      const registry=this.getResources(), fairness=new Map(this.db.prepare('SELECT project_id,last_fence FROM pool_fairness').all().map(r => [r.project_id as string,Number(r.last_fence)]));
      validateRegistry(registry);
      // Fairness applies to this coordinator's participating scopes. An absent/paused
      // coordinator must not block other owners; shared capacity still applies globally.
      const owned=ownedRequestIds === undefined ? null : new Set(ownedRequestIds);
      const queued=this.getRequests().filter(q => q.state === 'queued' && (owned === null || owned.has(q.request.id))).sort((a,b) => (fairness.get(a.request.projectId) ?? 0)-(fairness.get(b.request.projectId) ?? 0) || a.seq-b.seq);
      for (const entry of queued) {
        const q=entry.request;
        if (occupied.filter(l => l.request.projectId === q.projectId).length >= (Object.hasOwn(lim.projects,q.projectId) ? lim.projects[q.projectId] : 1)) continue;
        const candidates=registry.filter(r => compatible(q,r) && (allowedResourceIds===undefined||allowedResourceIds.includes(r.id))
          && occupied.filter(l => l.resource.id === r.id).length < (r.capacity ?? 1)
          && occupied.filter(l => l.resource.endpointId === r.endpointId).length < (Object.hasOwn(lim.endpoints,r.endpointId) ? lim.endpoints[r.endpointId] : 1)
          && occupied.filter(l => l.resource.accountDomain === r.accountDomain).length < (Object.hasOwn(lim.accounts,r.accountDomain) ? lim.accounts[r.accountDomain] : 1))
          .sort((a,b) => Number(b.provider === q.preferredProvider)-Number(a.provider === q.preferredProvider) || a.id.localeCompare(b.id));
        if (!candidates.length) continue;
        const id=randomUUID(), at=new Date().toISOString();
        const row=this.db.prepare('INSERT INTO pool_leases(id,request_id,value) VALUES(?,?,?)').run(id,q.id,'{}');
        const lease:Lease={id,fence:Number(row.lastInsertRowid),state:'held',request:q,resource:candidates[0],runId:null,createdAt:at,updatedAt:at};
        this.saveLease(lease);
        this.db.prepare('INSERT INTO pool_fairness(project_id,last_fence) VALUES(?,?) ON CONFLICT(project_id) DO UPDATE SET last_fence=excluded.last_fence').run(q.projectId,lease.fence);
        this.audit('lease.granted',{id:lease.id,fence:lease.fence,requestId:q.id,resourceId:lease.resource.id}); return lease;
      }
      return null;
    });
  }
  private guarded(id:string,fence:number):Lease {
    string(id); integer(fence,1,Number.MAX_SAFE_INTEGER);
    const row=this.db.prepare('SELECT value FROM pool_leases WHERE id=? AND fence=?').get(id,fence);
    if (!row) throw new Error('pool: lease identity/fence mismatch'); return JSON.parse(row.value as string);
  }
  private saveLease(lease:Lease):void { this.db.prepare('UPDATE pool_leases SET value=? WHERE id=? AND fence=?').run(encode(lease),lease.id,lease.fence); }
  bindRun(id:string,fence:number,runId:string):Lease {
    string(runId);
    return this.transaction(() => {
      const lease=this.guarded(id,fence);
      if (lease.runId === runId) return lease;
      if (lease.state !== 'held' || lease.runId !== null) throw new Error('pool: run binding is immutable and requires held lease');
      lease.runId=runId; lease.updatedAt=new Date().toISOString(); this.saveLease(lease); this.audit('lease.bound',{id,fence,runId}); return lease;
    });
  }
  release(id:string,fence:number,outcome:'completed'|'failed'|'unknown'):Lease {
    if (!['completed','failed','unknown'].includes(outcome)) throw new Error('pool: invalid lease outcome');
    return this.transaction(() => {
      const lease=this.guarded(id,fence);
      if (lease.state !== 'held') throw new Error('pool: only held lease can release; unknown requires explicit reconciliation');
      lease.state=outcome; lease.updatedAt=new Date().toISOString(); this.saveLease(lease); this.audit(`lease.${outcome}`,{id,fence,runId:lease.runId}); return lease;
    });
  }
  reconcile(id:string,fence:number,outcome:'completed'|'failed',evidence:ReconciliationEvidence):Lease {
    if (!['completed','failed'].includes(outcome)) throw new Error('pool: invalid reconciliation outcome');
    const e=object(evidence,['executionEnded','verifiedBy','reason','runId']);
    if (e.executionEnded !== true) throw new Error('pool: verified execution end required; time/PID alone is insufficient');
    const verifiedBy=string(e.verifiedBy), reason=string(e.reason,1024), runId=e.runId === null ? null : string(e.runId);
    return this.transaction(() => {
      const lease=this.guarded(id,fence);
      if (lease.state !== 'unknown' || lease.runId !== runId) throw new Error('pool: reconciliation must identify the unknown bound execution exactly');
      lease.state=outcome; lease.updatedAt=new Date().toISOString(); this.saveLease(lease); this.audit('lease.reconciled',{id,fence,outcome,runId,verifiedBy,reason,executionEnded:true}); return lease;
    });
  }
  status():PoolStatus {
    // A deferred read transaction gives observers one consistent cross-table snapshot.
    return this.inspect(()=>({resources:this.getResources(),limits:this.getLimits(),requests:this.getRequests(),leases:this.getLeases()}));
  }
  event(kind:string,data:unknown):void {
    if(!/^[a-z][a-z0-9_.-]{0,99}$/.test(kind))throw new Error('pool: invalid event kind');
    this.transaction(()=>this.audit(kind,data));
  }
  events(after=0,limit=100):PoolEvent[] {
    integer(after,0,Number.MAX_SAFE_INTEGER); integer(limit,1,1000);
    return this.db.prepare('SELECT * FROM pool_events WHERE event_id>? ORDER BY event_id LIMIT ?').all(after,limit).map(row => ({event_id:Number(row.event_id),kind:row.kind as string,data:JSON.parse(row.data as string),created_at:row.created_at as string}));
  }
}
