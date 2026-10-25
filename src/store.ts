import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, chmodSync } from 'node:fs';
import { dirname } from 'node:path';
import { validateConfig, validatePlan } from './plan.ts';
import type { Config, Event, Plan, Run, Instance, Endpoint } from './types.ts';

const statuses = ['prepared', 'verifying', 'running', 'exited', 'failed', 'unknown', 'stopped', 'verified', 'rejected', 'accepted'];
const immutable = ['id', 'taskId', 'instanceId', 'provider', 'sessionId', 'baseSha', 'worktree', 'createdAt'];
const mutable = ['status', 'pid', 'processStart', 'exitCode', 'commitSha', 'reviewBy'];
const safeId = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
function validateRun(run: Run): void {
  for (const field of ['id', 'taskId', 'instanceId', 'sessionId'] as const) if (typeof run[field] !== 'string' || !safeId.test(run[field])) throw new Error(`run: invalid ${field}`);
  if (!['fake', 'claude', 'codex'].includes(run.provider) || !statuses.includes(run.status)) throw new Error('run: invalid provider/status');
  if (!/^[a-f0-9]{40,64}$/.test(run.baseSha)) throw new Error('run: baseSha must be fixed commit SHA');
  if (typeof run.worktree !== 'string' || !run.worktree.startsWith('/') || run.worktree.includes('\0')) throw new Error('run: absolute worktree required');
  if (typeof run.createdAt !== 'string' || !Number.isFinite(Date.parse(run.createdAt))) throw new Error('run: invalid createdAt');
  if (run.pid !== null && (!Number.isSafeInteger(run.pid) || run.pid <= 0)) throw new Error('run: invalid pid');
  if (run.processStart !== null && (typeof run.processStart !== 'string' || !run.processStart)) throw new Error('run: invalid processStart');
  if (run.exitCode !== null && !Number.isSafeInteger(run.exitCode)) throw new Error('run: invalid exitCode');
  if (run.commitSha !== null && !/^[a-f0-9]{40,64}$/.test(run.commitSha)) throw new Error('run: invalid commitSha');
  if (run.reviewBy !== null && (typeof run.reviewBy !== 'string' || !safeId.test(run.reviewBy))) throw new Error('run: invalid reviewBy');
}
function encode(value: unknown): string {
  const encoded = JSON.stringify(value);
  if (encoded === undefined || Buffer.byteLength(encoded) > 2 * 1024 * 1024) throw new Error('stored value must be JSON and at most 2 MiB');
  return encoded;
}
/** Defense in depth for known credential shapes. Provider stream filtering remains required. */
function redact(value: unknown): unknown {
  if (typeof value === 'string') return value.replace(/\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,})\b/g, '[REDACTED]').replace(/(Bearer\s+)\S+/gi, '$1[REDACTED]').replace(/((?:api[_-]?key|access[_-]?token|password|secret)\s*[=:]\s*)[^\s,;]+/gi, '$1[REDACTED]');
  if (Array.isArray(value)) return value.map(redact);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, /^(?:authorization|cookie|token|password|secret|api[_-]?key|access[_-]?token|refresh[_-]?token)$/i.test(k) ? '[REDACTED]' : redact(v)]));
  return value;
}

export class Store {
  private db: DatabaseSync;
  private readOnly: boolean;
  private listeners = new Set<(event: Event) => void>();
  constructor(path: string, options: { readOnly?: boolean } = {}) {
    this.readOnly = options.readOnly ?? false;
    if (!this.readOnly && path !== ':memory:') mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    this.db = new DatabaseSync(path, { readOnly: this.readOnly });
    this.db.exec('PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;');
    if (!this.readOnly) {
      this.db.exec(`PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events(event_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT REFERENCES runs(id), run_seq INTEGER, kind TEXT NOT NULL, data TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(run_id,run_seq));
        CREATE INDEX IF NOT EXISTS events_run ON events(run_id,run_seq);`);
      if (path !== ':memory:') chmodSync(path, 0o600);
    }
  }
  close(): void { this.db.close(); this.listeners.clear(); }
  private write<T>(action: () => { value: T; event: Event }): T {
    if (this.readOnly) throw new Error('observer is read-only');
    this.db.exec('BEGIN IMMEDIATE');
    let result: { value: T; event: Event };
    try { result = action(); this.db.exec('COMMIT'); }
    catch (error) { this.db.exec('ROLLBACK'); throw error; }
    for (const listener of this.listeners) { try { listener(structuredClone(result.event)); } catch { /* a subscriber cannot undo a committed write */ } }
    return result.value;
  }
  private insertEvent(runId: string | null, kind: string, data: unknown): Event {
    if (!/^[A-Za-z0-9_.:-]{1,100}$/.test(kind)) throw new Error('event: invalid kind');
    const seq = runId === null ? null : Number((this.db.prepare('SELECT COALESCE(MAX(run_seq),0)+1 AS seq FROM events WHERE run_id=?').get(runId) as { seq:number }).seq);
    const at = new Date().toISOString(), encoded = encode(redact(data));
    const result = this.db.prepare('INSERT INTO events(run_id,run_seq,kind,data,created_at) VALUES(?,?,?,?,?)').run(runId, seq, kind, encoded, at);
    return { event_id: Number(result.lastInsertRowid), run_id: runId, run_seq: seq, kind, data: JSON.parse(encoded), created_at: at };
  }
  private saveMeta(key: string, value: unknown, kind: string): void {
    if (!key || key.length > 128) throw new Error('invalid metadata key');
    const encoded = encode(value);
    this.write(() => {
      this.db.prepare('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').run(key, encoded);
      return { value: undefined, event: this.insertEvent(null, kind, { key }) };
    });
  }
  setPlan(plan: Plan): void { this.saveMeta('plan', validatePlan(plan), 'plan.saved'); }
  getPlan(): Plan | null { const value = this.getMeta('plan'); return value === null ? null : validatePlan(value); }
  setConfig(config: Config): void { this.saveMeta('config', validateConfig(config), 'config.saved'); }
  getConfig(): Config | null { const value = this.getMeta('config'); return value === null ? null : validateConfig(value); }
  instances(): Instance[] { return [...(this.getConfig()?.instances??[]),...((this.getMeta('leasedInstances') as Instance[]|null)??[])]; }
  endpoints(): Endpoint[] { return [...(this.getConfig()?.endpoints??[]),...((this.getMeta('leasedEndpoints') as Endpoint[]|null)??[])]; }
  /** Durable per-lease snapshots; never change the original project configuration. */
  bindInstance(instance:Instance,endpoint:Endpoint):void {
    const config=this.getConfig();if(!config)throw new Error('Project configuration required');
    const old=this.instances().find(i=>i.id===instance.id);
    if(old){if(JSON.stringify(old)!==JSON.stringify(instance))throw new Error('Resource instance snapshot changed');return;}
    const endpoints=this.endpoints(),existing=endpoints.find(e=>e.id===endpoint.id);
    if(existing&&JSON.stringify(existing)!==JSON.stringify(endpoint))throw new Error('Resource endpoint snapshot changed');
    // Validate one new snapshot with the original configuration, without growing its instance limit.
    const validationPeers=(['manager','worker','reviewer'] as const).filter(role=>role!==instance.role).map(role=>({id:`validation-${role}`,provider:'fake' as const,role,model:'validation-only',capabilities:[],endpointId:endpoint.id}));
    validateConfig({...config,instances:[instance,...validationPeers],endpoints:[endpoint]});
    this.write(()=>{
      const snapshots=(this.getMeta('leasedInstances') as Instance[]|null)??[];
      this.db.prepare('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').run('leasedInstances',encode([...snapshots,instance]));
      if(!existing){const eps=(this.getMeta('leasedEndpoints') as Endpoint[]|null)??[];this.db.prepare('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').run('leasedEndpoints',encode([...eps,endpoint]));}
      return {value:undefined,event:this.insertEvent(null,'resource.instance_bound',{instance,endpoint})};
    });
  }
  setMeta(key: string, value: unknown): void {
    if (['plan', 'config'].includes(key)) throw new Error('use validated setPlan/setConfig');
    if(key==='gadget:evidence:v1')throw new Error('use gadget evidence compare-and-swap');
    this.saveMeta(key, value, 'meta.saved');
  }
  gadgetEvidenceSnapshot():{revision:number;records:Record<string,unknown>} {
    const value=this.getMeta('gadget:evidence:v1');
    if(value===null)return {revision:0,records:{}};
    const snapshot=value as {revision:number;records:Record<string,unknown>};
    if(!snapshot||!Number.isSafeInteger(snapshot.revision)||snapshot.revision<0||!snapshot.records||typeof snapshot.records!=='object'||Array.isArray(snapshot.records))throw new Error('Invalid gadget evidence snapshot');
    return snapshot;
  }
  /** One restricted evidence namespace. No run state, queue ACK, lease, or acceptance writes. */
  compareAndSwapGadgetEvidence(expectedRevision:number,records:Record<string,unknown>,events:Array<{kind:string;data:unknown}>):{revision:number;events:Event[]} {
    if(this.readOnly)throw new Error('observer is read-only');
    if(!Number.isSafeInteger(expectedRevision)||expectedRevision<0||expectedRevision>=Number.MAX_SAFE_INTEGER)throw new Error('Invalid gadget evidence revision');
    if(!records||typeof records!=='object'||Array.isArray(records)||Object.keys(records).length>256||Object.keys(records).some(key=>!/^[a-f0-9]{64}$/.test(key)))throw new Error('Invalid bounded gadget evidence records');
    if(!Array.isArray(events)||events.length>32||events.some(event=>!event||!/^gadget\.[A-Za-z0-9_.:-]{1,92}$/.test(event.kind)||Buffer.byteLength(encode(event.data))>32768)||Buffer.byteLength(encode(events))>262144)throw new Error('Invalid bounded gadget audit events');
    const revision=expectedRevision+1,encoded=encode({revision,records});
    this.db.exec('BEGIN IMMEDIATE');let committed:Event[];
    try{
      if(this.gadgetEvidenceSnapshot().revision!==expectedRevision)throw new Error('Gadget evidence revision conflict');
      this.db.prepare('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').run('gadget:evidence:v1',encoded);
      committed=events.map(event=>this.insertEvent(null,event.kind,event.data));
      this.db.exec('COMMIT');
    }catch(error){this.db.exec('ROLLBACK');throw error;}
    for(const event of committed)for(const listener of this.listeners){try{listener(structuredClone(event));}catch{/* Evidence already committed. */}}
    return {revision,events:committed};
  }
  getMeta(key: string): unknown | null {
    const row = this.db.prepare('SELECT value FROM meta WHERE key=?').get(key) as { value:string } | undefined;
    return row ? JSON.parse(row.value) : null;
  }
  createRun(run: Run): void {
    validateRun(run);
    if (run.status !== 'prepared' || run.pid !== null || run.processStart !== null || run.exitCode !== null || run.commitSha !== null || run.reviewBy !== null) throw new Error('new runs must be prepared with no execution evidence');
    const encoded = encode(run);
    this.write(() => { this.db.prepare('INSERT INTO runs(id,value) VALUES(?,?)').run(run.id, encoded); return { value: undefined, event: this.insertEvent(run.id, 'run.created', run) }; });
  }
  updateRun(id: string, patch: Partial<Run>, kind = 'run.updated', data: unknown = null): void {
    for (const key of Object.keys(patch)) if (immutable.includes(key) || !mutable.includes(key)) throw new Error(`run: immutable or unknown field ${key}`);
    this.write(() => {
      const old = this.getRun(id);
      if (!old) throw new Error(`unknown run ${id}`);
      const effective=old.status==='unknown'&&patch.status&&patch.status!=='unknown'?{...patch,status:'unknown' as const}:patch;
      const run = { ...old, ...effective }; validateRun(run);
      this.db.prepare('UPDATE runs SET value=? WHERE id=?').run(encode(run), id);
      return { value: undefined, event: this.insertEvent(id, effective!==patch?'run.unknown_preserved':kind, { patch:effective, attemptedStatus:patch.status, detail: data }) };
    });
  }
  recoverRun(id:string,data:unknown):Run['status'] {
    return this.write<Run['status']>(()=>{
      const run=this.getRun(id);if(!run)throw new Error('Unknown recovery run');
      if(!['prepared','running','verifying'].includes(run.status))return {value:run.status,event:this.insertEvent(id,'recovery.skipped',{reason:'Run completed before recovery transaction',status:run.status})};
      const phase=this.getMeta('verification:'+id) as any;
      if(run.status==='verifying'&&phase)this.db.prepare('UPDATE meta SET value=? WHERE key=?').run(encode({...phase,status:'unknown'}),'verification:'+id);
      this.db.prepare('UPDATE runs SET value=? WHERE id=?').run(encode({...run,status:'unknown'}),id);
      return {value:'unknown',event:this.insertEvent(id,'recovery.unknown',data)};
    });
  }
  finishVerification(id:string,verificationId:string,commitSha:string,worktree:string):boolean {
    return this.write(()=>{
      const run=this.getRun(id),phase=this.getMeta('verification:'+id) as any;
      if(!run||phase?.verificationId!==verificationId)throw new Error('Verification phase identity changed');
      if(run.status==='unknown'){
        this.db.prepare('UPDATE meta SET value=? WHERE key=?').run(encode({...phase,status:'unknown'}),'verification:'+id);
        return {value:false,event:this.insertEvent(id,'verification.unknown',{verificationId,reason:'Recovery uncertainty preserved at completion'})};
      }
      if(run.status!=='verifying'||phase.status!=='running')throw new Error('Verification state changed');
      const updated={...run,status:'verified',commitSha};validateRun(updated as Run);
      this.db.prepare('UPDATE runs SET value=? WHERE id=?').run(encode(updated),id);
      this.db.prepare('UPDATE meta SET value=? WHERE key=?').run(encode({...phase,status:'passed'}),'verification:'+id);
      return {value:true,event:this.insertEvent(id,'verification.passed',{verificationId,commitSha,worktree})};
    });
  }
  acceptReviewedRun(workerId:string,reviewRunId:string):Run {
    return this.write(()=>{
      const worker=this.getRun(workerId),review=this.getRun(reviewRunId);
      const evidence=this.getMeta('reviewEvidence:'+workerId) as any;
      const binding=this.getMeta('reviewBinding:'+reviewRunId) as any;
      const reviewer=this.instances().find(i=>i.id===review?.instanceId);
      if(!worker||worker.status!=='verified'||!review||review.status!=='exited'||reviewer?.role!=='reviewer'||review.instanceId===worker.instanceId||!worker.commitSha||review.baseSha!==worker.commitSha||evidence?.reviewRunId!==reviewRunId||evidence.workerRunId!==workerId||evidence.commitSha!==worker.commitSha||evidence.reviewerId!==review.instanceId||binding?.workerRunId!==workerId||binding.commitSha!==worker.commitSha||!['approve','reject'].includes(evidence.verdict))throw new Error('Review acceptance binding/state mismatch');
      const updated:Run={...worker,status:evidence.verdict==='approve'?'accepted':'rejected',reviewBy:review.instanceId};
      this.db.prepare('UPDATE runs SET value=? WHERE id=?').run(encode(updated),workerId);
      return {value:updated,event:this.insertEvent(workerId,'review.acceptance',{reviewRunId,commitSha:worker.commitSha,verdict:evidence.verdict,authority:'Program gate after independent verification and reviewer execution'})};
    });
  }
  getRun(id: string): Run | null {
    const row = this.db.prepare('SELECT value FROM runs WHERE id=?').get(id) as { value:string } | undefined;
    return row ? JSON.parse(row.value) : null;
  }
  listRuns(): Run[] { return (this.db.prepare('SELECT value FROM runs ORDER BY rowid').all() as { value:string }[]).map(row => JSON.parse(row.value)); }
  event(runId: string | null, kind: string, data: unknown): Event { return this.write(() => { const event = this.insertEvent(runId, kind, data); return { value:event, event }; }); }
  events(after = 0, limit = 100): Event[] {
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error('events: after >= 0 and limit 1..1000 required');
    return this.db.prepare('SELECT * FROM events WHERE event_id>? ORDER BY event_id LIMIT ?').all(after, limit).map(row => ({ ...row, data: JSON.parse(row.data as string) })) as Event[];
  }
  subscribe(listener: (event: Event) => void): () => void { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; }
}
