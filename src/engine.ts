import {mkdirSync,existsSync,realpathSync,readFileSync,openSync,closeSync,unlinkSync,writeFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {randomUUID,createHash} from 'node:crypto';
import {parsePlanProposal} from './structured.ts';
import {Store} from './store.ts';
import {validatePlan,validateConfig,defaultConfig} from './plan.ts';
import {git,head,repository,clean,snapshot} from './git.ts';
import {buildInvocation,createParser} from './adapters.ts';
import {supervise,processStart} from './process.ts';
import type {Run,Task,Instance,Plan,Config} from './types.ts';
import {createSession,selectSession,touchSession} from './sessions.ts';
import {ReverseCoordinator} from './remote.ts';
import {ResourcePool} from './resource-pool.ts';
import {PooledRemoteQueue,pooledProjectKey} from './pooled-remote.ts';

export class Engine {
  root:string; dir:string; sessionId:string; store:Store; readonly:boolean;
  controls = new Map<string,{stop:()=>boolean}>();
  beforeExecution?: (run:Run,instance:Instance)=>void;
  constructor(cwd:string,readOnly=false,sessionId?:string) {
    this.root=repository(cwd); const session=selectSession(this.root,sessionId); this.sessionId=session.id;this.dir=session.dir;this.readonly=readOnly;
    if(readOnly&&!existsSync(join(this.dir,'state.sqlite')))throw new Error('Session state unavailable');
    this.store=new Store(join(this.dir,'state.sqlite'),{readOnly});
  }
  static init(cwd:string,title='AgentFleet session') {
    const root=repository(cwd), session=createSession(root,title);
    const engine=new Engine(root,false,session.id); engine.store.setConfig(defaultConfig()); engine.store.setMeta('paused',false);
    engine.store.setMeta('logicalFormatVersion',2);engine.store.setMeta('sessionId',session.id);engine.store.setMeta('root',root); engine.store.setMeta('baseSha',head(root)); return engine;
  }
  formatVersion(){return this.store.getMeta('logicalFormatVersion')??1;}
  mutate(){if(this.readonly)throw new Error('Observer access cannot mutate state');if(this.formatVersion()!==2)throw new Error('Legacy session is read-only in phase2; explicit migration is not implemented. Use the stable version or initialize a separate phase2 session.');}
  close(){if(!this.readonly&&this.formatVersion()===2){const states:Record<string,number>={};for(const r of this.store.listRuns())states[r.status]=(states[r.status]??0)+1;try{touchSession(this.sessionId,{manager:this.store.instances().find(i=>i.role==='manager')?.id,taskStates:states});}catch{/* DB remains authoritative; index is best-effort metadata */}}this.store.close();}
  config(value?:unknown){if(value!==undefined){this.mutate();if(this.store.listRuns().length||this.remoteStatus().length)throw new Error('Configuration snapshot is immutable after first run; create another session');this.store.setConfig(validateConfig(value));}return this.store.getConfig();}
  plan(value:unknown){this.mutate();if(this.remoteStatus().length||this.store.listRuns().some(r=>this.store.instances().find(i=>i.id===r.instanceId)?.role==='worker'))throw new Error('Plan is immutable after first task run'); const p=validatePlan(value);this.store.setPlan(p);return p;}
  private poolPause(paused:boolean){const path=this.store.getMeta('resourcePoolPath') as string|null;if(!path)return;const pool=new ResourcePool(path,{mustExist:true});try{new PooledRemoteQueue(pool).setPaused(pooledProjectKey(this.root),this.sessionId,paused);}finally{pool.close();}}
  pause(){this.mutate();this.poolPause(true);this.store.setMeta('paused',true);this.store.event(null,'paused',{});}
  resume(){this.mutate();const result=this.reconcile();if(existsSync(join(this.dir,'coordinator.lock')))throw new Error('Coordinator lock retained; inspect recovery evidence before continuing');this.poolPause(false);this.store.setMeta('paused',false);this.store.event(null,'resumed',{});return result;}
  remoteStatus(){
    const p=join(this.dir,'remote.sqlite');let observations:ReturnType<ReverseCoordinator['list']>=[];
    if(existsSync(p)){const q=new ReverseCoordinator(p,{readOnly:true});try{observations=q.list();}finally{q.close();}}
    const poolPath=this.store.getMeta('resourcePoolPath') as string|null;
    if(poolPath){
      // A missing/unreadable shared authority is an error, never an empty queue that permits replay.
      const pool=new ResourcePool(poolPath,{readOnly:true});
      try{
        const pending=(this.store.getMeta('pooledRemoteOffers') as any[]|null)??[];
        const queue=new PooledRemoteQueue(pool,{readOnly:true}),registry=pool.status().resources;
        const endpoints=new Set([...registry.filter(r=>r.endpointKind!=='local').map(r=>r.endpointId),...pending.map(o=>o.dispatch.endpointId as string)]);
        for(const endpointId of endpoints){
          const resourceIds=registry.filter(r=>r.endpointId===endpointId&&r.endpointKind!=='local').map(r=>r.id);
          if(!resourceIds.length)throw new Error('Remote offer endpoint disappeared from shared registry');
          const scope={endpointId,resourceIds,projects:[{projectKey:pooledProjectKey(this.root),sessionId:this.sessionId}]};let cursor=0;
          while(true){const rows=queue.list(scope,cursor,1000);for(const row of rows)observations.push({...row,provider:row.provider,repositoryId:row.repositoryId,baseSha:row.baseSha,acknowledged:row.acknowledged});if(rows.length<1000)break;const next=rows.at(-1)!.rowCursor;if(next===undefined||!Number.isSafeInteger(next)||next<=cursor)throw new Error('Remote history cursor failed to advance');cursor=next;}
        }
        for(const offer of pending)if(!observations.some(row=>row.messageId===offer.dispatch.messageId)){const d=offer.dispatch;observations.push({messageId:d.messageId,taskId:d.taskId,runId:d.runId,attemptId:d.attemptId,endpointId:d.endpointId,claimId:null,state:'offer-pending',provider:d.provider,repositoryId:d.repositoryId,baseSha:d.baseSha,fence:d.fence,acknowledged:false});}
      }finally{pool.close();}
    }
    return observations;
  }
  status(){const runs=this.store.listRuns();return {logicalFormatVersion:this.formatVersion(),remote:this.remoteStatus(),sessionId:this.sessionId,project:this.root,baseSha:this.store.getMeta('baseSha'),paused:this.store.getMeta('paused'),plan:this.store.getPlan(),runs,resourcePoolPath:this.store.getMeta('resourcePoolPath'),resourceBindings:runs.flatMap(r=>{const binding=this.store.getMeta('resourceLease:'+r.id);return binding?[binding]:[];}),pendingManagerTurn:this.store.getMeta('fleetManagerPending')};}
  reconcile(){
    this.mutate(); const result=[];
    for(const r of this.store.listRuns().filter(r=>r.status==='running'||r.status==='prepared'||r.status==='verifying')) {
      if(this.controls.has(r.id))continue;
      const lockPath=join(this.dir,'coordinator.lock');let owner:any=null;try{owner=JSON.parse(readFileSync(lockPath,'utf8'));}catch{}
      const phase=r.status==='verifying'?this.store.getMeta('verification:'+r.id) as any:null;
      const execution=phase??r;const executionTree=phase?.worktree??r.worktree;const observed=execution.pid?processStart(execution.pid):null;let tree:unknown={exists:existsSync(executionTree)};try{tree={exists:true,head:head(executionTree),clean:clean(executionTree)};}catch{}
      const evidence={runId:r.id,verificationId:phase?.verificationId??null,childIdentityMatches:!!observed&&observed===execution.processStart,lockPresent:existsSync(lockPath),ownerIdentityMatches:!!owner?.start&&processStart(owner.pid)===owner.start,worktree:tree,replay:false};
      this.store.event(r.id,'recovery.inspected',evidence);
      if(evidence.ownerIdentityMatches){result.push(evidence);continue;}
      const recoveredStatus=this.store.recoverRun(r.id,{expected:execution.processStart,observed,reason:'No retained child handle; no replay or PID-based death inference'});
      result.push({...evidence,status:recoveredStatus});
    }return result;
  }
  task(id:string):Task {const t=this.store.getPlan()?.tasks.find(t=>t.id===id);if(!t)throw new Error('Unknown task');return t;}
  instance(id:string,role:'worker'|'reviewer'|'manager'):Instance {
    const i=this.store.instances().find(i=>i.id===id&&i.role===role);if(!i)throw new Error(`No ${role} instance: ${id}`);return i;
  }
  pick(task:Task):Instance {const i=this.store.instances().find(i=>i.role==='worker'&&task.capabilities.every(c=>i.capabilities.includes(c)));if(!i)throw new Error('No capable worker');return i;}
  async exclusive<T>(action:()=>Promise<T>):Promise<T> {
    this.mutate(); const path=join(this.dir,'coordinator.lock');let fd:number;
    try{fd=openSync(path,'wx',0o600);}catch{throw new Error('Coordinator lock exists; observe/reconcile manually, no stale PID unlock');}
    writeFileSync(fd,JSON.stringify({pid:process.pid,start:processStart(process.pid),sessionId:this.sessionId}));closeSync(fd);
    try{return await action();}finally{unlinkSync(path);}
  }
  localEndpoint(instance:Instance){
    const endpoint=this.store.endpoints().find(e=>e.id===(instance.endpointId??'local'));
    if(endpoint&&endpoint.kind!=='local')throw new Error('Remote execution uses explicit reverse protocol; local adapter cannot execute this endpoint');
  }
  async chat(message:string,live?:{approved:boolean,budgetUsd:number},planning=false,managerInstanceId?:string){
    if(this.store.getMeta('resourcePoolPath')&&!this.beforeExecution)throw new Error('Pooled project execution requires fleet resource arbitration');
    this.mutate();const instance=managerInstanceId?this.instance(managerInstanceId,'manager'):this.store.getConfig()!.instances.find(i=>i.role==='manager')!;
    this.localEndpoint(instance);
    if(instance.provider!=='fake'&&(!live?.approved||!Number.isFinite(live.budgetUsd)||live.budgetUsd<=0))throw new Error('Manager communication requires explicit live authorization');
    const id=randomUUID(),baseSha=this.store.getMeta('baseSha') as string,worktree=join(this.dir,'worktrees',id);
    mkdirSync(join(this.dir,'worktrees'),{recursive:true});
    git(this.root,'worktree','add','--detach',worktree,baseSha);
    const run:Run={id,taskId:'conversation',instanceId:instance.id,provider:instance.provider,sessionId:randomUUID(),baseSha,worktree,status:'prepared',pid:null,processStart:null,exitCode:null,commitSha:null,reviewBy:null,createdAt:new Date().toISOString()};
    this.store.createRun(run);this.store.event(id,'chat.user',{text:message});
    // Durable context is selected from the orchestration log, never --last.
    const history=this.chatHistory();
    await this.execute(run,instance,JSON.stringify({request:message,instruction:(planning?'Return exactly one JSON Plan object and no prose. ':'')+'You are the manager. Discuss and propose roadmap/task DAG JSON. Do not claim execution or acceptance. Task schema: version=1, goal, tasks[{id,title,prompt,dependencies,capabilities,acceptance:argv[][]}]. Plan proposals require explicit operator import.',history,message}),live);
    const finished=this.store.getRun(id)!;
    if(finished.status==='exited'){try{const raw=this.responseText(id),proposal=parsePlanProposal(raw);const hash=createHash('sha256').update(JSON.stringify(proposal)).digest('hex');this.store.setMeta('proposal:'+id,{managerRunId:id,hash,plan:proposal});this.store.event(id,'plan.proposed',{hash,tasks:proposal.tasks.map(t=>t.id)});}catch(error){if(planning){this.store.event(id,'plan.invalid',{reason:String(error)});throw error;}}}
    else if(planning)throw new Error('Manager execution did not produce a usable plan');
    return finished;
  }
  async propose(message:string,live?:{approved:boolean,budgetUsd:number},managerInstanceId?:string){return this.chat(message,live,true,managerInstanceId);}
  responseText(id:string):string {let cursor=0,text='';while(true){const events=this.store.events(cursor,1000);if(!events.length)break;for(const e of events.filter(e=>e.run_id===id)){const d=e.data as any;if(e.kind==='provider.message'){const value=typeof d?.text==='string'?d.text:Array.isArray(d?.blocks)?d.blocks.filter((b:any)=>b.type==='text').map((b:any)=>b.text).join('\n'):'';if(value)text=value;}else if(e.kind==='provider.provider_completed'&&typeof d?.result==='string'&&d.result)text=d.result;}cursor=events.at(-1)!.event_id;}if(Buffer.byteLength(text)>65536)throw new Error('Structured response exceeds 64KiB');return text;}
  adoptProposal(id:string){this.mutate();const run=this.store.getRun(id),p=this.store.getMeta('proposal:'+id) as any;if(!run||run.status!=='exited'||this.store.instances().find(i=>i.id===run.instanceId)?.role!=='manager'||p?.managerRunId!==id||createHash('sha256').update(JSON.stringify(p.plan)).digest('hex')!==p.hash)throw new Error('Valid manager proposal required');const plan=this.plan(p.plan);this.store.event(id,'plan.adopted',{hash:p.hash,executionStarted:false});return plan;}
  acceptReview(workerId:string,reviewRunId:string){this.mutate();const worker=this.store.getRun(workerId),review=this.store.getRun(reviewRunId);if(!worker?.commitSha||!review||!clean(worker.worktree)||head(worker.worktree)!==worker.commitSha||!clean(review.worktree)||head(review.worktree)!==worker.commitSha)throw new Error('Candidate/reviewer tree changed');return this.store.acceptReviewedRun(workerId,reviewRunId);}
  chatHistory(){let cursor=0;let history:unknown[]=[];while(true){const events=this.store.events(cursor,1000);if(!events.length)break;history=history.concat(events.filter(e=>e.kind==='chat.user'||e.kind==='provider.message')).slice(-20);cursor=events.at(-1)!.event_id;}return history;}
  async run(taskId:string,instanceId?:string,live?:{approved:boolean,budgetUsd:number},retryOf?:string):Promise<Run> {
    if(this.store.getMeta('resourcePoolPath')&&!this.beforeExecution)throw new Error('Pooled project execution requires fleet resource arbitration');
    this.mutate();if(this.store.getMeta('paused'))throw new Error('Scheduling paused');
    if(this.remoteStatus().some(r=>r.taskId===taskId))throw new Error('Task has remote attempt evidence; do not replay locally');
    const task=this.task(taskId), instance=instanceId?this.instance(instanceId,'worker'):this.pick(task);
    if(!task.capabilities.every(c=>instance.capabilities.includes(c)))throw new Error('Worker lacks required capability');
    if(instance.provider!=='fake'&&(!live?.approved||!Number.isFinite(live.budgetUsd)||live.budgetUsd<=0))throw new Error('Live provider requires explicit approval and positive budget; Codex monetary ceiling cannot be enforced');
    this.localEndpoint(instance);
    const previous=this.store.listRuns().filter(r=>r.taskId===taskId&&this.store.instances().find(i=>i.id===r.instanceId)?.role==='worker');
    if(previous.length&&!retryOf)throw new Error('Existing attempt: use retry explicitly');
    if(retryOf&&previous.at(-1)?.id!==retryOf)throw new Error('Retry must reference the latest attempt; never replay around unknown/newer attempts');
    if(previous.some(r=>['prepared','verifying','running','unknown','accepted','exited','verified'].includes(r.status)))throw new Error('Task has active, unknown, accepted or awaiting-verification work; investigate instead of replay');
    if(retryOf){const old=this.store.getRun(retryOf);if(!old||old.taskId!==taskId||!['failed','stopped','rejected'].includes(old.status))throw new Error('Retry allowed only for terminal failed/stopped/rejected attempts; unknown requires investigation');}
    const dependencies=task.dependencies.map(id=>{
      const r=this.store.listRuns().find(r=>r.taskId===id&&r.status==='accepted');
      if(!r?.commitSha)throw new Error(`Dependency ${id} lacks accepted commit`);return r;
    });
    const id=randomUUID(), worktree=join(this.dir,'worktrees',id),baseSha=this.store.getMeta('baseSha') as string;
    const run:Run={id,taskId,instanceId:instance.id,provider:instance.provider,sessionId:randomUUID(),baseSha,worktree,status:'prepared',pid:null,processStart:null,exitCode:null,commitSha:null,reviewBy:null,createdAt:new Date().toISOString()};
    this.store.createRun(run);
    try {
      mkdirSync(join(this.dir,'worktrees'),{recursive:true}); git(this.root,'worktree','add','--detach',worktree,baseSha);
      // Merge only accepted snapshots, never unverified working trees.
      for(const d of dependencies)git(worktree,'-c','user.name=AgentFleet','-c','user.email=agent-fleet@localhost','-c','core.hooksPath=/dev/null','-c','commit.gpgsign=false','merge','--no-edit',d.commitSha!);
      this.store.event(id,'dependencies.bound',{dependencies:dependencies.map(d=>({taskId:d.taskId,runId:d.id,commitSha:d.commitSha}))});
      await this.execute(run,instance,task.prompt,live);
    }catch(error){if(this.store.getRun(id)?.status==='prepared')this.store.updateRun(id,{status:'failed'},'setup.failed',{message:String(error)});throw error;}
    return this.store.getRun(id)!;
  }
  async execute(run:Run,instance:Instance,prompt:string,live?:{approved:boolean,budgetUsd:number}) {
    if(this.store.getMeta('resourcePoolPath')&&!this.beforeExecution)throw new Error('Pooled project execution requires fleet resource arbitration');
    this.beforeExecution?.(run,instance);
    const cfg=this.store.getConfig()!,parser=createParser(instance.provider);
    let protocolError=false;
    const invocation=buildInvocation(instance,{sessionId:run.sessionId,prompt,cwd:run.worktree,budgetUsd:live?.budgetUsd});
    const stopPoll=setInterval(()=>{if(this.store.getMeta('stop:'+run.id)===true)this.controls.get(run.id)?.stop();},150);
    const result=await supervise({...invocation,cwd:run.worktree,timeoutMs:cfg.timeoutMs,maxOutputBytes:cfg.maxOutputBytes,
      onStart:(pid,start)=>this.store.updateRun(run.id,{status:'running',pid,processStart:start},'process.started',{pid,start}),
      onControl:c=>this.controls.set(run.id,c),
      onEvent:(kind,data)=>{this.store.event(run.id,kind,data); if(kind==='stdout'&&typeof data==='object'&&data&&'line' in data){const parsed=parser.parse(String(data.line));if(parsed){if(['protocol_error','provider_error'].includes(parsed.kind))protocolError=true;this.store.event(run.id,`provider.${parsed.kind}`,parsed.data);if(parsed.kind==='session')this.store.setMeta('providerSession:'+run.id,parsed.data);}}}
    });
    clearInterval(stopPoll);this.controls.delete(run.id);
    const stopped=this.store.getRun(run.id)?.status==='stopped'||this.store.getMeta('stop:'+run.id)===true;
    this.store.updateRun(run.id,{status:this.store.getRun(run.id)?.status==='unknown'||result.error||result.timedOut||result.overflow?'unknown':stopped?'stopped':result.exitCode===0&&!protocolError?'exited':'failed',exitCode:result.exitCode},'process.completed',{...result,meaning:'Execution ended, not accepted'});
  }
  stop(id:string){this.mutate();const c=this.controls.get(id);if(!c){const r=this.store.getRun(id);if(!r||!['running','verifying'].includes(r.status))throw new Error('No running attempt to stop');this.store.setMeta('stop:'+id,true);this.store.event(id,'stop.requested',{delivery:'coordinator-owned handle only'});return;}if(!c.stop())throw new Error('Stop not confirmed');this.store.setMeta('stop:'+id,true);this.store.event(id,'stop.requested',{});}
  async verify(id:string){
    if(this.store.getMeta('resourcePoolPath')&&!this.beforeExecution)throw new Error('Pooled verification requires fleet resource arbitration');
    this.mutate();const run=this.store.getRun(id);if(!run||run.status!=='exited'||this.store.instances().find(i=>i.id===run.instanceId)?.role!=='worker')throw new Error('Verify requires exited worker');
    if(this.store.getMeta('verification:'+id))throw new Error('Existing verification phase requires inspection; do not replay acceptance commands');
    const verificationId=randomUUID(),cfg=this.store.getConfig()!;let candidate:string|null=null;
    const phase:any={verificationId,workerRunId:id,status:'prepared',pid:null,processStart:null};
    this.store.setMeta('verification:'+id,phase);
    this.store.updateRun(id,{status:'verifying'},'verification.prepared',{verificationId});
    try{
      if(realpathSync(run.worktree)!==resolve(this.dir,'worktrees',id))throw new Error('Worktree identity mismatch');
      candidate=snapshot(run.worktree,id);
      git(run.worktree,'merge-base','--is-ancestor',run.baseSha,candidate);
      for(const dep of this.task(run.taskId).dependencies){const accepted=this.store.listRuns().find(r=>r.taskId===dep&&r.status==='accepted');if(!accepted?.commitSha)throw new Error('Accepted dependency disappeared');git(run.worktree,'merge-base','--is-ancestor',accepted.commitSha,candidate);}
      this.store.event(id,'candidate.snapshot',{commitSha:candidate});
      const verification=join(this.dir,'verification',verificationId);mkdirSync(join(this.dir,'verification'),{recursive:true});
      git(this.root,'worktree','add','--detach',verification,candidate);
      Object.assign(phase,{status:'running',commitSha:candidate,worktree:verification});this.store.setMeta('verification:'+id,phase);
      for(const [commandIndex,argv] of this.task(run.taskId).acceptance.entries()){
        const poll=setInterval(()=>{if(this.store.getMeta('stop:'+id)===true)this.controls.get(id)?.stop();},150);
        let result;
        try{result=await supervise({command:argv[0],args:argv.slice(1),cwd:verification,timeoutMs:cfg.timeoutMs,maxOutputBytes:cfg.maxOutputBytes,
          onStart:(pid,start)=>{Object.assign(phase,{pid,processStart:start,commandIndex});this.store.setMeta('verification:'+id,phase);this.store.event(id,'verification.process_started',{verificationId,pid,start,commandIndex});},
          onControl:c=>this.controls.set(id,c),onEvent:(kind,data)=>{this.store.event(id,`verification.${kind}`,data);}});
        }finally{clearInterval(poll);this.controls.delete(id);}
        if(result.error||result.overflow||result.timedOut||this.store.getMeta('stop:'+id)===true){this.store.updateRun(id,{status:'unknown'},'verification.unknown',{verificationId,reason:result.error??'Resource bound or stop; side effects unknown'});throw new Error('Acceptance outcome unknown');}
        if(result.exitCode!==0)throw new Error('Acceptance command failed');
      }
      if(!clean(verification)||head(verification)!==candidate)throw new Error('Acceptance modified candidate');
      if(!clean(run.worktree)||head(run.worktree)!==candidate)throw new Error('Worker candidate changed during acceptance');
      if(this.store.getRun(id)?.status!=='verifying')throw new Error('Verification state changed while running');
      if(!this.store.finishVerification(id,verificationId,candidate,verification))throw new Error('Verification uncertainty preserved at completion');
    }catch(error){
      const status=this.store.getRun(id)?.status==='unknown'?'unknown':'rejected';phase.status=status;this.store.setMeta('verification:'+id,phase);
      this.store.updateRun(id,{status,commitSha:candidate},'verification.failed',{verificationId,message:String(error)});throw error;
    }
    return this.store.getRun(id);
  }
  review(id:string,reviewerId:string,evidence:unknown){
    this.mutate();const run=this.store.getRun(id),reviewer=this.instance(reviewerId,'reviewer');
    if(!run||run.status!=='verified'||run.instanceId===reviewer.id)throw new Error('Independent review requires verified candidate and distinct reviewer');
    const e=evidence as Record<string,unknown>;
    if(!e||e.runId!==id||e.commitSha!==run.commitSha||e.reviewerId!==reviewerId||!['approve','reject'].includes(String(e.verdict))||typeof e.notes!=='string'||!e.notes.trim())throw new Error('Review evidence must bind exact run, commit, reviewer, verdict and notes');
    if(!clean(run.worktree)||head(run.worktree)!==run.commitSha)throw new Error('Candidate changed since verification');
    this.store.updateRun(id,{status:e.verdict==='approve'?'accepted':'rejected',reviewBy:reviewerId},'review.recorded',{...e,authority:'Explicit operator-submitted independent review evidence'});
    return this.store.getRun(id);
  }
}
