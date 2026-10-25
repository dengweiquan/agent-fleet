import {randomUUID,createHash} from 'node:crypto';
import {resolve} from 'node:path';
import {Engine} from './engine.ts';
import {ResourcePool} from './resource-pool.ts';
import type {Resource,PoolRequest,Lease} from './resource-pool.ts';
import type {Instance,Role,Run} from './types.ts';
import {runIndependentReview} from './reviewer.ts';

export interface RolePolicy {capabilities?:string[];preferredProvider?:Resource['provider'];requiredProvider?:Resource['provider']}
export interface FleetProject {projectId:string;engine:Engine;roles?:Partial<Record<Role,RolePolicy>>;remoteClaimsAuthorized?:boolean}
type Live={approved:boolean;budgetUsd:number};
const uncertain=new Set(['prepared','running','verifying','unknown']);
const key=(role:Role,taskId:string)=>`fleet:${role}:${createHash('sha256').update(taskId).digest('hex')}`;
const resourceKey=(lease:Lease)=>`pool-${lease.id}`;
function workers(e:Engine){const roles=new Map(e.store.instances().map(i=>[i.id,i.role]));return e.store.listRuns().filter(r=>roles.get(r.instanceId)==='worker');}
function checkProjects(projects:FleetProject[]){
  if(!projects.length||projects.length>32)throw new Error('Fleet requires 1..32 explicit project sessions');
  const sessions=new Set<string>(),roots=new Set<string>();
  for(const p of projects){if(p.projectId!==p.engine.root)throw new Error('projectId must be the canonical Git root, never a session-specific capacity alias');if(sessions.has(p.engine.sessionId)||roots.has(p.projectId))throw new Error('One session per project per fleet dispatch');sessions.add(p.engine.sessionId);roots.add(p.projectId);}
}
function policy(p:FleetProject,role:Role):RolePolicy{
  const requested=p.roles?.[role]??{};
  // The manager explicitly chosen in project configuration is a hard provider constraint.
  if(role==='manager')return {...requested,requiredProvider:requested.requiredProvider??p.engine.config()!.instances.find(i=>i.role==='manager')!.provider};
  return requested;
}
function candidates(pool:ResourcePool,p:FleetProject,role:Role,capabilities:string[],excluded:string[]=[]){
  const rules=policy(p,role),required=[...new Set([...capabilities,...(rules.capabilities??[])])];
  return pool.status().resources.filter(r=>r.roles.includes(role)&&r.endpointKind==='local'&&r.provider!=='external-agent'&&required.every(c=>r.capabilities.includes(c))&&(!rules.requiredProvider||rules.requiredProvider===r.provider)&&!excluded.includes(r.id));
}
function authorize(resources:Resource[],live?:Live){
  for(const r of resources){if(['configured-default','user-selection-required'].includes(r.model))throw new Error(`Explicit model required for resource ${r.id}`);if(r.provider!=='fake'&&(!live?.approved||!Number.isFinite(live.budgetUsd)||live.budgetUsd<=0))throw new Error('Fleet has live eligible resources; explicit per-run model/budget authorization required before enqueue');}
}
function bindInstance(e:Engine,lease:Lease):Instance{
  const r=lease.resource;if(r.endpointKind!=='local'||r.provider==='external-agent')throw new Error('Remote resources require the reverse runner bridge; no local launch');
  // Local alias does not claim the resource's endpoint is the current host remotely.
  const endpointId=`pool-${createHash('sha256').update(r.endpointId).digest('hex').slice(0,24)}`;
  const instance:Instance={id:resourceKey(lease),provider:r.provider,role:lease.request.role,model:r.model,capabilities:r.capabilities,endpointId};
  e.store.bindInstance(instance,{id:endpointId,kind:'local'});return instance;
}
function request(pool:ResourcePool,p:FleetProject,role:Role,taskId:string,capabilities:string[],excluded:string[]=[]):PoolRequest|null{
  const e=p.engine,metaKey=key(role,taskId),old=e.store.getMeta(metaKey) as PoolRequest|null;
  if(old){const rules=policy(p,role),required=[...new Set([...capabilities,...(rules.capabilities??[])])].sort();if(old.projectId!==p.projectId||old.sessionId!==e.sessionId||old.role!==role||old.taskId!==taskId||JSON.stringify([...old.capabilities].sort())!==JSON.stringify(required)||old.requiredProvider!==rules.requiredProvider||old.preferredProvider!==rules.preferredProvider)throw new Error('Queued attempt policy is immutable; reconcile it before changing role constraints');pool.enqueue(old);return old;}
  const resources=candidates(pool,p,role,capabilities,excluded);if(!resources.length)return null;
  const rules=policy(p,role),req:PoolRequest={id:randomUUID(),projectId:p.projectId,sessionId:e.sessionId,taskId,attemptId:randomUUID(),role,capabilities:[...new Set([...capabilities,...(rules.capabilities??[])])],eligibleResourceIds:resources.map(r=>r.id),...(rules.requiredProvider?{requiredProvider:rules.requiredProvider}:{}),...(rules.preferredProvider?{preferredProvider:rules.preferredProvider}:{}),...(excluded.length?{excludedResourceIds:excluded}:{})};
  // Session identity first. A crash before enqueue can safely re-enqueue this same request, never a new attempt.
  e.store.setMeta(metaKey,req);pool.enqueue(req);return req;
}
function retainPool(e:Engine,poolPath:string){const previous=e.store.getMeta('resourcePoolPath');if(previous&&previous!==poolPath)throw new Error('Session is bound to another resource pool; reconcile before changing it');if(!previous){if(e.remoteStatus().length)throw new Error('Legacy remote evidence cannot be enrolled into shared capacity; use a new session or explicit migration');e.store.setMeta('resourcePoolPath',poolPath);}}
function installHook(e:Engine,pool:ResourcePool,leases:Map<string,Lease>){
  if(e.beforeExecution)throw new Error('Execution hook already owned');
  e.beforeExecution=(run,instance)=>{const lease=leases.get(instance.id);if(!lease)throw new Error('No owned resource lease for provider invocation');pool.bindRun(lease.id,lease.fence,run.id);e.store.setMeta('resourceLease:'+run.id,{leaseId:lease.id,fence:lease.fence,request:lease.request,resource:lease.resource,runId:run.id,baseSha:run.baseSha,worktree:run.worktree,providerSessionId:run.sessionId});e.store.event(run.id,'resource.run_bound',{leaseId:lease.id,fence:lease.fence,projectId:lease.request.projectId,attemptId:lease.request.attemptId,resourceId:lease.resource.id,endpointId:lease.resource.endpointId,accountDomain:lease.resource.accountDomain,costKnown:lease.resource.costKnown??false});};
}
async function locked<T>(projects:FleetProject[],action:()=>Promise<T>):Promise<T>{
  const ordered=[...projects].sort((a,b)=>a.engine.dir.localeCompare(b.engine.dir));
  const next=(n:number):Promise<T>=>n===ordered.length?action():ordered[n].engine.exclusive(()=>next(n+1));return next(0);
}

export function fleetPreview(pool:ResourcePool,projects:FleetProject[],budgetPerRunUsd=0){
  checkProjects(projects);return {executionStarted:false,budgetScope:'per-provider-run',budgetPerRunUsd,totalMoneyCapEnforced:false,remoteExecutionImplemented:false,resources:pool.status().resources,limits:pool.status().limits,projects:projects.map(p=>({projectId:p.projectId,sessionId:p.engine.sessionId,roles:p.roles??{},managerRequiredProvider:policy(p,'manager').requiredProvider,baseSha:p.engine.store.getMeta('baseSha'),paused:p.engine.store.getMeta('paused'),tasks:p.engine.store.getPlan()?.tasks.map(t=>({taskId:t.id,workers:candidates(pool,p,'worker',t.capabilities).map(r=>r.id),reviewers:candidates(pool,p,'reviewer',['review']).map(r=>r.id)}))??[]}))};
}

/** Finite shared coordinator: only explicit projects, no background discovery or automatic replay. */
export async function scheduleFleet(pool:ResourcePool,projects:FleetProject[],live?:Live){
  checkProjects(projects);
  // Capacity ownership is the shared database's identity, not per-project copies of its contents.
  const poolPath=resolve(pool.path);
  for(const p of projects){p.engine.mutate();const plan=p.engine.store.getPlan();if(!plan)throw new Error('Every fleet project needs an adopted plan');for(const task of plan.tasks)authorize(candidates(pool,p,'worker',task.capabilities),live);authorize(candidates(pool,p,'reviewer',['review']),live);}
  return locked(projects,async()=>{
    const bySession=new Map(projects.map(p=>[p.engine.sessionId,p])),active=new Map<string,Promise<void>>(),owned=new Map<string,Lease>(),errors:Array<{projectId:string,taskId:string,error:string}>=[];
    const initial=pool.status();const quarantined=new Set<string>();
    for(const p of projects){if(p.engine.beforeExecution)throw new Error('Execution hook already owned');const previous=p.engine.store.getMeta('resourcePoolPath');if(previous&&previous!==poolPath)throw new Error('Session is bound to another resource pool');}
    for(const p of projects)retainPool(p.engine,poolPath);
    for(const p of projects){if(p.engine.store.listRuns().some(r=>uncertain.has(r.status))||initial.leases.some(l=>l.request.sessionId===p.engine.sessionId&&['held','unknown'].includes(l.state)))quarantined.add(p.engine.sessionId);installHook(p.engine,pool,owned);}
    function isQuarantined(p:FleetProject){if(p.engine.store.listRuns().some(r=>r.status==='unknown'))quarantined.add(p.engine.sessionId);return quarantined.has(p.engine.sessionId);}
    function enqueueReady(){
      const ids:string[]=[];
      for(const p of projects){const e=p.engine;if(isQuarantined(p)||e.store.getMeta('paused'))continue;
        for(const task of e.store.getPlan()!.tasks){const attempts=workers(e).filter(r=>r.taskId===task.id),worker=attempts.at(-1);if(e.remoteStatus().some(r=>r.taskId===task.id))continue;
          if(!worker){if(!task.dependencies.every(id=>workers(e).some(r=>r.taskId===id&&r.status==='accepted')))continue;const req=request(pool,p,'worker',task.id,task.capabilities);if(req)ids.push(req.id);}
          else if(worker.status==='verified'&&!e.store.getMeta('reviewEvidence:'+worker.id)){
            const binding=e.store.getMeta('resourceLease:'+worker.id) as any;if(!binding)continue;
            if(e.store.listRuns().some(r=>(e.store.getMeta('reviewBinding:'+r.id) as any)?.workerRunId===worker.id))continue;
            const req=request(pool,p,'reviewer',task.id,['review'],[binding.resource.id]);if(req)ids.push(req.id);
          }
        }
      }
      return ids.filter(id=>{const entry=pool.status().requests.find(r=>r.request.id===id);if(!entry)return false;const p=bySession.get(entry.request.sessionId)!;const running=[...owned.values()].filter(l=>l.request.sessionId===p.engine.sessionId&&l.request.role===entry.request.role).length;return running<(entry.request.role==='worker'?(p.engine.config()!.maxWorkers??2):(p.engine.config()!.maxReviews??1));});
    }
    async function execute(lease:Lease){
      const p=bySession.get(lease.request.sessionId)!,e=p.engine;let run:Run|undefined;
      try{
        const instance=bindInstance(e,lease);owned.set(instance.id,lease);
        if(lease.request.role==='worker'){run=await e.run(lease.request.taskId,instance.id,live);if(run.status==='exited')run=await e.verify(run.id)??undefined;}
        else {const worker=workers(e).find(r=>r.taskId===lease.request.taskId&&r.status==='verified');if(!worker)throw new Error('Verified candidate disappeared');run=await runIndependentReview(e,worker.id,instance.id,live);if(run.status==='exited')e.acceptReview(worker.id,run.id);}
      }catch(error){errors.push({projectId:p.projectId,taskId:lease.request.taskId,error:String(error)});e.store.event(null,'fleet.task_error',{leaseId:lease.id,error:String(error)});}
      finally{
        const currentLease=pool.status().leases.find(l=>l.id===lease.id)!;const current=currentLease.runId?e.store.getRun(currentLease.runId):run;
        const outcome=!current||uncertain.has(current.status)?'unknown':['failed','rejected','stopped'].includes(current.status)?'failed':'completed';
        pool.release(lease.id,lease.fence,outcome);if(outcome==='unknown')quarantined.add(e.sessionId);
        owned.delete(resourceKey(lease));active.delete(lease.id);
      }
    }
    try{
      while(true){
        let ids=enqueueReady(),lease:Lease|null;
        while((lease=pool.claimNext(ids))){const reserved=lease;owned.set(resourceKey(reserved),reserved);const promise=Promise.resolve().then(()=>execute(reserved));active.set(reserved.id,promise);ids=enqueueReady();}
        if(!active.size)break;await Promise.race(active.values());
      }
    }finally{await Promise.allSettled([...active.values()]);for(const p of projects)p.engine.beforeExecution=undefined;}
    return {projects:projects.map(p=>{const accepted=workers(p.engine).filter(r=>r.status==='accepted');return {projectId:p.projectId,sessionId:p.engine.sessionId,accepted:accepted.length,pending:p.engine.store.getPlan()!.tasks.filter(t=>!accepted.some(r=>r.taskId===t.id)).map(t=>t.id),quarantined:isQuarantined(p),paused:p.engine.store.getMeta('paused')};}),leases:pool.status().leases,errors,executionScope:'local-adapters-only',remoteQueueUnchanged:true};
  });
}

/** One explicitly requested manager turn, leased from the same account/endpoint capacity domains. */
export async function proposeFleet(pool:ResourcePool,project:FleetProject,message:string,live?:Live){
  checkProjects([project]);const e=project.engine;authorize(candidates(pool,project,'manager',['planning']),live);
  return e.exclusive(async()=>{
    retainPool(e,resolve(pool.path));if(e.store.getMeta('paused')||e.store.listRuns().some(r=>uncertain.has(r.status)))throw new Error('Paused/uncertain project cannot start a manager turn');
    let pending=e.store.getMeta('fleetManagerPending') as {turn:string,message:string}|null;
    if(pending&&pending.message!==message)throw new Error('A manager turn is queued; repeat its original message to resume that turn before submitting another');
    if(!pending){pending={turn:'conversation-'+randomUUID(),message};e.store.setMeta('fleetManagerPending',pending);}
    const req=request(pool,project,'manager',pending.turn,['planning']);if(!req)return {queued:false,blocked:'No manager matches the configured required provider/capabilities'};
    const previous=pool.status().leases.find(l=>l.request.id===req.id);
    if(previous){if(['held','unknown'].includes(previous.state))throw new Error('Manager lease requires reconciliation, never replay');e.store.setMeta('fleetManagerPending',null);return {queued:false,alreadyExecuted:true,requestId:req.id,run:previous.runId?e.store.getRun(previous.runId):null};}
    const lease=pool.claimNext([req.id]);if(!lease)return {queued:true,requestId:req.id,executionStarted:false};
    const owned=new Map<string,Lease>();installHook(e,pool,owned);
    try{const instance=bindInstance(e,lease);owned.set(instance.id,lease);const run=await e.propose(message,live,instance.id);return {queued:false,requestId:req.id,run,proposal:e.store.getMeta('proposal:'+run.id)};}
    finally{const current=pool.status().leases.find(l=>l.id===lease.id)!;const run=current.runId?e.store.getRun(current.runId):null;const outcome=!run||uncertain.has(run.status)?'unknown':run.status==='exited'?'completed':'failed';pool.release(lease.id,lease.fence,outcome);if(outcome!=='unknown')e.store.setMeta('fleetManagerPending',null);e.beforeExecution=undefined;}
  });
}
