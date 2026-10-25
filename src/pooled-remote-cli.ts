import {randomUUID} from 'node:crypto';
import {ResourcePool} from './resource-pool.ts';
import type {PoolRequest} from './resource-pool.ts';
import {PooledRemoteQueue,pooledProjectKey} from './pooled-remote.ts';
import type {RemoteScope} from './pooled-remote.ts';
import {openFleet} from './fleet-cli.ts';
import {Engine} from './engine.ts';
import type {Dispatch} from './remote.ts';

function boundedRequest(value:any){if(!value||typeof value!=='object'||Array.isArray(value)||value.version!==1||typeof value.op!=='string')throw new Error('Pooled protocol version/op required');return value;}
async function input(){const chunks:Buffer[]=[];let bytes=0;for await(const chunk of process.stdin){bytes+=chunk.length;if(bytes>65536)throw new Error('Pooled protocol input exceeds 64KiB');chunks.push(Buffer.from(chunk));}return boundedRequest(JSON.parse(Buffer.concat(chunks).toString('utf8')));}
/** Trusted command pins manifest, endpoint and resource. JSON stdin never selects paths or project scope. */
export async function pooledRemoteCommand(words:string[],options:{poolPath:string;observer:boolean},providedInput?:unknown){
  const [op,file,...rest]=words;
  if(!['offer','list','events','request'].includes(op))throw new Error('Use fleet-remote offer FLEET SESSION TASK ENDPOINT REPOSITORY | list FLEET ENDPOINT | events FLEET ENDPOINT [CURSOR] | request FLEET ENDPOINT RESOURCE (stdin JSON)');
  if(options.observer&&op==='offer')throw new Error('Observer cannot offer pooled work');
  const r=op==='request'?(providedInput===undefined?await input():boundedRequest(providedInput)):null;
  const readonly=op==='list'||op==='events'||(op==='request'&&['hello','pull','list','events','reconcile','reconcile-claim'].includes(r.op));
  if(options.observer&&!readonly)throw new Error('Observer cannot claim, ACK or report pooled work');
  // Read project configuration without touching index/current pointers, even for runner operations.
  const pool=new ResourcePool(options.poolPath,{readOnly:readonly,mustExist:true});let projects:ReturnType<typeof openFleet>=[];
  try{
    projects=openFleet(file,true);
    if(projects.some(p=>p.engine.formatVersion()!==2))throw new Error('Pooled remote protocol requires phase2 sessions');
    const registry=pool.status().resources;
    const endpointId=op==='offer'?rest[2]:rest[0];
    const resources=registry.filter(resource=>resource.endpointId===endpointId&&resource.endpointKind!=='local'&&(op!=='request'||resource.id===rest[1]));
    if(!endpointId||!resources.length)throw new Error('Command must pin a registered remote endpoint and resource');
    const scope:RemoteScope={endpointId,resourceIds:resources.map(resource=>resource.id),projects:projects.map(p=>({projectKey:pooledProjectKey(p.projectId),sessionId:p.engine.sessionId}))};
    // All remote mutating calls must use the same shared pool recorded by the project.
    for(const p of projects){const bound=p.engine.store.getMeta('resourcePoolPath');if(bound&&bound!==pool.path)throw new Error('Project belongs to a different resource pool');}
    const queue=new PooledRemoteQueue(pool,{readOnly:readonly});
    if(op==='list')return queue.list(scope);
    if(op==='events')return queue.events(scope,Number(rest[1]??0),1000);
    if(op==='offer'){
      const [sessionId,taskId,,repositoryId]=rest,project=projects.find(p=>p.engine.sessionId===sessionId);if(!project)throw new Error('Session not in fixed fleet scope');
      const e=new Engine(project.projectId,false,sessionId);
      try{return await e.exclusive(async()=>{
        e.mutate();if(e.store.getMeta('paused'))throw new Error('Project is paused');
        if(!e.store.getMeta('resourcePoolPath')&&e.remoteStatus().length)throw new Error('Legacy remote evidence cannot be enrolled; use a new isolated session or explicit migration');
        const task=e.task(taskId),rules=project.roles?.worker??{},required=[...new Set([...task.capabilities,...(rules.capabilities??[])])];
        const eligible=resources.filter(resource=>resource.roles.includes('worker')&&required.every(c=>resource.capabilities.includes(c))&&(!rules.requiredProvider||resource.provider===rules.requiredProvider));
        if(!eligible.length)throw new Error('No remote resource satisfies project worker requirements');
        const providers=[...new Set(eligible.map(resource=>resource.provider))];if(providers.length!==1)throw new Error('Remote offer requires a single explicit provider constraint');
        if(e.store.listRuns().some(run=>run.taskId===taskId))throw new Error('Existing local attempt cannot be replayed remotely');
        const old=(e.store.getMeta('pooledRemoteOffers') as any[]|null)??[];
        const saved=old.find(item=>item.dispatch.taskId===taskId);
        if(saved){if(saved.dispatch.endpointId!==endpointId||saved.dispatch.repositoryId!==repositoryId||saved.dispatch.baseSha!==e.store.getMeta('baseSha')||saved.request.requiredProvider!==providers[0]||JSON.stringify([...saved.request.capabilities].sort())!==JSON.stringify([...required].sort()))throw new Error('Existing pooled offer is immutable; reconcile original attempt');return queue.offer(saved);}
        if(e.remoteStatus().some(run=>run.taskId===taskId))throw new Error('Existing remote attempt cannot be replaced by a pooled offer');
        const dependencies=task.dependencies.map(id=>{const accepted=e.store.listRuns().find(run=>run.taskId===id&&run.status==='accepted');if(!accepted?.commitSha||!accepted.reviewBy)throw new Error('Remote dependencies require project-local accepted commits and review evidence');return {taskId:id,commitSha:accepted.commitSha,acceptanceId:accepted.id,reviewId:accepted.reviewBy};});
        const messageId=randomUUID(),runId=randomUUID(),attemptId=randomUUID(),projectKey=pooledProjectKey(e.root);
        const request:PoolRequest={id:messageId,projectId:e.root,sessionId:e.sessionId,taskId,attemptId,role:'worker',capabilities:required,requiredProvider:providers[0],eligibleResourceIds:eligible.map(resource=>resource.id),...(rules.preferredProvider?{preferredProvider:rules.preferredProvider}:{})};
        const dispatch:Dispatch={version:1,messageId,runId,attemptId,taskId,endpointId,fence:1,repositoryId,baseSha:e.store.getMeta('baseSha') as string,provider:providers[0],requiredCapabilities:required,instructions:task.prompt,dependencies};
        const offer={request,dispatch,projectKey};
        // First bind the project and durable offer intent. A crash before the pool transaction
        // leaves an observable pending intent that blocks local replay; explicit re-offer reuses IDs.
        if(!e.store.getMeta('resourcePoolPath'))e.store.setMeta('resourcePoolPath',pool.path);
        e.store.setMeta('pooledRemoteOffers',[...old,offer]);
        return queue.offer(offer);
      });}finally{e.close();}
    }
    if(r.op==='hello')return {product:'AgentFleet',protocolVersion:1,transport:'ssh-stdio-jsonl',primaryConnection:true,watchdogOnlyFallback:true,endpointId,modelInvoked:false,museWakeInterface:'unverified',capabilitiesAreSelfReported:true};
    if(r.op==='pull')return queue.pull(scope,r.capabilities,r.limit??10);
    if(r.op==='list')return queue.list(scope);
    if(r.op==='events'){
      const waitMs=r.waitMs??0;if(!Number.isSafeInteger(waitMs)||waitMs<0||waitMs>20000)throw new Error('Event wait must be 0..20000 milliseconds');
      const deadline=Date.now()+waitMs;let events=queue.events(scope,r.after??0,r.limit??100);
      while(!events.length&&Date.now()<deadline){await new Promise(resolve=>setTimeout(resolve,Math.min(250,deadline-Date.now())));events=queue.events(scope,r.after??0,r.limit??100);}
      return events;
    }
    if(r.op==='reconcile')return queue.reconcile(scope,r.binding);
    if(r.op==='reconcile-claim')return queue.reconcileClaim(scope,r.claimId);
    if(r.op==='claim'){
      if(!projects.some(p=>p.remoteClaimsAuthorized))throw new Error('Remote task claims are disabled in the fixed manifest; capability self-report is not execution authorization');
      const activeScope={...scope,projects:projects.filter(p=>p.remoteClaimsAuthorized&&!p.engine.store.getMeta('paused')).map(p=>({projectKey:pooledProjectKey(p.projectId),sessionId:p.engine.sessionId}))};
      if(!activeScope.projects.length)return null;
      return queue.claim(activeScope,{claimId:r.claimId,capabilities:r.capabilities});
    }
    if(r.op==='ack')return queue.ack(scope,r.binding);
    if(r.op==='report')return queue.report(scope,r.binding,{sourceEventId:r.sourceEventId,kind:r.kind,data:r.data});
    if(r.op==='disconnected')return queue.disconnected(scope,r.binding);
    throw new Error('Unsupported pooled operation; JSON cannot execute commands or change connection scope');
  }finally{pool.close();for(const p of projects)p.engine.close();}
}
