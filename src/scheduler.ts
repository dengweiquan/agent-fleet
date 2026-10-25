import type {Engine} from './engine.ts';
import type {Instance} from './types.ts';
import {runIndependentReview} from './reviewer.ts';

export function schedulePreview(engine:Engine,reviewerId?:string,budgetPerRunUsd=0){
  const config=engine.config()!,plan=engine.store.getPlan();if(!plan)throw new Error('Adopt a validated plan first');
  const workers=config.instances.filter(i=>i.role==='worker'),reviewers=reviewerId?[engine.instance(reviewerId,'reviewer')]:config.instances.filter(i=>i.role==='reviewer');
  const runs=engine.store.listRuns(),remote=engine.remoteStatus();
  const tasks=plan.tasks.filter(t=>!runs.some(r=>r.taskId===t.id&&workers.some(i=>i.id===r.instanceId))&&!remote.some(r=>r.taskId===t.id));
  const workerCalls=tasks.length,reviewerCalls=tasks.length;
  const claudeCalls=(workers.some(i=>i.provider==='claude')?workerCalls:0)+(reviewers.some(i=>i.provider==='claude')?reviewerCalls:0);
  return {taskIds:tasks.map(t=>t.id),maxWorkers:config.maxWorkers??2,maxReviews:config.maxReviews??1,instances:[...workers,...reviewers].map(i=>({id:i.id,provider:i.provider,model:i.model,role:i.role,endpointId:i.endpointId})),maximumNewWorkerCalls:workerCalls,maximumNewReviewerCalls:reviewerCalls,budgetScope:'per-provider-run',budgetPerRunUsd,claudeMaximumConfiguredApiSpendUsd:claudeCalls*budgetPerRunUsd,codexSpendCap:'not-enforceable',totalScheduleMoneyCapEnforced:false,liveProviderCallsPossible:[...workers,...reviewers].some(i=>i.provider!=='fake'),executionStarted:false};
}

/** Called inside one owned coordinator lock. Slots cover the complete pipeline. */
export async function schedule(engine:Engine,reviewerId?:string,live?:{approved:boolean,budgetUsd:number}){
  engine.mutate();const config=engine.config()!,plan=engine.store.getPlan();if(!plan)throw new Error('Adopt a validated plan first');
  const workers=config.instances.filter(i=>i.role==='worker');
  const reviewers=reviewerId?[engine.instance(reviewerId,'reviewer')]:config.instances.filter(i=>i.role==='reviewer');
  const maxWorkers=config.maxWorkers??2,maxReviews=config.maxReviews??1;
  const busy=new Set<string>(),active=new Map<string,Promise<void>>(),claimed=new Set<string>();
  let halted=engine.store.listRuns().some(r=>['prepared','running','verifying','unknown'].includes(r.status));
  const reviewBusy=new Set<string>(),reviewWaiters:Array<()=>void>=[];const errors:Array<{taskId:string,error:string}>=[];
  // Fail before dispatching any task if the configured pool lacks explicit authorization.
  for(const instance of [...workers,...reviewers]){
    engine.localEndpoint(instance);
    if(instance.provider!=='fake'&&(!live?.approved||!Number.isFinite(live.budgetUsd)||live.budgetUsd<=0))throw new Error('Live schedule requires explicit per-run model/budget authorization for every worker and reviewer');
    if(['configured-default','user-selection-required'].includes(instance.model))throw new Error('Every scheduled instance needs an explicit model');
  }
  async function acquireReviewer():Promise<Instance>{
    while(true){if(engine.store.listRuns().some(r=>r.status==='unknown'))halted=true;if(halted)throw new Error('Uncertain execution quarantines scheduling; no new reviewer dispatched');const available=reviewers.find(i=>!reviewBusy.has(i.id));if(available&&reviewBusy.size<maxReviews){reviewBusy.add(available.id);return available;}
      await new Promise<void>(resolve=>reviewWaiters.push(resolve));
    }
  }
  function releaseReviewer(id:string){reviewBusy.delete(id);for(const resume of reviewWaiters.splice(0))resume();}
  engine.store.event(null,'schedule.started',{maxWorkers,maxReviews,budgetScope:'per-provider-run',unknownReplay:false});
  async function pipeline(taskId:string,instance:Instance){
    try{
      const run=await engine.run(taskId,instance.id,live);
      if(run.status==='unknown')halted=true;
      if(run.status!=='exited')return;
      const verified=await engine.verify(run.id);if(verified?.status!=='verified')return;
      const reviewer=await acquireReviewer();
      try{const reviewed=await runIndependentReview(engine,run.id,reviewer.id,live);if(reviewed.status==='unknown')halted=true;if(reviewed.status==='exited')engine.acceptReview(run.id,reviewed.id);}
      finally{releaseReviewer(reviewer.id);}
    }catch(error){if(engine.store.listRuns().some(r=>r.status==='unknown'))halted=true;errors.push({taskId,error:String(error)});engine.store.event(null,'schedule.task_error',{taskId,error:String(error)});}
    finally{if(engine.store.listRuns().some(r=>r.status==='unknown'))halted=true;for(const resume of reviewWaiters.splice(0))resume();busy.delete(instance.id);active.delete(taskId);}
  }
  try{
    while(true){
      if(engine.store.listRuns().some(r=>r.status==='unknown'))halted=true;
      if(!halted&&!engine.store.getMeta('paused')){
        for(const task of plan.tasks){
          if(active.size>=maxWorkers)break;
          const attempts=engine.store.listRuns();
          if(claimed.has(task.id)||attempts.some(r=>r.taskId===task.id&&workers.some(w=>w.id===r.instanceId))||engine.remoteStatus().some(r=>r.taskId===task.id))continue;
          if(!task.dependencies.every(id=>attempts.some(r=>r.taskId===id&&r.status==='accepted')))continue;
          const worker=workers.find(i=>!busy.has(i.id)&&task.capabilities.every(c=>i.capabilities.includes(c)));if(!worker)continue;
          // Reserve synchronously before the first await; no parallel branch can reuse it.
          claimed.add(task.id);busy.add(worker.id);const promise=Promise.resolve().then(()=>pipeline(task.id,worker));active.set(task.id,promise);
        }
      }
      if(!active.size)break;
      await Promise.race([...active.values()]);
    }
  }finally{await Promise.allSettled([...active.values()]);}
  const attempts=engine.store.listRuns();
  const pending=plan.tasks.filter(t=>!attempts.some(r=>r.taskId===t.id&&r.status==='accepted')).map(t=>t.id);
  const result={quarantined:halted,accepted:plan.tasks.length-pending.length,pending,errors,paused:engine.store.getMeta('paused')};
  engine.store.event(null,'schedule.finished',result);return result;
}
