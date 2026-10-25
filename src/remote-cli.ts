import {randomUUID} from 'node:crypto';
import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {ReverseCoordinator} from './remote.ts';
import type {Dispatch} from './remote.ts';
import type {Engine} from './engine.ts';

export async function remoteCommand(e:Engine,args:string[],observe:boolean):Promise<unknown>{
  const [op,...rest]=args,path=join(e.dir,'remote.sqlite');
  if(e.store.getMeta('resourcePoolPath')&&!['list','events','reconcile'].includes(op))throw new Error('Pooled sessions must use fleet-remote; legacy claims cannot bypass shared capacity');
  if(observe&&!['list','events','reconcile'].includes(op))throw new Error('Observer cannot offer, claim or report remote work');
  if(observe&&!existsSync(path))return [];
  const q=new ReverseCoordinator(path,{readOnly:observe});
  try{
    if(op==='list')return q.list(rest[0]);
    if(op==='events')return q.events(rest[0],Number(rest[1]??0),1000);
    if(op==='reconcile')return q.reconcile(rest[0],rest[1]);
    if(op==='offer')return await e.exclusive(async()=>{
      if(e.store.getMeta('resourcePoolPath'))throw new Error('Session enrolled in shared pool; legacy offer cannot bypass capacity');
      const [taskId,endpointId,repositoryId]=rest,task=e.task(taskId);
      if(e.store.listRuns().some(r=>r.taskId===taskId))throw new Error('Task already has local attempt evidence; do not replay remotely');
      const endpoint=e.store.getConfig()?.endpoints?.find(p=>p.id===endpointId);
      if(!endpoint||endpoint.kind!=='external-agent')throw new Error('Declare an external-agent endpoint before offering a Muse task');
      if(e.store.getMeta('paused'))throw new Error('Scheduling paused');
      const dependencies=task.dependencies.map(id=>{
        const accepted=e.store.listRuns().find(r=>r.taskId===id&&r.status==='accepted');
        if(!accepted?.commitSha||!accepted.reviewBy)throw new Error('Remote dependency lacks local accepted commit and review evidence');
        return {taskId:id,commitSha:accepted.commitSha,acceptanceId:accepted.id,reviewId:accepted.id};
      });
      const d:Dispatch={version:1,messageId:randomUUID(),taskId,runId:randomUUID(),attemptId:randomUUID(),endpointId,fence:1,repositoryId,baseSha:e.store.getMeta('baseSha') as string,provider:'external-agent',requiredCapabilities:task.capabilities,instructions:task.prompt,dependencies};
      const result=q.offer(d);e.store.event(null,'remote.offered',{messageId:d.messageId,runId:d.runId,taskId,endpointId,meaning:'Protocol offer only; no remote execution started'});return {dispatch:d,...result};
    });
    if(op==='request'){
      // Invocation is fixed by the SSH command; input can never select a path or run a shell.
      const endpointId=rest[0];if(!e.store.getConfig()?.endpoints?.some(p=>p.id===endpointId&&p.kind==='external-agent'))throw new Error('Unknown external endpoint');
      const chunks:Buffer[]=[];let size=0;for await(const chunk of process.stdin){size+=chunk.length;if(size>65536)throw new Error('Protocol input exceeds 64KiB');chunks.push(Buffer.from(chunk));}
      const r=JSON.parse(Buffer.concat(chunks).toString('utf8'));
      if(!r||r.version!==1||typeof r.op!=='string')throw new Error('Protocol version/op required');
      if(r.op==='pull')return q.pull(endpointId,r.capabilities,Math.min(r.limit??10,100));
      if(r.op==='claim')return q.claim(r.messageId,endpointId,r.claimId,r.capabilities);
      // Every mutation is scoped to the endpoint fixed in the command, not self-reported input.
      if(r.messageId)q.reconcile(r.messageId,endpointId);
      if(r.op==='ack')return q.ack(r.messageId,r.claimId);
      if(r.op==='report')return q.report(r.messageId,r.claimId,r.sourceEventId,r.kind,r.data);
      if(r.op==='reconcile')return q.reconcile(r.messageId,endpointId);
      if(r.op==='events')return q.events(endpointId,r.after??0,Math.min(r.limit??100,1000));
      throw new Error('Unknown protocol operation; no arbitrary execution');
    }
    throw new Error('Remote commands: ping, offer TASK ENDPOINT REPOSITORY, list, events ENDPOINT [CURSOR], reconcile MESSAGE [ENDPOINT], request ENDPOINT (stdin JSON)');
  }finally{q.close();}
}
