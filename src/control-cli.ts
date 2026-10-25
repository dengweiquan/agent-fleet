import {readFileSync,existsSync} from 'node:fs';
import {join,isAbsolute} from 'node:path';
import {Engine} from './engine.ts';
import {ControlLedger} from './control.ts';
import type {ControlOperation,ControlRequest} from './control.ts';
import {ResourcePool} from './resource-pool.ts';
import {redact} from './process.ts';

const operations:ControlOperation[]=['draft.create','note.append','pause','resume','stop'];
const maxBytes=65536;
function object(value:any,keys:string[]){
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!keys.includes(k)))throw new Error('Invalid control fields');
  return value;
}
function scopeFile(path:string){
  const text=readFileSync(path,'utf8');if(Buffer.byteLength(text)>16384)throw new Error('Control scope exceeds 16 KiB');
  const s=object(JSON.parse(text),['version','project','sessionId','permissions']);
  if(s.version!==1||typeof s.project!=='string'||!isAbsolute(s.project)||typeof s.sessionId!=='string'||!Array.isArray(s.permissions)||s.permissions.some((p:any)=>!operations.includes(p))||new Set(s.permissions).size!==s.permissions.length)throw new Error('Explicit project/session and control permissions required');
  return s as {version:1,project:string,sessionId:string,permissions:ControlOperation[]};
}
function requestShape(raw:any){
  const v=object(raw,['action','requestId','after','limit','request']);
  const allowed:Record<string,string[]>={status:['action'],resources:['action'],receipt:['action','requestId'],events:['action','after','limit'],artifacts:['action','after','limit'],submit:['action','request'],apply:['action','requestId']};
  if(!allowed[v.action]||Object.keys(v).some(k=>!allowed[v.action].includes(k)))throw new Error('Unsupported control action or fields');
  if(['receipt','apply'].includes(v.action)&&(typeof v.requestId!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(v.requestId)))throw new Error('Valid requestId required');
  if(v.after!==undefined&&(!Number.isSafeInteger(v.after)||v.after<0))throw new Error('Invalid event/artifact cursor');
  if(v.limit!==undefined&&(!Number.isSafeInteger(v.limit)||v.limit<1||v.limit>100))throw new Error('Invalid page limit 1..100');
  return v;
}

/** The local launcher pins a reviewed scope file. JSON input cannot grant permissions. */
export function controlEndpoint(file:string,observer=false){
  const initial=scopeFile(file);
  const probe=new Engine(initial.project,true,initial.sessionId);
  const identity={projectId:probe.root,sessionId:probe.sessionId},dir=probe.dir;probe.close();
  const permission=()=>{const now=scopeFile(file);if(now.project!==initial.project||now.sessionId!==initial.sessionId)throw new Error('Control scope changed; reopen endpoint');return new Set<ControlOperation>(observer?[]:now.permissions);};
  return async(raw:unknown):Promise<unknown>=>{
    const input=requestShape(raw);permission();
    const mutation=['submit','apply'].includes(input.action);
    if(mutation&&observer)throw new Error('Observer cannot submit or apply control commands');
    const e=new Engine(identity.projectId,true,identity.sessionId);
    try{
      if(input.action==='status')return {...e.status(),control:{scope:identity,modelInvoked:false}};
      if(input.action==='resources'){
        const path=e.store.getMeta('resourcePoolPath') as string|null;
        let leases:unknown[]=[],resources:unknown[]=[];
        if(path){const pool=new ResourcePool(path,{readOnly:true});try{const state=pool.status();const own=state.leases.filter(l=>l.request.projectId===identity.projectId&&l.request.sessionId===identity.sessionId);leases=own;const ids=new Set(own.map(l=>l.resource.id));resources=state.resources.filter(r=>ids.has(r.id));}finally{pool.close();}}
        return {scope:identity,instances:e.config()?.instances??[],resources,leases,capacityView:'session bindings only; no global admission guarantee',modelInvoked:false};
      }
      if(mutation&&e.formatVersion()!==2)throw new Error('Control mutations require a version 2 session');
      if(input.action==='submit'&&input.request?.operation==='note.append'&&input.request.payload?.taskId)e.task(input.request.payload.taskId);
      const path=join(dir,'control.sqlite');
      if(!mutation&&!existsSync(path))return input.action==='receipt'?null:[];
      const ledger=new ControlLedger(path,{scope:identity,allowedOperations:permission,readOnly:!mutation});
      try{
        if(input.action==='receipt')return ledger.receipt(input.requestId);
        if(input.action==='events')return ledger.events(input.after??0,input.limit??50);
        if(input.action==='artifacts')return ledger.artifacts(input.after??0,input.limit??20);
        if(input.action==='submit')return ledger.submit(input.request);
        // Known precondition failures do not create an uncertain-effect barrier.
        // A returned unknown receipt is never reconsidered or replayed here.
        const pending=ledger.receipt(input.requestId);
        if(pending?.state==='accepted'&&pending.operation==='resume'&&existsSync(join(dir,'coordinator.lock')))throw new Error('Coordinator lock retained; inspect recovery evidence before resume');
        return await ledger.apply(input.requestId,async(request:ControlRequest)=>{
          permission();
          const target=new Engine(identity.projectId,false,identity.sessionId);
          try{
            if(request.operation==='pause'){target.pause();return {schedulingPaused:true,existingWorkersStopped:false};}
            if(request.operation==='resume'){return {reconciled:target.resume(),schedulingPaused:false,runsReplayed:false};}
            if(request.operation==='stop'){
              const runId=(request.payload as {runId:string}).runId;
              const before=target.store.getRun(runId);
              if(!before||!['running','verifying'].includes(before.status))return {runId,stopRequestRecorded:false,reason:before?'attempt-not-running':'unknown-run',observedRunState:before?.status??null,processTerminationConfirmed:false};
              try{target.stop(runId);}catch(error){
                const after=target.store.getRun(runId);
                if(after&&after.status!==before.status&&!['running','verifying'].includes(after.status))return {runId,stopRequestRecorded:false,reason:'attempt-state-changed',observedRunState:after.status,processTerminationConfirmed:false};
                throw error;
              }
              return {runId,stopRequestRecorded:true,processTerminationConfirmed:false};
            }
            throw new Error('Unsupported control effect');
          }finally{target.close();}
        });
      }finally{ledger.close();}
    }finally{e.close();}
  };
}
async function* lines(input:AsyncIterable<Buffer|string>){
  let pending=Buffer.alloc(0);
  for await(const chunk of input){
    const bytes=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);
    let start=0;
    for(let i=0;i<bytes.length;i++)if(bytes[i]===10){
      if(pending.length+i-start>maxBytes)throw new Error('Control frame exceeds 64 KiB');
      yield Buffer.concat([pending,bytes.subarray(start,i)]).toString('utf8');pending=Buffer.alloc(0);start=i+1;
    }
    if(pending.length+bytes.length-start>maxBytes)throw new Error('Control frame exceeds 64 KiB');
    pending=Buffer.concat([pending,bytes.subarray(start)]);
  }
  if(pending.length)yield pending.toString('utf8');
}
async function emit(data:unknown){
  let line=JSON.stringify(data);if(Buffer.byteLength(line)>maxBytes)line=JSON.stringify({ok:false,error:{message:'Control response exceeds 64 KiB; use narrower queries'}});
  await new Promise<void>((resolve,reject)=>process.stdout.write(line+'\n',error=>error?reject(error):resolve()));
}
export async function controlCommand(words:string[],observer=false){
  const [file,action,arg,...extra]=words;
  if(!file||!action||extra.length)throw new Error('Use control <scope.json> status|resources|receipt ID|events [cursor]|artifacts [cursor]|submit|apply ID|serve');
  const endpoint=controlEndpoint(file,observer);
  if(action==='serve'){
    if(arg)throw new Error('Unexpected control argument');
    for await(const line of lines(process.stdin)){
      try{await emit({ok:true,data:await endpoint(JSON.parse(line))});}catch(error){await emit({ok:false,error:{message:redact(String(error))}});}
    }return;
  }
  let request:any={action};
  if(action==='submit'){
    if(arg)throw new Error('Submit reads one JSON frame from stdin');
    const frames=[];for await(const line of lines(process.stdin)){frames.push(line);if(frames.length>1)throw new Error('Submit requires one frame');}
    if(frames.length!==1)throw new Error('Submit requires one frame');request.request=JSON.parse(frames[0]);
  }else if(['receipt','apply'].includes(action))request.requestId=arg;
  else if(['events','artifacts'].includes(action)){if(arg!==undefined)request.after=Number(arg);}
  else if(arg)throw new Error('Unexpected control argument');
  await emit({ok:true,data:await endpoint(request)});
}
