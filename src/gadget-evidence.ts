import {createHash} from 'node:crypto';
import type {Engine} from './engine.ts';
import {createGadgetState,reduceGadget} from './gadget-events.ts';
import type {GadgetBinding,GadgetInput,GadgetState} from './gadget-events.ts';
import type {Event} from './types.ts';

export interface GadgetEvidenceRecord {binding:GadgetBinding;state:GadgetState;revision:number}
export interface GadgetEvidenceResult extends GadgetEvidenceRecord {events:Event[]}
function stable(value:unknown):string{
  if(value===null||typeof value!=='object')return JSON.stringify(value);
  if(Array.isArray(value))return `[${value.map(stable).join(',')}]`;
  return `{${Object.keys(value).sort().map(key=>`${JSON.stringify(key)}:${stable((value as Record<string,unknown>)[key])}`).join(',')}}`;
}
function requestKey(requestId:string):string{
  if(typeof requestId!=='string'||!requestId||Buffer.byteLength(requestId)>256||/[\x00-\x1f\x7f]/.test(requestId))throw new Error('Invalid gadget request identity');
  return createHash('sha256').update(requestId).digest('hex');
}
function trustedScope(engine:Engine):void{
  if(engine.store.getMeta('root')!==engine.root||engine.store.getMeta('sessionId')!==engine.sessionId)throw new Error('Gadget evidence requires fixed engine project/session identity');
}
function checkedRecord(value:unknown,key:string,engine:Engine):GadgetEvidenceRecord{
  const record=value as GadgetEvidenceRecord;
  if(!record||!record.binding||!record.state||!Number.isSafeInteger(record.revision)||record.revision<1||requestKey(record.binding.requestId)!==key||record.binding.projectId!==engine.root||record.binding.sessionId!==engine.sessionId||stable(record.state.binding)!==stable(record.binding))throw new Error('Gadget evidence snapshot identity mismatch');
  createGadgetState(record.binding);
  if(record.state.accepted!==false||record.state.transport!=='disabled-unpaired')throw new Error('Gadget evidence cannot enable transport or accept tasks');
  return record;
}

/** Observe one session's evidence only. Cross-session sidechat uniqueness requires a deployment registry. */
export function getGadgetEvidence(engine:Engine,requestId:string):GadgetEvidenceRecord|null{
  trustedScope(engine);const key=requestKey(requestId),raw=engine.store.gadgetEvidenceSnapshot().records[key];
  return raw===undefined?null:structuredClone(checkedRecord(raw,key,engine));
}

/** Reducer completion is evidence, never queue delivery/ACK, capacity release, or task acceptance. */
export function recordGadgetEvidence(engine:Engine,binding:GadgetBinding,input:GadgetInput):GadgetEvidenceResult{
  engine.mutate();trustedScope(engine);
  const initial=createGadgetState(binding),fixed=initial.binding;
  if(fixed.projectId!==engine.root||fixed.sessionId!==engine.sessionId)throw new Error('Gadget evidence binding is outside engine project/session');
  const offers=engine.remoteStatus();
  const matching=offers.filter(row=>row.taskId===fixed.taskId&&row.runId===fixed.runId&&row.attemptId===fixed.attemptId&&row.endpointId===fixed.endpointId);
  if(matching.length!==1||matching[0].state==='offer-pending'||offers.some(row=>(row.runId===fixed.runId||row.attemptId===fixed.attemptId)&&!(row.taskId===fixed.taskId&&row.runId===fixed.runId&&row.attemptId===fixed.attemptId&&row.endpointId===fixed.endpointId)))throw new Error('Gadget evidence requires one exact existing remote offer');
  const snapshot=engine.store.gadgetEvidenceSnapshot(),key=requestKey(fixed.requestId);
  const entries=Object.entries(snapshot.records).map(([recordKey,value])=>checkedRecord(value,recordKey,engine));
  const previous=entries.find(record=>record.binding.requestId===fixed.requestId);
  if(previous&&stable(previous.binding)!==stable(fixed))throw new Error('Gadget request binding is immutable');
  for(const other of entries){
    if(other.binding.requestId===fixed.requestId)continue;
    if(other.binding.runId===fixed.runId||other.binding.attemptId===fixed.attemptId)throw new Error('Gadget run/attempt already has an immutable request binding');
    if(other.binding.sidechatId===fixed.sidechatId)throw new Error('Gadget sidechat cannot be reused across attempts in this session');
  }
  const reduced=reduceGadget(previous?.state??initial,input);
  if(stable(reduced.state.binding)!==stable(fixed)||reduced.state.transport!=='disabled-unpaired'||reduced.state.accepted!==false)throw new Error('Gadget reducer crossed the evidence-only boundary');
  if(previous&&reduced.events.length===0&&stable(reduced.state)===stable(previous.state))return {...structuredClone(previous),events:[]};
  const next:GadgetEvidenceRecord={binding:fixed,state:reduced.state,revision:snapshot.revision+1};
  const records={...snapshot.records,[key]:next};
  const committed=engine.store.compareAndSwapGadgetEvidence(snapshot.revision,records,reduced.events.map(event=>({kind:event.kind,data:{binding:fixed,evidence:event.data,accepted:false,transport:'disabled-unpaired'}})));
  return {...structuredClone(next),revision:committed.revision,events:committed.events};
}
