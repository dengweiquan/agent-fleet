import {createHash} from 'node:crypto';

/** Original, offline interpretation of public Gadget wire fields; no SDK or transport. */
export interface GadgetBinding {
  projectId:string; sessionId:string; taskId:string; runId:string; attemptId:string;
  endpointId:string; requestId:string; sidechatId:string;
}
export interface GadgetInput {sourceId:string; kind:'ack'|'event'|'disconnect'|'quiet'; data:unknown}
export interface GadgetEvidenceEvent {kind:string; data:Record<string,unknown>}
export const GADGET_LIMITS=Object.freeze({inputBytes:32768,seen:256,pending:12,messages:128,tasks:64,textBytes:16384,totalTextBytes:65536,quarantine:32});
type WireKind='agent.status'|'task.status'|'delta.message_start'|'delta.text_append'|'delta.message_done'|'message.assistant';
interface WireEvent {
  event:WireKind; sessionId:string; messageId:string|null; parentId:string|null; taskId:string|null;
  role:string|null; status:string|null; activityCode:string|null; activityText:string|null;
  text:string|null; displayText:string|null; displayTextReady:boolean|null;
}
export interface GadgetMessage {
  messageId:string; parentMessageId:string|null; taskId:string|null;
  text:string; displayText:string|null; displayTextReady:boolean; final:boolean;
}
export interface GadgetTask {
  taskId:string; status:string|null; lastObservedStatus:string; terminal:boolean;
  terminalStatuses:string[]; conflict:boolean;
}
export interface GadgetState {
  version:1; binding:GadgetBinding; transport:'disabled-unpaired'; accepted:false;
  delivery:'awaiting-ack'|'delivered'|'unknown';
  ack:{messageId:string; replyToMessageId:string|null}|null;
  seen:Array<{sourceId:string; hash:string}>;
  pending:Array<{sourceId:string; event:WireEvent}>;
  messages:GadgetMessage[]; tasks:GadgetTask[];
  quarantine:Array<{sourceId:string; reason:string}>;
  unknownReasons:string[];
}
const bindingFields=['projectId','sessionId','taskId','runId','attemptId','endpointId','requestId','sidechatId'];
const wireKinds:WireKind[]=['agent.status','task.status','delta.message_start','delta.text_append','delta.message_done','message.assistant'];
const terminals=new Set(['completed','failed','errored','cancelled','canceled']);
function object(value:unknown,keys:string[],label:string):Record<string,unknown>{
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!keys.includes(k)))throw new Error(`gadget: invalid ${label} fields`);
  return value as Record<string,unknown>;
}
function identifier(value:unknown,label:string,max=256):string {
  if(typeof value!=='string'||!value.length||Buffer.byteLength(value)>max||(max===1024?/[\x00-\x1f\x7f-\x9f]/:/[\x00-\x20\x7f-\x9f]/).test(value))throw new Error(`gadget: invalid ${label}`);
  return value;
}
/** Bound canonical JSON before parsing, hashing or persistence. Raw content is never retained. */
function canonical(value:unknown):string {
  let bytes=0,nodes=0;
  const add=(s:string)=>{bytes+=Buffer.byteLength(s);if(bytes>GADGET_LIMITS.inputBytes)throw new Error('gadget: input exceeds byte limit');return s;};
  const visit=(v:unknown,depth:number):string=>{
    if(++nodes>2048||depth>8)throw new Error('gadget: input exceeds structural limit');
    if(v===null)return add('null');
    if(typeof v==='string'||typeof v==='boolean'||(typeof v==='number'&&Number.isFinite(v)))return add(JSON.stringify(v));
    if(Array.isArray(v))return add('[')+v.map(x=>visit(x,depth+1)).join(add(','))+add(']');
    if(v&&typeof v==='object'&&(Object.getPrototypeOf(v)===Object.prototype||Object.getPrototypeOf(v)===null)){
      const descriptors=Object.getOwnPropertyDescriptors(v);
      const entries=Object.keys(descriptors).sort().map(key=>{
        const descriptor=descriptors[key];if(!('value' in descriptor))throw new Error('gadget: non-JSON input');
        return add(JSON.stringify(key))+add(':')+visit(descriptor.value,depth+1);
      });
      return add('{')+entries.join(add(','))+add('}');
    }
    throw new Error('gadget: non-JSON input');
  };
  const encoded=visit(value,0);if(Buffer.byteLength(encoded)>GADGET_LIMITS.inputBytes)throw new Error('gadget: input exceeds byte limit');return encoded;
}
function redact(value:string):string {
  return value.replace(/\x1b\][^\x07]*(?:\x07|\x1b\\)/g,'').replace(/\x1b\[[0-?]*[ -/]*[@-~]/g,'')
    .replace(/[\x00-\x08\x0b-\x1f\x7f-\x9f]/g,'')
    .replace(/-----BEGIN [A-Z ]*(?:PRIVATE KEY|CERTIFICATE)-----[\s\S]*?(?:-----END [A-Z ]*-----|$)/g,'[REDACTED]')
    .replace(/\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,}|github_pat_[A-Za-z0-9_]{8,}|mgst_[A-Za-z0-9_-]+)\b/g,'[REDACTED]')
    .replace(/(Bearer\s+)\S+/gi,'$1[REDACTED]')
    .replace(/((?:password|token|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+/gi,'$1[REDACTED]');
}
function text(value:unknown,label:string):string|null {
  if(value===undefined)return null;
  if(typeof value!=='string'||Buffer.byteLength(value)>8192)throw new Error(`gadget: invalid ${label}`);
  return redact(value);
}
function alias(values:unknown[],label:string):string|null {
  const present=values.filter(v=>v!==undefined);if(!present.length)return null;
  const parsed=present.map(v=>identifier(v,label));if(parsed.some(v=>v!==parsed[0]))throw new Error(`gadget: conflicting ${label}`);
  return parsed[0];
}
function wire(value:unknown):WireEvent {
  const raw=object(value,['type','event','payload','session_id','message_id'],'raw event');
  if(raw.type!=='event'||!wireKinds.includes(raw.event as WireKind))throw new Error('gadget: unsupported raw event; lossy helpers are not task evidence');
  const p=object(raw.payload,['session_id','message_id','id','reply_to_message_id','parent_message_id','task_id','status','activity_code','activity_text','role','text','display_text','display_text_ready'],'payload');
  const sessionId=alias([p.session_id,raw.session_id],'provider session');if(!sessionId)throw new Error('gadget: provider session required');
  if(p.display_text_ready!==undefined&&typeof p.display_text_ready!=='boolean')throw new Error('gadget: invalid display readiness');
  const e:WireEvent={event:raw.event as WireKind,sessionId,messageId:alias([p.message_id,p.id,raw.message_id],'provider message'),parentId:alias([p.reply_to_message_id,p.parent_message_id],'parent message'),taskId:alias([p.task_id],'provider task'),role:alias([p.role],'role'),status:alias([p.status],'status'),activityCode:alias([p.activity_code],'activity code'),activityText:text(p.activity_text,'activity text'),text:text(p.text,'text'),displayText:text(p.display_text,'display text'),displayTextReady:p.display_text_ready as boolean|undefined??null};
  if(e.event==='task.status'&&(!e.taskId||!e.status))throw new Error('gadget: raw task status and task ID required');
  if(e.event.startsWith('delta.')||e.event==='message.assistant'){
    if(!e.messageId)throw new Error('gadget: message ID required');
    if(e.event==='delta.text_append'&&e.text===null)throw new Error('gadget: text delta required');
  }
  return e;
}
export function createGadgetState(value:GadgetBinding):GadgetState {
  const input=object(JSON.parse(canonical(value)),bindingFields,'binding');
  const binding=Object.fromEntries(bindingFields.map(k=>[k,identifier(input[k],k,k==='projectId'?1024:256)])) as unknown as GadgetBinding;
  return {version:1,binding,transport:'disabled-unpaired',accepted:false,delivery:'awaiting-ack',ack:null,seen:[],pending:[],messages:[],tasks:[],quarantine:[],unknownReasons:[]};
}
export function reduceGadget(previous:GadgetState,input:GadgetInput):{state:GadgetState;events:GadgetEvidenceEvent[]} {
  if(previous.version!==1||previous.transport!=='disabled-unpaired'||previous.accepted!==false)throw new Error('gadget: invalid snapshot authority');
  createGadgetState(previous.binding);
  const encoded=canonical(input),v=object(JSON.parse(encoded),['sourceId','kind','data'],'input');
  const sourceId=identifier(v.sourceId,'source ID');
  if(!['ack','event','disconnect','quiet'].includes(v.kind as string)||!Object.hasOwn(v,'data'))throw new Error('gadget: invalid input kind/data');
  const hash=createHash('sha256').update(encoded).digest('hex'),seen=previous.seen.find(s=>s.sourceId===sourceId);
  if(seen){if(seen.hash!==hash)throw new Error('gadget: conflicting source ID');return {state:previous,events:[]};}
  if(previous.seen.length>=GADGET_LIMITS.seen)throw new Error('gadget: source identity capacity exhausted; reconcile without replay');
  const state=structuredClone(previous),events:GadgetEvidenceEvent[]=[];
  const emit=(kind:string,data:Record<string,unknown>)=>events.push({kind,data:{...data,accepted:false}});
  const unknown=(id:string,reason:string)=>{state.delivery='unknown';if(!state.unknownReasons.includes(reason)&&state.unknownReasons.length<16)state.unknownReasons.push(reason);emit('gadget.unknown',{sourceId:id,reason,automaticResend:false});};
  const quarantine=(id:string,reason:string)=>{state.quarantine.push({sourceId:id,reason});if(state.quarantine.length>GADGET_LIMITS.quarantine)state.quarantine.shift();emit('gadget.quarantine',{sourceId:id,reason});};
  state.seen.push({sourceId,hash});
  const defer=(id:string,e:WireEvent,reason:string)=>{
    if(state.pending.length>=GADGET_LIMITS.pending){quarantine(id,'pending capacity exceeded');unknown(id,'pending capacity exceeded');return;}
    state.pending.push({sourceId:id,event:e});emit('gadget.deferred',{sourceId:id,reason});
  };
  // Returns false only for unresolved lineage. A known task can correlate parentless status/reply events.
  const apply=(id:string,e:WireEvent):boolean=>{
    if(e.sessionId!==state.binding.sidechatId){quarantine(id,'wrong sidechat');return true;}
    if(e.role!==null&&e.role!=='assistant'){quarantine(id,'non-assistant role');return true;}
    if(!state.ack)return false;
    const root=state.ack.messageId,msg=state.messages.find(m=>m.messageId===e.messageId),task=state.tasks.find(t=>t.taskId===e.taskId);
    if(e.parentId===e.messageId&&e.messageId!==null){quarantine(id,'self parent');return true;}
    if(msg&&e.parentId!==null&&msg.parentMessageId!==e.parentId){quarantine(id,'conflicting parent');return true;}
    if(msg&&e.taskId&&msg.taskId&&msg.taskId!==e.taskId){quarantine(id,'conflicting message task');return true;}
    if(e.parentId!==null&&e.parentId!==root&&!state.messages.some(m=>m.messageId===e.parentId))return false;
    const parent=e.parentId===null?null:state.messages.find(m=>m.messageId===e.parentId);
    if(parent?.taskId&&e.taskId&&parent.taskId!==e.taskId){quarantine(id,'conflicting parent task');return true;}
    const linked=e.parentId===root||!!parent||!!msg||e.messageId===root||!!task;
    if(!linked)return false;
    if(e.messageId===root&&(e.event.startsWith('delta.')||e.event==='message.assistant')){quarantine(id,'assistant cannot overwrite acknowledged user message');return true;}
    const isNewMessage=e.messageId!==null&&e.messageId!==root&&!msg;
    const isNewTask=e.taskId!==null&&!task;
    if((isNewMessage&&state.messages.length>=GADGET_LIMITS.messages)||(isNewTask&&state.tasks.length>=GADGET_LIMITS.tasks)){quarantine(id,'correlation capacity exceeded');unknown(id,'correlation capacity exceeded');return true;}
    // Calculate the prospective content before publishing any correlation or task state.
    let nextText=msg?.text??'',nextDisplay=msg?.displayText??null;
    const messageEvent=e.event.startsWith('delta.')||e.event==='message.assistant';
    if(messageEvent){
      if(msg?.final&&e.event!=='delta.message_done'&&e.event!=='message.assistant'){quarantine(id,'delta after final message');return true;}
      if(e.text!==null)nextText=redact(e.event==='delta.text_append'?nextText+e.text:e.text);
      if(e.displayText!==null)nextDisplay=e.displayText;
      if(msg?.final&&((e.text!==null&&nextText!==msg.text)||(e.displayText!==null&&nextDisplay!==msg.displayText))){quarantine(id,'conflicting final message');unknown(id,'conflicting final message');return true;}
      const bytes=Buffer.byteLength(nextText)+Buffer.byteLength(nextDisplay??'');
      const total=state.messages.reduce((n,m)=>n+Buffer.byteLength(m.text)+Buffer.byteLength(m.displayText??''),0)-(msg?Buffer.byteLength(msg.text)+Buffer.byteLength(msg.displayText??''):0)+bytes;
      if(bytes>GADGET_LIMITS.textBytes||total>GADGET_LIMITS.totalTextBytes){quarantine(id,'text capacity exceeded');unknown(id,'text capacity exceeded');return true;}
    }
    let bound=msg;
    if(isNewMessage){bound={messageId:e.messageId!,parentMessageId:e.parentId,taskId:e.taskId??parent?.taskId??null,text:'',displayText:null,displayTextReady:false,final:false};state.messages.push(bound);}
    if(bound&&e.taskId&&!bound.taskId)bound.taskId=e.taskId;
    if(isNewTask)state.tasks.push({taskId:e.taskId!,status:null,lastObservedStatus:'',terminal:false,terminalStatuses:[],conflict:false});
    if(e.event==='task.status'){
      const t=state.tasks.find(t=>t.taskId===e.taskId)!;t.lastObservedStatus=e.status!;
      if(terminals.has(e.status!)){
        if(!t.terminalStatuses.includes(e.status!))t.terminalStatuses.push(e.status!);
        if(t.terminal&&t.status!==e.status){t.conflict=true;unknown(id,'conflicting terminal task statuses');}
        if(!t.terminal)t.status=e.status;t.terminal=true;
      }else if(!t.terminal)t.status=e.status;
      emit('gadget.task.status',{sourceId:id,providerTaskId:t.taskId,status:t.status,observedStatus:e.status,terminal:t.terminal,terminalStatuses:[...t.terminalStatuses],conflict:t.conflict});
    }else if(e.event==='agent.status')emit('gadget.agent.status',{sourceId:id,providerMessageId:e.messageId,providerTaskId:e.taskId,status:e.status,activityCode:e.activityCode,activityText:e.activityText,publicActivityOnly:true});
    else if(bound){
      bound.text=nextText;bound.displayText=nextDisplay;
      if(e.displayTextReady!==null&&(!bound.final||e.displayTextReady))bound.displayTextReady=e.displayTextReady;
      if(e.event==='delta.message_done'||(e.event==='message.assistant'&&e.displayTextReady!==false))bound.final=true;
      emit('gadget.message',{sourceId:id,event:e.event,providerMessageId:bound.messageId,parentMessageId:bound.parentMessageId,providerTaskId:bound.taskId,text:bound.text,displayText:bound.displayText,displayTextReady:bound.displayTextReady,final:bound.final,publicOutputOnly:true});
    }
    return true;
  };
  const revisit=()=>{
    let changed=true;
    while(changed&&state.ack){changed=false;const pending=state.pending;state.pending=[];for(const item of pending){if(apply(item.sourceId,item.event))changed=true;else state.pending.push(item);}}
  };
  if(v.kind==='ack'){
    const raw=object(v.data,['result'],'ack'),ack=object(raw.result,['session_id','is_thread','message_id','reply_to_message_id'],'ack result');
    const sessionId=identifier(ack.session_id,'ACK session'),messageId=identifier(ack.message_id,'ACK message');
    const replyToMessageId=alias([ack.reply_to_message_id],'ACK parent');
    if(ack.is_thread!==true||sessionId!==state.binding.sidechatId)quarantine(sourceId,'ACK is not the fixed sidechat');
    else if(state.ack&&(state.ack.messageId!==messageId||state.ack.replyToMessageId!==replyToMessageId)){quarantine(sourceId,'conflicting ACK');unknown(sourceId,'conflicting ACK');}
    else if(!state.ack){state.ack={messageId,replyToMessageId};if(state.delivery!=='unknown')state.delivery='delivered';emit('gadget.ack',{sourceId,providerSessionId:sessionId,providerMessageId:messageId,isThread:true,delivery:state.delivery});revisit();}
  }else if(v.kind==='event'){
    const e=wire(v.data);
    if(!apply(sourceId,e))defer(sourceId,e,state.ack?'unresolved lineage':'awaiting ACK');
    revisit();
  }else{
    const info=object(v.data,['reason'],v.kind as string);if(info.reason!==undefined)text(info.reason,'reason');
    if(v.kind==='disconnect'){unknown(sourceId,'disconnected');emit('gadget.disconnected',{sourceId,delivery:'unknown',automaticResend:false});}
    else emit('gadget.quiet',{sourceId,quiescent:true,delivery:state.delivery});
  }
  return {state,events};
}
