import test from 'node:test';
import assert from 'node:assert/strict';
import {createGadgetState,reduceGadget,GADGET_LIMITS} from '../src/gadget-events.ts';
import type {GadgetBinding,GadgetInput,GadgetState} from '../src/gadget-events.ts';

const binding:GadgetBinding={projectId:'/synthetic/project with spaces',sessionId:'orchestration-synthetic',taskId:'task-synthetic',runId:'run-synthetic',attemptId:'attempt-synthetic',endpointId:'endpoint-synthetic',requestId:'request-synthetic',sidechatId:'sidechat-synthetic'};
const ack=(sourceId='ack-1'):GadgetInput=>({sourceId,kind:'ack',data:{result:{session_id:binding.sidechatId,is_thread:true,message_id:'user-synthetic'}}});
const event=(sourceId:string,name:string,fields:Record<string,unknown>={}):GadgetInput=>({sourceId,kind:'event',data:{type:'event',event:name,payload:{session_id:binding.sidechatId,...fields}}});
const reply=(sourceId:string,fields:Record<string,unknown>={})=>event(sourceId,'message.assistant',{id:'assistant-synthetic',parent_message_id:'user-synthetic',role:'assistant',text:'synthetic reply',display_text_ready:true,...fields});
const task=(sourceId:string,status:string,fields:Record<string,unknown>={})=>event(sourceId,'task.status',{message_id:'user-synthetic',task_id:'provider-task-synthetic',status,...fields});
const delivered=()=>reduceGadget(createGadgetState(binding),ack()).state;
function checkSafety(state:GadgetState){assert.equal(state.transport,'disabled-unpaired');assert.equal(state.accepted,false);assert.deepEqual(state.binding,binding);assert.deepEqual(JSON.parse(JSON.stringify(state)),state);}

test('fixed binding and ACK are serializable delivery evidence, never accepted or a live transport',()=>{
 const initial=createGadgetState(binding),before=structuredClone(initial),out=reduceGadget(initial,ack());
 assert.deepEqual(initial,before);assert.equal(out.state.delivery,'delivered');assert.deepEqual(out.state.ack,{messageId:'user-synthetic',replyToMessageId:null});
 assert.equal(out.events[0].kind,'gadget.ack');assert.equal(out.events[0].data.accepted,false);checkSafety(out.state);
 assert.throws(()=>createGadgetState({...binding,sidechatId:''}),/sidechatId/);
 assert.throws(()=>createGadgetState({...binding,unexpected:'field'} as any),/fields/);
});

test('source identities deduplicate exact canonical content and reject conflicting reuse without mutation',()=>{
 const first=reduceGadget(createGadgetState(binding),ack()),snapshot=structuredClone(first.state);
 const reordered={data:{result:{message_id:'user-synthetic',is_thread:true,session_id:binding.sidechatId}},kind:'ack' as const,sourceId:'ack-1'};
 assert.deepEqual(reduceGadget(first.state,reordered),{state:first.state,events:[]});
 assert.throws(()=>reduceGadget(first.state,{...ack(),data:{result:{session_id:binding.sidechatId,is_thread:true,message_id:'another-user'}}}),/conflicting source ID/);
 assert.deepEqual(first.state,snapshot);
 const repeated=reduceGadget(first.state,ack('ack-another-ingress'));assert.equal(repeated.events.length,0);assert.equal(repeated.state.ack?.messageId,'user-synthetic');
});

test('pre-ACK activity and replies are inert, bounded and replayed after the matching ACK',()=>{
 let s=createGadgetState(binding);
 s=reduceGadget(s,event('activity-before','agent.status',{message_id:'user-synthetic',task_id:'provider-task-synthetic',status:'working',activity_code:'coding',activity_text:'Public activity summary'})).state;
 const buffered=reduceGadget(s,reply('reply-before'));s=buffered.state;
 assert.equal(s.pending.length,2);assert.equal(s.tasks.length,0);assert.equal(s.messages.length,0);assert(buffered.events.every(e=>e.kind==='gadget.deferred'));
 const out=reduceGadget(s,ack());assert.equal(out.state.pending.length,0);assert.equal(out.state.messages[0].text,'synthetic reply');assert.equal(out.state.tasks[0].taskId,'provider-task-synthetic');
 assert(out.events.some(e=>e.kind==='gadget.agent.status'&&e.data.publicActivityOnly===true));assert(out.events.some(e=>e.kind==='gadget.message'&&e.data.publicOutputOnly===true));checkSafety(out.state);
});

test('wrong sidechat, main-chat ACK, conflicting ACK and unbound lineage cannot become evidence',()=>{
 let s=createGadgetState(binding);
 for(const [sourceId,result] of [ ['wrong-session',{session_id:'other-sidechat',is_thread:true,message_id:'user-synthetic'}],['main-chat',{session_id:binding.sidechatId,is_thread:false,message_id:'user-synthetic'}] ] as const){const out=reduceGadget(s,{sourceId,kind:'ack',data:{result}});s=out.state;assert.equal(s.ack,null);assert(out.events.every(e=>e.kind==='gadget.quarantine'));}
 s=reduceGadget(s,ack()).state;
 s=reduceGadget(s,reply('wrong-chat',{session_id:'other-sidechat'})).state;
 s=reduceGadget(s,reply('wrong-parent',{parent_message_id:'unrelated-root'})).state;
 s=reduceGadget(s,task('unbound-terminal','completed',{message_id:'unrelated-message',task_id:'unbound-task'})).state;
 assert.equal(s.messages.length,0);assert.equal(s.tasks.length,0);assert.equal(s.delivery,'delivered');assert.equal(s.pending.length,2);
 const conflict=reduceGadget(s,{sourceId:'conflict-ack',kind:'ack',data:{result:{session_id:binding.sidechatId,is_thread:true,message_id:'other-user'}}});
 assert.equal(conflict.state.delivery,'unknown');assert.equal(conflict.state.ack?.messageId,'user-synthetic');checkSafety(conflict.state);
});

test('out-of-order final before its parent waits then resolves the complete ACK-anchored lineage',()=>{
 const child=reply('child-final',{id:'child',parent_message_id:'parent',text:'child final'});
 let s=reduceGadget(delivered(),child).state;assert.equal(s.messages.length,0);assert.equal(s.pending.length,1);
 const parent=event('parent-start','delta.message_start',{id:'parent',parent_message_id:'user-synthetic',role:'assistant'});
 const out=reduceGadget(s,parent);s=out.state;assert.equal(s.pending.length,0);assert.equal(s.messages.find(m=>m.messageId==='child')?.final,true);assert.equal(s.messages.find(m=>m.messageId==='child')?.parentMessageId,'parent');
 assert.deepEqual(out.events.filter(e=>e.kind==='gadget.message').map(e=>e.data.providerMessageId),['parent','child']);checkSafety(s);
});

test('new ingress IDs preserve identical text deltas, while duplicate ingress is applied once',()=>{
 let s=reduceGadget(delivered(),event('start','delta.message_start',{id:'assistant-synthetic',parent_message_id:'user-synthetic'})).state;
 const append=(sourceId:string)=>event(sourceId,'delta.text_append',{id:'assistant-synthetic',text:'ha'});
 s=reduceGadget(s,append('delta-a')).state;s=reduceGadget(s,append('delta-a')).state;s=reduceGadget(s,append('delta-b')).state;
 assert.equal(s.messages[0].text,'haha');
 s=reduceGadget(s,event('done','delta.message_done',{id:'assistant-synthetic',display_text:'haha',display_text_ready:true})).state;
 assert.equal(s.messages[0].final,true);assert.equal(s.messages[0].displayTextReady,true);checkSafety(s);
});

test('all five raw terminal statuses are preserved separately and completed never means accepted',()=>{
 for(const status of ['completed','failed','errored','cancelled','canceled']){
  const out=reduceGadget(delivered(),task('terminal-'+status,status));assert.equal(out.state.tasks[0].status,status);assert.equal(out.state.tasks[0].terminal,true);assert.deepEqual(out.state.tasks[0].terminalStatuses,[status]);assert.equal(out.events[0].data.status,status);assert.equal(out.events[0].data.accepted,false);checkSafety(out.state);
 }
});

test('unknown statuses remain nonterminal and late running cannot undo a terminal observation',()=>{
 let s=reduceGadget(delivered(),task('future','provider-future-state')).state;assert.equal(s.tasks[0].status,'provider-future-state');assert.equal(s.tasks[0].terminal,false);
 s=reduceGadget(s,task('failed','failed')).state;const out=reduceGadget(s,task('late-running','running'));s=out.state;
 assert.equal(s.tasks[0].status,'failed');assert.equal(s.tasks[0].lastObservedStatus,'running');assert.equal(s.tasks[0].terminal,true);assert.equal(out.events[0].data.observedStatus,'running');checkSafety(s);
});

test('conflicting terminal reports retain original statuses and make the outcome sticky unknown',()=>{
 let s=reduceGadget(delivered(),task('first-terminal','completed')).state;s=reduceGadget(s,task('second-terminal','failed')).state;
 assert.equal(s.delivery,'unknown');assert.equal(s.tasks[0].conflict,true);assert.deepEqual(s.tasks[0].terminalStatuses,['completed','failed']);
 s=reduceGadget(s,ack('late-ack')).state;s=reduceGadget(s,{sourceId:'quiet',kind:'quiet',data:{reason:'three seconds'}}).state;
 assert.equal(s.delivery,'unknown');checkSafety(s);
});

test('known provider tasks may correlate parentless updates but cannot override contradictory message ancestry',()=>{
 let s=reduceGadget(delivered(),task('task-bound','running')).state;
 const parentless=event('parentless','message.assistant',{id:'task-reply',task_id:'provider-task-synthetic',text:'Task-correlated public text'});
 s=reduceGadget(s,parentless).state;assert.equal(s.messages[0].text,'Task-correlated public text');assert.equal(s.messages[0].parentMessageId,null);
 s=reduceGadget(s,reply('parent-contradiction',{id:'task-reply',parent_message_id:'foreign-parent',task_id:'provider-task-synthetic',text:'bad'})).state;
 assert.equal(s.messages[0].text,'Task-correlated public text');assert.equal(s.quarantine.at(-1)?.reason,'conflicting parent');
 s=reduceGadget(s,event('task-only-update','task.status',{task_id:'provider-task-synthetic',status:'completed'})).state;
 assert.equal(s.tasks[0].status,'completed');checkSafety(s);
});

test('disconnect before ACK is sticky unknown: late ACK, text, completion and quiet never authorize replay',()=>{
 let s=createGadgetState(binding);const lost=reduceGadget(s,{sourceId:'disconnect',kind:'disconnect',data:{reason:'lost connection'}});s=lost.state;
 assert.equal(s.delivery,'unknown');assert(lost.events.every(e=>e.data.automaticResend===false));
 s=reduceGadget(s,reply('late-preack-text')).state;s=reduceGadget(s,ack()).state;s=reduceGadget(s,task('late-done','completed')).state;
 const quiet=reduceGadget(s,{sourceId:'quiet-after',kind:'quiet',data:{}});assert.equal(quiet.state.delivery,'unknown');assert.equal(quiet.events[0].kind,'gadget.quiet');assert.equal(quiet.events[0].data.accepted,false);checkSafety(quiet.state);
});

test('quiet alone is presentation evidence and never creates delivery or task completion',()=>{
 const out=reduceGadget(createGadgetState(binding),{sourceId:'quiet-no-ack',kind:'quiet',data:{reason:'presentation idle'}});
 assert.equal(out.state.delivery,'awaiting-ack');assert.equal(out.state.tasks.length,0);assert.equal(out.state.ack,null);assert.equal(out.events[0].data.quiescent,true);checkSafety(out.state);
});

test('SDK TaskFinished helper, absent raw status, conflicting aliases and unknown raw fields are rejected',()=>{
 const s=delivered(),before=structuredClone(s);
 for(const data of [{type:'TaskFinished',task_id:'provider-task-synthetic'}, {type:'event',event:'TaskFinished',payload:{}}, {type:'event',event:'task.status',payload:{session_id:binding.sidechatId,task_id:'task'}}, {type:'event',event:'agent.status',session_id:'different',payload:{session_id:binding.sidechatId}}, {type:'event',event:'agent.status',payload:{session_id:binding.sidechatId,private_reasoning:'not allowed'}}])assert.throws(()=>reduceGadget(s,{sourceId:'invalid',kind:'event',data}),/gadget:/);
 assert.deepEqual(s,before);
});

test('public text and activity strip controls and redact known credentials without retaining raw payloads',()=>{
 const secret='mgst_syntheticToken0123456789',text='\x1b[31mPublic\x1b[0m\x00 '+secret+' token=synthetic-secret';
 let s=reduceGadget(delivered(),reply('redacted',{text,display_text:'Bearer synthetic-access-secret'})).state;
 const out=reduceGadget(s,event('redacted-activity','agent.status',{message_id:'user-synthetic',activity_text:'\x1b[32mPlanning\x1b[0m api_key=synthetic-api-secret'}));s=out.state;
 const encoded=JSON.stringify({s,events:out.events});for(const forbidden of [secret,'synthetic-secret','synthetic-access-secret','synthetic-api-secret','\\u001b','\\u0000'])assert(!encoded.includes(forbidden),forbidden);
 assert(s.messages[0].text.includes('[REDACTED]'));assert(out.events[0].data.publicActivityOnly);checkSafety(s);
});

test('non-assistant roles, self parents and acknowledged user-message overwrite are inert',()=>{
 let s=delivered();
 for(const e of [reply('system',{role:'system'}),reply('cycle',{id:'cycle',parent_message_id:'cycle'}),reply('user-overwrite',{id:'user-synthetic',parent_message_id:'unrelated'})])s=reduceGadget(s,e).state;
 assert.equal(s.messages.length,0);assert.equal(s.tasks.length,0);checkSafety(s);
});

test('final text cannot be reopened by late deltas or replaced by a conflicting final',()=>{
 let s=reduceGadget(delivered(),reply('final')).state;
 s=reduceGadget(s,event('late-delta','delta.text_append',{id:'assistant-synthetic',text:' changed'})).state;assert.equal(s.messages[0].text,'synthetic reply');
 s=reduceGadget(s,reply('conflicting-final',{text:'different final'})).state;assert.equal(s.messages[0].text,'synthetic reply');assert.equal(s.delivery,'unknown');checkSafety(s);
});

test('raw input, pending, source identity and text storage limits fail closed without evicting dedup identities',()=>{
 const fresh=createGadgetState(binding);assert.throws(()=>reduceGadget(fresh,reply('oversized',{text:'x'.repeat(GADGET_LIMITS.inputBytes)})),/limit/);
 assert.throws(()=>reduceGadget(fresh,{sourceId:'non-json',kind:'quiet',data:{reason:()=>null}}),/non-JSON/);
 let s=fresh;for(let i=0;i<GADGET_LIMITS.pending+1;i++)s=reduceGadget(s,reply('pending-'+i,{id:'pending-message-'+i})).state;
 assert.equal(s.pending.length,GADGET_LIMITS.pending);assert.equal(s.delivery,'unknown');checkSafety(s);
 s=delivered();for(let i=0;i<GADGET_LIMITS.seen-1;i++)s=reduceGadget(s,{sourceId:'quiet-'+i,kind:'quiet',data:{}}).state;
 assert.equal(s.seen.length,GADGET_LIMITS.seen);assert.equal(reduceGadget(s,ack()).events.length,0);assert.throws(()=>reduceGadget(s,{sourceId:'one-too-many',kind:'quiet',data:{}}),/capacity exhausted/);
 s=reduceGadget(delivered(),event('start-text','delta.message_start',{id:'long-text',parent_message_id:'user-synthetic'})).state;
 for(let i=0;i<3;i++)s=reduceGadget(s,event('text-'+i,'delta.text_append',{id:'long-text',text:'x'.repeat(8192)})).state;
 assert.equal(Buffer.byteLength(s.messages[0].text),GADGET_LIMITS.textBytes);assert.equal(s.delivery,'unknown');checkSafety(s);
});

test('bounded pre-ACK replay stays within the persistent bridge event and payload limits',()=>{
 let s=createGadgetState(binding);for(let i=0;i<GADGET_LIMITS.pending;i++)s=reduceGadget(s,reply('before-'+i,{id:'message-'+i,text:'x'.repeat(4000)})).state;
 const out=reduceGadget(s,ack());assert(out.events.length<=32);assert(Buffer.byteLength(JSON.stringify(out.events))<256*1024);assert(out.events.every(e=>Buffer.byteLength(JSON.stringify(e.data))<32*1024));assert(out.state.messages.length===GADGET_LIMITS.pending);checkSafety(out.state);
});


test('assistant display_text_ready false allows later text; absent readiness follows raw SDK final semantics',()=>{
 let s=reduceGadget(delivered(),reply('not-ready',{text:'initial',display_text_ready:false})).state;assert.equal(s.messages[0].final,false);
 s=reduceGadget(s,event('later-text','delta.text_append',{id:'assistant-synthetic',text:' more'})).state;assert.equal(s.messages[0].text,'initial more');
 s=reduceGadget(s,reply('ready',{text:'initial more',display_text_ready:true})).state;assert.equal(s.messages[0].final,true);checkSafety(s);
 const absent=reply('no-readiness');delete (absent.data as any).payload.display_text_ready;
 assert.equal(reduceGadget(delivered(),absent).state.messages[0].final,true);
 s=reduceGadget(s,reply('late-readiness',{text:'initial more',display_text_ready:false})).state;assert.equal(s.messages[0].final,true);assert.equal(s.messages[0].displayTextReady,true);
});
