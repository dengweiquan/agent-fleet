import { test } from 'node:test';import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { buildInvocation,createParser,adapterCapabilities } from '../src/adapters.ts';
import type { Instance } from '../src/types.ts';
for(const provider of ['claude','codex'] as const)test(`${provider} uses safe explicit invocation and distinct sessions`,()=>{
 const instance:Instance={id:'worker',provider,role:'worker',model:'explicit-model',capabilities:['code']};
 const a=buildInvocation(instance,{sessionId:randomUUID(),prompt:'hello',cwd:'/tmp'});const b=buildInvocation(instance,{sessionId:randomUUID(),prompt:'hello',cwd:'/tmp'});
 assert(!a.args.some(x=>/dangerously|--last|approve-for-me|acceptEdits|bypassPermissions/.test(x)));assert.equal(a.input,'hello');
 if(provider==='claude')assert.notDeepEqual(a.args,b.args);else assert(a.args.includes('--ephemeral'));
 assert.equal(adapterCapabilities(provider).quota,'unknown');
});
test('parsers preserve run session identity and never accept provider completion',()=>{
 const p=createParser('codex');p.parse('{"type":"thread.started","thread_id":"run-A"}');assert.equal(p.sessionId,'run-A');
 const r=p.parse('{"type":"turn.completed","usage":{"input_tokens":2}}');assert.equal((r?.data as any).accepted,false);assert.equal(p.state,'completed');assert.equal(createParser('codex').sessionId,null);
});
test('Claude messages expose labeled public blocks and result is not acceptance',()=>{
 const p=createParser('claude');p.parse('{"type":"system","subtype":"init","session_id":"C"}');assert.equal(p.sessionId,'C');
 const e=p.parse(JSON.stringify({type:'assistant',message:{content:[{type:'thinking',thinking:'Public summary'},{type:'tool_use',input:{secret:'never expose'}}]}}));assert.match(JSON.stringify(e),/not complete/);assert(!JSON.stringify(e).includes('never expose'));
 assert.equal((p.parse('{"type":"result","is_error":false}')?.data as any).accepted,false);
});
test('malformed and unknown events are conservative',()=>{const p=createParser('codex');assert.equal(p.parse('broken')?.kind,'protocol_error');assert.deepEqual(p.parse('{"type":"future","secret":"no"}'),{kind:'provider_event',data:{type:'future'}});});
test('protocol and provider errors remain sticky after later completion',()=>{
 for(const provider of ['claude','codex'] as const){
 const p=createParser(provider);p.parse('broken');p.parse(provider==='claude'?'{"type":"result","is_error":false}':'{"type":"turn.completed"}');assert.equal(p.hasError,true);assert.equal(p.state,'protocol_error');
 const q=createParser(provider);q.parse(provider==='claude'?'{"type":"result","is_error":true}':'{"type":"turn.failed"}');q.parse(provider==='claude'?'{"type":"result","is_error":false}':'{"type":"turn.completed"}');assert.equal(q.hasError,true);assert.equal(q.state,'provider_error');
 }
});
test('placeholder models cannot trigger external calls',()=>{for(const model of ['configured-default','user-selection-required'])assert.throws(()=>buildInvocation({id:'w',provider:'codex',role:'worker',model,capabilities:['code']},{sessionId:randomUUID(),prompt:'test',cwd:'/tmp'}),/Explicit model/);});
