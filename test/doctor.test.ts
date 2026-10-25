import { test } from 'node:test';import assert from 'node:assert/strict';
import { providerHealth } from '../src/doctor.ts';
import type { ProbeResult } from '../src/doctor.ts';
const result=(stdout:string,exitCode:number|null=0,stderr=''):ProbeResult=>({stdout,stderr,exitCode});
test('doctor checks only version/status and returns sanitized categories',async()=>{
 const calls:string[][]=[];const h=await providerHealth('claude',async(cmd,args)=>{calls.push([cmd,...args]);return args[0]==='--version'?result('2.1.292 (Claude Code)'):result('{"loggedIn":true,"authMethod":"oauth","email":"secret@example.org","access_token":"secret"}');});
 assert.deepEqual(calls,[['claude','--version'],['claude','auth','status','--json']]);assert.equal(h.auth,'logged-in');assert.equal(h.version,'2.1.292');assert.equal(h.quota,'unknown');assert(!JSON.stringify(h).includes('secret'));
});
test('missing CLI and failed auth are unknown',async()=>{
 const a=await providerHealth('codex',async()=>result('',null));assert.equal(a.available,false);assert.equal(a.auth,'unknown');
 const b=await providerHealth('codex',async(_c,a)=>a[0]==='--version'?result('codex-cli 0.164.0'):result('Logged in using ChatGPT',null));assert.equal(b.auth,'unknown');
});
test('not logged in is never mistaken for logged in',async()=>{const h=await providerHealth('codex',async(_c,a)=>a[0]==='--version'?result('codex-cli 0.164.0'):result('Not logged in',1));assert.equal(h.auth,'not-logged-in');});
