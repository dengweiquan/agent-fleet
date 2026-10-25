import {test} from 'node:test';import assert from 'node:assert/strict';import {spawn} from 'node:child_process';import {mkdtempSync,readFileSync,rmSync,symlinkSync,writeFileSync,existsSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';import {fileURLToPath} from 'node:url';
import {parsePlanProposal,parseReviewVerdict} from '../src/structured.ts';
const fixture=fileURLToPath(new URL('./fake-cli.ts',import.meta.url));
async function run(mode:string,request:unknown,cwd:string){return new Promise<{code:number|null;events:any[]}>(resolve=>{const child=spawn(process.execPath,['--experimental-strip-types',fixture,mode,'test-session'],{cwd,stdio:['pipe','pipe','ignore']});let text='';child.stdout.on('data',c=>text+=c);child.on('close',code=>resolve({code,events:text.trim().split('\n').filter(Boolean).map(line=>JSON.parse(line))}));child.stdin.end(JSON.stringify(request));});}
function response(events:any[]):string{return events.find(e=>e.type==='item.completed')?.item.text;}
test('planner fixture emits valid parallel DAG and rejects cyclic variant',async()=>{const dir=mkdtempSync(join(tmpdir(),'agent-fleet-structured-'));try{
 const r=await run('planner',{request:'Build fixture'},dir);assert.equal(r.code,0);const p=parsePlanProposal(response(r.events));assert.equal(p.goal,'Build fixture');assert.deepEqual(p.tasks.map(t=>t.dependencies),[[],[],['left','right']]);assert(p.tasks.every(t=>t.acceptance.length));
 const invalid=await run('planner-invalid',{message:'fixture'},dir);assert.throws(()=>parsePlanProposal(response(invalid.events)));
 }finally{rmSync(dir,{recursive:true,force:true});}});
test('review fixtures bind approve/reject and invalid remains unaccepted',async()=>{const dir=mkdtempSync(join(tmpdir(),'agent-fleet-structured-'));try{
 const binding={workerRunId:'run-1',commitSha:'c'.repeat(40),reviewerId:'reviewer-1'};
 for(const verdict of ['approve','reject'])assert.equal(parseReviewVerdict(response((await run('review-'+verdict,{binding},dir)).events),binding).verdict,verdict);
 const invalid=await run('review-invalid',{binding},dir);assert.throws(()=>parseReviewVerdict(response(invalid.events),binding));
 }finally{rmSync(dir,{recursive:true,force:true});}});
test('delay-write fixtures overlap, emit start/finish and only write safe distinct files',async()=>{const dir=mkdtempSync(join(tmpdir(),'agent-fleet-structured-'));try{
 const runs=await Promise.all(['left','right'].map(name=>run('delay-write',{task:{prompt:JSON.stringify({filename:name+'.txt',content:name,delayMs:200})}},dir)));
 assert(runs.every(r=>r.code===0));const starts=runs.map(r=>r.events.find(e=>e.type==='fixture.started').time),finishes=runs.map(r=>r.events.find(e=>e.type==='fixture.finished').time);assert(Math.max(...starts)<Math.min(...finishes));assert.equal(readFileSync(join(dir,'left.txt'),'utf8'),'left');assert.equal(readFileSync(join(dir,'right.txt'),'utf8'),'right');
 for(const filename of ['../escaped.txt','/tmp/escaped.txt','nested/file.txt'])assert.equal((await run('delay-write',{filename,content:'bad',delayMs:0},dir)).code,7);
 assert.equal((await run('delay-write',{filename:'left.txt',content:'overwrite',delayMs:0},dir)).code,7);assert.equal(readFileSync(join(dir,'left.txt'),'utf8'),'left');
 const target=join(dir,'target.txt');writeFileSync(target,'keep');symlinkSync(target,join(dir,'link.txt'));assert.equal((await run('delay-write',{filename:'link.txt',content:'bad',delayMs:0},dir)).code,7);assert.equal(readFileSync(target,'utf8'),'keep');assert(!existsSync(join(dir,'escaped.txt')));
 }finally{rmSync(dir,{recursive:true,force:true});}});
