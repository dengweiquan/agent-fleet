import {mkdtempSync,mkdirSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import assert from 'node:assert/strict';

// Entire demo is local, uses no models, credentials, ports, or user repositories.
const dir=mkdtempSync(join(tmpdir(),'agent-fleet-demo-')),root=join(dir,'project'),index=join(dir,'history');mkdirSync(root);
const cli=fileURLToPath(new URL('../src/cli.ts',import.meta.url));
const env={...process.env,AGENT_FLEET_HOME:index};
function git(...args:string[]){const r=spawnSync('git',args,{cwd:root,env,encoding:'utf8'});if(r.status!==0)throw new Error(r.stderr);}
function call(args:string[],input?:unknown):any{const r=spawnSync(process.execPath,['--experimental-strip-types',cli,'--json',...args],{cwd:root,env,input:input===undefined?undefined:JSON.stringify(input),encoding:'utf8',timeout:15000});if(r.status!==0)throw new Error(r.stderr);return JSON.parse(r.stdout).data;}
function file(name:string,value:unknown){const p=join(dir,name);writeFileSync(p,JSON.stringify(value,null,2));return p;}
git('init');writeFileSync(join(root,'README.md'),'AgentFleet synthetic fixture only.\n');git('add','.');git('-c','user.name=AgentFleet Demo','-c','user.email=demo@localhost','-c','commit.gpgsign=false','commit','-m','Synthetic fixture base');
const session=call(['init','Muse pull/ACK local demonstration']);
const cfg=call(['config']);cfg.instances=cfg.instances.map((i:any)=>i.role==='manager'?{...i,provider:'fake',model:'success'}:i);cfg.endpoints.push({id:'muse-demo',kind:'external-agent',target:'not-connected'});call(['config',file('config.json',cfg)]);
call(['plan',file('plan.json',{version:1,goal:'Synthetic local worker then reverse Muse protocol',tasks:[{id:'local-artifact',title:'Create a fixture',prompt:'Write artifact.txt',dependencies:[],capabilities:['code'],acceptance:[[process.execPath,'-e',"if(require('node:fs').readFileSync('artifact.txt','utf8')!=='AgentFleet fixture artifact\\n')process.exit(1)"]]},{id:'muse-protocol',title:'Protocol-only external task',prompt:'Synthetic demo: confirm receipt only. Do not execute a model or business task.',dependencies:['local-artifact'],capabilities:['code'],acceptance:[[process.execPath,'-e','process.exit(0)']]}]})]);
call(['run','local-artifact','fake-worker']);let run:any;const deadline=Date.now()+10000;
while(Date.now()<deadline){run=call(['status']).runs.find((r:any)=>r.taskId==='local-artifact');if(run?.status==='exited')break;await new Promise(r=>setTimeout(r,80));}
assert.equal(run?.status,'exited');await new Promise(r=>setTimeout(r,100));
const verified=call(['verify',run.id]);call(['review',run.id,'fake-reviewer',file('review.json',{runId:run.id,commitSha:verified.commitSha,reviewerId:'fake-reviewer',verdict:'approve',notes:'Synthetic demo attestation after independent fixture command; no live reviewer model invoked.'})]);
const offer=call(['remote','offer','muse-protocol','muse-demo','synthetic-repo']);
const capabilities={protocolVersion:1,provider:'external-agent',capabilities:['code'],auth:'available',quota:'unknown'};
const request=(request:unknown)=>call(['remote','request','muse-demo'],request);
const pulled=request({version:1,op:'pull',capabilities});assert.equal(pulled[0].messageId,offer.messageId);
const claimId=randomUUID(),claimed=request({version:1,op:'claim',messageId:offer.messageId,claimId,capabilities});
assert.equal(claimed.claimId,claimId);
const ack=request({version:1,op:'ack',messageId:offer.messageId,claimId});assert.equal(ack.meaning,'durably-received-only');
const sourceEventId=randomUUID(),report={version:1,op:'report',messageId:offer.messageId,claimId,sourceEventId,kind:'result',data:{exitCode:0,providerSessionId:'synthetic-no-model'}};
const first=request(report),duplicate=request(report);assert.equal(first.eventId,duplicate.eventId);assert.equal(first.state,'execution-completed');
const resumed=call(['resume',session.sessionId]);assert.equal(resumed.remote[0].state,'execution-completed');assert.equal(resumed.runs[0].status,'accepted');
console.log(JSON.stringify({ok:true,realSshTested:false,modelsInvoked:false,sessionId:session.sessionId,project:root,index,localAcceptedRun:run.id,remoteMessageId:offer.messageId,remoteState:first.state,remoteAccepted:false,duplicateReportDeduplicated:true,resumeCommand:`AGENT_FLEET_HOME='${index}' agent-fleet resume ${session.sessionId}`,next:'Run remote ping from Muse using an existing user-confirmed SSH alias.'},null,2));
