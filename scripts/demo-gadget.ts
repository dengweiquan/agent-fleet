import {mkdtempSync,mkdirSync,writeFileSync,realpathSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import assert from 'node:assert/strict';
import {Engine} from '../src/engine.ts';
import {git} from '../src/git.ts';
import {ResourcePool} from '../src/resource-pool.ts';
import {pooledRemoteCommand} from '../src/pooled-remote-cli.ts';
import {recordGadgetEvidence,getGadgetEvidence} from '../src/gadget-evidence.ts';
import type {GadgetBinding} from '../src/gadget-events.ts';

// Original synthetic fixtures only. No SDK import, transport, model or business repository.
const directory=realpathSync(mkdtempSync(join(tmpdir(),'agent-fleet-gadget-demo-'))),project=join(directory,'project');
mkdirSync(project);git(project,'init');writeFileSync(join(project,'README.md'),'Synthetic offline Gadget event fixture.\n');writeFileSync(join(project,'.gitignore'),'.agent-fleet/\n');git(project,'add','.');git(project,'-c','user.name=Fixture','-c','user.email=fixture@localhost','-c','commit.gpgsign=false','commit','-m','Offline fixture seed');
const previousHome=process.env.AGENT_FLEET_HOME;process.env.AGENT_FLEET_HOME=join(directory,'history');
const engine=Engine.init(project,'Offline Gadget evidence demo');
try{
  engine.plan({version:1,goal:'Observe fixtures without executing work',tasks:[{id:'observe-only',title:'Offline fixture',prompt:'No execution authorized',dependencies:[],capabilities:['diagnostic'],acceptance:[[process.execPath,'-e','process.exit(0)']]}]});
  const poolPath=join(directory,'pool.sqlite'),fleetPath=join(directory,'fleet.json');
  const pool=new ResourcePool(poolPath);try{pool.upsertResource({id:'offline-gadget',provider:'external-agent',model:'unverified',endpointId:'offline-endpoint',endpointKind:'external-agent',accountDomain:'synthetic-account',roles:['worker'],capabilities:['diagnostic'],capacity:1});}finally{pool.close();}
  writeFileSync(fleetPath,JSON.stringify({version:1,projects:[{root:project,sessionId:engine.sessionId,remoteClaimsAuthorized:false}]}));
  const offer:any=await pooledRemoteCommand(['offer',fleetPath,engine.sessionId,'observe-only','offline-endpoint','synthetic-repository'],{poolPath,observer:false});
  const binding:GadgetBinding={projectId:project,sessionId:engine.sessionId,taskId:offer.taskId,runId:offer.runId,attemptId:offer.attemptId,endpointId:offer.endpointId,requestId:randomUUID(),sidechatId:randomUUID()};
  const ack={sourceId:'fixture-ack',kind:'ack' as const,data:{result:{session_id:binding.sidechatId,is_thread:true,message_id:'user-fixture'}}};
  recordGadgetEvidence(engine,binding,ack);
  recordGadgetEvidence(engine,binding,{sourceId:'fixture-reply',kind:'event',data:{type:'event',event:'message.assistant',payload:{session_id:binding.sidechatId,message_id:'reply-fixture',reply_to_message_id:'user-fixture',role:'assistant',display_text:'Synthetic nonce echoed.',display_text_ready:true}}});
  recordGadgetEvidence(engine,binding,{sourceId:'fixture-failed',kind:'event',data:{type:'event',event:'task.status',payload:{session_id:binding.sidechatId,task_id:'provider-task-fixture',reply_to_message_id:'user-fixture',status:'failed'}}});
  recordGadgetEvidence(engine,binding,ack); // Stable producer identity replay; no second evidence event.
  recordGadgetEvidence(engine,binding,{sourceId:'fixture-quiet',kind:'quiet',data:{reason:'three seconds of quiet is not task success'}});
  const evidence=getGadgetEvidence(engine,binding.requestId),queue=engine.remoteStatus();
  assert.equal(evidence?.state.accepted,false);assert.equal(evidence?.state.transport,'disabled-unpaired');
  assert.equal(evidence?.state.tasks[0].status,'failed');assert.equal(queue.length,1);
  assert.equal(queue[0].state,'offered');assert.equal(queue[0].acknowledged,false);assert.equal(engine.store.listRuns().length,0);
  const report={directory,transport:'disabled-unpaired',modelInvoked:false,sdkExecuted:false,messagesSent:0,evidence,queue,taskAccepted:false};
  const reportPath=join(directory,'report.json');writeFileSync(reportPath,JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({...report,reportPath},null,2));
}finally{engine.close();if(previousHome===undefined)delete process.env.AGENT_FLEET_HOME;else process.env.AGENT_FLEET_HOME=previousHome;}
