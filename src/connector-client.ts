import {readFileSync,existsSync} from 'node:fs';
import {resolve,isAbsolute} from 'node:path';
import {hostname,platform} from 'node:os';
import {execFileSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {Connector,ConnectorJournal,SshStdioTransport,sshConnectionKey} from './connector.ts';
import type {ConnectorTransport,ConnectorStatus} from './connector.ts';
import {processStart} from './process.ts';

export interface ConnectorConfig {version:1;journalPath:string;sshAlias:string;connectionApproved:true;server:{node:string;cli:string;pool:string;fleet:string;history?:string;endpointId:string;resourceId:string}}
export function validateConnectorConfig(value:unknown):ConnectorConfig{
  const c=value as ConnectorConfig;if(!c||typeof c!=='object'||Array.isArray(c)||Object.keys(c).some(k=>!['version','journalPath','sshAlias','connectionApproved','server'].includes(k))||c.version!==1||c.connectionApproved!==true||typeof c.journalPath!=='string'||!isAbsolute(c.journalPath)||typeof c.sshAlias!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/.test(c.sshAlias))throw new Error('Explicit approved existing SSH alias and absolute persistent journal path required');
  const s=c.server;if(!s||typeof s!=='object'||Array.isArray(s)||['node','cli','pool','fleet','endpointId','resourceId'].some(k=>!Object.hasOwn(s,k))||Object.keys(s).some(k=>!['node','cli','pool','fleet','history','endpointId','resourceId'].includes(k)))throw new Error('Fixed AgentFleet server scope required');
  if(s.history!==undefined&&(typeof s.history!=='string'||!isAbsolute(s.history)||/[\x00-\x1f]/.test(s.history)))throw new Error('Server history must be an explicit absolute path');
  for(const field of ['node','cli','pool','fleet'] as const)if(typeof s[field]!=='string'||!isAbsolute(s[field])||/[\x00-\x1f]/.test(s[field]))throw new Error('Server paths must be explicit absolute paths on the coordinator');
  for(const field of ['endpointId','resourceId'] as const)if(typeof s[field]!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/.test(s[field]))throw new Error('Fixed endpoint and resource IDs required');return c;
}
export function serverCommand(config:ConnectorConfig){const s=config.server;return [...(s.history?['/usr/bin/env',`AGENT_FLEET_HOME=${s.history}`]:[]),s.node,'--experimental-strip-types',s.cli,'--pool',s.pool,'--observe','fleet-remote','serve',s.fleet,s.endpointId,s.resourceId];}
function bootIdentity(){if(platform()==='linux'&&existsSync('/proc/sys/kernel/random/boot_id'))return readFileSync('/proc/sys/kernel/random/boot_id','utf8').trim();if(platform()==='darwin')return execFileSync('/usr/sbin/sysctl',['-n','kern.boottime'],{encoding:'utf8'}).trim();throw new Error('OS boot identity unavailable; deployment supervisor contract required');}
export function selfcheck(){const [major,minor]=process.versions.node.split('.').map(Number);if(major<22||(major===22&&minor<18))throw new Error('Node >=22.18 required; this command never installs a runtime');return {ok:true,node:process.versions.node,sqliteBuiltIn:true,dependenciesInstalled:false,connectionStarted:false,modelWakeImplemented:false,mode:'read-only-observer-primary-channel'};}

/** A finite foreground primary channel. Intentionally no Muse model wake/task handler. */
export async function runObserver(input:ConnectorConfig,seconds=60,options:{transport?:ConnectorTransport;signals?:boolean}={}){
  selfcheck();const config=validateConnectorConfig(input);if(!Number.isInteger(seconds)||seconds<1||seconds>3600)throw new Error('Foreground run duration must be 1..3600 seconds');
  const journal=new ConnectorJournal(config.journalPath),command=serverCommand(config);
  const transport=options.transport??new SshStdioTransport({alias:config.sshAlias,command,approved:true,timeoutMs:15000});
  const owner={ownerId:randomUUID(),pid:process.pid,processStart:processStart(process.pid)??'',bootId:hostname()+':'+bootIdentity()};if(!owner.processStart){journal.close();throw new Error('Cannot establish connector process identity');}
  const connectionKey=sshConnectionKey(config.sshAlias,command);
  async function reconcile(channel:ConnectorTransport,generation:number,snapshot:ConnectorStatus){
    const hello=await channel.request({id:'hello-'+randomUUID(),generation,payload:{version:1,op:'hello'}});
    if(!hello.ok||(hello.data as any)?.product!=='AgentFleet'||(hello.data as any)?.protocolVersion!==1||(hello.data as any)?.endpointId!==config.server.endpointId)throw new Error('AgentFleet hello identity mismatch');
    let cursor=snapshot.serverCursor;
    for(let page=0;page<1000;page++){
      let limit=50,response;
      while(true){
        response=await channel.request({id:'replay-'+randomUUID(),generation,payload:{version:1,op:'events',after:cursor,limit}});
        if(response.ok)break;
        if(limit>1&&String(response.error).includes('Response exceeds 64KiB')){limit=Math.max(1,Math.floor(limit/2));continue;}
        throw new Error('Event replay failed');
      }
      if(!Array.isArray(response.data))throw new Error('Invalid event replay page');
      cursor=journal.recordServerEvents(generation,response.data);
      if(response.data.length<limit)return {helloConfirmed:true as const,replayCursor:cursor};
    }
    throw new Error('Replay exceeded bounded synchronization window; inspect event backlog');
  }
  const connector=new Connector(journal,transport,owner,{connectionKey,reconcileConnection:reconcile});
  let stopping=false;const stop=()=>{stopping=true;journal.setDisabled(true);void connector.close().catch(()=>{});};if(options.signals){process.once('SIGINT',stop);process.once('SIGTERM',stop);}
  const deadline=Date.now()+seconds*1000;let received=0,result:any={state:'idle'};
  try{
    result=await connector.verifyConnection();
    if(result.state!=='idle')return {...result,received:0,modelInvoked:false,museWakeInterface:'unverified',connectionKey};
    while(!stopping&&Date.now()<deadline&&!journal.isDisabled()){
      // step performs a validated hello/replay barrier before journaled observations.
      const id='events-'+randomUUID(),cursor=journal.status().serverCursor;
      // One bounded event always fits a reply; replay adaptively batches historical pages.
      journal.enqueue(id,{version:1,op:'events',after:cursor,limit:1,waitMs:Math.min(5000,Math.max(0,deadline-Date.now()))},{readOnly:true});
      result=await connector.step();if(result.state!=='replied')break;
      const snapshot=journal.status(),message=snapshot.inbox.find(i=>i.id===result.id);
      if(!message?.reply.ok||!Array.isArray(message.reply.data))throw new Error('Primary event observation returned an invalid response');
      journal.recordServerEvents(snapshot.generation,message.reply.data);received+=message.reply.data.length;
    }
    return {state:journal.isDisabled()?'disabled':result.state,received,serverCursor:journal.status().serverCursor,modelInvoked:false,museWakeInterface:'unverified',connectionKey,watchdogIsFallback:true};
  }finally{
    if(options.signals){process.removeListener('SIGINT',stop);process.removeListener('SIGTERM',stop);}
    try{await connector.close();if(journal.isOwner(owner))journal.releaseOwner(owner);}finally{journal.close();}
  }
}

async function main(){const [command,file,arg]=process.argv.slice(2);if(command==='selfcheck'){console.log(JSON.stringify(selfcheck()));return;}if(!file)throw new Error('Use selfcheck | run CONFIG [SECONDS] | status/disable/enable CONFIG');const config=validateConnectorConfig(JSON.parse(readFileSync(resolve(file),'utf8')));
  if(command==='run'){console.log(JSON.stringify(await runObserver(config,Number(arg??60),{signals:true})));return;}
  if(!['status','disable','enable'].includes(command))throw new Error('Unsupported connector command');
  const journal=new ConnectorJournal(config.journalPath,{readOnly:command==='status'});try{if(command!=='status')journal.setDisabled(command==='disable');const state=journal.status();console.log(JSON.stringify({disabled:state.disabled,owner:state.owner,generation:state.generation,serverCursor:state.serverCursor,unknown:state.outbox.filter(q=>q.state==='unknown').length,watchdog:state.watchdog,modelWakeImplemented:false}));}finally{journal.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)main().catch(error=>{console.error(JSON.stringify({ok:false,error:String(error)}));process.exitCode=1;});
