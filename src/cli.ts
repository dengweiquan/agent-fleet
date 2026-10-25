import {readFileSync,existsSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createInterface} from 'node:readline/promises';
import {stdin,stdout} from 'node:process';
import {Engine} from './engine.ts';
import {schedule,schedulePreview} from './scheduler.ts';
import {runIndependentReview} from './reviewer.ts';
import {doctor} from './doctor.ts';
import {listSessions,findSession,setCurrentSession} from './sessions.ts';
import {adapterCapabilities} from './adapters.ts';
import {redact} from './process.ts';
import {remoteCommand} from './remote-cli.ts';
import {remotePing} from './remote.ts';
import {defaultPoolPath,poolCommand,fleetCommand} from './fleet-cli.ts';
import {repository} from './git.ts';
import {pooledRemoteCommand} from './pooled-remote-cli.ts';
import {serveConnector} from './connector-server.ts';
import {controlCommand} from './control-cli.ts';

const args=process.argv.slice(2), json=args.includes('--json'), observer=args.includes('--observe');
const remove=(name:string)=>{const i=args.indexOf(name);if(i<0)return undefined;const v=args[i+1];args.splice(i,2);return v;};
const sessionOption=remove('--session');
const projectOption=remove('--project'),poolPath=remove('--pool')??defaultPoolPath();
const perRunBudget=remove('--budget-per-run-usd'),dryRun=args.includes('--dry-run');
const budget=Number(perRunBudget??remove('--budget-usd')??0),approved=args.includes('--allow-live');
for(const flag of ['--json','--observe','--allow-live','--dry-run']){const i=args.indexOf(flag);if(i>=0)args.splice(i,1);}
let cwd=process.cwd(), current=sessionOption;
const live=approved?{approved:true,budgetUsd:budget}:undefined;
function out(data:unknown){console.log(JSON.stringify(json?{ok:true,data}:data,null,json?undefined:2));}
function load(path:string){if(!path)throw new Error('JSON file required');return JSON.parse(readFileSync(path,'utf8'));}
const help=`AgentFleet — private orchestration CLI (M1)
  init [title]                  New durable session for current Git project
  sessions list                 List sessions across projects
  sessions list --project PATH   Filter history by canonical project
  pool configure <json>         Register resources and shared capacity domains
  pool status | events [cursor] Inspect resource leases and shared audit cursor
  control <scope.json> <action> Scoped local JSON control; submit then explicit apply
  fleet-remote offer/list/events/request  Reverse runner uses the same shared pool
  fleet schedule <fleet.json>   Dispatch explicit projects through shared pool
  fleet propose <fleet.json> <session> <text>  Lease configured manager for a proposal
  --pool PATH --dry-run         Explicit shared SQLite registry / read-only preview
  resume [session-id]            Preview historical session, then choose continue
  status | events [cursor]      Read current session; --observe forbids mutation
  config [json-file]            Inspect or replace role/provider/endpoint config
  plan [json-file]              Inspect or validate/import DAG and acceptance argv
  propose <text>                Ask manager for a validated durable Plan proposal
  proposal <manager-run>        Inspect proposal and hash before adopting
  plan accept <manager-run>     Adopt proposal; does not execute commands
  schedule [reviewer-id] [--dry-run]  Preview or execute concurrent worker → verify → reviewer → acceptance
  review-run <run> <reviewer>    Execute independent fixed-commit reviewer
  accept-review <run> <review>   Apply program gate to recorded reviewer output
  chat <text>                   Send ordinary conversation to manager
  run <task-id> [instance-id]    Start detached persistent coordinator
  retry <run-id>                 New attempt for terminal failed/rejected/stopped run
  verify <run-id>                Run acceptance in separate fixed-commit worktree
  review <run> <reviewer> <file>  Record explicit independent review evidence
  pause | continue | stop <run>  Scheduling/control (not observer permission)
  doctor | capabilities         Official local version/login status; quota unknown
  remote ping                   No-data, no-port reverse SSH connectivity probe
  --session <id> --json --observe
Live calls require --allow-live --budget-usd N and explicitly selected model.
Live schedule uses --budget-per-run-usd N for EACH worker/reviewer call, not a total cap.
Claude supports API cost cap; Codex has no enforceable money cap (timeout/output only).
No arguments starts interactive mode. Worker exit 0 never implies acceptance.`;

function detach(command:string,params:string[]) {
  const child=spawn(process.execPath,['--experimental-strip-types',fileURLToPath(import.meta.url),'--session',current!,...(live?['--allow-live',perRunBudget!==undefined?'--budget-per-run-usd':'--budget-usd',String(budget)]:[]),'_coordinator',command,...params],{cwd,detached:true,stdio:'ignore'});
  child.unref();return {launched:true,coordinatorPid:child.pid,sessionId:current,note:'Inspect status/events for durable run identity; launch is not completion'};
}
async function command(words:string[]) {
  const [cmd,...rest]=words;
  if(cmd==='control'){await controlCommand(rest,observer);return;}
  if(cmd==='fleet-remote'){if(rest[0]==='serve'){await serveConnector(rest.slice(1),{poolPath,observer});return;}out(await pooledRemoteCommand(rest,{poolPath,observer}));return;}
  if(cmd==='pool'){out(poolCommand(rest,poolPath,observer));return;}
  if(cmd==='fleet'||cmd==='_fleet'){
    if(cmd==='_fleet'&&observer)throw new Error('Observer cannot start a fleet coordinator');
    out(await fleetCommand(rest,{path:poolPath,observer,dryRun,execute:cmd==='_fleet',live,perRunBudgetExplicit:perRunBudget!==undefined,detach:params=>{
      const child=spawn(process.execPath,['--experimental-strip-types',fileURLToPath(import.meta.url),'--pool',poolPath,...(live?['--allow-live','--budget-per-run-usd',String(budget)]:[]),'_fleet',...params],{cwd,detached:true,stdio:'ignore'});child.unref();return {launched:true,coordinatorPid:child.pid,note:'Inspect pool status/events and project session histories; launch is not completion'};
    }}));return;
  }
  if(observer&&!['status','events','proposal','config','plan','doctor','capabilities','sessions','resume','help','--help','remote'].includes(cmd)&&!(cmd==='schedule'&&dryRun))throw new Error('Observer access cannot control execution');
  if(observer&&['config','plan'].includes(cmd)&&rest.length)throw new Error('Observer access cannot write configuration');
  if(cmd==='help'||cmd==='--help'){console.log(help);return;}
  if(cmd==='doctor'){out(await doctor());return;}
  if(cmd==='capabilities'){out(['claude','codex','fake'].map(p=>adapterCapabilities(p as any)));return;}
  if(cmd==='remote'&&rest[0]==='ping'){out({...remotePing(),ping:'pong',listener:false,credentialsCreated:false,applicationPortTested:false});return;}
  if(cmd==='sessions'){if(rest[0]!=='list')throw new Error('Use sessions list');out(listSessions(projectOption?repository(projectOption):undefined));return;}
  if(cmd==='resume') {
    const sessions=listSessions(projectOption?repository(projectOption):undefined);let id=rest[0];
    if(!id){out(sessions);if(!stdin.isTTY)return;const rl=createInterface({input:stdin,output:stdout});id=await rl.question('Session ID to preview (empty cancels): ');rl.close();if(!id)return;}
    const s=findSession(id);if(!s||s.availability!=='available'||!sessions.some(item=>item.id===s.id))throw new Error('Session unavailable in selected project scope');
    const e=new Engine(s.root,true,s.id);try{out(e.status());}finally{e.close();}
    if(observer||!stdin.isTTY)return;
    const rl=createInterface({input:stdin,output:stdout});const answer=await rl.question('Continue this orchestration session? [y/N] ');rl.close();
    if(answer.toLowerCase()!=='y')return;
    const writable=new Engine(s.root,false,s.id);try{writable.mutate();setCurrentSession(s);cwd=s.root;current=s.id;out({reconciled:writable.reconcile(),note:'No run was replayed. Use status/events before explicit actions.'});}finally{writable.close();}
    await interactive();return;
  }
  if(cmd==='init'){if(observer)throw new Error('Observer cannot initialize');const e=Engine.init(cwd,rest.join(' ')||'AgentFleet session');current=e.sessionId;out(e.status());e.close();return;}
  const readOnly=observer||(cmd==='schedule'&&dryRun)||['status','events','proposal'].includes(cmd)||(cmd==='config'||cmd==='plan')&&!rest.length;
  const e=new Engine(cwd,readOnly,current);current=e.sessionId;
  try {
    if(cmd==='status')out(e.status());
    else if(cmd==='remote')out(await remoteCommand(e,rest,observer));
    else if(cmd==='proposal')out(e.store.getMeta('proposal:'+rest[0]));
    else if(cmd==='events')out(e.store.events(Number(rest[0]??0),1000));
    else if(cmd==='config')out(rest[0]?await e.exclusive(async()=>e.config(load(rest[0]))):e.config());
    else if(cmd==='plan')out(rest[0]?await e.exclusive(async()=>rest[0]==='accept'?e.adoptProposal(rest[1]):e.plan(load(rest[0]))):e.store.getPlan());
    else if(cmd==='pause'){e.pause();out({paused:true});}
    else if(cmd==='continue'){out({reconciled:e.resume(),paused:false});}
    else if(cmd==='stop'){e.stop(rest[0]);out({stopRequested:rest[0]});}
    else if(cmd==='verify')out(await e.exclusive(()=>e.verify(rest[0])));
    else if(cmd==='review')out(await e.exclusive(async()=>e.review(rest[0],rest[1],load(rest[2]))));
    else if(cmd==='accept-review')out(await e.exclusive(async()=>e.acceptReview(rest[0],rest[1])));
    else if(cmd==='schedule'){
      const disclosure=schedulePreview(e,rest[0],budget);if(dryRun){out(disclosure);return;}
      if(disclosure.liveProviderCallsPossible&&perRunBudget===undefined)throw new Error('Live schedule requires --budget-per-run-usd: it is NOT a total budget. Inspect schedule --dry-run first; Codex has no enforceable monetary cap');
      for(const i of e.config()!.instances.filter(i=>i.role==='worker'||(i.role==='reviewer'&&(!rest[0]||i.id===rest[0])))){e.localEndpoint(i);if(i.provider!=='fake'&&(!live||budget<=0))throw new Error('Schedule requires explicit per-run authorization for every worker/reviewer');if(['user-selection-required','configured-default'].includes(i.model))throw new Error('Explicit model required');}
      out({...detach(cmd,rest),disclosure});
    }
    else if(cmd==='run'||cmd==='chat'||cmd==='retry'||cmd==='propose'||cmd==='review-run') {
      // Validate before detaching so a denied live call has an immediate reviewable error.
      const instance=cmd==='review-run'?e.instance(rest[1],'reviewer'):['chat','propose'].includes(cmd)?e.store.getConfig()!.instances.find(i=>i.role==='manager')!:cmd==='retry'?e.instance(e.store.getRun(rest[0])?.instanceId??'','worker'):rest[1]?e.instance(rest[1],'worker'):e.pick(e.task(rest[0]));
      e.localEndpoint(instance);
      if(instance.provider!=='fake'&&(!live||budget<=0))throw new Error(`Live ${instance.provider}/${instance.model} requires --allow-live --budget-usd; no external call made`);
      if(['user-selection-required','configured-default'].includes(instance.model))throw new Error('Select an explicit available model in configuration');
      out(detach(cmd,rest));
    } else if(cmd==='_coordinator') {
      await e.exclusive(async()=>{
        if(rest[0]==='schedule')return schedule(e,rest[1],live);
        if(rest[0]==='propose')return e.propose(rest.slice(1).join(' '),live);
        if(rest[0]==='review-run')return runIndependentReview(e,rest[1],rest[2],live);
        if(rest[0]==='run')return e.run(rest[1],rest[2],live);
        if(rest[0]==='chat')return e.chat(rest.slice(1).join(' '),live);
        if(rest[0]==='retry'){const old=e.store.getRun(rest[1]);if(!old)throw new Error('Unknown run');return e.run(old.taskId,old.instanceId,live,old.id);}
        throw new Error('Unknown coordinator operation');
      });
    }else throw new Error(`Unknown command: ${cmd}`);
  }catch(error){if(!readOnly&&e.formatVersion()===2)e.store.event(null,'command.error',{command:cmd,message:redact(String(error))});throw error;}
  finally{e.close();}
}
async function interactive(){
  console.log('AgentFleet · /help for commands, /exit leaves workers running. Ordinary text goes to manager.');
  const rl=createInterface({input:stdin,output:stdout});
  try{while(true){const line=(await rl.question('fleet> ')).trim();if(!line)continue;if(['/exit','/quit'].includes(line))break;
    try{await command(line.startsWith('/')?line.slice(1).split(/\s+/):['chat',line]);}catch(error){console.error(redact(String(error)));}
  }}finally{rl.close();}
}
try{if(!args.length){if(!stdin.isTTY)throw new Error('Use a command or an interactive terminal');await interactive();}else await command(args);}
catch(error){console.error(JSON.stringify({ok:false,error:{message:redact(String(error))}}));process.exitCode=1;}
