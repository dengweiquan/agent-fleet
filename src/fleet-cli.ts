import {readFileSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {homedir} from 'node:os';
import {Engine} from './engine.ts';
import {ResourcePool} from './resource-pool.ts';
import type {Resource,PoolLimits} from './resource-pool.ts';
import {fleetPreview,scheduleFleet,proposeFleet} from './fleet-scheduler.ts';
import type {FleetProject,RolePolicy} from './fleet-scheduler.ts';
import {redact,processStart} from './process.ts';
import {randomUUID} from 'node:crypto';

export function defaultPoolPath(){return resolve(process.env.AGENT_FLEET_HOME||join(homedir(),'.agent-fleet'),'resources.sqlite');}
function object(value:any,required:string[],optional:string[]=[]){if(!value||typeof value!=='object'||Array.isArray(value)||required.some(k=>!Object.hasOwn(value,k))||Object.keys(value).some(k=>![...required,...optional].includes(k)))throw new Error('Invalid fleet configuration fields');return value;}
function load(path:string){if(!path)throw new Error('Explicit JSON configuration file required');const text=readFileSync(path,'utf8');if(Buffer.byteLength(text)>1024*1024)throw new Error('Configuration exceeds 1 MiB');return JSON.parse(text);}
export function openFleet(path:string,readOnly:boolean):FleetProject[]{
  const spec=object(load(path),['version','projects']);if(spec.version!==1||!Array.isArray(spec.projects)||!spec.projects.length||spec.projects.length>32)throw new Error('Fleet version 1 and 1..32 project sessions required');const projects:FleetProject[]=[];
  try{for(const value of spec.projects){const p=object(value,['root','sessionId'],['roles','remoteClaimsAuthorized']);if(typeof p.root!=='string'||!p.root.startsWith('/')||typeof p.sessionId!=='string')throw new Error('Absolute project root and explicit sessionId required');if(p.remoteClaimsAuthorized!==undefined&&typeof p.remoteClaimsAuthorized!=='boolean')throw new Error('remoteClaimsAuthorized must be an explicit boolean');
      const roles:Partial<Record<'manager'|'worker'|'reviewer',RolePolicy>>={};if(p.roles!==undefined){object(p.roles,[],['manager','worker','reviewer']);for(const [role,raw] of Object.entries(p.roles)){const r=object(raw,[],['capabilities','preferredProvider','requiredProvider']);for(const k of ['preferredProvider','requiredProvider'])if(r[k]!==undefined&&!['fake','claude','codex','external-agent'].includes(r[k]))throw new Error('Unsupported role provider; xAI/Grok interfaces are not implemented');if(r.capabilities!==undefined&&(!Array.isArray(r.capabilities)||r.capabilities.length>32||r.capabilities.some((s:unknown)=>typeof s!=='string'||!/^[A-Za-z0-9_-]{1,80}$/.test(s))))throw new Error('Invalid role capabilities');roles[role as keyof typeof roles]=r;}}
      const engine=new Engine(p.root,readOnly,p.sessionId);projects.push({projectId:engine.root,engine,roles,remoteClaimsAuthorized:p.remoteClaimsAuthorized===true});
    }return projects;
  }catch(error){for(const p of projects)p.engine.close();throw error;}
}
export function poolCommand(words:string[],path:string,observer=false){
  const [cmd,file]=words;if(!['status','events','configure'].includes(cmd))throw new Error('Use pool status | events [cursor] | configure <json>');if(observer&&cmd==='configure')throw new Error('Observer cannot configure resource pool');
  if(cmd==='configure'){
    const value=object(load(file),['version','resources','limits']);if(value.version!==1||!Array.isArray(value.resources)||value.resources.length>1000)throw new Error('Invalid pool registry');
    // Validate all submitted definitions before touching the real registry. Never install or log in.
    const check=new ResourcePool(':memory:');try{for(const r of value.resources)check.upsertResource(r);check.setLimits(value.limits);}finally{check.close();}
    const pool=new ResourcePool(path);try{for(const r of value.resources as Resource[])pool.upsertResource(r);pool.setLimits(value.limits as PoolLimits);return {...pool.status(),modelsInvoked:false};}finally{pool.close();}
  }
  const pool=new ResourcePool(path,{readOnly:true});try{return cmd==='events'?pool.events(Number(file??0),1000):pool.status();}finally{pool.close();}
}
export async function fleetCommand(words:string[],options:{path:string,observer:boolean,dryRun:boolean,execute:boolean,live?:{approved:boolean,budgetUsd:number},perRunBudgetExplicit:boolean,detach:(words:string[])=>unknown}){
  const [cmd,file,...rest]=words;if(!['schedule','propose'].includes(cmd))throw new Error('Use fleet schedule <fleet.json> or fleet propose <fleet.json> <session-id> <message>');
  if(options.observer&&!options.dryRun)throw new Error('Observer may only preview fleet with --dry-run');
  const previewOnly=options.dryRun||!options.execute,pool=new ResourcePool(options.path,{readOnly:previewOnly,mustExist:true});let projects:FleetProject[]=[];
  const coordinatorId=randomUUID();
  try{
    projects=openFleet(file,previewOnly);
    const preview=fleetPreview(pool,projects,options.live?.budgetUsd??0);
    if(options.dryRun)return preview;
    if(projects.some(p=>p.engine.formatVersion()!==2))throw new Error('Fleet requires separate phase2 sessions; no legacy migration');
    const selected=cmd==='propose'?projects.find(p=>p.engine.sessionId===rest[0]):undefined;if(cmd==='propose'&&(!selected||!rest.slice(1).join(' ').trim()))throw new Error('Manager proposal requires a fleet session ID and message');
    // Conservative preflight: no undisclosed live provider dispatch after detaching.
    const ids=new Set(cmd==='schedule'?preview.projects.flatMap(p=>p.tasks.flatMap(t=>[...t.workers,...t.reviewers])):pool.status().resources.filter(r=>r.roles.includes('manager')&&r.endpointKind==='local'&&r.provider===preview.projects.find(p=>p.sessionId===rest[0])?.managerRequiredProvider).map(r=>r.id));
    if(pool.status().resources.some(r=>ids.has(r.id)&&r.provider!=='fake')&&(!options.perRunBudgetExplicit||!options.live?.approved||!Number.isFinite(options.live.budgetUsd)||options.live.budgetUsd<=0))throw new Error('Live fleet requires --allow-live --budget-per-run-usd N; this is not a total cap and Codex money cap is unavailable');
    if(!options.execute)return {launch:options.detach([cmd,resolve(file),...rest]),preview};
    pool.event('fleet.started',{coordinatorId,pid:process.pid,processStart:processStart(process.pid),command:cmd,sessions:projects.map(p=>p.engine.sessionId)});
    const result=cmd==='schedule'?await scheduleFleet(pool,projects,options.live):await proposeFleet(pool,selected!,rest.slice(1).join(' '),options.live);
    pool.event('fleet.finished',{coordinatorId,command:cmd});return result;
  }catch(error){if(!previewOnly)pool.event('fleet.error',{coordinatorId,command:cmd,error:redact(String(error))});throw error;
  }finally{for(const p of projects)p.engine.close();pool.close();}
}
