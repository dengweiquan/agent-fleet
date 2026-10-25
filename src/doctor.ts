import { execFile } from 'node:child_process';
import { adapterCapabilities } from './adapters.ts';

export interface ProbeResult { stdout:string;stderr:string;exitCode:number|null;error?:string }
export type Probe = (command:string,args:string[])=>Promise<ProbeResult>;
export interface ProviderHealth {
  provider:'claude'|'codex';available:boolean;version:string|null;
  auth:'logged-in'|'not-logged-in'|'unknown';authMethod:string;quota:'unknown';note?:string;
}
/** Local official status commands only. Output is never returned verbatim. */
export const localProbe:Probe=(command,args)=>new Promise(resolve=>{
  execFile(command,args,{encoding:'utf8',timeout:3000,maxBuffer:16384,killSignal:'SIGKILL'},(error,stdout,stderr)=>{
    resolve({stdout,stderr,exitCode:error?(typeof error.code==='number'?error.code:null):0,...(error?{error:typeof error.code==='string'?error.code:'command-failed'}:{})});
  });
});
export async function providerHealth(provider:'claude'|'codex',probe:Probe=localProbe):Promise<ProviderHealth> {
  const version=await probe(provider,['--version']);
  const health:ProviderHealth={provider,available:version.exitCode===0,version:null,auth:'unknown',authMethod:'unknown',quota:'unknown'};
  if(!health.available){health.note='CLI missing or local version probe failed';return health;}
  health.version=version.stdout.match(/\b\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?\b/)?.[0]??null;
  const args=adapterCapabilities(provider).authStatus.slice(1);
  const status=await probe(provider,args);
  if(provider==='claude') {
    try {
      const result=JSON.parse(status.stdout);
      if((status.exitCode===0||status.exitCode===1)&&typeof result.loggedIn==='boolean'){
        health.auth=result.loggedIn?'logged-in':'not-logged-in';
        // Keep known method categories, never arbitrary provider output.
        const method=String(result.authMethod??'unknown').toLowerCase();
        health.authMethod=['oauth','claude.ai','api_key','apikey','none'].includes(method)?method:'unknown';
      }
    }catch{}
  }else if(status.exitCode===0||status.exitCode===1){
    const result=status.stdout+'\n'+status.stderr;
    if(/\bnot logged in\b/i.test(result)){health.auth='not-logged-in';health.authMethod='none';}
    else if(status.exitCode===0&&/\blogged in\b/i.test(result)){
      health.auth='logged-in';health.authMethod=/chatgpt/i.test(result)?'ChatGPT':/api key/i.test(result)?'API key':'unknown';
    }
  }
  if(health.auth==='unknown')health.note='Local authentication status unavailable; no login or model request attempted';
  return health;
}
export async function doctor(){
  let sqliteAvailable=false;
  try {const {DatabaseSync}=await import('node:sqlite');const db=new DatabaseSync(':memory:');db.prepare('SELECT 1').get();db.close();sqliteAvailable=true;}catch{}
  return {nodeVersion:process.version,sqliteAvailable,providers:await Promise.all([providerHealth('claude'),providerHealth('codex')])};
}
export const localDoctor=doctor;
