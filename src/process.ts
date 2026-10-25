import { spawn, execFileSync } from 'node:child_process';
import type { Readable } from 'node:stream';
import { StringDecoder } from 'node:string_decoder';
import { createHash } from 'node:crypto';

export function redact(value:string):string {
  return value
    .replace(/\x1b\[[0-?]*[ -/]*[@-~]/g,'')
    .replace(/\b(?:sk|pk|ghp|github_pat|xox[baprs])-?[A-Za-z0-9_-]{12,}\b/g,'[REDACTED]')
    .replace(/(\b(?:authorization|proxy-authorization)\s*[":=]+\s*["']?)(?:Bearer\s+)?[^\s"',;}]+/gi,'$1[REDACTED]')
    .replace(/(\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret|cookie|set-cookie)\s*[":=]+\s*["']?)[^\s"',;}]+/gi,'$1[REDACTED]');
}

/** Positive evidence only. A missing result is UNKNOWN, never proof of death. */
export function processStart(pid:number):string|null {
  if(!Number.isSafeInteger(pid)||pid<=0)return null;
  try {
    const value=execFileSync('/bin/ps',['-p',String(pid),'-o','lstart=','-o','command='],{encoding:'utf8',timeout:1500,maxBuffer:65536,stdio:['ignore','pipe','ignore']}).trim();
    const match=value.match(/^(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+([\s\S]+)$/);
    if(!match)return null;
    return `${match[1]} sha256:${createHash('sha256').update(match[2]).digest('hex')}`;
  }catch{return null;}
}
export interface ProcessOptions {
  command:string;args:string[];input?:string;cwd:string;timeoutMs:number;maxOutputBytes:number;
  onStart?:(pid:number,start:string|null)=>void;
  onEvent?:(kind:string,data:{line:string})=>void|Promise<void>;
  onControl?:(control:{stop:()=>boolean})=>void;
}
export interface ProcessResult { exitCode:number|null;signal:string|null;overflow:boolean;timedOut:boolean;identity:{pid:number;start:string|null}|null;error?:string }

export async function supervise(opts:ProcessOptions):Promise<ProcessResult> {
  if(!Number.isSafeInteger(opts.maxOutputBytes)||opts.maxOutputBytes<128||opts.maxOutputBytes>64*1024*1024)throw new Error('maxOutputBytes must be 128..67108864');
  if(!Number.isSafeInteger(opts.timeoutMs)||opts.timeoutMs<10||opts.timeoutMs>3600000)throw new Error('timeoutMs must be 10..3600000');
  const child=spawn(opts.command,opts.args,{cwd:opts.cwd,stdio:['pipe','pipe','pipe'],shell:false});
  let active=true,overflow=false,timedOut=false,total=0,error:string|undefined,identity:ProcessResult['identity']=null;
  let killTimer:NodeJS.Timeout|undefined;
  let drainTimer:NodeJS.Timeout|undefined;
  const stop=()=>{
    if(!active)return false;
    child.kill('SIGTERM');
    if(!killTimer)killTimer=setTimeout(()=>{if(active)child.kill('SIGKILL');},1000);
    // A descendant may retain pipes after its parent exits. Do not signal unknown
    // descendants: bound our drain and explicitly report uncertain output instead.
    if(!drainTimer)drainTimer=setTimeout(()=>{if(active){error=error??'Stream drain deadline exceeded; descendant state unknown';child.stdout.destroy();child.stderr.destroy();}},2000);
    return true;
  };
  // Callbacks never receive a PID-based kill capability. Ownership is the live child handle.
  const done=new Promise<{exitCode:number|null;signal:string|null}>(resolve=>{
    child.once('error',e=>{error=redact(e.message);});
    child.once('close',(exitCode,signal)=>{active=false;resolve({exitCode,signal});});
  });
  child.once('spawn',()=>{
    identity={pid:child.pid!,start:processStart(child.pid!)};
    try{opts.onStart?.(identity.pid,identity.start);opts.onControl?.({stop});}catch(e){error=redact(String(e));stop();}
  });
  const timer=setTimeout(()=>{timedOut=true;stop();},opts.timeoutMs);
  const maxLine=Math.min(opts.maxOutputBytes,65536);
  async function drain(stream:Readable,kind:string) {
    let pending='';const decoder=new StringDecoder('utf8');
    try {
      for await(const chunk of stream) {
        total+=chunk.length;
        if(total>opts.maxOutputBytes){overflow=true;pending='';stop();continue;}
        if(overflow)continue;
        pending+=decoder.write(chunk);
        let newline:number;
        while((newline=pending.indexOf('\n'))>=0){
          const line=pending.slice(0,newline).replace(/\r$/,'');pending=pending.slice(newline+1);
          if(Buffer.byteLength(line)>maxLine){overflow=true;pending='';stop();break;}
          await opts.onEvent?.(kind,{line:redact(line)});
        }
        if(Buffer.byteLength(pending)>maxLine){overflow=true;pending='';stop();}
      }
      if(!overflow){pending+=decoder.end();if(pending)await opts.onEvent?.(kind,{line:redact(pending)});}
    } catch(e){error=redact(String(e));stop();stream.resume();}
  }
  const pumps=[drain(child.stdout,'stdout'),drain(child.stderr,'stderr')];
  child.stdin.on('error',()=>{});
  child.stdin.end(opts.input??'');
  const result=await done;await Promise.all(pumps);clearTimeout(timer);if(killTimer)clearTimeout(killTimer);if(drainTimer)clearTimeout(drainTimer);
  return {...result,overflow,timedOut,identity,...(error?{error}: {})};
}
