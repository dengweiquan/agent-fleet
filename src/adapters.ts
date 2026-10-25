import { fileURLToPath } from 'node:url';
import type { Instance, Provider } from './types.ts';

export interface Invocation { command:string; args:string[]; input?:string }
export interface InvocationOptions { sessionId:string; prompt:string; cwd:string; budgetUsd?:number }
export interface ProviderEvent { kind:string; data:unknown }
export interface Parser { sessionId:string|null; state:string; hasError:boolean; parse(line:string):ProviderEvent|null }

export function adapterCapabilities(provider:Provider) {
  const common = { provider, quota:'unknown', fullReasoning:false, automaticApproval:false };
  if(provider==='claude') return { ...common, availableInterface:'Claude Code print stream-json', explicitSessionId:true, publicReasoningSummary:true, authStatus:['claude','auth','status','--json'], hardBudgetUsd:true, taskTypes:['conversation','plan','code','review'] };
  if(provider==='codex') return { ...common, availableInterface:'Codex exec JSONL', explicitSessionId:false, publicReasoningSummary:true, authStatus:['codex','login','status'], hardBudgetUsd:false, taskTypes:['plan','code','review'] };
  if(provider==='fake') return { ...common, availableInterface:'local fault-injection fixture', explicitSessionId:true, publicReasoningSummary:false, authStatus:[], hardBudgetUsd:false, taskTypes:['conversation','plan','code','review'] };
  throw new Error('Unsupported provider');
}

export function buildInvocation(instance:Instance, opts:InvocationOptions):Invocation {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(opts.sessionId)) throw new Error('A fresh UUID sessionId is required');
  if (!instance.model || ['configured-default','user-selection-required'].includes(instance.model) || /[\r\n\0]/.test(instance.model)) throw new Error('Explicit model is required');
  if (opts.budgetUsd!==undefined && (!Number.isFinite(opts.budgetUsd)||opts.budgetUsd<=0)) throw new Error('Invalid budget');
  if(instance.provider==='claude') {
    const args=['--print','--verbose','--output-format','stream-json','--session-id',opts.sessionId,'--model',instance.model,'--permission-mode','manual','--permission-prompts','none','--safe-mode','--strict-mcp-config','--mcp-config','{"mcpServers":{}}'];
    if(instance.role!=='worker') args.push('--tools','');
    if(opts.budgetUsd!==undefined) args.push('--max-budget-usd',String(opts.budgetUsd));
    return {command:'claude',args,input:opts.prompt};
  }
  if(instance.provider==='codex') {
    // No resume/--last: each exec obtains its own thread ID from thread.started.
    // 'never' means deny requests requiring approval; it does NOT grant permission.
    return {command:'codex',args:['exec','--json','--ephemeral','--ignore-user-config','--sandbox',instance.role==='worker'?'workspace-write':'read-only','--config','approval_policy="never"','--model',instance.model,'--cd',opts.cwd,'-'],input:opts.prompt};
  }
  if(instance.provider==='fake') return {command:process.execPath,args:['--experimental-strip-types',fileURLToPath(new URL('../test/fake-cli.ts',import.meta.url)),instance.model,opts.sessionId],input:opts.prompt};
  throw new Error('Unsupported provider');
}

function asObject(value:unknown):Record<string,any>|null { return value && typeof value==='object' && !Array.isArray(value) ? value as Record<string,any> : null; }
export function createParser(provider:Provider):Parser {
  return {
    sessionId:null,state:'starting',hasError:false,
    parse(line:string):ProviderEvent|null {
      if(!line.trim())return null;
      let event:Record<string,any>|null;
      try {event=asObject(JSON.parse(line));} catch {this.hasError=true;this.state='protocol_error';return {kind:'protocol_error',data:{reason:'Invalid JSON event'}};}
      if(!event || typeof event.type!=='string'){this.hasError=true;this.state='protocol_error';return {kind:'protocol_error',data:{reason:'Missing event type'}};}
      if (provider==='claude') {
        if(event.type==='system' && event.subtype==='init') {if(typeof event.session_id==='string')this.sessionId=event.session_id;if(!this.hasError)this.state='running';return {kind:'session',data:{sessionId:this.sessionId}};}
        if(event.type==='assistant') {
          const blocks=Array.isArray(event.message?.content)?event.message.content:[];
          return {kind:'message',data:{blocks:blocks.flatMap((block:any)=>block?.type==='text' && typeof block.text==='string'?[{type:'text',text:block.text}]:block?.type==='thinking' && typeof block.thinking==='string'?[{type:'public_reasoning',text:block.thinking,scope:'CLI-exposed block; not complete internal reasoning'}]:[])}};
        }
        if(event.type==='result'){if(event.is_error){this.hasError=true;this.state='provider_error';}else if(!this.hasError)this.state='completed';return {kind:event.is_error?'provider_error':'provider_completed',data:{result:typeof event.result==='string'?event.result:undefined,subtype:event.subtype,usage:event.usage,costUsd:event.total_cost_usd,accepted:false}};}
      } else {
        if(event.type==='thread.started'){if(typeof event.thread_id==='string')this.sessionId=event.thread_id;if(!this.hasError)this.state='running';return {kind:'session',data:{sessionId:this.sessionId}};}
        if(event.type==='item.completed') {
          const item=asObject(event.item);
          if(item?.type==='agent_message')return {kind:'message',data:{text:item.text}};
          if(item?.type==='reasoning')return {kind:'public_reasoning',data:{text:item.text,scope:'CLI-exposed summary; not complete internal reasoning'}};
          return {kind:'activity',data:{type:item?.type ?? 'unknown',status:item?.status}};
        }
        if(event.type==='turn.completed'){if(!this.hasError)this.state='completed';return {kind:'provider_completed',data:{usage:event.usage,accepted:false}};}
        if(event.type==='turn.failed'||event.type==='error'){this.hasError=true;this.state='provider_error';return {kind:'provider_error',data:{message:event.error?.message ?? event.message ?? 'Provider error'}};}
      }
      return {kind:'provider_event',data:{type:event.type}};
    }
  };
}
export function parseProviderLine(provider:Provider,line:string):ProviderEvent|null {return createParser(provider).parse(line);}
