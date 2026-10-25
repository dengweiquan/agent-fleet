import {once} from 'node:events';
import type {Readable,Writable} from 'node:stream';
import {pooledRemoteCommand} from './pooled-remote-cli.ts';
import {redact} from './process.ts';

/** Fixed-scope JSON-lines over an existing SSH stdio channel; no listener or auth creation. */
export async function serveConnector(args:string[],options:{poolPath:string;observer:boolean},input:Readable=process.stdin,output:Writable=process.stdout){
  if(args.length!==3)throw new Error('serve requires fixed FLEET_FILE ENDPOINT RESOURCE');
  const [fleet,endpoint,resource]=args;const limit=65536;let pending=Buffer.alloc(0);
  async function frame(line:Buffer){
    if(line.length>limit)throw new Error('Connector frame exceeds 64KiB');if(!line.length)return;
    let id:string|null=null,generation:number|null=null,response:unknown;
    try{
      const request=JSON.parse(line.toString('utf8'));
      if(!request||typeof request!=='object'||Array.isArray(request)||Object.keys(request).some(k=>!['id','generation','payload'].includes(k))||typeof request.id!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(request.id)||!Number.isSafeInteger(request.generation)||request.generation<1)throw new Error('Invalid connector frame identity');
      id=request.id;generation=request.generation;
      const data=await pooledRemoteCommand(['request',fleet,endpoint,resource],options,request.payload);
      response={id,generation,ok:true,data};
    }catch(error){response={id,generation,ok:false,error:redact(String(error))};}
    let encoded=JSON.stringify(response)+'\n';if(Buffer.byteLength(encoded)>limit)encoded=JSON.stringify({id,generation,ok:false,error:'Response exceeds 64KiB; request a smaller page'})+'\n';
    if(!output.write(encoded))await once(output,'drain');
  }
  for await(const chunk of input){pending=Buffer.concat([pending,Buffer.from(chunk)]);let newline:number;
    while((newline=pending.indexOf(10))>=0){const line=pending.subarray(0,newline);pending=pending.subarray(newline+1);await frame(line);}
    if(pending.length>limit)throw new Error('Connector unterminated frame exceeds 64KiB');
  }
  if(pending.length)throw new Error('Connector closed with an incomplete frame; reconcile before resending mutations');
}
