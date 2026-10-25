import { writeFileSync } from 'node:fs';
const mode=process.argv[2]??'success';const session=process.argv[3]??'fake-session';
const send=(x:unknown)=>process.stdout.write(JSON.stringify(x)+'\n');
const message=(text:string)=>send({type:'item.completed',item:{type:'agent_message',text}});
let input='';for await(const c of process.stdin){input+=c;if(Buffer.byteLength(input)>1024*1024)throw new Error('Fixture input exceeds 1 MiB');}
function decode(value:unknown):any {return typeof value==='string'?JSON.parse(value):value;}
function envelope():any {return decode(input);}
function writeRequest():{filename:string;content:string;delayMs:number} {
  const root=envelope();
  const value=decode(root?.task?.prompt??root?.request??root?.message??root);
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(key=>!['filename','content','delayMs'].includes(key)))throw new Error('Invalid fixture write request');
  if(typeof value.filename!=='string'||!/^[A-Za-z0-9][A-Za-z0-9._-]{0,80}$/.test(value.filename)||typeof value.content!=='string'||value.content.length>4096||!Number.isSafeInteger(value.delayMs)||value.delayMs<0||value.delayMs>5000)throw new Error('Invalid fixture write fields');
  return value;
}
function plan() {
  let request:unknown=input;try{const root=envelope();request=root?.request??root?.message??root;}catch{}
  const goal=typeof request==='string'&&request.trim()?request.slice(0,4096):'Parallel fixture files and dependency join';
  const check=(filename:string,content:string)=>['node','-e',`const fs=require('node:fs');if(fs.readFileSync(${JSON.stringify(filename)},'utf8')!==${JSON.stringify(content)})process.exit(1)`];
  return {version:1,goal,tasks:[
    {id:'left',title:'Write left fixture',prompt:JSON.stringify({filename:'left.txt',content:'left\n',delayMs:150}),dependencies:[],capabilities:['code'],acceptance:[check('left.txt','left\n')]},
    {id:'right',title:'Write right fixture',prompt:JSON.stringify({filename:'right.txt',content:'right\n',delayMs:150}),dependencies:[],capabilities:['code'],acceptance:[check('right.txt','right\n')]},
    {id:'join',title:'Join accepted fixture dependencies',prompt:JSON.stringify({filename:'join.txt',content:'joined\n',delayMs:20}),dependencies:['left','right'],capabilities:['code'],acceptance:[check('left.txt','left\n'),check('right.txt','right\n'),check('join.txt','joined\n')]},
  ]};
}
if(mode==='hang'){setInterval(()=>{},1000);}
else if(mode==='flood'){for(let i=0;i<20000;i++)process.stdout.write('x'.repeat(100)+'\n');}
else if(mode==='malformed'){process.stdout.write('{broken\n');}
else if(mode==='partial-secret'){process.stdout.write('api_key=abc');setTimeout(()=>process.stdout.write('defghijklmnopqrstuvwxyz\n'),20);}
else if(mode==='stderr'){process.stderr.write('fixture stderr\n');send({type:'turn.completed'});}
else if(mode==='fail'){process.stderr.write('fixture failure\n');process.exitCode=7;}
else {
  send({type:'thread.started',thread_id:session});
  try {
    if(mode==='planner'||mode==='planner-invalid'){
      const proposal=plan();if(mode==='planner-invalid')proposal.tasks[0].dependencies=['join'];
      message(JSON.stringify(proposal));
    }else if(['review-approve','review-reject','review-invalid'].includes(mode)){
      const root=envelope(),bound=decode(root?.binding??root);
      message(JSON.stringify({workerRunId:mode==='review-invalid'?'wrong-run':bound?.workerRunId,commitSha:bound?.commitSha,reviewerId:bound?.reviewerId,verdict:mode==='review-reject'?'reject':'approve',notes:mode==='review-reject'?'Fixture rejects the supplied evidence.':'Fixture approves the supplied evidence.'}));
    }else if(mode==='delay-write'){
      const request=writeRequest();
      send({type:'fixture.started',filename:request.filename,sessionId:session,time:Date.now()});
      await new Promise(resolve=>setTimeout(resolve,request.delayMs));
      // 'wx' refuses overwrite and symlink targets; only a safe cwd-local leaf is allowed.
      writeFileSync(request.filename,request.content,{flag:'wx'});
      send({type:'fixture.finished',filename:request.filename,sessionId:session,time:Date.now()});
      message('Fixture wrote '+request.filename);
    }else{
      if(mode==='write')writeFileSync('artifact.txt','AgentFleet fixture artifact\n');
      message(mode==='review'?'Fixture review: inspect persisted commit and acceptance evidence before decision.':`Fixture finished (${input.length} input characters)`);
    }
    send({type:'turn.completed',usage:{input_tokens:0,output_tokens:0}});
  }catch{
    process.stderr.write('Invalid or unsafe fixture request\n');
    send({type:'turn.failed',error:{message:'Invalid or unsafe fixture request'}});process.exitCode=7;
  }
}
