import {execFileSync} from 'node:child_process';
import {realpathSync} from 'node:fs';
export function git(cwd:string,...args:string[]):string { return execFileSync('git',['-c','core.hooksPath=/dev/null',...args],{cwd,encoding:'utf8',maxBuffer:2*1024*1024,stdio:['ignore','pipe','pipe']}).trim(); }
export function repository(cwd:string):string { return realpathSync(git(cwd,'rev-parse','--show-toplevel')); }
export function head(cwd:string):string { return git(cwd,'rev-parse','HEAD'); }
export function clean(cwd:string):boolean { return git(cwd,'status','--porcelain')===''; }
export function snapshot(cwd:string,runId:string):string {
  git(cwd,'add','--all');
  if(git(cwd,'diff','--cached','--name-only')) git(cwd,'-c','user.name=AgentFleet','-c','user.email=agent-fleet@localhost','-c','core.hooksPath=/dev/null','-c','commit.gpgsign=false','commit','-m',`AgentFleet candidate ${runId}`);
  return head(cwd);
}
