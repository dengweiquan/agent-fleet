import {closeSync,existsSync,fsyncSync,lstatSync,mkdirSync,openSync,readFileSync,realpathSync,renameSync,unlinkSync,writeFileSync} from 'node:fs';
import {homedir} from 'node:os';
import {dirname,isAbsolute,join,resolve} from 'node:path';
import {randomUUID} from 'node:crypto';

export interface SessionSummary {manager?:string; taskStates?:Record<string,number>}
export interface SessionRecord {
  id:string; root:string; title:string; createdAt:string; lastActivity:string; dir:string;
  summary?:SessionSummary; availability:'available'|'unavailable';
}
type SavedRecord = Omit<SessionRecord,'availability'>;
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const maxIndexBytes=2*1024*1024;
function home(){return resolve(process.env.AGENT_FLEET_HOME||join(homedir(),'.agent-fleet'));}
function checkedText(value:unknown,name:string,max=160):string {
  if(typeof value!=='string'||!value.trim()||value.length>max||/[\x00-\x1f\x7f]/.test(value))throw new Error(`Invalid ${name}`);
  return value;
}
function noSymlink(path:string,directory=false){
  const s=lstatSync(path);if(s.isSymbolicLink()||(directory?!s.isDirectory():!s.isFile()))throw new Error(`Unsafe filesystem identity: ${path}`);
}
function ensureDirectory(path:string){
  if(existsSync(path)){noSymlink(path,true);return;}
  const parent=dirname(path);if(parent!==path)ensureDirectory(parent);
  mkdirSync(path,{mode:0o700});
}
function summary(value:unknown):SessionSummary|undefined {
  if(value===undefined)return undefined;
  if(!value||typeof value!=='object'||Array.isArray(value))throw new Error('Invalid session summary');
  const v=value as Record<string,unknown>;
  if(Object.keys(v).some(k=>!['manager','taskStates'].includes(k)))throw new Error('Unknown summary property');
  const out:SessionSummary={};
  if(v.manager!==undefined)out.manager=checkedText(v.manager,'manager');
  if(v.taskStates!==undefined){
    if(!v.taskStates||typeof v.taskStates!=='object'||Array.isArray(v.taskStates))throw new Error('Invalid task states');
    const entries=Object.entries(v.taskStates);if(entries.length>32)throw new Error('Too many task states');
    out.taskStates={};
    for(const [key,value] of entries){if(!/^[a-z][a-z_-]{0,31}$/.test(key)||!Number.isSafeInteger(value)||Number(value)<0)throw new Error('Invalid task state count');out.taskStates[key]=Number(value);}
  }return out;
}
function validate(value:unknown):SavedRecord {
  if(!value||typeof value!=='object'||Array.isArray(value))throw new Error('Invalid session record');
  const r=value as SavedRecord;
  if(typeof r.id!=='string'||!uuid.test(r.id))throw new Error('Invalid session id');
  checkedText(r.root,'project path',4096);checkedText(r.title,'session title');
  if(!isAbsolute(r.root)||resolve(r.root)!==r.root||r.dir!==join(r.root,'.agent-fleet','sessions',r.id))throw new Error('Session path mismatch');
  for(const date of [r.createdAt,r.lastActivity])if(typeof date!=='string'||!Number.isFinite(Date.parse(date)))throw new Error('Invalid activity timestamp');
  return {id:r.id,root:r.root,title:r.title,createdAt:r.createdAt,lastActivity:r.lastActivity,dir:r.dir,...(r.summary===undefined?{}:{summary:summary(r.summary)})};
}
function indexPath(){return join(home(),'sessions.json');}
function readIndex():SavedRecord[]{
  const path=indexPath();if(!existsSync(path))return [];
  noSymlink(home(),true);noSymlink(path);
  if(lstatSync(path).size>maxIndexBytes)throw new Error('Session index exceeds capacity');
  const raw=JSON.parse(readFileSync(path,'utf8'));
  if(raw.version!==1||!Array.isArray(raw.sessions)||raw.sessions.length>5000)throw new Error('Invalid session index');
  const records=raw.sessions.map(validate);if(new Set(records.map((r:SavedRecord)=>r.id)).size!==records.length)throw new Error('Duplicate session id');
  return records;
}
function atomicJSON(path:string,value:unknown){
  const text=JSON.stringify(value,null,2)+'\n';if(Buffer.byteLength(text)>maxIndexBytes)throw new Error('Session index exceeds capacity');
  if(existsSync(path))noSymlink(path);
  const temp=join(dirname(path),`.${randomUUID()}.tmp`),fd=openSync(temp,'wx',0o600);
  try{writeFileSync(fd,text);fsyncSync(fd);}finally{closeSync(fd);}
  try{renameSync(temp,path);const directory=openSync(dirname(path),'r');try{fsyncSync(directory);}finally{closeSync(directory);}}
  catch(error){if(existsSync(temp))unlinkSync(temp);throw error;}
}
function withIndex<T>(fn:(records:SavedRecord[])=>T):T {
  ensureDirectory(home());const lock=join(home(),'sessions.lock');let fd:number;
  try{fd=openSync(lock,'wx',0o600);}catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')throw new Error(`Session index locked: ${lock}. Inspect the interrupted writer; no automatic lock deletion.`);throw error;}
  try{writeFileSync(fd,JSON.stringify({owner:randomUUID(),createdAt:new Date().toISOString()}));fsyncSync(fd);return fn(readIndex());}
  finally{closeSync(fd);unlinkSync(lock);}
}
function validateDirectory(record:SavedRecord){
  if(realpathSync(record.root)!==record.root)throw new Error('Project path identity changed');
  for(const path of [join(record.root,'.agent-fleet'),join(record.root,'.agent-fleet','sessions'),record.dir])noSymlink(path,true);
  if(realpathSync(record.dir)!==record.dir)throw new Error('Session directory identity changed');
  const db=join(record.dir,'state.sqlite');if(existsSync(db))noSymlink(db);
}
function available(record:SavedRecord):SessionRecord {
  try{validateDirectory(record);return {...record,availability:existsSync(join(record.dir,'state.sqlite'))?'available':'unavailable'};}
  catch{return {...record,availability:'unavailable'};}
}
function writeCurrent(record:SavedRecord){
  validateDirectory(record);atomicJSON(join(record.root,'.agent-fleet','current.json'),{version:1,id:record.id,root:record.root});
}
export function createSession(root:string,title:string):SessionRecord {
  root=realpathSync(root);noSymlink(root,true);title=checkedText(title,'session title');
  return withIndex(records=>{
    const id=randomUUID(),createdAt=new Date().toISOString(),dir=join(root,'.agent-fleet','sessions',id);
    ensureDirectory(join(root,'.agent-fleet'));ensureDirectory(dirname(dir));mkdirSync(dir,{mode:0o700});
    const record:SavedRecord={id,root,title,createdAt,lastActivity:createdAt,dir};
    // Index first: an interrupted pointer write still leaves a discoverable history entry.
    atomicJSON(indexPath(),{version:1,sessions:[...records,record]});writeCurrent(record);return available(record);
  });
}
/** Exact canonical project-directory match; never a name or path-prefix match. */
export function listSessions(project?:string):SessionRecord[]{
  const root=project===undefined?undefined:realpathSync(resolve(checkedText(project,'project filter',4096)));
  if(root!==undefined)noSymlink(root,true);
  return readIndex().filter(record=>root===undefined||record.root===root).map(available).sort((a,b)=>b.lastActivity.localeCompare(a.lastActivity));
}
export function findSession(id:string):SessionRecord {
  if(!uuid.test(id))throw new Error('Invalid session id');const record=readIndex().find(r=>r.id===id);if(!record)throw new Error(`Unknown session: ${id}`);return available(record);
}
export function selectSession(root:string,id?:string):SessionRecord {
  root=realpathSync(root);
  if(!id){const path=join(root,'.agent-fleet','current.json');noSymlink(join(root,'.agent-fleet'),true);noSymlink(path);if(lstatSync(path).size>8192)throw new Error('Invalid current-session pointer');const pointer=JSON.parse(readFileSync(path,'utf8'));if(pointer.version!==1||pointer.root!==root)throw new Error('Current-session project mismatch');id=pointer.id;}
  if(typeof id!=='string')throw new Error('Missing session id');const record=findSession(id);if(record.root!==root)throw new Error('Session belongs to another project');validateDirectory(record);return record;
}
export function setCurrentSession(record:SessionRecord):void {
  const stored=findSession(record.id);if(stored.root!==record.root||stored.dir!==record.dir)throw new Error('Session identity mismatch');withIndex(()=>writeCurrent(stored));
}
export function touchSession(id:string,newSummary?:SessionSummary):void {
  if(!uuid.test(id))throw new Error('Invalid session id');const checkedSummary=summary(newSummary);
  withIndex(records=>{const record=records.find(r=>r.id===id);if(!record)throw new Error('Unknown session');record.lastActivity=new Date().toISOString();if(checkedSummary!==undefined)record.summary=checkedSummary;atomicJSON(indexPath(),{version:1,sessions:records});});
}
