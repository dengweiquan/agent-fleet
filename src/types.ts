export type Provider = 'fake' | 'claude' | 'codex';
export type Role = 'manager' | 'worker' | 'reviewer';
export interface Endpoint { id:string; kind:'local'|'ssh-stdio'|'external-agent'; target?:string }
export interface Instance { id:string; provider:Provider; role:Role; model:string; capabilities:string[]; endpointId?:string }
export interface Config { version:1; instances:Instance[]; maxOutputBytes:number; timeoutMs:number; endpoints?:Endpoint[]; maxWorkers?:number; maxReviews?:number }
export interface Task { id:string; title:string; prompt:string; dependencies:string[]; capabilities:string[]; acceptance:string[][] }
export interface Plan { version:1; goal:string; tasks:Task[] }
export type RunStatus = 'prepared'|'verifying'|'running'|'exited'|'failed'|'unknown'|'stopped'|'verified'|'rejected'|'accepted';
export interface Run { id:string; taskId:string; instanceId:string; provider:Provider; sessionId:string; baseSha:string; worktree:string; status:RunStatus; pid:number|null; processStart:string|null; exitCode:number|null; commitSha:string|null; reviewBy:string|null; createdAt:string }
export interface Event { event_id:number; run_id:string|null; run_seq:number|null; kind:string; data:unknown; created_at:string }
