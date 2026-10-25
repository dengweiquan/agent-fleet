import { validatePlan } from './plan.ts';
import type { Plan } from './types.ts';

export interface ReviewBinding { workerRunId:string; commitSha:string; reviewerId:string }
export interface ReviewVerdict extends ReviewBinding { verdict:'approve'|'reject'; notes:string }

/** Accept one complete JSON document, optionally enclosed by one whole JSON fence.
 * Never search prose or select a convenient fragment of a model response. */
function document(text:string):unknown {
  if(typeof text!=='string'||Buffer.byteLength(text,'utf8')>4*1024*1024)throw new Error('Structured response must be at most 4 MiB');
  let source=text.trim();
  if(source.startsWith('```')) {
    const fenced=/^```(?:json)?\r?\n([\s\S]*)\r?\n```$/.exec(source);
    if(!fenced)throw new Error('Expected a single complete JSON document or JSON code fence');
    source=fenced[1];
  }
  try {return JSON.parse(source);}catch {throw new Error('Structured response is not one complete JSON document');}
}

export function parsePlanProposal(text:string):Plan { return validatePlan(document(text)); }

function binding(value:ReviewBinding):ReviewBinding {
  if(!value || typeof value!=='object')throw new Error('Review binding is required');
  for(const name of ['workerRunId','reviewerId'] as const) {
    if(typeof value[name]!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(value[name]))throw new Error(`Invalid review ${name}`);
  }
  if(typeof value.commitSha!=='string'||!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(value.commitSha))throw new Error('Review requires a full commit SHA');
  return {workerRunId:value.workerRunId,commitSha:value.commitSha,reviewerId:value.reviewerId};
}

export function parseReviewVerdict(text:string, expected:ReviewBinding):ReviewVerdict {
  const target=binding(expected);
  const parsed=document(text);
  if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw new Error('Review verdict must be an object');
  const value=parsed as Record<string,unknown>;
  const keys=['workerRunId','commitSha','reviewerId','verdict','notes'];
  if(Object.keys(value).length!==keys.length||keys.some(key=>!Object.hasOwn(value,key))||Object.keys(value).some(key=>!keys.includes(key)))throw new Error('Review verdict has missing or unknown fields');
  const actual=binding(value as unknown as ReviewBinding);
  for(const key of ['workerRunId','commitSha','reviewerId'] as const)if(actual[key]!==target[key])throw new Error(`Review ${key} does not match the requested evidence`);
  if(value.verdict!=='approve'&&value.verdict!=='reject')throw new Error('Review verdict must be approve or reject');
  if(typeof value.notes!=='string'||!value.notes.trim()||value.notes.length>16384||value.notes.includes('\0'))throw new Error('Review notes must be nonempty text of at most 16384 characters');
  return {...actual,verdict:value.verdict,notes:value.notes};
}
