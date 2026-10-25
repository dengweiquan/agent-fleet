import {test} from 'node:test';import assert from 'node:assert/strict';
import {parsePlanProposal,parseReviewVerdict} from '../src/structured.ts';
const proposal={version:1,goal:'test',tasks:[{id:'a',title:'A',prompt:'write A',dependencies:[],capabilities:['code'],acceptance:[['node','-e','process.exit(0)']]}]};
const bound={workerRunId:'worker-run-1',commitSha:'a'.repeat(40),reviewerId:'reviewer-1'};
const verdict={...bound,verdict:'approve',notes:'Checked evidence'};
test('plan accepts exactly raw JSON or a whole JSON fence and validates schema',()=>{
 assert.deepEqual(parsePlanProposal(JSON.stringify(proposal)),proposal);
 assert.deepEqual(parsePlanProposal('```json\n'+JSON.stringify(proposal)+'\n```'),proposal);
 for(const text of ['Plan follows:\n'+JSON.stringify(proposal),JSON.stringify(proposal)+'\nDone','```js\n'+JSON.stringify(proposal)+'\n```',JSON.stringify({...proposal,extra:1}),'{}',JSON.stringify({...proposal,tasks:[{...proposal.tasks[0],dependencies:['a']}]})])assert.throws(()=>parsePlanProposal(text));
});
test('review validates complete evidence binding and exact fields',()=>{
 assert.deepEqual(parseReviewVerdict(JSON.stringify(verdict),bound),verdict);
 assert.equal(parseReviewVerdict('```json\n'+JSON.stringify({...verdict,verdict:'reject'})+'\n```',bound).verdict,'reject');
 for(const patch of [{workerRunId:'other'},{commitSha:'b'.repeat(40)},{reviewerId:'another'},{verdict:'accepted'},{notes:''},{notes:null},{extra:'field'},{commitSha:'abcd'}])assert.throws(()=>parseReviewVerdict(JSON.stringify({...verdict,...patch}),bound));
 assert.throws(()=>parseReviewVerdict('I approve: '+JSON.stringify(verdict),bound));
 assert.throws(()=>parseReviewVerdict(JSON.stringify(verdict),{...bound,commitSha:'aaaa'}));
});
test('structured input is size bounded and cannot contain multiple documents',()=>{
 assert.throws(()=>parsePlanProposal(' '.repeat(4*1024*1024+1)));
 assert.throws(()=>parsePlanProposal(JSON.stringify(proposal)+' '+JSON.stringify(proposal)));
 assert.throws(()=>parseReviewVerdict(JSON.stringify(verdict)+'\n{}',bound));
});
