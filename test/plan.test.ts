import test from 'node:test';
import assert from 'node:assert/strict';
import { defaultConfig, validateConfig, validatePlan } from '../src/plan.ts';

const task = (id: string, dependencies: string[] = []) => ({ id, title:id, prompt:'Make a fixture', dependencies, capabilities:['code'], acceptance:[['node', '--test']] });
test('plan validates a DAG and creates independent values', () => {
  const input = { version:1, goal:'fixture', tasks:[task('a'), task('b', ['a'])] };
  const result = validatePlan(input);
  input.tasks[0].dependencies.push('b');
  assert.deepEqual(result.tasks[0].dependencies, []);
});
test('plan rejects cycles, dangling dependencies, duplicate identifiers/dependencies, unsafe ids, empty acceptance and unknown fields', () => {
  for (const tasks of [
    [task('a', ['b']), task('b', ['a'])], [task('a', ['missing'])], [task('a'),task('a')],
    [task('a'), task('b', ['a','a'])], [task('../escape')], [{...task('a'), acceptance:[]}],
    [{...task('a'), acceptance:[[]]}], [{...task('a'), automaticApproval:true}],
  ]) assert.throws(() => validatePlan({version:1, goal:'fixture', tasks}));
  assert.throws(() => validatePlan({version:1, goal:'fixture', tasks:[]}));
  assert.throws(() => validatePlan({version:1, goal:'fixture', tasks:Array.from({length:129}, (_,i) => task(`t${i}`))}));
});
test('config requires manager, separate worker/reviewer, verified providers and bounded resource limits', () => {
  const c = defaultConfig();
  assert.deepEqual(validateConfig(c), c);
  assert.throws(() => validateConfig({...c, instances:c.instances.filter(i => i.role !== 'reviewer')}));
  assert.throws(() => validateConfig({...c, instances:[...c.instances,c.instances[0]]}));
  assert.throws(() => validateConfig({...c, instances:c.instances.map(i => ({...i, provider:'muse'}))}));
  assert.throws(() => validateConfig({...c, maxOutputBytes:Infinity}));
  assert.throws(() => validateConfig({...c, timeoutMs:0}));
  assert.throws(() => validateConfig({...c, token:'do-not-store'}));
});
test('legacy configuration normalizes to local and explicitly unselected Codex model', () => {
  const config = defaultConfig();
  const legacy = {...config} as Record<string, unknown>;
  delete legacy.endpoints;
  legacy.instances = config.instances.map(({id, provider, role, model, capabilities}) => ({id, provider, role, model, capabilities}));
  assert.deepEqual(validateConfig(legacy), config);
  assert.equal(config.instances.find(i => i.id === 'codex-worker')?.model, 'user-selection-required');
  assert.equal(config.instances.find(i => i.id === 'fake-worker')?.model, 'write');
  assert.equal(config.instances.find(i => i.id === 'fake-reviewer')?.model, 'review');
});
test('endpoint transports are distinct from providers/roles, declarative only, and reference checked', () => {
  const config = defaultConfig();
  const endpoints = [{id:'local', kind:'local'}, {id:'remote', kind:'ssh-stdio', target:'user@host'}, {id:'service', kind:'external-agent'}];
  const input = {...config, endpoints, instances:config.instances.map(i => ({...i, endpointId:i.role === 'worker' ? 'remote' : 'local'}))};
  assert.deepEqual(validateConfig(input), input);
  assert.throws(() => validateConfig({...input, endpoints:[]}), /endpoint/);
  assert.throws(() => validateConfig({...input, endpoints:null}), /endpoints/);
  assert.throws(() => validateConfig({...input, instances:input.instances.map(i => ({...i, endpointId:null}))}), /endpointId/);
  assert.throws(() => validateConfig({...input, endpoints:[endpoints[0],endpoints[0]]}), /duplicate endpoint/);
  assert.throws(() => validateConfig({...input, endpoints:[endpoints[0]]}), /unknown endpoint/);
  assert.throws(() => validateConfig({...input, endpoints:[{id:'local', kind:'muse'}]}), /unsupported endpoint/);
  assert.throws(() => validateConfig({...input, endpoints:[{id:'local', kind:'local', target:'host'}]}), /cannot specify target/);
  assert.throws(() => validateConfig({...input, endpoints:[...endpoints,{id:'bad', kind:'external-agent', target:'https://user:password@example.test'}]}), /credentials/);
  assert.throws(() => validateConfig({...input, endpoints:[...endpoints,{id:'bad', kind:'ssh-stdio', token:'secret'}]}), /unknown field/);
  assert.throws(() => validateConfig({...input, instances:input.instances.map(i => ({...i, provider:'ssh-stdio'}))}), /unsupported provider/);
});
