import type { Config, Plan } from './types.ts';

function object(value: unknown, keys: string[], at: string, optional: string[] = []): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${at}: expected object`);
  const out = value as Record<string, unknown>;
  for (const key of Object.keys(out)) if (!keys.includes(key) && !optional.includes(key)) throw new Error(`${at}: unknown field ${key}`);
  for (const key of keys) if (!Object.hasOwn(out, key)) throw new Error(`${at}: missing ${key}`);
  return out;
}
function text(value: unknown, at: string, max = 16384): string {
  if (typeof value !== 'string' || !value.trim() || value.length > max || value.includes('\0')) throw new Error(`${at}: invalid text`);
  return value;
}
function id(value: unknown, at: string): string {
  const result = text(value, at, 80);
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(result)) throw new Error(`${at}: unsafe identifier`);
  return result;
}
function array(value: unknown, at: string, max: number): unknown[] {
  if (!Array.isArray(value) || value.length > max) throw new Error(`${at}: expected array of at most ${max} items`);
  return value;
}
function strings(value: unknown, at: string, max = 128): string[] {
  const result = array(value, at, max).map((v, i) => id(v, `${at}[${i}]`));
  if (new Set(result).size !== result.length) throw new Error(`${at}: duplicate values`);
  return result;
}
function boundedInteger(value: unknown, at: string, min: number, max: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < min || (value as number) > max) throw new Error(`${at}: expected integer ${min}..${max}`);
  return value as number;
}

/** Validation constructs a fresh value; callers cannot mutate the validated source by alias. */
export function validatePlan(value: unknown): Plan {
  const p = object(value, ['version', 'goal', 'tasks'], 'plan');
  if (p.version !== 1) throw new Error('plan: unsupported version');
  const tasks = array(p.tasks, 'tasks', 128).map((value, index) => {
    const at = `tasks[${index}]`;
    const t = object(value, ['id', 'title', 'prompt', 'dependencies', 'capabilities', 'acceptance'], at);
    const acceptance = array(t.acceptance, `${at}.acceptance`, 32).map((command, i) => {
      const args = array(command, `${at}.acceptance[${i}]`, 64).map((arg, j) => text(arg, `${at}.acceptance[${i}][${j}]`, 4096));
      if (!args.length) throw new Error(`${at}: empty acceptance command`);
      return args;
    });
    if (!acceptance.length) throw new Error(`${at}: independent acceptance commands required`);
    return { id: id(t.id, `${at}.id`), title: text(t.title, `${at}.title`, 256), prompt: text(t.prompt, `${at}.prompt`), dependencies: strings(t.dependencies, `${at}.dependencies`), capabilities: strings(t.capabilities, `${at}.capabilities`, 32), acceptance };
  });
  if (!tasks.length) throw new Error('plan: at least one task required');
  const lookup = new Map(tasks.map(t => [t.id, t]));
  if (lookup.size !== tasks.length) throw new Error('plan: duplicate task id');
  const visiting = new Set<string>(), visited = new Set<string>();
  const visit = (taskId: string) => {
    if (visiting.has(taskId)) throw new Error(`plan: dependency cycle at ${taskId}`);
    if (visited.has(taskId)) return;
    const task = lookup.get(taskId);
    if (!task) throw new Error(`plan: unknown dependency ${taskId}`);
    visiting.add(taskId);
    for (const dep of task.dependencies) visit(dep);
    visiting.delete(taskId); visited.add(taskId);
  };
  for (const task of tasks) visit(task.id);
  return { version: 1, goal: text(p.goal, 'goal', 4096), tasks };
}

export function validateConfig(value: unknown): Config {
  const c = object(value, ['version', 'instances', 'maxOutputBytes', 'timeoutMs'], 'config', ['endpoints','maxWorkers','maxReviews']);
  if (c.version !== 1) throw new Error('config: unsupported version');
  const endpoints = array(Object.hasOwn(c, 'endpoints') ? c.endpoints : [{id:'local', kind:'local'}], 'endpoints', 64).map((value, index) => {
    const at = `endpoints[${index}]`;
    const e = object(value, ['id', 'kind'], at, ['target']);
    if (!['local', 'ssh-stdio', 'external-agent'].includes(e.kind as string)) throw new Error(`${at}: unsupported endpoint kind`);
    const endpoint: { id:string; kind:'local'|'ssh-stdio'|'external-agent'; target?:string } = {
      id:id(e.id, `${at}.id`), kind:e.kind as 'local'|'ssh-stdio'|'external-agent',
    };
    if (Object.hasOwn(e, 'target')) {
      endpoint.target = text(e.target, `${at}.target`, 1024);
      if (endpoint.kind === 'local') throw new Error(`${at}: local endpoint cannot specify target`);
      if (/[\r\n\t]/.test(endpoint.target) || /(?:password|token|secret|api[_-]?key)\s*[=:]/i.test(endpoint.target) || /:\/\/[^/\s]*:[^/\s]*@/.test(endpoint.target)) throw new Error(`${at}: target must not contain credentials or control characters`);
    }
    return endpoint;
  });
  if (!endpoints.length) throw new Error('config: at least one endpoint required');
  const endpointIds = new Set(endpoints.map(e => e.id));
  if (endpointIds.size !== endpoints.length) throw new Error('config: duplicate endpoint id');
  const instances = array(c.instances, 'instances', 64).map((value, index) => {
    const at = `instances[${index}]`;
    const i = object(value, ['id', 'provider', 'role', 'model', 'capabilities'], at, ['endpointId']);
    if (!['fake', 'claude', 'codex'].includes(i.provider as string)) throw new Error(`${at}: unsupported provider; interface identity must be verified first`);
    if (!['manager', 'worker', 'reviewer'].includes(i.role as string)) throw new Error(`${at}: invalid role`);
    const endpointId = id(Object.hasOwn(i, 'endpointId') ? i.endpointId : 'local', `${at}.endpointId`);
    if (!endpointIds.has(endpointId)) throw new Error(`${at}: unknown endpoint ${endpointId}`);
    return { id: id(i.id, `${at}.id`), provider: i.provider as Config['instances'][number]['provider'], role: i.role as Config['instances'][number]['role'], model: text(i.model, `${at}.model`, 128), capabilities: strings(i.capabilities, `${at}.capabilities`, 32), endpointId };
  });
  if (new Set(instances.map(i => i.id)).size !== instances.length) throw new Error('config: duplicate instance id');
  if (instances.filter(i => i.role === 'manager').length !== 1) throw new Error('config: exactly one manager required');
  if (!instances.some(i => i.role === 'worker') || !instances.some(i => i.role === 'reviewer')) throw new Error('config: separate worker and reviewer required');
  const result = { maxWorkers:boundedInteger(c.maxWorkers??2,'maxWorkers',1,8),maxReviews:boundedInteger(c.maxReviews??1,'maxReviews',1,4),version: 1 as const, instances, endpoints, maxOutputBytes: boundedInteger(c.maxOutputBytes, 'maxOutputBytes', 1024, 16 * 1024 * 1024), timeoutMs: boundedInteger(c.timeoutMs, 'timeoutMs', 100, 3600000) };
  return result;
}

export function defaultConfig(): Config {
  return validateConfig({
    version: 1, maxOutputBytes: 1024 * 1024, timeoutMs: 120000,
    instances: [
      { id: 'claude-manager', provider: 'claude', role: 'manager', model: 'sonnet', capabilities: ['planning', 'conversation'] },
      { id: 'codex-worker', provider: 'codex', role: 'worker', model: 'user-selection-required', capabilities: ['code', 'tests'] },
      { id: 'claude-reviewer', provider: 'claude', role: 'reviewer', model: 'sonnet', capabilities: ['review'] },
      { id: 'fake-worker', provider: 'fake', role: 'worker', model: 'write', capabilities: ['code', 'tests'] },
      { id: 'fake-reviewer', provider: 'fake', role: 'reviewer', model: 'review', capabilities: ['review'] },
    ],
  });
}
