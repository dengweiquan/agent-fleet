import {existsSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const tsc=process.env.AGENT_FLEET_TSC,types=process.env.AGENT_FLEET_NODE_TYPES;
if(!tsc||!types||!existsSync(tsc)||!existsSync(types+'/node/package.json'))throw new Error('Set AGENT_FLEET_TSC to an existing TypeScript compiler file and AGENT_FLEET_NODE_TYPES to an existing @types directory. This command never installs packages. Runtime execution does not require tsc.');
const r=spawnSync(process.execPath,[tsc,'--project',fileURLToPath(new URL('../tsconfig.json',import.meta.url)),'--typeRoots',types],{stdio:'inherit'});process.exitCode=r.status??1;
