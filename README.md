# AgentFleet

A CLI-first, TypeScript/Node source preview for multi-project agent orchestration. Projects define roles and task dependencies; a persistent resource pool assigns available provider instances across projects. Execution, verification and independent review are separate stages. A successful process exit never means a task was accepted.

## Run locally

Requires Node **22.18 or newer**, Git, and an existing SSH client for the optional connector. Runtime code uses Node built-ins, including `node:sqlite`. No npm install, native database package or TypeScript compilation is required: Node executes the TypeScript sources with type stripping.

```sh
git clone https://github.com/dengweiquan/agent-fleet.git
cd agent-fleet
node --experimental-strip-types src/connector-client.ts selfcheck
npm test
npm run demo:parallel
npm run demo:fleet
```

The demos create isolated synthetic Git repositories and fake provider processes. They do not call a model or contact a remote host. SQLite experimental warnings on stderr may be expected on supported Node versions.

From a Git project directory, run `/absolute/agent-fleet/bin/agent-fleet init`, then the CLI without arguments for interactive mode. Use `sessions list`, `resume`, `status`, `config`, `plan`, `propose` and `schedule`. Add `--observe --json` for machine-readable observation. Exiting the frontend does not cancel detached workers.

## Shared resources and project isolation

`pool configure` registers provider/model, endpoint, account domain, capabilities and capacity. `fleet schedule` leases resources dynamically for the explicitly listed project sessions. Limits cover global, project, endpoint, account and instance capacity; unknown capacity defaults to one. Role requirements and preferred providers are distinct, and an explicitly configured manager provider is respected.

Each run receives a separate provider session, immutable base SHA and worktree. Dependencies consume accepted commits. Independent verification and a distinct reviewer produce evidence for the program's acceptance gate. Unknown outcomes retain their leases and cannot be replayed automatically. See [resource-pool usage](docs/resource-pool.md).

Local adapters support Claude Code, Codex and controlled fake processes. Real calls require an explicit model and per-run authorization/budget flags. Existing official authentication stays with its provider. Quota can be unknown; Codex has no enforced monetary ceiling. OpenCode and xAI/Grok adapters are not implemented.

## Primary reverse connector

The remote execution host initiates an SSH stdio JSON-lines connection to a fixed coordinator handler. `fleet-remote` offers, claims, ACKs and reports share the same SQLite transaction and capacity pool as local workers. Project/session/run/attempt/endpoint/resource/lease/fence identities are checked, reports are deduplicated, and late results cannot free an unknown lease. No application listener, new credential or public port is needed.

The shipped [foreground connector](docs/client-start.md) currently maintains a **read-only primary observation channel** with bounded event waits, durable outbox/inbox and replay cursors. Its lifecycle library implements single-owner generations, explicit disable, bounded reconnect/backoff and a watchdog that requires verified hello/replay health. A five-minute native watchdog is a fallback lifecycle mechanism, not the main task scheduler.

**A working connection does not wake a Muse model.** Muse's capability report states that ordinary Node programs have no supported official task-start CLI, SDK or HTTP API; native agent tools are available only inside the agent platform. This release does not implement a Muse task executor, automatic artifact import or automatic remote task acceptance. Real remote deployment, existing SSH scope, persistent workspace and any supervisor integration must be configured explicitly. The software never creates a timer, system service, SSH key, OAuth token or firewall rule.

Remote claims are disabled unless the fixed coordinator fleet manifest explicitly grants `remoteClaimsAuthorized: true` for a session. A capability report is not an execution authorization. The observer client cannot claim tasks even when such a grant exists.

## Verification and limitations

The [project-scoped control interface](docs/local-control.md) accepts structured local CLI/stdio requests with durable idempotent receipts. It can inspect status/resources, record inert drafts and notes, and explicitly apply pause/resume/stop requests. Command receipt, control completion and task acceptance remain separate. Uncertain control effects are not replayed automatically.

This is a local integration surface for a future unified voice/chat entry point, **not an installed Dot/platform MCP integration**. No client wake API, model invocation or remote service is added. Muse model wake is unavailable to the Node connector through a supported interface. Upgrading this source does not upgrade an existing connector checkout or deploy a watchdog; pinned clients and coordinator copies stay on their selected commits.

`npm test` runs local fake-process and protocol failure-injection tests, including overlapping workers, shared account capacity, cross-project identities, lost replies, duplicate messages, unknown recovery, singleton ownership and bounded watchdog behavior. These tests are not evidence of real Muse wake-up or production deployment.

The finite centralized fleet is fair among its participating projects; independent coordinators share atomic capacity enforcement without a strict fairness guarantee. Configuration is additive per-resource updates, not a whole-file atomic replacement. Older session formats are read-only in this source preview; there is no automatic in-place migration.

Optional static checking uses an existing TypeScript 5.9 compiler and Node typings: set `AGENT_FLEET_TSC` and `AGENT_FLEET_NODE_TYPES`, then run `npm run typecheck`. The script never installs dependencies.

## License and references

Independent AgentFleet source is released under [MIT](LICENSE). [Reference projects](references/upstream-index.json) retain their own licenses; their repositories are not bundled or runtime dependencies. No provider affiliation is implied.
