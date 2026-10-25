# Shared resources across project sessions

This is a source preview. Use isolated trial repositories and journals. No real provider or SSH execution is needed for the examples below. Use separate temporary repositories and an isolated `AGENT_FLEET_HOME`; old session histories are not migrated automatically.

The pool separates provider, model, endpoint, account capacity and executable resource from project roles. A resource may fill a role for one task and later serve another project. Initializing a project does not reserve a permanent team. Every dispatched invocation has its own run, provider session, worktree and persisted resource/lease snapshot. A downstream task consumes accepted commits belonging to its own project, never another project's worktree or unaccepted result.

## Registry and capacity

`pool configure FILE` validates and upserts explicit resource definitions and limits. It does not install tools, authenticate providers or discover account quotas. Omitting a previously registered resource does not delete it. Conflicting endpoint/account identities are rejected; definitions cannot change while queued, held or unknown work exists. Status and events are read-only.

Configuration is additive and is **not one atomic transaction for the entire file**. Submitted definitions are validated first, but each resource update and the limits update commit individually. A later failure can leave earlier updates applied. Inspect `pool status` and `pool events` before an explicit correction; do not assume the whole configuration rolled back or automatically repeat a changed configuration while work is active.

An offline pool for two projects can use the following `pool.json`. Replace the project keys with canonical absolute roots of prepared isolated repositories:

```json
{
  "version": 1,
  "resources": [
    {"id":"worker-a","provider":"fake","model":"delay-write","endpointId":"local","endpointKind":"local","accountDomain":"worker","capabilities":["code"],"roles":["worker"],"capacity":1},
    {"id":"worker-b","provider":"fake","model":"delay-write","endpointId":"local","endpointKind":"local","accountDomain":"worker","capabilities":["code"],"roles":["worker"],"capacity":1},
    {"id":"reviewer","provider":"fake","model":"review-approve","endpointId":"local","endpointKind":"local","accountDomain":"review","capabilities":["review"],"roles":["reviewer"],"capacity":1}
  ],
  "limits": {
    "global":3,
    "projects":{"/absolute/project-alpha":2,"/absolute/project-beta":2},
    "endpoints":{"local":3},
    "accounts":{"worker":2,"review":1}
  }
}
```

The same SQLite pool path must be shared by participating coordinators. Copies are separate pools and do not enforce a common capacity limit. Limits apply at global, canonical project, endpoint, account-domain and individual-resource scopes. Missing capacity information defaults conservatively to one; omission is not unlimited capacity. Price information is unknown unless explicitly supplied, and configured prices are metadata rather than billing guarantees.

All local resources on this host must use the same endpoint identity. All local resources for the same real provider must also share one account domain: no authentication-profile selector exists, so assigning different account labels would manufacture false capacity. The fixture's separate fake worker/review account labels are only test scopes. Registering an account domain neither authenticates it nor copies credentials.

## Explicit fleet participants and role policies

A `fleet.json` names the exact sessions participating in one finite scheduling pass. Each project must have an adopted plan; only one session per canonical project is included in a fleet dispatch.

```json
{
  "version":1,
  "projects":[
    {
      "root":"/absolute/project-alpha",
      "sessionId":"REPLACE_WITH_ALPHA_SESSION_UUID",
      "roles":{
        "worker":{"capabilities":["code"],"preferredProvider":"fake","requiredProvider":"fake"},
        "reviewer":{"capabilities":["review"],"requiredProvider":"fake"}
      }
    },
    {
      "root":"/absolute/project-beta",
      "sessionId":"REPLACE_WITH_BETA_SESSION_UUID",
      "roles":{"worker":{"preferredProvider":"fake"},"reviewer":{"requiredProvider":"fake"}}
    }
  ]
}
```

Required provider is a hard eligibility constraint. Preferred provider ranks eligible resources; it may fall back only to another already-authorized compatible provider. Capabilities from role policy are added to the task's requirements. The configured manager provider is required by default; a manager proposal never silently substitutes an arbitrary worker provider. Register an eligible manager resource before using `fleet propose`.

The centralized scheduler arbitrates fairly among eligible queued work from its explicit participating projects, subject to dependencies, role limits and shared capacity. It is finite: it does not discover every project, poll forever or install a daemon. Independent coordinators sharing this pool remain capacity-safe, but strict fairness between separately running coordinators is not guaranteed. Paused, absent or quarantined participants do not create a promise of eventual progress.

## Run from the source checkout

After preparing the temporary projects/configuration files, use the source checkout path:

```sh
PHASE2="$PWD"
export AGENT_FLEET_HOME='/absolute/isolated-trial/history'
POOL='/absolute/isolated-trial/resources.sqlite'

node --experimental-strip-types "$PHASE2/src/cli.ts" --pool "$POOL" --json pool configure /absolute/isolated-trial/pool.json
node --experimental-strip-types "$PHASE2/src/cli.ts" --pool "$POOL" --observe --json pool status
node --experimental-strip-types "$PHASE2/src/cli.ts" --pool "$POOL" --observe --json fleet schedule /absolute/isolated-trial/fleet.json --dry-run
node --experimental-strip-types "$PHASE2/src/cli.ts" --pool "$POOL" --json fleet schedule /absolute/isolated-trial/fleet.json
node --experimental-strip-types "$PHASE2/src/cli.ts" --pool "$POOL" --observe --json pool events 0
node --experimental-strip-types "$PHASE2/src/cli.ts" --project /absolute/project-alpha --observe --json sessions list
node --experimental-strip-types "$PHASE2/src/cli.ts" --project /absolute/project-alpha --observe --json resume ALPHA_SESSION_UUID
```

`fleet schedule --dry-run` reads eligible resources, roles and candidate tasks without reserving leases or mutating session events. `--observe` permits this preview and pool status/events, but rejects registry configuration and execution. Project filtering resolves the actual Git root; explicit resume cannot escape its selected project scope.

A successful schedule launch returns a detached coordinator PID and preview. It does not mean completion. Observe the pool leases and each project's run/evidence history. A normal successful task passes worker execution, independent fixed-commit verification, a separate reviewer invocation, and the transactional acceptance gate. Lease completion alone is not task acceptance. Leaving the frontend does not cancel its coordinator.

Pool events also record coordinator start, finish and error. A coordinator finish describes the end of its finite pass; paused, blocked or unresolved tasks may remain. Confirm the per-project task outcomes rather than treating that audit event as blanket acceptance.

For a manager turn, `fleet propose FLEET_FILE SESSION_ID MESSAGE` obtains a manager lease. Its validated proposal still requires explicit operator adoption before executable acceptance commands become a plan. A pool preview may list several eligible real models; approve that exact scope before using a live fleet.

## Unknown outcomes, retries and live gates

Held and unknown leases continue to occupy capacity. Timeout, frontend exit, absent PID, stale heartbeat or restart does not recycle unknown capacity. Session run identity, lease fence, worktree and side effects need reconciliation; no automatic unlock or reallocation is implied. Repeating a finite schedule does not blindly replay an existing attempt. Retry, resource changes or a change of pool are separate explicit boundaries after inspection; this CLI does not expose a blanket unknown-lease reset.

Real model calls require explicit models, verified official login availability, approved project/worktree scope and `--allow-live --budget-per-run-usd N`. N applies to each worker/reviewer/manager call separately, not the entire fleet. Claude supports its CLI cost bound; Codex has no enforced monetary cap. No new OAuth/token, permission auto-approval or billing control is introduced. An arbitrary `preferredProvider` or configured price cannot authorize paid calls.

The fleet scheduler invokes local adapters. Remote endpoints use the separate fleet-remote reverse protocol, which atomically shares the same SQLite resource leases. A remote endpoint never falls back to local execution. See connector.md for the primary channel and its model-wake limitation. Muse model/interface identity and any live remote task need their own verified contract. xAI/Grok is unsupported, only a future integration slot.

## Offline reproducible evidence

```sh
node --experimental-strip-types --test test/fleet-cli.test.ts
```

This test creates two temporary Git projects and invokes actual CLI subprocesses. It validates registry and observer behavior, no-write previews, malformed roles and unsupported providers, project-scoped history/resume, and detached execution with one or two shared fake workers plus an independent fake reviewer. It polls durable leases, per-project accepted states and coordinator lock release within a bounded 10-second completion window; it does not treat a launch response as success. Synthetic Git commits are fixture/candidate artifacts only, not commits to the product worktree. No package installation, live model or SSH call occurs.
