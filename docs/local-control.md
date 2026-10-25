# Local project control candidate

This interface runs locally through CLI/stdio. It introduces no listener, MCP server, credentials, model call or Dot/Muse wake mechanism. The published connector trial remains on its pinned version. The command interpreter is not a security sandbox for another process already holding the same OS account: a trusted launcher must pin a reviewed scope file. Do not expose a caller-selectable scope-file path as a remote tool.

## Scope and invocation

Create a local JSON scope for an **existing** project/session. The launcher chooses the scope, not request JSON:

```json
{"version":1,"project":"/absolute/example-repository","sessionId":"EXISTING_SESSION_ID","permissions":["draft.create","note.append","pause","resume","stop"]}
```

Each permission must reflect existing project authorization. Use an empty permissions array and `--observe` for observation. Project and session are pinned for the lifetime of `serve`; permissions are reread for each request. The control layer never starts a scheduler or provider. `resume` enables scheduling after existing reconciliation; any independent scheduler still needs its own execution/model authorization.

```sh
node --experimental-strip-types src/cli.ts --observe control /absolute/scope.json status
node --experimental-strip-types src/cli.ts --observe control /absolute/scope.json resources
node --experimental-strip-types src/cli.ts control /absolute/scope.json submit < request.json
node --experimental-strip-types src/cli.ts control /absolute/scope.json receipt draft-001
node --experimental-strip-types src/cli.ts control /absolute/scope.json apply draft-001
node --experimental-strip-types src/cli.ts control /absolute/scope.json artifacts
node --experimental-strip-types src/cli.ts control /absolute/scope.json events 0
```

Example `request.json` (use the canonical project path returned by `status`):

```json
{"version":1,"requestId":"draft-001","projectId":"/absolute/example-repository","sessionId":"EXISTING_SESSION_ID","operation":"draft.create","payload":{"title":"Clamp example","text":"Draft a bounded pure-function task; do not execute yet."}}
```

The ID belongs to this project/session ledger. Preserve the same ID and body after a lost response; changed content with the same ID is a conflict. The server computes the deterministic content hash. Clients do not supply a hash or permission assertion. Validation rejects extra fields such as shell commands, arbitrary paths or authorization flags.

## Implemented operations

| Operation | Payload | Completed command means |
| --- | --- | --- |
| `draft.create` | `title`, `text` | A draft artifact is durable. It is not a DAG, adopted plan or authorized job. |
| `note.append` | `text`, optional existing `taskId` | An append-only note is durable. It has not been injected into an active provider prompt or automatically adopted by the scheduler. |
| `pause` | `{}` | Scheduling pause recorded through the existing engine, including its shared remote queue binding. Existing workers may still run. |
| `resume` | `{}` | Existing reconciliation ran and scheduling pause was cleared. No run was replayed, and unknown leases remain reserved. A retained coordinator lock blocks this operation. |
| `stop` | `runId` | Stop request recorded for a local running/verifying attempt, or an explicit no-op result when it is absent/no longer running. Inspect `result.details.stopRequestRecorded`; completion alone does not imply delivery. Process termination remains unverified; remote cancellation is not implemented here. |

`submit` commits an `accepted` receipt without applying any of these effects. `apply ID` is the explicit authorized handling step. `completed` is a command state and **never task acceptance**. Applying draft/note artifacts and their completion receipt uses one SQLite transaction. Engine controls span the session/pool databases: `applying` is committed before the effect, then completion is recorded. Errors or an interrupted apply remain `unknown`; repeated apply does not replay. An unresolved control effect blocks new pause/resume/stop effects in that session; inert drafts and notes remain available. There is no automatic reconciliation/reset API for unknown control commands in this slice. Inspect session/pool evidence before further control decisions; do not invent a new ID to force the same uncertain action.

The ledger lives at the selected session's `control.sqlite`. Query-only operations open it read-only, and absent ledgers return empty results without creating a database. This ledger has its own ordered `event_id` cursor; it is not the session run journal or the shared pool journal. Artifact pagination uses `artifactId`. Pass the last returned cursor as `after`; pages are byte-bounded and can contain fewer records than requested. Stdio events/artifacts requests accept optional `limit` (1–100); defaults are 50 events and 20 artifacts. Resource queries expose the selected session's configured instances and bound leases, not other projects' queue entries. Their capacity view is not a global admission promise. Large status responses fail with a bounded error rather than truncate authoritative data.

## Stdio framing and recovery

```sh
node --experimental-strip-types src/cli.ts control /absolute/scope.json serve
```

One JSON request per line, processed serially. Examples:

```json
{"action":"status"}
{"action":"resources"}
{"action":"submit","request":{"version":1,"requestId":"pause-001","projectId":"/absolute/example-repository","sessionId":"EXISTING_SESSION_ID","operation":"pause","payload":{}}}
{"action":"apply","requestId":"pause-001"}
{"action":"receipt","requestId":"pause-001"}
{"action":"events","after":0}
{"action":"artifacts","after":0}
```

Responses are `{ok:true,data:...}` or `{ok:false,error:{message:...}}`. Input and output frames are at most 64 KiB, output is awaited for backpressure, and each frame gets one response unless the input exceeds the bound or transport closes. Request IDs correlate durable mutation receipts; the local serial stream has no multiplexed query IDs. EOF leaves session workers alone. Reconnect and read receipts/events; the client remains responsible for persisting request IDs and its event cursor. No client outbox or unsolicited reverse event delivery is claimed.

Muse's reported capability boundary is now explicit: ordinary Node programs have no supported official task-start CLI, SDK or HTTP API. Agent-internal tools and native cron can invoke agents within their own authorized context, but the connector cannot call those tools. Connector recovery or a five-minute connection watchdog does not itself start a model. An already active Muse agent or an explicitly configured native agent schedule remains a separate execution path. Unsupported internal sockets are not an integration fallback. Real-time automatic execution would require a provider with a supported programmatic execution interface and explicit user selection; no model substitution occurs automatically. See [capability matrix](capabilities.md).
