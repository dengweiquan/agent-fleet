# Future voice and chat control interface

This document is a future contract and staged roadmap, not an implemented API or a release prerequisite. It does not expand the current deployment. AgentFleet remains an independent runtime: voice and chat provide a unified control surface, while the program owns scheduling, durable state, verification and acceptance.

The initial transport can reuse existing bounded CLI invocations and structured SSH requests. A verified MCP interface and event delivery can follow later. No new listener, message queue, key, token or authentication flow is introduced by this proposal. A generic remote-shell command is not part of the control API.

## Scope and command envelope

Every request identifies one project and, where applicable, one orchestration session. Scope is resolved against an authorized project registry; a human-readable project name or a caller-supplied filesystem path cannot grant access. The same task name in two projects does not identify the same task. A controller must show the resolved project/session before consequential actions when the user's reference is ambiguous.

Proposed envelope fields:

| Field | Contract |
| --- | --- |
| `version` | Explicit control-contract version |
| `requestId` | Client-generated stable idempotency identifier, persisted before delivery |
| `operation` | One allowlisted operation; never arbitrary shell or executable code |
| `projectId` / `sessionId` | Exact authorized scope; creation may omit the not-yet-created session ID |
| `expectedRevision` | Optional state precondition that rejects a stale control decision |
| `payload` | Operation-specific validated data with documented size limits |
| `payloadHash` | SHA-256 of a versioned deterministic encoding of operation, scope, revision and payload |
| `artifactBindings` | Expected plan, task, candidate commit or review hashes when the operation depends on an artifact |

Authentication and effective permissions come from the verified transport/controller identity, not from a payload claiming to be an administrator. The deterministic encoding must have published fixtures before implementation; callers must not independently guess how to hash JSON.

Within the durable ledger's idempotency scope, the same `requestId` and hash returns the original receipt/result. Reusing an ID with different content is a conflict. Network retries preserve the original ID and content; changing the instruction is a new request. Unknown delivery or side effects require lookup and reconciliation, not a new ID intended to force another execution.

## Receipts, results and authority

A proposed receipt contains `requestId`, `payloadHash`, resolved scope, durable receipt ID, command state and event cursor. A receipt marked `accepted` means the command was durably received and authorized for handling. It does **not** mean the requested work completed, a worker succeeded, or a task passed acceptance. Expose the distinction explicitly as `commandState` and `taskState`.

Durable command states may distinguish received, queued, applying, completed, rejected and unknown. Results bind the effective state revision and resulting artifact IDs/hashes. A command can complete by successfully recording a pause or cancellation request while the associated worker is still running. Task acceptance remains a separate program gate over the exact candidate, independent verification and independent review. A model's statement of completion or approval never overrides this gate.

## Allowlisted operations and permissions

These are proposed operation families. They do not imply that equivalent formal control handlers already exist.

| Operation | Required authority and binding | Meaning and boundary |
| --- | --- | --- |
| Create a session | Create permission for one registered project; approved configuration snapshot | Creates durable orchestration state; does not automatically dispatch models |
| Propose or adopt a plan | Planning permission; separate adoption permission; exact proposal/schema/DAG hash | A proposal is not authorization to execute its acceptance commands; adoption records the reviewed version |
| Read status/events | Observation permission for the project/session; bounded cursor/page | Reads evidence without starting work, advancing execution or broadening scope |
| Append instructions | Instruction permission; exact session/task/run target and expected revision | Records a new instruction at a defined scheduling boundary; never silently changes an in-flight prompt or verified candidate |
| Pause scheduling | Control permission for the exact session or task set | Stops new dispatch at the defined boundary; does not imply existing processes were terminated |
| Cancel work | Cancellation permission; exact run/attempt and ownership binding | Requests cancellation through the owning coordinator; returns a request receipt until outcome is verified |
| Resume a session | Resume permission; exact session and retained state | Reconciles locks, run identity, worktrees, leases and uncertain effects first; does not blindly replay unknown work |
| Read review/evidence | Observation permission; candidate/review hashes | Returns bounded review findings and verification evidence; reading is not acceptance |
| Apply an acceptance decision | Explicit acceptance authority; exact verified candidate and independent review bindings | Invokes the program gate; a voice/chat summary or an unbound approval string is insufficient |

Observation, planning, instruction and execution/control permissions stay distinct. A conversation that can read status cannot automatically stop, retry or change resources. Existing permissions and the user's instructions remain authoritative; the interface must not add a blanket approval prompt to every harmless read or silently widen an earlier authorization.

Plan adoption, resource/model selection and real execution continue to respect explicit model, endpoint, repository, artifact and budget boundaries. A control request cannot enable a new provider, install a dependency, approve CLI permissions, publish code or change credentials unless that separate action is specifically authorized and supported.

## Durable delivery and asynchronous operation

The controller persists its command outbox before sending. AgentFleet commits a validated command inbox/ledger entry and receipt before publishing acknowledgment. Its event journal provides an ordered cursor for bounded replay; the controller persists processed events and its cursor before acknowledging consumption. Duplicate events are idempotent, conflicting identities are rejected, and a reconnect resumes from committed evidence.

Instruction delivery needs two distinct facts: command received and instruction applied. For an already-running task, the runtime either records an instruction for the next supported boundary or reports that the provider cannot accept it in that run. It must not fabricate live prompt injection or claim an instruction took effect merely because the chat frontend displayed it.

AgentFleet continues authorized work independently of whether voice/chat is online. Closing a conversation or losing audio does not cancel workers or free uncertain leases. When the user returns, the interface reads the persisted project/session state and explains what happened; it does not rely on the conversational model remembering a past status report.

Push notifications and a Dot wake mechanism are unverified integration capabilities. Do not promise immediate delivery, automatic model wake-up or real-time execution. Until an authenticated event/wake path is implemented and tested, bounded explicit status/event reads remain the fallback. A live connection is not evidence that a model is awake.

## Incremental roadmap

| Milestone | Deliverable | Explicit limit |
| --- | --- | --- |
| M0: existing CLI readback | A limited controller maps reviewed requests to existing bounded CLI/SSH operations, reads structured results and preserves project/session scope | No general command ledger or transport-wide exactly-once guarantee; do not automatically retry an uncertain mutation |
| M1: persistent command ledger | Versioned schemas, validated permissions, request/hash conflict checks, durable receipts/results, outbox/inbox and event cursor recovery | Implement and fault-test before claiming replay-safe control; task side effects still require reconciliation |
| M2: verified MCP and events | Narrow MCP tools and authenticated event delivery using the same ledger, scopes and artifact bindings | Verify actual client support, push/wake behavior and failure recovery; no invented APIs or new deployment permissions |

M1 verification should include lost receipts after a committed action, duplicate and conflicting requests, stale revisions, wrong-project targets, revoked permissions, interrupted cancellation, disconnected UI recovery and instruction acknowledgment before application. M2 adds capability discovery, event duplication/reordering, reconnect replay and unsupported wake behavior. These are future acceptance criteria, not completed test claims.

Current release work proceeds without implementing this interface. The document establishes a small compatible direction: a unified human control surface over an independently running, evidence-driven AgentFleet core.
