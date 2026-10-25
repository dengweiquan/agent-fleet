# Muse primary connector and five-minute watchdog

The architecture uses a lightweight connector on the Muse host as the primary path. Muse initiates one bounded SSH stdio/JSONL connection to the Mac's fixed AgentFleet operation handler. A native five-minute timer supervises that connector; it is a watchdog, not a daily task puller and not a second task runner. This phase prepares local code and fake-transport evidence; it does not deploy a connector or change timer configuration.

Connection availability and model availability are different states. A live SSH stream proves neither that the Muse model is awake nor that it can immediately execute a task. Muse's formal wake API and sustained execution interface remain unverified. Do not invent a `muse` executable, promise real-time task execution, or use periodic timer invocations as implicit permission for paid model calls.

## Ownership, persistence and message recovery

The connector needs a Muse-approved persistent workspace for its journal, configuration, singleton lock and ownership record. Keep that directory separate from transient VM/sandbox scratch paths. Its durable state includes connector identity, monotonically increasing connection generation, request outbox, received-message inbox, event cursor, unresolved outcomes, disabled/blocked state and retry/circuit state. These records contain identifiers and bounded redacted summaries, never copied credentials or complete private reasoning transcripts.

Callers must redact/sanitize application payloads before enqueueing them; bounded storage and error/audit redaction are not a general secret detector. The journal is bound to a fixed connection-target fingerprint. Repointing an existing journal at another SSH target cannot reuse its cursor or task identities without an explicit reviewed migration.

Persist a request and its stable identity before transport. Persist a reply/inbox entry and committed cursor before publishing it. After any fresh process start or reconnection, reconcile the recorded message/claim/run/lease identities and unresolved reports before permitting new mutations. A lost reply is an unknown outcome; it is not authorization to repeat task execution. Re-delivery of the same protocol report may be deduplicated, but a new task run/claim cannot be manufactured to resolve uncertainty. Ignore or reject a reply from an older connection generation; it cannot acknowledge the current generation's work or advance its cursor.

The local inbox receipt sequence and the replayed server event cursor are separate values. Event IDs may contain gaps, but a new event cannot move the server cursor backward; replaying an identical recorded event is idempotent and conflicting content is rejected. Each new connection generation must confirm the hello/replay barrier against the committed server cursor. Pending sent or unknown outcomes prevent that barrier from opening for new mutations. Without an explicit reconciliation callback/proof, the connector cannot assume it is synchronized. The watchdog may make bounded read-only hello/events health probes; it never pumps task mutations. Spawning an SSH process alone is not a successful health check.

Only one active connector owns the persistent lock. A second process observes the existing owner and cannot silently take over. Missing PID, missing heartbeat, VM restart or old timestamps do not prove that effects ended or that the lock is free. Recovery must establish that the recorded owner belongs to this connector and that its process/connection generation has terminated, using explicit ownership and termination evidence. If that evidence is unavailable, preserve the lock and unknown state and report the blocker.

An explicit disabled flag has the highest priority across process starts and watchdog ticks. It blocks connect, reconnect and automatic restart even if the connection was previously healthy. Clearing it is an explicit configuration/control action, not a watchdog repair. Disabling does not erase unresolved tasks, leases, inbox/outbox entries or event cursors.

## Decision matrix

| Observed condition | Primary connector | Five-minute native watchdog |
| --- | --- | --- |
| Explicitly disabled | Do not connect/reconnect; retain evidence | Report disabled; do not start/restart |
| Healthy owned connection | Maintain the narrow stream and commit protocol events | Bounded read-only health observation; no duplicate connection or task claim |
| Transient connection loss | Mark unanswered mutations unknown; bounded backoff; reconcile before any new mutation | Observe backoff/circuit and ownership; do not race the primary reconnect |
| Retry circuit open | Stop automatic retries for the bounded circuit policy; preserve pending identities | Report circuit state; do not bypass it or reset it every tick |
| Owner/process state uncertain | Retain lock and unknown state | Do not infer death from PID/heartbeat or start a second connector |
| Verified previous owner termination | Recover ownership with explicit evidence; new generation begins with reconciliation | May request the narrow connector restart only if enabled; declare healthy/restarted only after read-only hello/replay confirmation |
| Connected but synchronization unconfirmed | Block new mutations until hello/replay and unresolved outcomes are checked | Report reconciliation-required; do not claim successful health from process spawn |
| Authentication or expired login | Block; report the official interaction needed | No login, new OAuth/token, credential copy or permission approval |
| Invalid config, host fingerprint mismatch or missing dependency | Block with the exact reason | No config rewrite, trust bypass, package install or dependency repair |
| Network-policy/network-access block | Block and report the policy/access condition | No VPN, proxy, route, firewall or network-settings repair |
| Model unavailable or wake interface unknown | Transport may stay available; report execution capability as unknown/unavailable | Never claim a task or invoke a model to "keep it awake" |

Backoff and circuit state persist rather than resetting whenever a process restarts. Transient disconnects are distinct from network-policy blocks: the former may receive bounded retries; the latter require the blocked condition to be resolved explicitly. The watchdog's only automatic action is the narrowly defined connector lifecycle decision. It never submits a task claim, runs acceptance, accepts results, changes a resource pool or starts another project's agent.

## Fixed operation boundary

SSH uses the existing confirmed connection and authentication; no listener, message queue, new key or token is introduced. The Mac side exposes only bounded, validated AgentFleet JSONL operations against the approved shared pool and endpoint identity. It is not a generic shell bridge: protocol payloads cannot choose arbitrary commands, filesystem roots, providers or security flags. Ordinary SSH account permissions still matter; this application protocol is not a claim that the host account has been newly restricted.

Resource registration/connection evidence is not model execution authorization. The shared pool retains project/session/worktree and accepted-commit boundaries. A connector heartbeat cannot free an unknown lease. Real provider tasks, repository/artifact access and spending continue to require their existing explicit scope; an authorized connector start does not broaden that scope.

## Delivery sequence and remaining prerequisites

1. Validate the local state machine and fake transport: persistent journals, reply loss, generation fencing, singleton ownership, disabled priority, conservative VM recovery, bounded retries and blocked conditions.
2. Connect that state machine to the narrow shared-pool SSH JSONL handler. Keep protocol reads/reconciliation separate from new mutations and preserve audit identities across reconnects.
3. Have Muse identify its persistent workspace and actual supported startup mechanism. Record the existing SSH alias, host identity, endpoint/pool scope, connector ownership identity and actual model/wake capability separately. Do not treat self-reported capability as a successful live test.
4. Deploy/start the primary connector only after Muse has approved those concrete host-side locations and lifecycle details; configure the native five-minute watchdog for connector supervision.
5. Verify connection recovery with a synthetic no-model exchange before any separately scoped real task. Record transport success without claiming model wake-up or task acceptance.

No actual Muse-side persistent path, startup deployment or wake API has been established by this local preparation. Native timer installation and live SSH/model behavior remain unexecuted here.

Offline verification command:

```sh
node --experimental-strip-types --test test/connector.test.ts
```

The tests use temporary journals and a controllable fake transport; they do not contact Muse, modify a timer or invoke a model. Their result establishes local protocol/lifecycle behavior only.
