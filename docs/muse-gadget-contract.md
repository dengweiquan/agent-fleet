# Conditional Muse Gadget message bridge — offline contract

Product priority: deferred personal isolated experiment. The offline mapper is retained as tested evidence-processing code; live Gadget integration is not on the mainline. Continue native scheduled Muse wake-up and the queue loop without requiring Gadget hardware or pairing.

Status: an original offline event mapper and evidence journal are implemented; the live bridge below remains a source-verified design. No SDK code has been installed or executed, no token created/read/copied, no device paired, and no message sent. This document corrects the earlier overbroad statement that Muse has no official programmatic message entry point. The current unpaired Muse VM's limitations do not establish that claim for other environments.

## Pinned official evidence

Meta `facebookincubator/muse-gadget-sdk`, commit **c9354b7a765cfb7dd3c4f1945a9ca838d871630c**:

- [Linux README](https://github.com/facebookincubator/muse-gadget-sdk/blob/c9354b7a765cfb7dd3c4f1945a9ca838d871630c/linux/README.md): paired Linux device setup and `send-user-msg --session-id` side chats.
- [CLI](https://github.com/facebookincubator/muse-gadget-sdk/blob/c9354b7a765cfb7dd3c4f1945a9ca838d871630c/linux/src/musegadget/cli.py): local Unix socket delivery; successful command prints delivery confirmation, not a task result or returned message ID.
- [LinkSession](https://github.com/facebookincubator/muse-gadget-sdk/blob/c9354b7a765cfb7dd3c4f1945a9ca838d871630c/linux/src/musegadget/link_client.py): `send_chat` returns an acknowledgment and `subscribe_chat` exposes raw chat events through the paired session. These are SDK internals to integrate through the supported device lifecycle, not a credential-free public HTTP endpoint to call directly.
- [MuseTurn](https://github.com/facebookincubator/muse-gadget-sdk/blob/c9354b7a765cfb7dd3c4f1945a9ca838d871630c/linux/src/musegadget/muse_turn.py): message/session/parent/task correlation and presentation events. `complete()` can return after three seconds of quiet; `TaskFinished` erases the distinction among completed, failed, errored, cancelled and canceled.
- [Pebble bridge example](https://github.com/facebookincubator/muse-gadget-sdk/blob/c9354b7a765cfb7dd3c4f1945a9ca838d871630c/linux/examples/pebble_ring_bridge.py): an official example forwards webhook text through the CLI. AgentFleet will not copy or start its listener, shared secret or deployment configuration.

Only protocol observations are used here; no upstream implementation is copied into AgentFleet. Upstream code is Apache-2.0; future reuse must preserve applicable notices and attribution. SDK source licensing and the SDK-token service terms are separate. The [official token terms](https://gadgets.muse.ai/sdk-terms), checked 2026-10-25, limit use to personal, non-commercial use with the owner's own Muse account and at most 50 total devices per token. They describe the SDK/token service as unsupported and not a developer platform. This integration is labeled `personal-experimental`, not a generally available commercial provider. Reading these terms does not request a token or accept them on the user's behalf. AgentFleet's original code remains MIT; the [upstream SDK code](https://github.com/facebookincubator/muse-gadget-sdk/blob/c9354b7a765cfb7dd3c4f1945a9ca838d871630c/LICENSE) is Apache-2.0, not MIT. No SDK runtime dependency or upstream implementation is bundled.

## Endpoint and approval boundary

Proposed path: AgentFleet coordinator → explicitly authorized, already paired Linux Gadget bridge → dedicated Muse sidechat → subscribed reply events. Existing Muse-to-Mac SSH pull/claim/ACK/result remains the execution evidence path. A chat reply cannot replace a bound commit, independent verification or review.

Minimum documented setup is a networked Linux computer with Bluetooth LE (a supported Raspberry Pi is one option), a supported Linux distribution, an installer account with sudo, an SDK token, and the phone Muse app with Developer Mode enabled for pairing. The current plan uses a separate stable physical Linux/BLE bridge near the phone for the ten-minute pairing window. It is not the task execution host. The reported headless Muse VM has no BLE pairing path; do not bypass pairing or assume native Mac support, phone proximity to a remote VM, or usable BLE in a VM. After approved pairing, messages use the bridge's existing internet connection and local service socket.

The stock installer installs dependencies and a persistent service. The paired device exposes shell/file commands with its selected account's permissions, including sudo if available to that account. This is broader than a message sender. The proposed minimum-permission bridge uses a dedicated non-sudo execution account, no business repositories, no copied provider credentials, and only explicitly approved state/socket access. An installer may require sudo; that does not authorize giving Muse the installer/admin account. No stock message-only sandbox is assumed. Before any deployment, review the exact host, service/dependency changes, execution account and device-command exposure; token creation, Developer Mode and pairing each need explicit user authorization. Local message producers reuse the paired service without their own Muse credential, but initial pairing still requires a token. AgentFleet must not read/copy the service's tokens or reproduce its private authentication plumbing.

## Proposed message and event contract

Bind `{projectId, sessionId, taskId, runId, attemptId, endpointId, requestId, sidechatId, nonce}` in a durable local outbox before sending. Keep an immutable sidechat per attempt for the first experiment. Treat the SDK's chat session ID and acknowledged user-message ID as provider identifiers, distinct from AgentFleet run/session IDs. A reviewed bridge must expose the SDK acknowledgment and raw subscription events; the stock CLI's success text alone cannot supply a complete correlation contract.

Subscribe before sending, buffer a bounded number of pre-ACK events, then require an ACK confirming the exact sidechat (`session_id`, `is_thread`) and user-message identity. Never reuse a main chat. Preserve raw relevant fields and correlation evidence in a bounded, redacted journal. The offline mapper records the following evidence; it is not wired into scheduler completion or a live transport:

| Input | AgentFleet evidence | Must not imply |
| --- | --- | --- |
| CLI exit 0 / delivery ACK | `message.delivered`; persist provider message ID only if actually returned | Agent started, task finished, or accepted |
| Bound `agent.status` | Public activity code/text, matching session and message/parent/task bindings | Full private reasoning or business progress |
| Bound assistant message/delta | Reply with its message ID, parent lineage and final-text indicator | Code committed, verification passed or task success |
| Bound `task.status` | Task ID plus **original** status: completed/failed/errored/cancelled/canceled | `completed` is independent acceptance |
| Three-second quiet / `MuseTurn.complete()` | Presentation quiescence only | Success or proof that execution ended |
| Disconnect or lost send ACK | Unknown delivery/outcome; reserve attempt and reconcile | Safe automatic resend |

Session match alone is insufficient on a reused/shared chat. Require a validated parent chain to the acknowledged message or a task binding established for that attempt. Reject unrelated, retired or conflicting IDs. Do not convert all `TaskFinished` objects into success: consume the raw status before the helper loses that field. An unbound task terminal is quarantined, not adopted. If provider event IDs are absent, do not hash identical deltas into a false exactly-once guarantee: persist ingress sequence/connection generation, and treat uncertain replay as a reconciliation problem.

The SDK creates transport request IDs, but the inspected methods do not establish application-level send idempotency for our requestId. AgentFleet's durable request/ingress identity is application evidence, not an official exactly-once guarantee or an SDK offline send queue. The bridge must persist send intent before any future send and record the start of its single attempt. Interruption before a trustworthy result leaves outcome unknown; a lost ACK must not trigger automatic resend. Cancellation is unsupported by this adapter, and no official programmatic cancellation or durable replay-cursor guarantee is established by the inspected files. Do not translate a stop request into an invented SDK cancel call.

## First future live check and offline acceptance cases

After the specific host/setup/message approval, the first live check sends exactly one bounded, non-business nonce prompt into a new dedicated sidechat and asks for the same nonce in its reply. No repository work, shell task, scheduled follow-up or file request is part of this test. Record SDK version, send attempt, ACK identifiers, bound reply, any raw task status and the timeout result. A nonce echo demonstrates message roundtrip only; task acceptance remains unavailable in this diagnostic.

Offline adapter acceptance cases: ACK-only stays delivered; three seconds quiet never succeeds; failed/cancelled status is preserved; wrong sidechat and wrong parent are rejected; pre-ACK events remain pending; duplicate ACK cannot start a second attempt; lost ACK produces unknown; repeated identical text deltas are not blindly collapsed; reconnect does not resend; a completed task still requires the existing independent acceptance gate. No live transport or live-device test is claimed by this document.

## Offline integration boundary

`src/gadget-events.ts` is a pure typed reducer over bounded JSON fixtures. It never opens a socket, starts a process or imports the SDK. `src/gadget-evidence.ts` validates the project/session/task/run/attempt/endpoint against an existing remote queue offer before writing reducer state and session audit events atomically. External attempts are represented with explicit binding data on session events, not forged local-run foreign keys. Querying evidence does not dispatch work.

The evidence journal is separate from the queue's claim/ACK/result protocol: observing even a raw `completed` status neither releases a resource lease nor accepts a candidate. Stable bridge ingress IDs deduplicate exact replay; conflicting reuse is rejected. The store uses a revision compare-and-swap so concurrent updates cannot overwrite one another. After a stale write, reload evidence before retrying the same input. No automatic network send is part of this retry.

Within one orchestration session, immutable attempt bindings and unique sidechats are enforced. A future deployed bridge must also reserve sidechat identity across sessions/projects; this offline session journal is not a global sidechat registry. Buffers, histories and the stored snapshot have finite capacity and fail closed rather than evict deduplication evidence. The program records an event as public observation, never as complete model reasoning or independent verification.
