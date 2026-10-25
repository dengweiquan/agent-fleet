# Execution and integration capability boundaries

These are distinct layers. Transport health is not agent availability, and a provider's report is not an independently tested execution result.

| Surface | Available mechanism | Evidence and limit |
| --- | --- | --- |
| Local control | Project/session-scoped CLI and JSONL stdio; receipt lookup, inert drafts/notes and explicit controls | Implemented and tested with local synthetic fixtures. No installed Dot/platform MCP or push/wake integration. |
| Remote connector | Remote-initiated SSH stdio observation with durable event replay | A finite read-only Muse-host trial was reported successful and matched local coordinator records. It has ended; this is not proof of a resident service or model execution. |
| Current unpaired Muse VM | No directly usable task-start API reported for ordinary Node | Environment-specific capability report, not a universal absence of official message interfaces. Agent-internal tools are not callable by the current connector. |
| Official paired Linux Gadget | `send-user-msg --session-id`, SDK `send_chat` and `subscribe_chat` | Source-verified at Meta upstream `c9354b7a765cfb7dd3c4f1945a9ca838d871630c`. Requires approved installation, SDK token and phone/BLE pairing. Not installed, paired or live-tested by AgentFleet. Message ACK is not task success. See [contract](muse-gadget-contract.md). |
| Muse execution inside its platform | An already active agent or an authorized native agent schedule | Platform capability report. A specific enabled schedule must still be verified; queued/disabled schedule state does not prove execution. No schedule is created by AgentFleet. |
| Connector watchdog | Bounded lifecycle recovery library | Local fault tests cover reconnect, ownership, disable and circuit breaking. Remote watchdog deployment is separate. Restoring a connection does not start a Muse model. |
| Claude Code / Codex | Official CLI adapters with explicit per-run execution authorization | Implemented and fake-stream tested; real provider authentication/model availability and budgets must be checked for each execution environment. This release does not claim a real model development task passed. |
| OpenCode / other providers | Future adapters | No execution capability claimed. |

The primary connector carries transport and observations. The five-minute fallback is a lifecycle recovery mechanism, not a promise of real-time model wake-up. Native agent schedules are a separate platform mechanism. If automatic real-time execution is required, choose a provider with a supported programmatic execution interface explicitly; the scheduler must not silently replace a requested provider or retry an uncertain task with another model.

The earlier blanket no-interface statement is superseded by the official Gadget evidence. Native macOS support, BLE availability on a remote VM and access to a paired Linux host must not be assumed.
