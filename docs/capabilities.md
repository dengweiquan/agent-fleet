# Execution and integration capability boundaries

These are distinct layers. Transport health is not agent availability, and a provider's report is not an independently tested execution result.

| Surface | Available mechanism | Evidence and limit |
| --- | --- | --- |
| Local control | Project/session-scoped CLI and JSONL stdio; receipt lookup, inert drafts/notes and explicit controls | Implemented and tested with local synthetic fixtures. No installed Dot/platform MCP or push/wake integration. |
| Remote connector | Remote-initiated SSH stdio observation with durable event replay | A finite read-only Muse-host trial was reported successful and matched local coordinator records. It has ended; this is not proof of a resident service or model execution. |
| Muse model start from Node | No supported official CLI, SDK or HTTP task-start API reported | Muse capability response, October 2026. Ordinary Node cannot call agent-internal subagent/cron/workflow tools. No hidden socket or token extraction fallback. |
| Muse execution inside its platform | An already active agent or an authorized native agent schedule | Platform capability report. A specific enabled schedule must still be verified; queued/disabled schedule state does not prove execution. No schedule is created by AgentFleet. |
| Connector watchdog | Bounded lifecycle recovery library | Local fault tests cover reconnect, ownership, disable and circuit breaking. Remote watchdog deployment is separate. Restoring a connection does not start a Muse model. |
| Claude Code / Codex | Official CLI adapters with explicit per-run execution authorization | Implemented and fake-stream tested; real provider authentication/model availability and budgets must be checked for each execution environment. This release does not claim a real model development task passed. |
| OpenCode / other providers | Future adapters | No execution capability claimed. |

The primary connector carries transport and observations. The five-minute fallback is a lifecycle recovery mechanism, not a promise of real-time model wake-up. Native agent schedules are a separate platform mechanism. If automatic real-time execution is required, choose a provider with a supported programmatic execution interface explicitly; the scheduler must not silently replace a requested provider or retry an uncertain task with another model.
