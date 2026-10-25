# What can be operated today

This inventory separates implemented operations, synthetic validation and real external evidence. A command existing in the CLI does not establish that a real model development task has passed.

| User story | Current operation | Evidence and remaining gap |
| --- | --- | --- |
| Start from an existing Git project and configure roles | `init`, interactive CLI, `config`, `capabilities` | Durable configuration and role validation implemented. Provider login/model availability remain environment-specific; no credential copying. |
| Talk to a manager and obtain a detailed plan | `chat`, `propose`, `proposal`, `plan accept` | Structured plan parsing and adoption implemented; fake manager tests cover the chain. Real manager invocation requires explicit model/budget authorization and has not been end-to-end validated here. |
| Validate dependencies and acceptance steps | `plan`, `schedule --dry-run` | DAG/schema validation and accepted-commit dependencies implemented. Imported acceptance argv is executable project policy and must be authorized before execution. |
| Share agent resources across projects | `pool configure`, `fleet schedule`, `fleet propose` | Synthetic subprocess tests cover concurrent work, account/endpoint capacities and finite-fleet fairness. Unknown leases stay reserved. Independent coordinators do not have a strict shared fairness guarantee. |
| Verify and independently review work | `verify`, `review-run`, `accept-review`; fake parallel/fleet demos | Independent fixed-commit worktrees and review gates implemented. A local fake development chain passes; real Muse/Claude/Codex development plus review has not been demonstrated. |
| Exit and return to an orchestration session | `sessions list`, `resume`, project filtering, status/events | Durable history and conservative recovery implemented; process identity and uncertain effects are inspected. A retained uncertain lock can block progress and requires evidence, not automatic PID-based cleanup. |
| Request control through voice/chat | Structured `control` CLI/stdio with fixed scope file | Actual Mac child-task read-only status was verified against a synthetic session. Draft/note and pause/resume/stop receipts pass local tests. No installed Dot/platform MCP, automatic reverse notifications or active-prompt injection. |
| Reach the coordinator from Muse | `remote` / `fleet-remote`; finite observer connector | User-reported SSH diagnostic roundtrip and finite observer trial agree with coordinator state. Neither proves resident service health or model task execution. |
| Relate official Gadget messages to AgentFleet work | Offline typed reducer plus session evidence journal | Public-schema fixture tests only; transport is disabled/unpaired. No SDK installed, credentials created, paired device or live send. Queue ACK/result and acceptance stay separate. |
| Recover a broken primary connection | Connector lifecycle/watchdog library | Fake disconnect/ownership/backoff/circuit tests exist. No remote watchdog or service installed by this release. Recovering transport alone does not wake a Muse agent. |

## Finite next steps

1. Keep the existing fixed observer trial and installation intact. Exercise local scoped reads, then explicitly authorized inert draft/note writes, through the existing task bridge.
2. Finish a bounded, authorized real small development task with an available provider and independent review. Record the exact model, project, base commit, budget boundary and actual artifact evidence before calling it complete.
3. For Gadget, first establish an explicitly approved Linux/BLE host, account exposure, token and phone pairing. Then test one nonce sidechat roundtrip; do not begin with a development task. Formal send idempotency, reply subscription access and cancellation remain separate verification points.
4. Only after the execution environment and process ownership are known, approve a finite watchdog deployment and test disable, interruption and reconciliation. Do not add a permanent service or timer as a side effect of this source release.

No additional provider shell, web frontend or message broker is needed to close these gaps. The immediate work is integration and evidence for the existing paths, not more empty adapters.
