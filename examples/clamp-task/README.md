# First real-provider acceptance task (prepared, not executed)

User instruction: implement `clamp(value, min, max)` as an exported pure JavaScript function. Return value limited to inclusive bounds. Reject nonfinite arguments and min > max with RangeError. No dependencies, network, filesystem side effects, publishing or deployment.

Create a new temporary Git repository from `seed/`, commit that fixed seed, and initialize an AgentFleet session. The missing implementation is deliberate. The manager should read this instruction and propose the DAG in `plan.json`; the operator reviews/imports it. Muse may pull the offered task over the already-confirmed SSH path. Its execution-end repository mapping must contain exactly the fixed base SHA in an independent worktree. Do not send the Mac's absolute path as a remote repository path.

Independent acceptance: `node --test test/clamp.test.mjs`. Independent reviewer checks bounds, errors, purity, source/test diff, and the exact candidate commit. Preserve worker and reviewer provider session IDs. Importing remote Git objects and binding remote reviewer evidence is an M2 gate; M1 protocol alone cannot claim this task accepted.

Before a real run record: tool/interface identity, explicit model, execution endpoint, fixed base SHA, timeout/output bounds and approved spend. Muse's actual interface/model and existing SSH alias remain unknown. No real call was made to prepare this task. Claude requires interactive login; Codex requires an explicit selected model and acceptance that it lacks a monetary cap. Stop for missing authorization rather than using another project's running agent.

Exit/resume check: exit CLI after claim; reopen `resume SESSION`, inspect identical message/claim/run/attempt IDs. Simulate lost ACK by reading reconciliation only. Duplicate result delivery with the same sourceEventId must return the same event ID; unknown work must never be assigned a new run automatically.
