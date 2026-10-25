# Reverse protocol and shared capacity

An external runner initiates the connection. The coordinator does not assume it can connect to Muse or launch a Muse executable. SSH stdio is the primary transport; no listener or new authentication material is created.

`fleet-remote offer FLEET SESSION TASK ENDPOINT REPOSITORY` creates an immutable, project-scoped offer from an adopted plan. Remote dispatch carries a logical repository identifier, fixed base SHA and accepted dependency commits; it never treats a local project directory as a remote checkout path. A pending offer intent is recorded before publishing to the shared queue, so interruption cannot authorize local replay.

The reverse queue and resource registry live in the same SQLite authority. Atomic claim reserves global/project/endpoint/account/instance capacity, binds the run, records the claim and issues its fence. Unknown leases remain occupied. Result publication, report deduplication and known execution-slot release commit together; execution completion is not task acceptance.

ACK/report/reconcile require the exact project key, session, task, message, run, attempt, endpoint, resource, account domain, lease, fence and claim identity. A trusted fixed command supplies the allowed scope; stdin cannot select filesystem roots, resources or arbitrary commands. Capabilities and authentication availability reported by a runner remain self-reports, not proof of model availability or permission to spend.

Repeated claims return the original claim. Source-event identifiers are globally idempotent; conflicting reuse is rejected. Disconnection records unknown effects, and late results cannot automatically clear unknown capacity. Global event IDs and per-run sequences support scoped replay with gaps in the global cursor. The connector validates generations and durable replay before permitting new mutations.

The supplied client is intentionally an observer and does not claim or execute tasks. Server-side claims additionally require `remoteClaimsAuthorized: true` in the fixed project manifest. Existing legacy remote sessions cannot be silently enrolled into pooled capacity; use an isolated new session or an explicitly reviewed migration.

Fencing cannot undo an external side effect or guarantee distributed exactly-once execution. Remote isolated-checkout setup, artifact import, independent acceptance and a verified Muse wake/task API remain separate integration work. The older remote.ts push-shaped fake transport is retained solely as a fault-injection laboratory; it is not the Muse connection direction.
