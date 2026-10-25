# Session history and recovery

The session index lives at `~/.agent-fleet/sessions.json`; `AGENT_FLEET_HOME` changes only this index location for isolated tests. Each project has `.agent-fleet/sessions/<UUID>/state.sqlite`, and `.agent-fleet/current.json` identifies its current session. Multiple sessions may belong to one project. The index stores project path, title, activity time, manager name and task-state counts. It never stores authentication credentials or provider transcripts.

The SQLite session database is the authority for conversations, configuration snapshots, roadmap/DAG, tasks, runs, worktrees, provider session identifiers and review evidence. Selecting a history entry does not start or replay a run. The engine must reconcile persisted run identity, process start identity, locks, worktrees and possible side effects before continuing. A missing process or stale heartbeat alone cannot prove a side effect did not occur. UI exit does not mean cancellation.

`sessions list` should show the metadata and availability. `resume` should present this list and allow inspection before continuation; `resume <session-id>` selects an exact session. Unknown or mismatched IDs fail closed. Deleted/moved projects remain in history as unavailable, rather than disappearing. Directory symlinks or identity changes block selection.

Index writes are serialized by an exclusive lock and use a synced temporary file plus atomic rename. Metadata files use mode 0600 and newly created metadata directories use 0700. An interrupted writer can leave `sessions.lock`. The application reports this condition and does not delete the lock based on PID or age. Read-only history remains available. Inspect the writer and index before manually resolving the lock.

The index and each project pointer are separate atomic files; they are not a distributed transaction. Creation writes the index before the current-session pointer so an interruption still leaves a discoverable history entry. A session without its database is shown as unavailable. This metadata module does not claim process liveness, approve permissions, authenticate providers, or resume any provider by itself.
