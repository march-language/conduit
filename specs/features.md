# Conduit — Shipped Features

> Updated as phases land. Each entry reflects what is actually in the codebase, not what is planned.

---

## Phase 1 — Simple Job Queue ✅

**Merged:** 2026-03-31

### Types
- `ConduitError` — `Retry(String) | Discard(String) | Snooze(Duration) | WorkerError(String)`
- `Conduit.Backoff` — `Linear | Exponential | Fibonacci | Custom(fn Int -> Duration)`
- `Conduit.JobConfig` — per-job config record with defaults (queue, max_attempts, timeout, backoff, priority, tags, …)
- `Conduit.JobInfo` — read-only job snapshot for callbacks
- `Conduit.Config` — application-level config (queues, workers_per_queue, poll_interval_ms)
- `Conduit.UniqueConstraint` — duration + field list for uniqueness
- `Conduit.Schema(a)` — opaque optional arg validator
- `Conduit.JobRow` — internal DB row type

### Interfaces
- `Conduit.Job(j)` — implement to define a background job; provides `perform`, defaulted `config` and `schema`
- `Conduit.Storage(s)` — pluggable storage backend: `enqueue`, `fetch_next`, `mark_running`, `mark_completed`, `mark_failed`, `mark_snoozed`, `heartbeat`

### Modules
- `Conduit.Backoff.delay/2` — computes retry delay for any backoff strategy
- `Conduit.Schema.none/0`, `build/1`, `validate/2`
- `Conduit.JobConfig.default/0`
- `Conduit.Storage.Postgres` — Postgres backend using `SKIP LOCKED`
- `Conduit.Queue.enqueue/4`, `enqueue_in/5`, `register_performer/2`, `dispatch/2`
- `Conduit.Worker.start_workers/4` — spawns N polling tasks per queue; handles all `ConduitError` variants
- `Conduit` (main API) — `start/2`, `enqueue/4`, `enqueue_in/5`, `enqueue_with/4`, `register/2`

### Known gaps (addressed in later phases)
- Serialisation uses `show/1` placeholder — Phase 2 adds proper JSON encode/decode
- No `LISTEN/NOTIFY` — workers poll on an interval
- No backoff delay applied to retried jobs (jobs re-enter queue immediately)
- No stale-job rescue for crashed workers
- No dead-letter table or `on_dead_letter` callback
