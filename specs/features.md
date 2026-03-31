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
- Serialisation uses `show/1` placeholder — future phase adds proper JSON encode/decode
- No `LISTEN/NOTIFY` — workers poll on an interval

---

## Phase 2 — Retries, Dead Letters & Error Control ✅

**Merged:** 2026-03-31

### New types
- `Conduit.DeadLetter` — record type for `conduit_dead_letters` table
- `backoff` and `dead_letter_queue` fields added to `Conduit.JobRow`

### New storage methods (added to `Conduit.Storage` interface + Postgres impl)
- `schedule_retry/4` — record error, set `status=pending`, set future `run_at`
- `discard/3` — permanently fail a job in-place (`status=dead`), no DLQ
- `move_to_dead_letter/2` — insert `conduit_dead_letters` row, mark source job dead
- `rescue_stale/2` — return stale `running` jobs (heartbeat expired) to `pending`

### Registry upgrade
- `Conduit.Queue.register_performer/3` now stores `(performer, JobConfig)` pairs
- `Conduit.Queue.lookup_config/1` — retrieve a job's config at runtime (used by worker for backoff and callbacks)
- `Conduit.Queue.pbackoff_name/1` — serialises `Backoff` variant to string for DB storage

### Worker upgrades
- Backoff delay computed via `Conduit.Backoff.delay/2` with ±1s random jitter
- Retry vs dead-letter decision: `next_attempt >= max_attempts` → dead-letter
- `Err(ConduitError.Discard)` with no DLQ → `discard/3`; with DLQ → `move_to_dead_letter/2`
- `on_dead_letter` callback fires with `Conduit.JobInfo` when job is dead-lettered
- Heartbeat task spawned per job execution; ticks at `timeout/3` interval
- Stale-rescue background task per worker pool; runs every `poll_interval × 10`
- `Conduit.DeadLetter.from_job/2` and `to_job_info/1` helpers

### Migrations
- `priv/migrations/20260401000001_conduit_jobs.sql` — Phase 1 table (now includes `backoff`, `dead_letter_queue` columns)
- `priv/migrations/20260401000002_conduit_dead_letters.sql` — dead letters table + `idx_conduit_jobs_heartbeat`

### Public API changes
- `Conduit.register/3` — now requires a `JobConfig` arg (was 2-arg in Phase 1)
- `Conduit.register_default/2` — convenience wrapper using `JobConfig.default()`

### Known gaps
- `LISTEN/NOTIFY` — workers still poll; instant pickup deferred to Phase 3/5
- Serialisation still uses `show/1` placeholder
