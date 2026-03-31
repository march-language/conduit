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

---

## Phase 3 — Cron Scheduling ✅

**Merged:** 2026-03-31

### New types
- `Conduit.CronOverlap` — `Skip | Allow | Replace`
- `Conduit.CronConfig` — schedule string, timezone, queue, overlap, tags
- `Conduit.CronSchedule` — internal DB row for `conduit_cron_schedules` (static and dynamic crons)

### New interfaces
- `Conduit.Cron(c)` — implement for a recurring job with no payload; provides `perform` and defaulted `config`
- `Conduit.CronWithArgs(c)` — recurring job with a typed payload stored in the DB

### New modules
- `Conduit.CronRegistry` — in-process Vault map from cron_type → `(CronConfig, performer)`
  - `register/3`, `lookup/1`, `fire/2`, `all/0`
  - `overlap_to_string/1`, `overlap_from_string/1`
- `Conduit.CronParser` — 5-field cron expression parser
  - `next_run/2` — compute next fire time from a given DateTime (bounded to ~1 year)
  - `parse/1` — parse expression into internal `Expr` type
  - `matches/2` — check whether a DateTime satisfies a parsed expression
  - Supports: `*`, `N`, `N-M`, `*/N`, `N-M/S`, `N,M,...`
- `Conduit.CronScheduler` — background task that fires due crons
  - `start/2` — spawn the scheduler task
  - `upsert_static/3` — upsert a static cron into the DB at startup
  - Tick loop: acquire Postgres advisory lock → load due schedules → handle overlap → enqueue → update next_run_at
  - Multi-node safe via `pg_try_advisory_lock` (lock id 7326482)
- `Conduit.CronConfig.default/0`

### Storage additions (interface + Postgres impl)
- `cron_upsert/2` — insert or update a cron schedule row
- `cron_load_due/1` — load enabled schedules with `next_run_at <= NOW()`
- `cron_load_by_id/2` — load a single schedule by id
- `cron_mark_fired/4` — update `last_run_at`, `last_job_id`, `next_run_at`
- `cron_job_active/2` — return true if a job is still pending/running/snoozed
- `cron_cancel_job/2` — cancel a pending/running job (used by `Replace` overlap)
- `cron_delete/2` — delete a cron schedule permanently
- `cron_advisory_lock/2`, `cron_advisory_unlock/2`

### Migration
- `priv/migrations/20260401000003_conduit_cron.sql` — `conduit_cron_schedules` table + `idx_conduit_cron_due` index

### Public API (`lib/conduit.march`)
- `Conduit.start/2` — now also upserts static crons and launches the cron scheduler task
- `Conduit.register_cron/3` — register a static cron (stored in CronRegistry)
- `Conduit.schedule_cron/2` — insert/update a dynamic cron in the DB
- `Conduit.pause_cron/2` — disable a cron schedule
- `Conduit.resume_cron/2` — re-enable a paused cron schedule
- `Conduit.cancel_cron/2` — permanently delete a cron schedule

### Known gaps
- Full IANA timezone support — scheduler stores timezone in DB but applies it as a UTC offset approximation; full TZ database support deferred to Phase 5+
- `LISTEN/NOTIFY` push for instant cron tick — still polling

---

## Phase 4 — Imperative Workflows & Checkpoints ✅

**Merged:** 2026-03-31

### New types
- `Conduit.WorkflowError` — `Failed(String) | Cancelled(String) | TimedOut | StorageError(String)`
- `Conduit.ExecutionMode` — `Checkpoint | DeterministicReplay`
- `Conduit.WorkflowConfig` — `execution_mode`, `timeout`, `tags`
- `Conduit.WorkflowHandle` — returned by `start_workflow`; carries `id`, `workflow_type`, `started_at`
- `Conduit.WorkflowRow` — internal DB row for `conduit_workflows`
- `Conduit.WorkflowCheckpoint` — row in `conduit_checkpoints`
- `Conduit.WorkflowSignal` — row in `conduit_workflow_signals`
- `Conduit.WorkflowContext` — runtime context passed to `run()`

### New interface
- `Conduit.Workflow(w)` — `run : w -> String -> WorkflowContext -> Result(String, WorkflowError)` and defaulted `config`

### New modules
- `Conduit.WorkflowConfig.default/0`
- `Conduit.WorkflowRegistry` — in-process Vault map; `register/3`, `lookup/1`, `run/3`
- `Conduit.WorkflowContext` — durable execution primitives:
  - `new/1` — create root context
  - `child/2` — create namespaced sub-context for parallel branches
  - `checkpoint/4` — check cache → DB → execute fn → persist → return; on re-run skips fn
  - `parallel/4` — sequential fan-out with per-item checkpoint sub-namespace
  - `wait_for_signal/4` — poll DB for pending signal, sleep 1s between checks, respect timeout
  - `warm_cache/2` — bulk-load all checkpoints from DB into Vault before run()
- `Conduit.WorkflowRunner` — job performer registered as `"conduit.workflow_runner"`:
  - `execute/2` — load workflow, warm cache, run, handle all WorkflowError variants
  - `enqueue/3` — enqueue a runner job for a given workflow_id
  - `job_type/0` — returns `"conduit.workflow_runner"`

### Storage additions (interface + Postgres impl)
- `workflow_insert`, `workflow_load`, `workflow_update_status`, `workflow_cancel`
- `checkpoint_get`, `checkpoint_set`, `checkpoint_load_all`
- `signal_insert`, `signal_peek`, `signal_mark_delivered`

### Public API (`lib/conduit.march`)
- `Conduit.start/2` — now registers `conduit.workflow_runner` performer at startup
- `Conduit.register_workflow/3` — register workflow at startup
- `Conduit.start_workflow/3` — insert row + enqueue runner, return `WorkflowHandle`
- `Conduit.workflow_status/2` — query current status string
- `Conduit.signal_workflow/4` — insert signal row
- `Conduit.cancel_workflow/3` — cancel a running workflow

### Migration
- `priv/migrations/20260401000004_conduit_workflows.sql` — three new tables + `workflow_id` FK on `conduit_jobs`

### Known gaps
- `start_workflow_and_wait` (synchronous variant) — deferred; needs task coordination
- `checkpoint_loop!` macro — usable via plain recursion today
- `parallel!` runs sequentially in-process; true concurrent fan-out deferred to Phase 5+
- Checkpoint values are plain Strings (JSON by convention); typed serialization deferred

---

## Phase 5 — Multi-Node Coordination ✅

**Merged:** 2026-03-31

### New types
- `Conduit.NodeInfo` — live node snapshot: `id`, `hostname`, `pid`, `queues`, `pool_size`, `started_at`, `last_seen_at`, `status`
- `Conduit.Config` extended — `node_id`, `heartbeat_interval_ms`, `stale_node_threshold_ms`

### New modules
- `Conduit.Node` — manages node lifecycle:
  - `start/2` — registers node in `conduit_workers`, stores `node_id` and `is_leader` in Vault, spawns heartbeat / stale-detection / leader-election tasks
  - `stop/2` — deregisters node, releases advisory leader lock if held
  - `is_leader/0` — fast in-process check (reads Vault, no DB round-trip)
  - `leader_node_id/0` — returns most-recently-observed leader node id from Vault
  - Private loops: `pheartbeat_loop`, `pstale_loop`, `pleader_loop`

### Storage additions (interface + Postgres impl)
- `node_register/2` — upsert a `conduit_workers` row on startup
- `node_heartbeat/2` — update `last_seen_at` for a node
- `node_deregister/2` — mark a node as `stopped`
- `node_list_active/1` — return all nodes with `status = 'running'`
- `node_reclaim_jobs/2` — return stale-node in-flight jobs to `pending`
- `node_cleanup_stale/2` — mark expired nodes as `stopped`; returns count
- `node_try_leader_lock/2` — attempt `pg_try_advisory_lock(8473625)`; returns Bool
- `node_release_leader_lock/2` — release advisory lock on graceful shutdown

### Migration
- `priv/migrations/20260401000005_conduit_multi_node.sql` — `conduit_workers` table + `idx_conduit_workers_active` index + `conduit_cleanup_stale_workers` stored procedure + `conduit_cluster_status` view

### Public API (`lib/conduit.march`)
- `Conduit.start/2` — now also registers the node and spawns background coordination tasks
- `Conduit.stop/2` — graceful shutdown: deregisters node and releases leader lock
- `Conduit.cluster_nodes/1` — return all active cluster nodes
- `Conduit.cluster_leader/0` — return leader node id from this node's Vault cache
- `Conduit.is_cluster_leader/0` — fast boolean leader check

### Known gaps
- Cron scheduler already multi-node safe (advisory lock 7326482); explicit leader-only scheduling guard deferred to Phase 6
- Full IANA DST-aware timezone support for cron — deferred
- `parallel!` true concurrent fan-out across cluster nodes — deferred to Phase 7

---

## Phase 6 — Bastion Dashboard ✅

**Merged:** 2026-03-31

### New types
- `Conduit.Dashboard.QueueSummary` — per-queue aggregate stats: `queue`, `pending`, `running`, `failed`, `dead`, `throughput` (24 h), `avg_latency` (ms)
- `Conduit.Dashboard.Auth` — `None | Token(String) | Fn(fn String -> Bool)` — pluggable auth adapter

### New modules
- `Conduit.Dashboard` — public API:
  - `start_standalone/3` — starts a standalone HTTP server on a given port
  - `get_summary/1` — returns current `List(QueueSummary)` (used by polling endpoint)
  - `subscribe_to_summary/1` — best-effort Postgres LISTEN on `conduit_jobs`
- `Conduit.Dashboard.Auth` — `check/2` — validates a conn against the auth adapter
- `Conduit.Dashboard.Router` — HTTP request dispatcher; `handle/4` routes all dashboard requests
- `Conduit.Dashboard.Queries` — data-fetching helpers wrapping storage calls
- `Conduit.Dashboard.Actions` — admin mutation helpers:
  - `retry_job/2`, `cancel_job/2`, `delete_job/2`
  - `retry_dead_letter/2`, `delete_dead_letter/2`, `retry_all_dead_letters/1`
  - `trigger_cron_now/2`, `pause_cron/2`, `resume_cron/2`, `delete_cron/2`
  - `cancel_workflow/2`
- `Conduit.Dashboard.Html` — shared layout, nav, table, status badge, pagination, flash, action_btn helpers
- Page modules (server-rendered HTML):
  - `Conduit.Dashboard.Pages.Overview` — queue summary + cluster health
  - `Conduit.Dashboard.Pages.Queues` — queue list + tabbed job list with per-row admin actions
  - `Conduit.Dashboard.Pages.Jobs` — full job detail with error history
  - `Conduit.Dashboard.Pages.Workflows` — workflow list + detail with checkpoints and signals
  - `Conduit.Dashboard.Pages.Crons` — cron schedule list with trigger/pause/resume/delete
  - `Conduit.Dashboard.Pages.DeadLetters` — dead letter list with bulk retry/delete
  - `Conduit.Dashboard.Pages.Nodes` — cluster node list with leader highlight

### Storage additions (interface + Postgres impl)
- `dashboard_queue_summary/1` — GROUP BY queue aggregate query
- `dashboard_jobs_list/4` — paginated job list with queue + status filter
- `dead_letter_list/2` — paginated dead letters
- `dead_letter_load/2` — load single dead letter by id
- `job_retry/2` — reset job to pending with attempt = 0
- `job_cancel/3` — set status = dead with admin reason
- `job_delete/2` — hard DELETE job row
- `dead_letter_delete/2` — hard DELETE dead letter row
- `notify_subscribe/2` — LISTEN on Postgres channel (no-op with pool; polling fallback used)

### Routes (all mounted under the configured base_path, default `/conduit`)
| Method | Path | Description |
|--------|------|-------------|
| GET | / | Overview |
| GET | /queues | Queue list |
| GET | /queues/:queue | Queue detail (?status=&page=) |
| GET | /jobs/:id | Job detail |
| POST | /jobs/:id/retry | Retry job |
| POST | /jobs/:id/cancel | Cancel job |
| POST | /jobs/:id/delete | Delete job |
| GET | /workflows | Workflow list |
| GET | /workflows/:id | Workflow detail |
| POST | /workflows/:id/cancel | Cancel workflow |
| GET | /crons | Cron list |
| POST | /crons/:id/trigger | Trigger cron now |
| POST | /crons/:id/pause | Pause cron |
| POST | /crons/:id/resume | Resume cron |
| POST | /crons/:id/delete | Delete cron |
| GET | /dead-letters | Dead letters |
| POST | /dead-letters/retry-all | Retry all |
| POST | /dead-letters/:id/retry | Retry one |
| POST | /dead-letters/:id/delete | Delete one |
| GET | /nodes | Cluster nodes |
| GET | /api/summary | JSON summary for 5-second polling |

### Known gaps
- Workflow list page requires a new `dashboard_workflows_list` storage method (deferred; currently renders empty)
- Charts (throughput sparklines) deferred
- Schema-aware job enqueue form deferred
- LISTEN/NOTIFY real-time push — 5-second polling fallback used (full LISTEN requires dedicated non-pooled connection)

---

## Phase 7 — Advanced Features (Rate Limiting, Priority, Unique Jobs) ✅

**Merged:** 2026-03-31

### New types
- `Conduit.UniqueConflict` — `Ignore | Replace | Raise` — what to do on duplicate enqueue
- `Conduit.PerKeyRateLimit` — `{ key_fn, limit }` — per-key rate limit config
- `Conduit.RateLimit` — `{ max_per_second, per_key }` — global + optional per-key rate limit
- `Conduit.JobConfig` extended — `on_conflict`, `rate_limit`, `on_dead_letter` fields added

### New modules
- `Conduit.RateLimiter` — token bucket rate limiter:
  - `acquire/4` — atomically refill + consume; returns `Ok(true/false)`
  - `snooze_duration/1` — computes how long to sleep when rate-limited (base + jitter)
- `Conduit.Unique` — unique job fingerprinting:
  - `fingerprint/3` — SHA-256 of (job_type + selected payload fields via `by`)
  - `conflicts/2` — check DB for active job with same unique_key
  - `release/2` — set unique_key = NULL on a job (frees the slot)

### Storage additions (interface + Postgres impl)
- `rate_limit_acquire/3` — atomic token bucket upsert on `conduit_rate_limit_buckets`
- `unique_check/2` — COUNT active jobs with the given unique_key
- `unique_release/2` — NULL out unique_key for a job id
- `unique_key` column added to `Conduit.JobRow` type and all SELECT/INSERT queries

### Queue changes
- `Queue.enqueue/4` and `Queue.enqueue_in/5` now:
  1. Compute `unique_key` via `Conduit.Unique.fingerprint` (if `unique_for` is set)
  2. Call `punique_guard` to check for conflicts before INSERT
  3. Handle `on_conflict`: Ignore (return existing id), Replace (allow overwrite), Raise (return error)
  4. Pass `unique_key` to the storage layer

### Worker changes
- Rate limit check inserted before dispatch: if `config.rate_limit` is set, `RateLimiter.acquire` is called; if denied the job is snoozed
- Unique key released on: completion, discard (no DLQ), dead-letter

### Migration
- `priv/migrations/20260401000007_conduit_advanced.sql`:
  - `ALTER TABLE conduit_jobs ADD COLUMN unique_key TEXT`
  - Partial unique index `idx_conduit_jobs_unique_key` (active jobs only)
  - `conduit_rate_limit_buckets` table (key, tokens, last_refill)

### Known gaps
- Pluggable rate limiter backend (in-process for single-node) — Postgres-only for now
- Unique scope per-queue vs global — currently global only
- `unique_for.duration` not enforced as a TTL on the constraint (the partial index keeps it active until complete/dead)
