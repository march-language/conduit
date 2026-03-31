# Conduit — Todo

> Tracks upcoming work by phase. Move items to features.md when shipped.

---

## Phase 2 — Retries, Dead Letters & Error Control ✅

- [x] Apply backoff delay when scheduling a retry
- [x] `ConduitError.Discard` moves job to dead state (or DLQ if configured)
- [x] `ConduitError.Snooze(dur)` sets `status = snoozed`, `run_at = now + dur`
- [x] `ConduitError.Retry` re-queues with correct `run_at` based on attempt + backoff
- [x] Jobs exhausting `max_attempts` move to `conduit_dead_letters` table
- [x] `on_dead_letter` callback fires when a job is dead-lettered
- [x] `dead_letter_queue` config field respected
- [x] Jitter added to backoff delays (0–1000ms random)
- [x] Stale-job detection: rescue `running` jobs with expired heartbeat
- [x] Worker heartbeats during execution (every timeout/3 seconds)
- [x] Migration: `conduit_dead_letters` + `idx_conduit_jobs_heartbeat`
- [x] Tests: retry scheduling, discard, DLQ, on_dead_letter callback, DeadLetter.from_job

---

## Phase 3 — Cron Scheduling ✅

- [x] `Conduit.Cron(c)` interface — `perform`, `config` (schedule, timezone, queue, overlap)
- [x] `Conduit.CronWithArgs(c)` interface — cron with a typed payload
- [x] `Conduit.CronConfig` type — schedule string, timezone, queue, overlap, tags
- [x] `Conduit.CronOverlap` type — `Skip | Allow | Replace`
- [x] Cron scheduler actor: wakes up at next tick, fires due crons
- [x] `overlap: Skip` — silently drop if previous run still in queue
- [x] `overlap: Replace` — cancel previous run, enqueue fresh
- [x] Multi-node: Postgres advisory lock prevents duplicate fires
- [x] Dynamic cron: register/unregister at runtime (schedule stored in DB)
- [x] Timezone-aware scheduling (IANA timezone names stored; full DST support Phase 5+)
- [x] Cron schedules survive node restarts (loaded from DB)
- [x] Migration: `conduit_cron_schedules` table
- [x] Tests: expression parsing, overlap modes, registry round-trip, fire/dispatch

---

## Phase 4 — Imperative Workflows & Checkpoints ✅

- [x] `Conduit.Workflow(w)` interface — `run`, `config`
- [x] `Conduit.WorkflowContext` type passed to `run`
- [x] `Conduit.WorkflowContext.checkpoint` — store result in DB, skip on resume
- [x] `Conduit.WorkflowContext.parallel` — fan-out with per-item checkpoint sub-namespace
- [x] `Conduit.WorkflowContext.wait_for_signal` — poll for signal or timeout
- [x] `Conduit.WorkflowContext.warm_cache` — bulk-load checkpoints before run()
- [x] `Conduit.start_workflow/3` — insert row, enqueue runner, return handle
- [x] `Conduit.signal_workflow/4` — insert signal row for running workflow
- [x] `Conduit.cancel_workflow/3` — cancel with reason
- [x] `Conduit.workflow_status/2` — query current status
- [x] `Conduit.register_workflow/3` — register runner in WorkflowRegistry
- [x] `Conduit.WorkflowRegistry` — in-process registry with register/lookup/run
- [x] `Conduit.WorkflowRunner` — job performer for conduit.workflow_runner
- [x] Crash recovery: warm_cache restores checkpoint state; runner re-runs from start but completed checkpoints are no-ops
- [x] Workflow timeout check at runner entry point
- [x] Sub-workflow support via parent_id field (runner enqueues child as normal workflow)
- [x] `Conduit.WorkflowConfig` type — execution_mode, timeout, tags
- [x] `Conduit.WorkflowError` type — Failed | Cancelled | TimedOut | StorageError
- [x] Migration: conduit_workflows, conduit_checkpoints, conduit_workflow_signals tables
- [x] Tests: checkpoint cache hit/miss, parallel, registry dispatch, error variants, runner discard on missing workflow
- [ ] `start_workflow_and_wait/3` — synchronous variant (deferred; needs task coordination)
- [ ] `checkpoint_loop!` — explicit loop primitive (usable via plain recursion for now)

---

## Phase 5 — Multi-Node Coordination ✅

- [x] Node registration on startup (row in `conduit_workers`)
- [x] Node heartbeat every N seconds (configurable via `heartbeat_interval_ms`)
- [x] Stale node detection — mark nodes stopped if `last_seen_at` exceeds threshold
- [x] Reclaim orphaned jobs from stale nodes (return to `pending`)
- [x] Leader election via Postgres advisory lock (lock id 8473625)
- [x] Graceful shutdown: deregister node, release leader lock
- [x] `Conduit.cluster_nodes/1` — return live node list
- [x] `Conduit.cluster_leader/0` — return leader node id from Vault cache
- [x] `Conduit.is_cluster_leader/0` — fast boolean leader check
- [x] `Conduit.Config` extended with `node_id`, `heartbeat_interval_ms`, `stale_node_threshold_ms`
- [x] Migration: `conduit_workers` table + stored procedure + cluster status view
- [x] Tests: config defaults, NodeInfo fields, leader/follower state, register/list, reclaim/cleanup, lock acquire/release
- [ ] Cron fires exactly once per schedule across cluster (leader-only guard) — deferred to Phase 6

---

## Phase 6 — Bastion Dashboard ✅

- [x] Dashboard router: `Conduit.Dashboard.Router.handle/4` (embed via `forward` or use standalone)
- [x] Standalone mode: `Conduit.Dashboard.start_standalone/3`
- [x] Pages: queue overview, queue detail, job detail, dead letters, workflows, crons, nodes
- [x] Admin actions: retry job, cancel job, delete job, retry/delete dead letter, retry-all, trigger cron, pause/resume/delete cron, cancel workflow
- [x] Real-time updates: 5-second JS polling of `/api/summary` (LISTEN/NOTIFY deferred — needs dedicated non-pooled connection)
- [x] Pluggable auth adapter: `Auth.None | Auth.Token(t) | Auth.Fn(f)`
- [x] Per-queue aggregate stats: pending, running, failed, dead, throughput (24h), avg latency
- [x] Storage additions: queue_summary, jobs_list, dead_letter_list/load, job_retry/cancel/delete, dl_delete, notify_subscribe
- [x] Tests: QueueSummary type, Auth variants, Actions (retry/cancel/delete/dl), Queries via fake storage
- [ ] Workflow list page (requires dashboard_workflows_list storage method — deferred)
- [ ] Charts: throughput sparklines, error rate — deferred
- [ ] Schema-aware job enqueue form — deferred
- [ ] Separate `conduit_dashboard` package — deferred (lives in core for now)

---

## Phase 7 — Advanced Features

- [ ] Rate limiting per job type (e.g. max 100/minute)
- [ ] Rate limiting per key (e.g. per `user_id`)
- [ ] Pluggable rate limiter backend (in-process default, Postgres for coordinated)
- [ ] Priority ordering within a queue (higher `priority` runs first)
- [ ] Unique jobs: `unique_for` config prevents duplicate enqueues within window
- [ ] Unique constraint released on job complete/fail
- [ ] Unique scope: per-queue (default) or global

---

## Phase 8 — Deterministic Replay (Opt-In)

- [ ] `execution_mode: DeterministicReplay` workflow config option
- [ ] Event store: all workflow decisions logged as immutable events
- [ ] Replay: re-run workflow from event store, skip side effects
- [ ] Deterministic time, randomness, and external calls on replay
- [ ] Point-in-time replay
- [ ] Workflow history in dashboard
- [ ] Migration: `conduit_workflow_events` table

---

## Open Questions

See `specs/conduit.md` § Appendix for full discussion. Short list:

- **Q1** Checkpoint retention policy (default: 24h after completion)
- **Q2** Workflow versioning strategy (leaning: version field, new code = new instances)
- **Q3** `parallel!` granularity — actor-level vs job-level (leaning: configurable)
- **Q4** Checkpoint serialisation format (leaning: pluggable, JSON default)
- **Q5** Dashboard auth — session vs token (leaning: both, adapter sees full request)
- **Q6** Workflow output size limit (leaning: soft 1MB warn, hard 10MB)
- **Q7** Cron expression validation — compile-time vs runtime (leaning: runtime)
- **Q8** Unique job lock scope — per-queue vs global (leaning: per-queue default)
- **Q9** Rate limiter backend (leaning: pluggable, in-process default)
- **Q10** Migration directory default (leaning: `priv/migrations/`)
