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

## Phase 7 — Advanced Features ✅

- [x] Rate limiting per job type (`RateLimit.max_per_second` in JobConfig)
- [x] Rate limiting per key (`PerKeyRateLimit` with `key_fn` and `limit`)
- [x] Postgres-backed token bucket (atomic upsert on `conduit_rate_limit_buckets`)
- [x] Worker checks rate limit before dispatch; snoozes job if rate-limited
- [x] Priority ordering within a queue (`priority DESC` in `fetch_next` ORDER BY)
- [x] Unique jobs: `unique_for` config with `duration` and `by` field list
- [x] Unique fingerprint: SHA-256 of (job_type + selected payload fields)
- [x] `on_conflict` handling: `Ignore` (default, return existing id), `Replace`, `Raise`
- [x] Unique constraint released on job complete, discard, or dead-letter
- [x] `on_dead_letter` callback field added to JobConfig
- [x] `Queue.enqueue/enqueue_in` enforce unique constraints via `punique_guard`
- [x] Migration: `unique_key` column + partial unique index + `conduit_rate_limit_buckets` table
- [x] Tests: RateLimit/PerKeyRateLimit types, UniqueConflict variants, fingerprint determinism, unique check/release, rate limiter acquire/block/snooze, priority ordering, on_dead_letter callback
- [x] Pluggable rate limiter backend — `RateLimiterBackend.StorageBacked` (Postgres default) + `Custom(fn)` for user-supplied backends
- [x] Unique scope per-queue vs global — per-queue default, global opt-in via `UniqueScope.Global`

---

## Phase 8 — Deterministic Replay (Opt-In) ✅

- [x] `execution_mode: DeterministicReplay` workflow config option
- [x] Event store: all checkpoint decisions logged as immutable events in `conduit_workflow_events`
- [x] Replay: re-run workflow from event store, skip side-effect functions
- [x] Point-in-time replay: `replay_workflow_to(wf_id, max_sequence, storage)`
- [x] Workflow event history: `workflow_events(wf_id, storage)` returns `List(WorkflowEvent)`
- [x] `Conduit.EventStore` module: append, load_all, load_up_to, count, warm_replay, warm_replay_to
- [x] `WorkflowContext.checkpoint` gains replay branch: Vault-based cursor consumes recorded events, then executes fn for new checkpoints
- [x] `WorkflowRunner.prun` auto-warms replay cache when `execution_mode = DeterministicReplay`
- [x] Depot-style migration: `conduit_workflow_events` table with unique (workflow_id, sequence) constraint
- [x] Tests: WorkflowEvent type, EventStore append/load/count, replay checkpoint (first run/replay/mixed), cursor advances, order preserved, point-in-time, parallel branches
- [x] End-to-end test app: `test/test_deterministic_app.march` — 3-step payment workflow with first run, replay, point-in-time, and history query
- [ ] Deterministic time/randomness interception (wrapping DateTime.now() / Random.int()) — deferred
- [ ] Workflow event history visible in dashboard — deferred (events queryable via API)

---

## Resolved Design Questions

All open questions have been decided. See `specs/conduit.md` § Appendix for full discussion.

- **Q1** Checkpoint retention — Configurable per-workflow, 24h default after completion
- **Q2** Workflow versioning — CAS-based: module content hash determines version; in-flight instances keep original version
- **Q3** `parallel!` granularity — Configurable: actor-level default, job-level opt-in
- **Q4** Checkpoint serialization — Pluggable: JSON default (human-readable), March native types opt-in (typed round-tripping)
- **Q5** Dashboard auth — Both session and token; `Auth.Fn` adapter receives full request
- **Q6** Workflow output size — 1MB soft limit (warn), 10MB hard limit (reject)
- **Q7** Cron expression validation — Runtime now; compile-time as future enhancement
- **Q8** Unique job lock scope — Per-queue default, global opt-in
- **Q9** Rate limiter backend — Pluggable: Postgres default (already implemented), in-process optimization later
- **Q10** Migration directory — `priv/migrations/` matching Depot conventions
