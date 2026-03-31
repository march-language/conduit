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

## Phase 3 — Cron Scheduling

- [ ] `Conduit.Cron(c)` interface — `perform`, `config` (schedule, timezone, queue, overlap)
- [ ] `Conduit.CronWithArgs(c)` interface — cron with a typed payload
- [ ] `Conduit.CronConfig` type — schedule string, timezone, queue, overlap, tags
- [ ] `Conduit.CronOverlap` type — `Skip | Allow | Replace`
- [ ] Cron scheduler actor: wakes up at next tick, fires due crons
- [ ] `overlap: Skip` — silently drop if previous run still in queue
- [ ] `overlap: Replace` — cancel previous run, enqueue fresh
- [ ] Multi-node: Postgres advisory lock prevents duplicate fires
- [ ] Dynamic cron: register/unregister at runtime (schedule stored in DB)
- [ ] Timezone-aware scheduling (IANA timezone names)
- [ ] Cron schedules survive node restarts (loaded from DB)
- [ ] Migration: `conduit_crons` table
- [ ] Tests: schedule firing, overlap modes, timezone, dynamic crons

---

## Phase 4 — Imperative Workflows & Checkpoints

- [ ] `Conduit.Workflow(w)` interface — `run`, `config`, `schema`
- [ ] `WorkflowContext` type passed to `run`
- [ ] `checkpoint!(ctx, name, fn)` — store result in DB, skip on resume
- [ ] `checkpoint_loop!` — checkpoint a recursive/iterative loop
- [ ] `parallel!(ctx, items, fn)` — fan-out with per-item checkpoints
- [ ] `wait_for_signal!(ctx, signal_name)` — suspend workflow until signalled
- [ ] `Conduit.start_workflow/2` — enqueue and return handle
- [ ] `Conduit.start_workflow_and_wait/3` — block until completion
- [ ] `Conduit.signal_workflow/3` — deliver a signal to a running workflow
- [ ] `Conduit.cancel_workflow/2` — cancel with reason
- [ ] `Conduit.workflow_status/1` — query workflow state
- [ ] Workflow actor: each in-flight workflow is a supervised actor
- [ ] Crash recovery: resume from last checkpoint on restart
- [ ] Workflow timeout (per `WorkflowConfig`)
- [ ] Sub-workflow support (workflow enqueuing another workflow)
- [ ] `Conduit.WorkflowConfig` type — `execution_mode`, timeout
- [ ] Migration: `conduit_workflows`, `conduit_workflow_checkpoints` tables
- [ ] Tests: checkpoint idempotency, crash resume, parallel, signals, cancel

---

## Phase 5 — Multi-Node Coordination

- [ ] Node registration on startup (row in `conduit_nodes`)
- [ ] Node heartbeat every N seconds
- [ ] Stale node detection — mark nodes dead if heartbeat exceeds threshold
- [ ] Reclaim orphaned jobs from dead nodes
- [ ] Leader election via Postgres advisory lock
- [ ] Cron fires exactly once per schedule across cluster (leader-only)
- [ ] Graceful shutdown: stop accepting new jobs, drain in-progress
- [ ] `Conduit.cluster_nodes/0` — return live node list
- [ ] Migration: `conduit_nodes` table

---

## Phase 6 — Bastion Dashboard

- [ ] Separate `conduit_dashboard` package (no hard Bastion dep in core)
- [ ] Dashboard router: `forward "/conduit", Conduit.Dashboard.Router`
- [ ] Standalone mode: `Conduit.Dashboard.start_standalone/1`
- [ ] Pages: queue overview, job list, job detail, dead letters, workflows, crons
- [ ] Admin actions: retry job, cancel job, delete job, replay dead letter
- [ ] Real-time updates via `LISTEN/NOTIFY` or polling
- [ ] Pluggable auth adapter
- [ ] Charts: throughput, latency, error rate

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
