# Conduit — Todo

> Actionable checklist. The rationale, positioning, and design sketches live in
> [roadmap.md](./roadmap.md); shipped work is catalogued in [features.md](./features.md).
> Item IDs (T0.1 …) map to roadmap §4.
>
> **Last updated:** 2026-06-15

---

## Shipped

Phases 1–8 are complete (simple queue, retries/DLQ, cron, workflows/checkpoints,
multi-node, dashboard, rate-limit/priority/unique, deterministic replay). See
[features.md](./features.md) for the full inventory. The items below are the
work that remains to be competitive with Oban + Temporal.

---

## Tier 0 — Make the backend real *(in progress)*

- [x] **T0.1** Fix `Bytes.slice` FBIP-reuse-under-aliasing bug in the March compiler *(done — fix in `llvm_emit.ml` ECase: dup extracted heap fields at branch entry when the scrutinee is both reused and shared; installed)*. Unblocked depot row decoding → `fetch_next` returns rows.
- [x] `Conduit.Storage.Postgres` — job-lifecycle slice (enqueue, fetch_next w/ `SKIP LOCKED`, mark_running/completed/snoozed, heartbeat, schedule_retry, discard, move_to_dead_letter, rescue_stale). **Verified live end-to-end: 9/9 postgres tests, full suite 204/0.**
- [x] **T0.2a** Postgres impl — cron methods (`cron_upsert` via `ON CONFLICT`, `cron_load_due`, `cron_load_by_id`, `cron_mark_fired`, `cron_job_active`, `cron_cancel_job`, `cron_delete`, `cron_advisory_lock/unlock`). 8 live tests. *(Advisory lock is single-node-correct only until T0.3 pooling — session-scoped lock can't span a tick on a per-op connection; documented in code.)* Added missing `jitter_ms` column to the schema.
- [x] **T0.2b** Postgres impl — workflow + checkpoint + signal methods (`workflow_insert/load/list_all/status_get/update_status` w/ COALESCE semantics`/cancel`, `checkpoint_get/set` (first-write-wins via `ON CONFLICT DO NOTHING`)`/load_all`, `signal_insert` (RETURNING serial id)`/peek/mark_delivered`). Row decoders for all three types. 8 live tests.
- [x] **T0.2c** Postgres impl — node methods (`node_register` upsert`/heartbeat/deregister/list_active`, `node_reclaim_jobs` (stale-heartbeat orphans — worker_id is a per-exec UUID, not a node link)`/cleanup_stale` (RETURNING count)`/try_leader_lock/release_leader_lock`). `NodeInfo` decoder. 5 live tests. *(Leader lock single-node-correct only until T0.3, same advisory-lock caveat as cron.)*
- [ ] **T0.2d** Postgres impl — dashboard queries (`dashboard_queue_summary/jobs_list/job_by_id/crons_list/workflows_list`).
- [ ] **T0.2e** Postgres impl — dead-letter admin + rate-limit + unique + event store (`dead_letter_list/list_all/load/delete/delete_all`, `job_retry/cancel/delete`, `notify_subscribe`, `rate_limit_acquire`, `unique_check/release`, `event_append/load_all/load_up_to/count`).
- [ ] **T0.3** Connection pooling — replace per-operation `pwith_conn` with depot's `Pool` (single choke-point swap).
- [ ] **T0.4** Live integration test matrix: job lifecycle, cron fire, workflow run, multi-node reclaim, DLQ, rate-limit, unique. Each new `Storage` method ships with its in-memory test-store stub too.

---

## Tier 1 — Oban table stakes

- [ ] **T1.1** Real serialization — replace the `show/1` payload placeholder.
  - [ ] Define `Conduit.Codec(a)` interface (`encode`/`decode`) as the explicit path.
  - [ ] Generated JSON codec per job-payload type once type-directed derivation is available.
  - [ ] Versioned/tagged payloads so an old encoder's output is detectable on read.
  - [ ] Keep `Conduit.Schema` as the optional validation layer on top.
  - [ ] Apply to checkpoint values too (currently plain Strings by convention).
- [ ] **T1.2** Pruning / retention.
  - [ ] `Config.retention = { completed_after_ms, dead_after_ms, workflow_after_ms }` with sane defaults.
  - [ ] Leader-only periodic pruner: batched `DELETE` of terminal jobs past horizon (avoid long locks).
  - [ ] Prune completed workflows + their checkpoints + events past horizon.
  - [ ] Surface table/row counts on the dashboard so growth is visible.

---

## Tier 2 — Temporal-lane workflow depth

> **Build the keystone first:** a "suspend & re-enqueue the runner" primitive
> (re-enqueue the workflow runner with a future `run_at` or on a signal, instead
> of blocking a worker). T2.1, T2.2, T2.3 all reuse it.

- [ ] **T2.0** Runner-suspension primitive (suspend a workflow without holding a worker; resume by re-enqueue).
- [ ] **T2.1** Durable `ctx.sleep(duration)` — records `wake_at` as a checkpoint, suspends the runner until then; replay fast-forwards elapsed sleeps. No worker held.
- [ ] **T2.2** True parallel `ctx.parallel` — each branch a child runner job (namespaced checkpoints exist); parent join-checkpoint counts completions. Actor-level default, job-level opt-in.
- [ ] **T2.3** Event-driven `wait_for_signal` — suspend instead of poll; `signal_workflow` re-enqueues the waiting runner (via `pg_notify`, polling fallback); timeout becomes the suspended runner's `run_at`.
- [ ] **T2.4** Activities — `ctx.activity(name, opts, fn)` with independent `max_attempts`/`timeout`/heartbeat, distinct from the workflow retry policy.
- [ ] **T2.5** `ctx.continue_as_new(args)` — truncate history, restart with fresh state (unbounded loops).
- [ ] **T2.6** Workflow versioning enforcement — pin a run to its starting module-content hash (CAS); detect/repair on mismatch.
- [ ] **T2.7** Deterministic `ctx.now()` / `ctx.random()` — record value as an event on first run, replay deterministically.
- [ ] `start_workflow_and_wait/3` — synchronous variant (carried over; needs the suspension primitive + task coordination).
- [ ] `checkpoint_loop!` — explicit loop primitive (usable via plain recursion today; revisit with T2.5).

---

## Tier 3 — Operability & trust

- [ ] **T3.1** Visibility/search: `dashboard_workflows_list` with status/type/time filters; populate the empty workflows page; "list running workflows where …" query API.
- [ ] **T3.2** Telemetry exporters: Prometheus scrape endpoint and/or OpenTelemetry export; span timing for jobs + checkpoints (the telemetry bus is already wired).
- [ ] **T3.3** Production hardening: soak tests, backpressure under load, connection-exhaustion behavior, graceful drain verified against the real backend.
- [ ] **T3.4** Dashboard depth (carried over): throughput sparklines, error-rate, workflow checkpoint timeline, schema-aware enqueue form.

---

## Non-Goals (do not build)

- Airflow-style DAG authoring, backfill/catchup, datasets, sensors, operators, grid/gantt UI.
- A bespoke query language (SQL via the storage interface suffices).
- Non-Postgres backends beyond the in-memory test store (keep the interface clean; revisit on real demand).
- Cross-node `parallel` fan-out beyond "children are jobs."

---

## Resolved Design Questions

All open questions have been decided. See `specs/conduit.md` § Appendix for full discussion.

- **Q1** Checkpoint retention — Configurable per-workflow, 24h default after completion → drives **T1.2**.
- **Q2** Workflow versioning — CAS-based: module content hash; in-flight instances keep original version → drives **T2.6**.
- **Q3** `parallel!` granularity — Configurable: actor-level default, job-level opt-in → drives **T2.2**.
- **Q4** Checkpoint serialization — Pluggable: JSON default, March native types opt-in → drives **T1.1**.
- **Q5** Dashboard auth — Both session and token; `Auth.Fn` adapter receives full request *(shipped)*.
- **Q6** Workflow output size — 1MB soft limit (warn), 10MB hard limit (reject).
- **Q7** Cron expression validation — Runtime now; compile-time as future enhancement.
- **Q8** Unique job lock scope — Per-queue default, global opt-in *(shipped)*.
- **Q9** Rate limiter backend — Pluggable: Postgres default *(shipped)*, in-process optimization later.
- **Q10** Migration directory — `priv/migrations/` matching Depot conventions *(shipped)*.
