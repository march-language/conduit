# Conduit — Competitive Roadmap

> **Purpose:** Where Conduit stands against Oban, Temporal, and Airflow, what's
> genuinely missing, and the sequenced plan to close the gaps that matter.
> The actionable checklist lives in [todo.md](./todo.md); this document is the
> *why* and the *design*.
>
> **Last updated:** 2026-06-15

---

## 1. Positioning

Conduit is implicitly compared to three different products aimed at three
different audiences. The strategic decision that shapes this roadmap:

> **Conduit is "Oban + a lighter Temporal" for the March ecosystem. It is
> explicitly NOT chasing Airflow.**

| Product | Category | Conduit's stance |
|---------|----------|------------------|
| **Oban** | Postgres-backed typed job queue (Elixir) | **Home turf.** Match and exceed on type-safety; reach parity on operability. |
| **Temporal** | Durable workflow engine | **Differentiation.** Conduit has the right model (imperative + checkpoint/replay); close the depth gap on the primitives that make it production-grade. |
| **Airflow** | Data-pipeline orchestrator (DAGs) | **Non-goal.** Different paradigm and audience. Imperative typed workflows are a *better* answer than DAG-YAML for app workflows; we will not build DAG authoring, backfills, datasets, sensors, or a grid/gantt UI. |

The thing that is actually differentiated — and must never be diluted — is
**compile-time-typed jobs and workflows**: refactoring breaks the compiler, not
production. Every roadmap item is judged against whether it strengthens that
core or spreads us thin chasing a third product.

### Why not Airflow

Airflow's value is declarative DAGs, backfill/catchup over data intervals,
dataset-aware scheduling, sensors, and a rich operational UI for data engineers.
That is a large surface for a *different* user than Conduit's (application
developers running background work and business workflows). Imperative
code-as-workflow already subsumes the "express a pipeline" need without YAML or a
separate scheduler mental model. Chasing Airflow parity would cost enormous
effort for an audience we are not built for.

---

## 2. Current State (2026-06-15)

What is **actually real**, as opposed to designed:

- ✅ Typed job & workflow API; `ConduitError` retry semantics; backoff strategies
- ✅ Cron with timezone + overlap modes + jitter
- ✅ Unique-job fingerprinting; token-bucket rate limiting
- ✅ Checkpoint/replay workflow model + opt-in deterministic event replay
- ✅ Wired telemetry bus (enqueue/start/complete/snooze/fail/dead-letter events) with pluggable handlers + a logger handler
- ✅ Server-rendered dashboard (overview, queues, jobs, crons, dead-letters, nodes)
- ✅ **Postgres storage backend — job-lifecycle slice, verified live end-to-end** (enqueue, claim via `SELECT … FOR UPDATE SKIP LOCKED`, mark/retry/discard/dead-letter/rescue, *and* `fetch_next` returning decoded rows). 9/9 postgres integration tests pass against a live DB; full conduit suite 204/0. The remaining 50 `Storage` methods are still stubs (Tier 0.2).
- ✅ **Read-path blocker resolved** — the March compiler `Bytes.slice` FBIP-reuse-under-aliasing bug is fixed in `llvm_emit.ml` (dup extracted heap fields at branch entry when the scrutinee is both reused and shared) and installed; depot's wire-protocol row decoding works compiled.

The backend is now genuinely real for the job lifecycle; Tier 0 is about
completing it (the other 50 methods + pooling + integration coverage).

---

## 3. The Gap Analysis

### vs Oban (job queue) — near-parity in design, gaps in operability

| Capability | Status | Gap |
|---|---|---|
| Postgres `SKIP LOCKED` queue | 🚧 write path done | finish read path (RC fix) + remaining methods |
| Real serialization | 🔴 `show/1` placeholder | typed payloads can't safely round-trip through the DB |
| Job pruning / retention | 🔴 none (only stale *nodes*) | tables grow unbounded — an immediate prod failure |
| Cron / unique / rate-limit / DLQ / telemetry / dashboard | ✅ | parity |
| Production maturity | 🔴 unproven | needs tests against the real backend + soak |

### vs Temporal (workflows) — right model, "cheap" version of the primitives

| Capability | Status | Gap |
|---|---|---|
| Durable checkpoints / replay | ✅ | solid foundation |
| Durable timers / `sleep(duration)` | 🔴 in-process `task_yield` only | "wait 3 days" must survive a crash as a first-class primitive |
| `parallel` fan-out | ⚠️ runs **sequentially** | defeats the purpose |
| Event-driven signal waits | ⚠️ polls DB every 1s on a **pinned worker** | Temporal parks a waiting workflow at zero cost |
| Activities w/ own retry/timeout/heartbeat | 🔴 checkpoints are memoized fns | no independent activity semantics |
| Continue-as-new | 🔴 | long-lived workflows accumulate unbounded history |
| Versioning enforcement | 🔴 (decided, not enforced) | in-flight workflows break on code change |
| Visibility / search / query handlers | 🔴 dashboard workflow list renders empty | can't list/filter/query running workflows |
| Deterministic time/random interception | 🔴 deferred | replay isn't truly deterministic if `run()` calls `DateTime.now()`/`Random` |

### vs Airflow — **non-goal** (documented for completeness)

DAG authoring, backfill/catchup, sensors, concurrency pools, dataset/data-aware
scheduling, grid/gantt UI. Not planned. The one item worth stealing is **true
parallel task execution**, which we want anyway for Temporal-lane fan-out.

---

## 4. Roadmap

Tiers are ordered by dependency and leverage. Within a tier, items can largely
proceed in parallel.

### Tier 0 — Make the backend real *(in progress)*

The precondition for everything else. Nothing below is trustworthy until jobs
durably survive a process restart and a real multi-node cluster works.

- **T0.1** Fix the `Bytes.slice` Perceus RC bug (march compiler) — *in flight*.
- **T0.2** Finish `Conduit.Storage.Postgres`: the remaining 50 methods (cron, workflow, checkpoint, signal, node, dashboard query, rate-limit, unique, event store), each backed by SQL against the existing schema.
- **T0.3** Connection pooling — replace the per-operation `pwith_conn` with depot's `Pool` (single choke-point change).
- **T0.4** Integration test matrix against a live DB (job lifecycle, cron fire, workflow run, multi-node reclaim, dead-letter, rate-limit, unique).

### Tier 1 — Oban table stakes

- **T1.1 Real serialization.** Replace the `show/1` placeholder with a typed
  encode/decode for job payloads and checkpoint values.
  - *Design:* derive a JSON codec per job-payload type (March types are static;
    the codec is generated, not hand-written). Store `payload` as validated JSON
    text. Versioned: include a codec/schema tag so a payload written by an old
    encoder is detectable. Keep `Conduit.Schema` as the optional validation layer
    on top. Until type-directed derivation lands, provide an explicit
    `Conduit.Codec(a)` interface (`encode : a -> String`, `decode : String -> Result(a, e)`)
    that jobs implement, replacing the implicit `show`.
- **T1.2 Pruning / retention.** A background, leader-only pruner that deletes
  terminal rows past a configurable horizon.
  - *Design:* `Config.retention = { completed_after_ms, dead_after_ms, workflow_after_ms }`
    (default 24h completed, longer for dead/workflow). A periodic task on the
    cluster leader runs `DELETE … WHERE status IN ('completed','dead') AND
    completed_at < $cutoff LIMIT $batch` in batches to avoid long locks.
    Mirror for `conduit_workflows`/`conduit_checkpoints`/`conduit_workflow_events`
    once their workflow completes + horizon. Dashboard surfaces row counts so
    growth is visible.

### Tier 2 — Temporal-lane workflow depth *(the differentiation)*

- **T2.1 Durable `sleep` / timers.** `ctx.sleep(duration)` that survives crashes.
  - *Design:* model a sleep as a checkpoint whose value is the wake time. On
    first execution it records `wake_at` and **suspends the workflow** (the runner
    job re-enqueues itself with `run_at = wake_at` rather than blocking a worker).
    On resume, replay fast-forwards past elapsed sleeps. No worker is held during
    the wait. Reuses the existing checkpoint + runner machinery.
- **T2.2 True parallel fan-out.** Make `ctx.parallel` actually concurrent.
  - *Design:* each branch becomes its own child runner job (namespaced
    checkpoints already exist via `ctx.child`). Parent suspends until children
    complete (a join checkpoint that counts completions), rather than looping
    sequentially in-process. Degrades to in-process `task_spawn` for the
    single-node/actor-level mode (config Q3 already decided: actor-level default,
    job-level opt-in).
- **T2.3 Event-driven signal waits.** Stop pinning a worker on `wait_for_signal`.
  - *Design:* when no signal is pending, **suspend** the workflow (like sleep)
    instead of polling. `signal_workflow` flips the waiting workflow back to
    runnable (enqueue its runner) — driven by the Postgres `pg_notify` already in
    the schema, with a polling fallback. Optional timeout becomes a `run_at` on
    the suspended runner.
- **T2.4 Activities with independent semantics.** Promote checkpoints that wrap
  side effects to first-class **activities** with their own `max_attempts`,
  `timeout`, and heartbeat — distinct from the workflow's retry policy.
  - *Design:* `ctx.activity(name, opts, fn)` records an activity attempt row;
    on failure it retries per `opts` without failing the whole workflow; a
    heartbeat guards against stuck activities. Builds on the worker's existing
    heartbeat/rescue.
- **T2.5 Continue-as-new.** `ctx.continue_as_new(args)` truncates history and
  restarts the workflow with fresh state, for unbounded loops.
- **T2.6 Versioning enforcement.** Pin a running workflow to the module-content
  hash it started under (Q2 decided CAS-based); refuse/repair on mismatch.
- **T2.7 Deterministic time/random.** Provide `ctx.now()` / `ctx.random()` that
  record their value as events on first run and replay it deterministically.

### Tier 3 — Operability & trust

- **T3.1 Visibility/search API + dashboard workflow list.** `dashboard_workflows_list`
  with status/type/time filters; populate the (currently empty) workflows page;
  expose a query API for "list running workflows where …".
- **T3.2 Telemetry exporters.** The bus is wired; add concrete sinks —
  Prometheus scrape endpoint and/or OpenTelemetry export — plus span timing for
  jobs/checkpoints.
- **T3.3 Production hardening.** Soak tests, backpressure under load, connection
  exhaustion behavior, graceful drain verified against the real backend.
- **T3.4 Dashboard depth.** Throughput sparklines, workflow checkpoint timeline,
  schema-aware enqueue form (all previously deferred).

---

## 5. Sequencing & Milestones

1. **M1 — Durable backend (Tier 0).** Postgres backend complete + pooled +
   integration-tested. *Conduit survives a restart and runs a real cluster.*
2. **M2 — Oban-credible (Tier 1).** Real serialization + pruning. *Safe to run
   in production without unbounded growth or stringly-typed payloads.*
3. **M3 — Temporal-lite (Tier 2.1–2.3).** Durable sleep + true parallel +
   event-driven signals. *Long-running workflows that wait days, fan out
   concurrently, and react to signals without burning workers.*
4. **M4 — Workflow-complete (Tier 2.4–2.7).** Activities, continue-as-new,
   versioning, deterministic time. *Feature-comparable to Temporal's core for
   the common cases.*
5. **M5 — Operable (Tier 3).** Visibility, exporters, hardening, dashboard depth.

Tiers 0→1 are sequential (storage must be real first). Within Tier 2, T2.1/T2.3
share a "suspend & re-enqueue the runner" mechanism — build that primitive once
and all three (sleep, parallel-join, signal-wait) reuse it.

---

## 6. Non-Goals (reaffirmed)

- Airflow-style DAG authoring, backfill/catchup, datasets, sensors, operators.
- A bespoke query language; SQL via the storage interface is sufficient.
- Pluggable non-Postgres backends beyond the in-memory test store, until there's
  real demand (the interface stays clean so it remains possible).
- Distributed cross-node `parallel` fan-out beyond what the job queue already
  gives us for free (children are just jobs).

---

## 7. Cross-Cutting Engineering Notes

- **The "suspend the runner" primitive** (re-enqueue the workflow runner with a
  future `run_at` / on a signal, instead of blocking a worker) is the keystone for
  T2.1, T2.2, and T2.3. Design it deliberately and reuse it.
- **March compiled-path caveats** apply: keep Vault values as flat primitives;
  be mindful of FBIP/RC behavior in hot byte-handling code (the depot decode bug
  is the cautionary tale). Prefer the storage interface over clever in-memory
  tricks for anything that must be durable.
- **Every new `Storage` method** must land with both the Postgres impl and the
  in-memory test-store stub, and a live integration test.
