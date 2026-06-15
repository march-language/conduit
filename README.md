# Conduit — Job Queue & Durable Workflow Engine for March

Conduit is a **typed, crash-safe job queue and durable workflow platform** for March. Define background jobs as strongly-typed modules, schedule them with compile-time safety, and execute them across a distributed cluster with automatic retries, checkpoints, and deadletter queues.

> **Built on March's strengths**: static types catch mismatches at compile time (not 3am), the actor model provides built-in supervision, and typed workflows eliminate YAML and DAGs.

## Features

- **Typed Jobs** — Job payloads are March types; refactoring breaks the compiler, not production
- **Durable Workflows** — Write imperative multi-step workflows with `checkpoint!` calls; crashed nodes resume from the last checkpoint
- **Cron Scheduling** — Define recurring jobs with IANA timezone support and overlap handling (`Skip`, `Allow`, `Replace`)
- **Multi-Node Cluster** — Automatic leader election, heartbeats, and orphaned job recovery via Postgres
- **Retries & Backoff** — `Linear`, `Exponential`, `Fibonacci`, or custom backoff strategies with jitter
- **Rate Limiting** — Global and per-key token buckets (e.g., per user, per tenant)
- **Unique Constraints** — Deduplicate identical jobs within a duration; per-queue or global scope
- **Web Dashboard** — Real-time queue overview, job/workflow details, dead-letter triage, manual retry/cancel
- **Dead-Letter Queues** — Jobs exhausting retries move to a DLQ with `on_dead_letter` callbacks
- **Pluggable Storage** — Interface-based design; Postgres backend with VaultStore (in-memory) for testing

## Quick Start

### 1. Define a Job

```march
-- my_jobs.march
use Conduit

mod SendEmail do
  impl Conduit.Job(SendEmail) do
    fn perform(payload : String) do
      -- Deserialize and send email
      match Result.unwrap(show(payload)) do
        Ok(email) ->
          let result = Email.send(email)
          if result.ok do Ok() else Conduit.Retry("email service down") end
        Err(e) ->
          Conduit.Discard("invalid email: " ++ e)
      end
    end

    fn config() : Conduit.JobConfig do
      let default = Conduit.JobConfig.default()
      { ...default,
        queue = "email",
        max_attempts = 5,
        backoff = Conduit.Backoff.exponential,
        timeout = 60000  -- 60 seconds
      }
    end
  end
end
```

### 2. Register and Enqueue

```march
-- main.march
use Conduit
use MyJobs

let storage = { repo: MyApp.Repo }
let config = Conduit.Config.default()

-- Register the performer at startup
Conduit.register("SendEmail", fn payload ->
  SendEmail.perform(payload)
, SendEmail.config())

-- Start workers, cron scheduler, and node coordination
Conduit.start(config, storage)

-- Later, enqueue a job
let result = Conduit.enqueue(SendEmail, "SendEmail", show({to: "user@example.com"}), storage)
match result do
  Ok(job_id) -> Logger.info("Enqueued job " ++ job_id)
  Err(e)     -> Logger.error("Enqueue failed: " ++ e)
end

-- Enqueue for later
Conduit.enqueue_in(SendEmail, "SendEmail", payload, 3600000, storage) -- 1 hour from now
```

### 3. Handle Failures

The `ConduitError` type returned by performers controls retry behavior:

```march
fn perform(payload : String) : Conduit.ConduitError do
  match do_work(payload) do
    Ok(result)  -> Ok()
    Err(TemporaryFailure(msg)) ->
      Conduit.Retry(msg)  -- Retry using backoff
    Err(PermanentFailure(msg)) ->
      Conduit.Discard(msg)  -- No retry; move to DLQ
    Err(TryAgainLater(ms)) ->
      Conduit.Snooze(ms)  -- Postpone without consuming a retry
  end
end
```

### 4. Cron Jobs

```march
mod DailyReport do
  impl Conduit.Cron(DailyReport) do
    fn perform() : Conduit.ConduitError do
      -- Generate and send report
      Ok()
    end

    fn config() : Conduit.CronConfig do
      {
        schedule = "0 9 * * *",      -- 9am every day
        timezone = "America/New_York",
        queue = "default",
        overlap = Conduit.CronOverlap.Skip,  -- Don't fire if previous run is still active
        tags = Nil,
        jitter_ms = Some(30000)  -- Add 0-30s random delay to prevent thundering herd
      }
    end
  end
end

-- Register at startup
Conduit.register_cron("DailyReport", fn () -> DailyReport.perform(), DailyReport.config())
```

### 5. Workflows (Durable Multi-Step)

```march
mod PaymentWorkflow do
  impl Conduit.Workflow(PaymentWorkflow) do
    fn run(ctx : Conduit.WorkflowContext) : Result(String, Conduit.WorkflowError) do
      -- Step 1: Charge card
      let charge_result = ctx.checkpoint("charge", fn () ->
        Stripe.charge(payment.amount)
      )
      match charge_result do
        Err(e) -> Err(Conduit.WorkflowError.Failed("charge failed: " ++ e))
        Ok(charge_id) ->
          -- Step 2: Fulfill order
          let fulfill_result = ctx.checkpoint("fulfill", fn () ->
            Warehouse.ship(order_id)
          )
          match fulfill_result do
            Err(e) ->
              -- Rollback: refund the charge
              Stripe.refund(charge_id)
              Err(Conduit.WorkflowError.Failed("fulfillment failed: " ++ e))
            Ok(tracking_id) ->
              Ok(tracking_id)
          end
      end
    end

    fn config() : Conduit.WorkflowConfig do
      {
        execution_mode = Conduit.ExecutionMode.Checkpoint,
        timeout = Some(3600000),  -- 1 hour max
        tags = Nil
      }
    end
  end
end

-- Start a workflow
let result = Conduit.start_workflow("payment_123", "PaymentWorkflow", show(payment_args), storage)
match result do
  Ok(handle) -> Logger.info("Workflow started: " ++ handle.id)
  Err(e) -> Logger.error("Failed: " ++ e)
end
```

### 6. Monitor via Dashboard

Start the standalone dashboard:

```march
Conduit.Dashboard.start_standalone(
  storage,
  Conduit.Dashboard.Auth.None,  -- or Auth.Token("secret") for production
  3000  -- port
)
```

Then visit `http://localhost:3000` to see:
- Queue overview (pending, running, failed, completed counts)
- Job details (payload, error history, stack trace)
- Dead-letter queue with manual retry/triage
- Workflow state and checkpoint history
- Cron schedule status and last fires
- Cluster node health and job distribution

## Documentation

- **[CLAUDE.md](./CLAUDE.md)** — Developer guide (language rules, critical runtime notes, architecture)
- **[specs/conduit.md](./specs/conduit.md)** — Full design spec with phased roadmap and design principles
- **[specs/features.md](./specs/features.md)** — Feature inventory by phase (what's shipped)
- **[specs/todo.md](./specs/todo.md)** — Implementation checklist (upcoming work)

## Project Structure

```
lib/conduit.march            # Public API + Job and Storage interfaces (Conduit.enqueue, register, start_workflow, …)
lib/conduit/
  ├── api.march              # Conduit.API.start — boots workers + node + cron scheduler
  ├── backoff.march          # Retry strategies (Linear, Exponential, Fibonacci, Custom)
  ├── config.march           # JobConfig and Config defaults
  ├── storage/postgres.march # Postgres Storage implementation (recommended)
  ├── queue.march            # Queue operations and performer registry
  ├── worker.march           # Task-based worker pool with heartbeat
  ├── cron.march             # Cron job definition
  ├── cron_scheduler.march   # Background cron tick loop
  ├── workflow.march         # Workflow types and context
  ├── workflow_context.march # Checkpoint and parallel execution
  ├── workflow_runner.march  # Workflow execution as a job
  ├── event_store.march      # Event log for deterministic replay
  ├── node.march             # Multi-node cluster registration & leader election
  └── dashboard/             # Web UI (pages, router, auth)

test/
  ├── test_conduit.march        # Core tests (jobs, crons, workflows, dashboard)
  └── test_deterministic_app.march  # Deterministic replay tests
```

## Implementation Status

| Phase | Feature | Status |
|-------|---------|--------|
| 1 | Simple job queue (enqueue, execute, complete/fail) | ✅ Complete |
| 2 | Retries, backoff, dead-letter queues | ✅ Complete |
| 3 | Cron scheduling with timezone support | ✅ Complete |
| 4 | Imperative workflows, checkpoints, signals | ✅ Complete |
| 5 | Multi-node cluster (leader election, heartbeats) | ✅ Complete |
| 6 | Web dashboard (queues, jobs, workflows, crons, nodes) | ✅ Complete |
| 7 | Rate limiting, priorities, unique constraints | ✅ Complete |
| 8 | Deterministic replay and event sourcing | ✅ Complete |
| 9 | Postgres storage backend | 🚧 In Progress |
| 10 | Telemetry, middleware hooks, rate-limit polish | 🚧 Planned |

## Getting Help

- **For March syntax**: Read [.claude/skills/march-lang/SKILL.md](./.claude/skills/march-lang/SKILL.md)
- **To search the codebase**: Use `forge search "function_name"` (preferred over grep)
- **To run tests**: `forge test`
- **To typecheck**: `forge check`
- **To lint**: `forge lint --strict`

## Design Principles

1. **Make invalid states unrepresentable** — Job status transitions are type-enforced; a job cannot be both running and completed
2. **Crash-safe by default** — Jobs acknowledged only after successful completion; crashed workers leave jobs available for re-pickup
3. **The queue is not the bottleneck** — Postgres advisory locks eliminate lock contention under high concurrency
4. **Observability is built-in** — Job spans, workflow checkpoints, and cluster health are always visible
5. **Migrations are yours** — We generate migration files; you review, commit, and run them
6. **Testing is first-class** — Fake in-memory backend, time-travel helpers, and inline execution mode

## Contributing

- Write March code following the language rules in CLAUDE.md
- Use `forge test` to validate changes
- Add property tests for probabilistic behavior (e.g., backoff distribution, retry ordering)
- Keep the architecture table in CLAUDE.md in sync with new modules
