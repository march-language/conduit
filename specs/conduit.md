# Conduit — Job/Workflow Platform for March

> **Status:** Implementation complete (Phases 1–8); Postgres storage backend complete (all 60 Storage methods, connection pooling); CI green
> **Last Updated:** 2026-06-16

---

## Table of Contents

1. [Philosophy & Goals](#1-philosophy--goals)
2. [API Design](#2-api-design)
3. [Architecture](#3-architecture)
4. [Data Model](#4-data-model)
5. [Phased Implementation](#5-phased-implementation)
6. [Bastion Dashboard](#6-bastion-dashboard)
7. [Comparison Table](#7-comparison-table)
8. [Error Handling & Edge Cases](#8-error-handling--edge-cases)
9. [Forge Generators & Project Templates](#9-forge-generators--project-templates)
10. [Appendix: Open Questions](#10-appendix-open-questions)

---

## 1. Philosophy & Goals

### 1.1 What Is Conduit?

Conduit is March's native job queue and durable workflow engine. It handles background jobs, scheduled tasks, and long-running workflows — the unglamorous but critical infrastructure that every production application eventually needs.

Conduit is not a port of an existing system. It is designed from scratch to exploit March's specific strengths: static types, the actor model, pattern matching, and first-class functions. The result is a system that catches more bugs at compile time and expresses async coordination more naturally than any dynamically-typed equivalent.

### 1.2 What Makes Conduit Different

#### Typed Jobs — Errors at Compile Time, Not at 3am

Every job in Conduit is a typed struct. The payload is defined as a March type. The serializer is derived automatically. This means:

- Mismatched producer/consumer is a compiler error, not a runtime exception
- Refactoring a job's fields breaks the compiler, which tells you every enqueue site to update
- No stringly-typed magic. No missing field surprises. No JSON schema drift.

In contrast: Sidekiq, Oban, and most job queues treat payloads as opaque maps or JSON. You discover mismatches when a job explodes.

#### Workflows Are Just Code

Conduit workflows use the imperative style pioneered by Temporal — you write sequential March code with explicit parallel blocks. There is no DAG builder, no YAML, no visual graph. The workflow *is* the code, with all the benefits of language tooling: type checking, refactoring, testing, readability.

Checkpoints make this durable. The workflow engine persists state at designated `checkpoint!` calls, so a crashed node resumes from the last checkpoint rather than restarting from scratch.

#### Actor Model Advantage

March has native actors. Conduit uses them for:

- **Worker pool management** — each worker is an actor, monitored and restarted by a supervisor
- **Workflow execution** — each in-flight workflow instance runs as a supervised actor
- **Queue coordination** — queue actors own their state and communicate via messages
- **Heartbeats** — actors naturally express "send me a tick every N seconds"

This means the concurrency story is built on March's existing primitives rather than reimplemented ad hoc. Supervision trees handle crashes. Backpressure happens at message boundaries. Timeouts are natural.

#### Pluggable Storage, Postgres First

The storage layer is an interface. You can implement it for any backend. But we don't pretend all backends are equal — Postgres-specific features (advisory locks for multi-node coordination, `LISTEN`/`NOTIFY` for instant job pickup) are first-class and recommended for production. SQLite works for development and small deployments.

#### Zero Bastion Dependency in Core

The core `conduit` library has no dependency on Bastion. You can use Conduit in a pure CLI tool, a standalone service, or a microservice. The Bastion integration and dashboard are separate packages (`conduit_bastion`, `conduit_dashboard`) that you opt into.

### 1.3 Design Principles

**1. Make invalid states unrepresentable.**
Job status transitions are enforced at the type level. A job cannot be both `Running` and `Completed`. Workflow step results are typed and matched exhaustively.

**2. Crash-safe by default.**
Jobs are acknowledged only after successful completion. Checkpoints are atomic. A dead worker leaves its jobs available for re-pickup, not lost.

**3. The queue is not the bottleneck.**
Conduit uses Postgres advisory locks instead of row-level locks for job claiming. This eliminates lock contention under high concurrency. `SKIP LOCKED` is used where advisory locks are insufficient.

**4. Observability is not optional.**
Every job emit spans compatible with OpenTelemetry. Every workflow step is a traceable event. The dashboard shows real numbers, not estimates.

**5. Migrations are yours.**
We generate migration files into your project. You review them, commit them, and run them. Conduit never auto-migrates in production. You own your schema.

**6. Testing is a first-class workflow.**
The testing module provides a fake backend, inline execution mode, and time-travel helpers. Testing workflows with multi-step durable state should be as easy as testing a function.

### 1.4 Non-Goals (v1)

- **Distributed transactions across services** — Conduit coordinates work within a single application cluster. Cross-service sagas are out of scope.
- **Visual workflow builder** — workflows are code. No drag-and-drop.
- **Multi-tenant queue isolation** — single-tenant by design. Multi-tenancy is application logic.
- **Deterministic replay** — this is a Phase 8 feature. Early phases use checkpoint-based resumption. Deterministic replay (full Temporal-style event sourcing) is explicitly deferred.
- **Priority queues across multiple queue types simultaneously** — priority is per-queue in Phase 7.

---

## 2. API Design

### 2.0 Core Interfaces

Conduit defines its extension points as March interfaces. These live in the `conduit` library and are what users implement. They are shown here once; all examples in this section use them.

```march
-- Job interface: implement to define a background job.
-- `j` is the module implementing the interface (the job type itself).
interface Conduit.Job(j) do
  type Args  -- associated type: the serializable args struct

  fn perform: j -> Args -> Result((), ConduitError)

  -- Default implementations — override any or all of these per-job
  fn config: j -> Conduit.JobConfig do
    Conduit.JobConfig.default()
  end

  -- Optional runtime schema validation. Default: no validation (args pass through).
  -- When provided, Conduit validates args BEFORE calling perform.
  -- Validation failure → job moves to failed state immediately, no retry.
  fn schema: j -> Conduit.Schema(Args) do
    Conduit.Schema.none()
  end
end

-- Workflow interface: implement to define a durable multi-step workflow.
interface Conduit.Workflow(w) do
  type Input
  type Output

  fn run: w -> Input -> WorkflowContext -> Result(Output, WorkflowError)

  fn config: w -> Conduit.WorkflowConfig do
    Conduit.WorkflowConfig.default()
  end

  -- Optional runtime schema validation for workflow Input.
  -- Validated before run() is called. Failure → workflow moves to failed immediately.
  fn schema: w -> Conduit.Schema(Input) do
    Conduit.Schema.none()
  end
end

-- Cron interface: implement to define a scheduled job.
interface Conduit.Cron(c) do
  fn perform: c -> Result((), ConduitError)
  fn config: c -> Conduit.CronConfig
end

-- Dynamic cron with a payload (schedule stored in DB alongside args).
interface Conduit.CronWithArgs(c) do
  type Args
  fn perform: c -> Args -> Result((), ConduitError)
  fn config: c -> Conduit.CronConfig
end

-- Middleware interface: wraps job execution for cross-cutting concerns.
interface Conduit.Middleware(m) do
  fn call: m -> JobInfo -> (fn () -> Result((), ConduitError)) -> Result((), ConduitError)
end

-- Storage interface: implement to add a new backend.
-- See §3.3 for the full definition.

-- Telemetry interface: implement to receive Conduit events.
-- See §3.9 for the full definition.
```

**Config types** — used in `fn config` overrides:

```march
type Conduit.Backoff =
  | Linear
  | Exponential
  | Fibonacci
  | Custom(fn: fn Int -> Duration)

type Conduit.CronOverlap = Skip | Allow | Replace

type Conduit.ExecutionMode = Checkpoint | DeterministicReplay

type Conduit.JobConfig = {
  queue: String,              -- default: "default"
  max_attempts: Int,          -- default: 3
  timeout: Duration,          -- default: Duration.seconds(30)
  backoff: Conduit.Backoff,   -- default: Exponential
  dead_letter_queue: Option(String),
  unique_for: Option({ duration: Duration, by: List(String) }),
  priority: Int,              -- default: 0 (higher runs first)
  tags: List(String),
  discard_after: Option(Duration),
  on_dead_letter: Option(fn JobInfo -> ())
}

type Conduit.CronConfig = {
  schedule: String,
  timezone: String,           -- default: "UTC"
  queue: String,              -- default: "default"
  overlap: Conduit.CronOverlap,
  tags: List(String)
}

type Conduit.WorkflowConfig = {
  execution_mode: Conduit.ExecutionMode  -- default: Checkpoint
}

-- Schema type — opaque, constructed via Conduit.Schema.build/1
-- `a` is the Args/Input type being validated
type Conduit.Schema(a) = Opaque
```

### 2.1 Simple Jobs

The simplest Conduit program: define a job module with `type Args`, implement `Conduit.Job`, enqueue it.

```march
mod SendWelcomeEmail do
  type Args = {
    user_id: Int,
    email: String,
    name: String
  }

  impl Conduit.Job(SendWelcomeEmail) do
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      let body = "Welcome, #{args.name}!"
      Email.send(to: args.email, subject: "Welcome", body: body)
    end
  end
end
```

Enqueue from anywhere:

```march
-- Enqueue with default options
Conduit.enqueue(SendWelcomeEmail, { user_id: 42, email: "alice@example.com", name: "Alice" })

-- Enqueue with a delay
Conduit.enqueue_in(SendWelcomeEmail, Duration.minutes(5), {
  user_id: 42,
  email: "alice@example.com",
  name: "Alice"
})

-- Enqueue at a specific time
Conduit.enqueue_at(SendWelcomeEmail, DateTime.utc_now() |> DateTime.add(hours: 24), {
  user_id: 42,
  email: "alice@example.com",
  name: "Alice"
})
```

### 2.2 Job Configuration

Jobs override `fn config` in their `impl` block to customise behaviour. Jobs that need no special configuration can omit `fn config` entirely and get the defaults from the interface.

```march
mod ProcessVideoUpload do
  type Args = {
    video_id: String,
    user_id: Int,
    source_url: String,
    target_formats: List(String)
  }

  impl Conduit.Job(ProcessVideoUpload) do
    fn config(_self) -> Conduit.JobConfig do
      {
        queue: "media_processing",
        max_attempts: 5,
        timeout: Duration.minutes(30),
        backoff: Conduit.Backoff.Exponential,
        unique_for: Some({ duration: Duration.minutes(10), by: ["video_id"] }),
        tags: ["video", "media"],
        ..Conduit.JobConfig.default()
      }
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      args.target_formats |> List.each(fn fmt ->
        VideoEncoder.transcode(args.video_id, args.source_url, fmt)
      end)
    end
  end
end
```

All available `Conduit.JobConfig` fields and their defaults:

```march
-- Full config with every option shown explicitly.
-- In practice, use ..Conduit.JobConfig.default() to inherit unset fields.
fn config(_self) -> Conduit.JobConfig do
  {
    queue: "media_processing",               -- default: "default"
    max_attempts: 5,                         -- default: 3  (0 = no retries)
    timeout: Duration.minutes(30),           -- default: Duration.seconds(30)
    backoff: Conduit.Backoff.Exponential,    -- or Linear, Fibonacci, Custom(fn)
    dead_letter_queue: Some("dead_letters"), -- default: None
    unique_for: Some({                       -- default: None
      duration: Duration.minutes(10),
      by: ["user_id"]
    }),
    priority: 10,                            -- default: 0 (higher runs first)
    tags: ["billing", "critical"],           -- default: []
    discard_after: Some(Duration.hours(24)), -- default: None
    on_dead_letter: Some(fn job ->           -- default: None
      Alert.page_oncall("Job #{job.id} dead-lettered", job)
    end),
    ..Conduit.JobConfig.default()
  }
end
```

### 2.3 Retry & Backoff Strategies

Conduit ships three built-in backoff strategies and supports custom functions:

```march
-- Linear backoff: attempt 1 -> 5s, 2 -> 10s, 3 -> 15s
mod LinearRetryJob do
  type Args = { id: Int }
  impl Conduit.Job(LinearRetryJob) do
    fn config(_self) -> Conduit.JobConfig do
      { max_attempts: 10, backoff: Conduit.Backoff.Linear, ..Conduit.JobConfig.default() }
    end
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      ExternalService.call(args.id)
    end
  end
end

-- Exponential backoff: 1s, 2s, 4s, 8s, 16s...
mod ExponentialRetryJob do
  type Args = { id: Int }
  impl Conduit.Job(ExponentialRetryJob) do
    fn config(_self) -> Conduit.JobConfig do
      { max_attempts: 8, backoff: Conduit.Backoff.Exponential, ..Conduit.JobConfig.default() }
    end
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      ExternalService.call(args.id)
    end
  end
end

-- Fibonacci backoff: 1s, 1s, 2s, 3s, 5s, 8s, 13s...
mod FibonacciRetryJob do
  type Args = { id: Int }
  impl Conduit.Job(FibonacciRetryJob) do
    fn config(_self) -> Conduit.JobConfig do
      { max_attempts: 6, backoff: Conduit.Backoff.Fibonacci, ..Conduit.JobConfig.default() }
    end
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      ExternalService.call(args.id)
    end
  end
end

-- Custom backoff function
mod CustomRetryJob do
  type Args = { id: Int }
  impl Conduit.Job(CustomRetryJob) do
    fn config(_self) -> Conduit.JobConfig do
      {
        max_attempts: 5,
        backoff: Conduit.Backoff.Custom(fn attempt ->
          let base = Duration.seconds(10)
          let jitter = Duration.milliseconds(Random.int(0, 5000))
          Duration.add(Duration.multiply(base, attempt), jitter)
        end),
        ..Conduit.JobConfig.default()
      }
    end
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      ExternalService.call(args.id)
    end
  end
end
```

#### Controlling Retries From Inside a Job

```march
mod SmartRetryJob do
  type Args = { user_id: Int }

  impl Conduit.Job(SmartRetryJob) do
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      match ExternalApi.fetch_user(args.user_id) do
        Ok(user) ->
          process_user(user)
        Err(NotFound) ->
          -- Discard permanently — no point retrying
          Err(ConduitError.Discard("User #{args.user_id} not found"))
        Err(RateLimited(retry_after)) ->
          -- Snooze until the API says we can retry
          Err(ConduitError.Snooze(retry_after))
        Err(ServiceUnavailable) ->
          -- Normal retry with default backoff
          Err(ConduitError.Retry("Service unavailable"))
        Err(e) ->
          Err(ConduitError.WorkerError(e))
      end
    end
  end
end
```

#### Dead Letter Queues

When a job exhausts all retries, it moves to the dead letter queue (if configured):

```march
mod CriticalEmailJob do
  type Args = { to: String, subject: String, body: String }

  impl Conduit.Job(CriticalEmailJob) do
    fn config(_self) -> Conduit.JobConfig do
      {
        queue: "email",
        max_attempts: 5,
        dead_letter_queue: Some("email_dead_letters"),
        on_dead_letter: Some(fn job ->
          Alert.page_oncall("Job #{job.id} moved to dead letter queue", job)
        end),
        ..Conduit.JobConfig.default()
      }
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      Email.send(to: args.to, subject: args.subject, body: args.body)
    end
  end
end
```

### 2.4 Imperative Workflows with Typed Steps

Workflows are the heart of Conduit's power. They are sequential March code with durable checkpoints.

```march
mod OnboardUserWorkflow do
  type Input = {
    user_id: Int,
    email: String,
    plan: String
  }

  type Output = {
    account_id: String,
    provisioned_at: DateTime
  }

  impl Conduit.Workflow(OnboardUserWorkflow) do
  fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
    -- Step 1: Create account (durable — if we crash, we resume after this)
    let account_id = checkpoint!(ctx, "create_account", fn () ->
      AccountService.create(input.user_id, input.plan)
    end)

    -- Step 2: Send welcome email (durable)
    checkpoint!(ctx, "send_welcome_email", fn () ->
      Email.send_welcome(input.email, account_id)
    end)

    -- Step 3: Provision resources (can take minutes)
    checkpoint!(ctx, "provision_resources", fn () ->
      ResourceProvisioner.provision(account_id, input.plan)
    end)

    -- Step 4: Update billing (durable)
    checkpoint!(ctx, "setup_billing", fn () ->
      BillingService.setup(input.user_id, account_id, input.plan)
    end)

    -- Step 5: Fire analytics event (fire-and-forget, no checkpoint needed)
    Analytics.track("user.onboarded", { user_id: input.user_id })

    Ok({ account_id: account_id, provisioned_at: DateTime.utc_now() })
  end
  end  -- impl
end
```

#### Starting a Workflow

```march
-- Start and get back a handle to track it
let handle = Conduit.start_workflow(OnboardUserWorkflow, {
  user_id: 42,
  email: "alice@example.com",
  plan: "pro"
})

-- Start and wait for completion (synchronous from caller's perspective)
let result = Conduit.start_workflow_and_wait(OnboardUserWorkflow, {
  user_id: 42,
  email: "alice@example.com",
  plan: "pro"
}, timeout: Duration.minutes(10))

-- Query workflow status
let status = Conduit.workflow_status(handle.id)

-- Signal a running workflow
Conduit.signal_workflow(handle.id, "approve", { approved_by: "admin@company.com" })

-- Cancel a running workflow
Conduit.cancel_workflow(handle.id, reason: "User requested cancellation")
```

### 2.5 Checkpoint-Based Durable Execution

Checkpoints are the durability primitive. Each `checkpoint!` call:
1. Checks if this checkpoint has already completed (by name within the workflow instance)
2. If yes: returns the stored result without re-executing
3. If no: executes the function, stores the result, returns it

```march
mod DataMigrationWorkflow do
  type Input = {
    source_db: String,
    target_db: String,
    table_name: String,
    batch_size: Int
  }

  type Output = {
    rows_migrated: Int,
    duration_ms: Int
  }

  impl Conduit.Workflow(DataMigrationWorkflow) do
  fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
    let start_time = DateTime.utc_now()

    -- Get total count (checkpoint so we don't re-query on resume)
    let total_rows = checkpoint!(ctx, "count_rows", fn () ->
      Database.count(input.source_db, input.table_name)
    end)

    -- Migrate in batches — each batch is a checkpoint
    let rows_migrated = checkpoint_loop!(ctx, "migrate_batches", fn loop_ctx ->
      loop_migrate(loop_ctx, input, 0, total_rows, 0)
    end)

    let duration_ms = DateTime.diff_ms(DateTime.utc_now(), start_time)
    Ok({ rows_migrated: rows_migrated, duration_ms: duration_ms })
  end

  pfn loop_migrate(ctx, input, offset, total, migrated) do
    if offset >= total do
      migrated
    else do
      let batch_key = "batch_#{offset}"
      let count = checkpoint!(ctx, batch_key, fn () ->
        Database.migrate_batch(
          input.source_db,
          input.target_db,
          input.table_name,
          offset,
          input.batch_size
        )
      end)
      loop_migrate(ctx, input, offset + input.batch_size, total, migrated + count)
    end
  end
  end  -- impl
end
```

#### Checkpoint Options

```march
-- Default checkpoint (at-least-once semantics, idempotent function expected)
checkpoint!(ctx, "step_name", fn () -> do_something() end)

-- Checkpoint with timeout
checkpoint!(ctx, "slow_step", fn () ->
  SlowService.call()
end, timeout: Duration.minutes(5))

-- Checkpoint with retry (different from job-level retries)
checkpoint!(ctx, "flaky_step", fn () ->
  FlakyService.call()
end, max_attempts: 3, backoff: :exponential)

-- Checkpoint with custom options
checkpoint!(ctx, "payment", fn () ->
  PaymentGateway.charge(amount)
end, timeout: Duration.seconds(30), idempotency_key: "payment_#{order_id}")
```

### 2.6 Fan-Out / Fan-In

Parallel work within workflows:

```march
mod ProcessOrderWorkflow do
  type Input = { order_id: Int }
  type Output = { success: Bool }

  impl Conduit.Workflow(ProcessOrderWorkflow) do
  fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
    -- Fetch order items
    let items = checkpoint!(ctx, "fetch_items", fn () ->
      OrderDB.get_items(input.order_id)
    end)

    -- Fan out: process each item in parallel
    let results = parallel!(ctx, items, fn (item, item_ctx) ->
      checkpoint!(item_ctx, "process_item_#{item.id}", fn () ->
        InventoryService.reserve(item.product_id, item.quantity)
      end)
    end)

    -- Fan in: check all results
    let all_ok = results |> List.all(fn r -> Result.is_ok(r) end)

    if all_ok do
      checkpoint!(ctx, "confirm_order", fn () ->
        OrderDB.confirm(input.order_id)
      end)
      Ok({ success: true })
    else do
      -- Roll back reservations for items that succeeded
      let successful = results |> List.filter_map(fn r -> Result.to_option(r) end)
      successful |> List.each(fn reservation ->
        InventoryService.release(reservation)
      end)
      Err(WorkflowError.failed("Some items could not be reserved"))
    end
  end
  end  -- impl
end
```

#### Parallel with Typed Results

```march
mod ReportGenerationWorkflow do
  type Input = { report_id: Int, sections: List(String) }
  type Output = { report_url: String }

  type SectionResult = {
    section: String,
    content: String,
    generated_at: DateTime
  }

  impl Conduit.Workflow(ReportGenerationWorkflow) do
  fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
    -- Generate all sections in parallel, typed results
    let sections: List(SectionResult) = parallel!(ctx, input.sections, fn (section, s_ctx) ->
      checkpoint!(s_ctx, "generate_#{section}", fn () ->
        ReportEngine.generate_section(input.report_id, section)
      end)
    end) |> Result.all()

    -- Assemble the report
    let report_url = checkpoint!(ctx, "assemble", fn () ->
      ReportEngine.assemble(input.report_id, sections)
    end)

    Ok({ report_url: report_url })
  end
  end  -- impl
end
```

### 2.7 Waiting for External Events (Signals)

Workflows can pause and wait for signals from the outside world:

```march
mod ApprovalWorkflow do
  type Input = {
    request_id: Int,
    requested_by: Int,
    amount: Float,
    description: String
  }

  type Output = {
    approved: Bool,
    reviewed_by: Option(String),
    reviewed_at: Option(DateTime)
  }

  -- Signal types are defined explicitly
  type Signal =
    | Approved(approver: String)
    | Rejected(approver: String, reason: String)

  impl Conduit.Workflow(ApprovalWorkflow) do
  fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
    -- Notify approvers
    checkpoint!(ctx, "notify_approvers", fn () ->
      Approvals.notify_pending(input.request_id, input.amount, input.description)
    end)

    -- Wait for approval signal (with timeout)
    let decision = wait_for_signal!(ctx, Signal,
      timeout: Duration.days(3),
      on_timeout: Rejected("system", "Auto-rejected after timeout")
    )

    match decision do
      Approved(approver) ->
        checkpoint!(ctx, "process_approved", fn () ->
          Finance.process_request(input.request_id, input.amount)
        end)
        Ok({ approved: true, reviewed_by: Some(approver), reviewed_at: Some(DateTime.utc_now()) })
      Rejected(approver, reason) ->
        checkpoint!(ctx, "process_rejected", fn () ->
          Approvals.notify_rejected(input.request_id, reason)
        end)
        Ok({ approved: false, reviewed_by: Some(approver), reviewed_at: Some(DateTime.utc_now()) })
    end
  end
  end  -- impl
end
```

### 2.8 Cron Jobs

```march
-- Static cron: schedule is fixed at compile time
mod DailyReportJob do
  impl Conduit.Cron(DailyReportJob) do
    fn config(_self) -> Conduit.CronConfig do
      {
        schedule: "0 9 * * *",      -- 9am every day
        timezone: "America/New_York",
        queue: "reports",
        overlap: Conduit.CronOverlap.Skip,
        tags: []
      }
    end

    fn perform(_self) -> Result((), ConduitError) do
      let yesterday = Date.today() |> Date.subtract_days(1)
      Report.generate_daily(yesterday) |> Report.email_to_all_managers()
    end
  end
end

-- Dynamic cron: schedule and args stored in DB at runtime
mod UserReminderCron do
  type Args = { user_id: Int, schedule: String, message: String }

  impl Conduit.CronWithArgs(UserReminderCron) do
    fn config(_self) -> Conduit.CronConfig do
      { schedule: "", timezone: "UTC", queue: "default", overlap: Conduit.CronOverlap.Skip, tags: [] }
      -- Note: schedule is overridden per-registration when using CronWithArgs
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      User.send_reminder(args.user_id, args.message)
    end
  end
end

-- Register dynamic cron
Conduit.schedule_cron(UserReminderCron, {
  user_id: 42,
  schedule: "0 10 * * MON",
  message: "Weekly standup reminder"
}, id: "reminder_#{user_id}")

-- Cancel dynamic cron
Conduit.cancel_cron("reminder_42")
```

### 2.9 Middleware

Middleware wraps job execution for cross-cutting concerns:

```march
-- Define middleware
mod LoggingMiddleware do
  impl Conduit.Middleware(LoggingMiddleware) do
    fn call(_self, job: JobInfo, next: fn () -> Result((), ConduitError)) -> Result((), ConduitError) do
      let start = Clock.monotonic_ms()
      Logger.info("Job starting", { job_id: job.id, job_type: job.job_type })
      let result = next()
      let duration = Clock.monotonic_ms() - start
      match result do
        Ok(()) ->
          Logger.info("Job completed", { job_id: job.id, duration_ms: duration })
          Ok(())
        Err(e) ->
          Logger.error("Job failed", { job_id: job.id, duration_ms: duration, error: e })
          Err(e)
      end
    end
  end
end

mod TracingMiddleware do
  impl Conduit.Middleware(TracingMiddleware) do
    fn call(_self, job: JobInfo, next: fn () -> Result((), ConduitError)) -> Result((), ConduitError) do
      Telemetry.with_span("conduit.job", { job_type: job.job_type, queue: job.queue }, fn () ->
        next()
      end)
    end
  end
end

mod RateLimitMiddleware do
  impl Conduit.Middleware(RateLimitMiddleware) do
    fn call(_self, job: JobInfo, next: fn () -> Result((), ConduitError)) -> Result((), ConduitError) do
      match RateLimiter.acquire(job.job_type, tokens: 1) do
        Ok(()) -> next()
        Err(RateLimited(retry_after)) ->
          Err(ConduitError.Snooze(retry_after))
      end
    end
  end
end
```

Register middleware in config:

```march
-- config/conduit.march
Conduit.configure do
  middleware [
    TracingMiddleware,
    LoggingMiddleware,
    RateLimitMiddleware
  ]
end
```

### 2.10 Error Handling

Errors in Conduit are typed and compositional:

```march
type ConduitError =
  | Retry(message: String)
  | RetryAt(at: DateTime, message: String)
  | Snooze(duration: Duration)
  | Discard(reason: String)
  | WorkerError(source: Error)
  | Timeout(after: Duration)

-- Usage in jobs
fn perform(payload: Payload) -> Result((), ConduitError) do
  match do_work(payload) do
    Ok(result) -> Ok(())
    Err(NotFound(id)) ->
      -- Don't retry — the resource won't appear
      Err(ConduitError.Discard("Resource #{id} not found"))
    Err(TemporaryFailure) ->
      -- Retry with default backoff
      Err(ConduitError.Retry("Temporary failure, will retry"))
    Err(ServiceDown) ->
      -- Retry in 5 minutes specifically
      Err(ConduitError.RetryAt(DateTime.utc_now() |> DateTime.add(minutes: 5), "Service down"))
    Err(e) ->
      -- Unexpected error — wrap and let retry policy handle it
      Err(ConduitError.WorkerError(e))
  end
end
```

### 2.11 Global Configuration

```march
-- config/conduit.march (or inline in Bastion app config)
Conduit.configure do
  -- Storage backend
  storage Conduit.Storage.Postgres, url: System.env("DATABASE_URL")
  -- storage Conduit.Storage.SQLite, path: "dev.db"

  -- Worker pool
  worker_pool_size 10          -- thread count (opaque to jobs)
  poll_interval Duration.milliseconds(500)
  claim_limit 5                -- max jobs claimed per poll cycle

  -- Queue defaults (overridden per-job)
  queue "default" do
    max_concurrency 20
    timeout Duration.seconds(30)
    max_attempts 3
    backoff :exponential
  end

  queue "media_processing" do
    max_concurrency 4          -- expensive work, fewer workers
    timeout Duration.minutes(30)
    max_attempts 5
  end

  queue "critical" do
    max_concurrency 10
    timeout Duration.seconds(10)
    max_attempts 10
    backoff :linear
  end

  -- Global middleware (applied to all jobs)
  middleware [TracingMiddleware, LoggingMiddleware]

  -- Telemetry
  telemetry do
    handler Conduit.Telemetry.Logger
    handler Conduit.Telemetry.StatsD, host: "localhost", port: 8125
  end

  -- Workflow engine
  workflow do
    checkpoint_storage Conduit.Storage.Postgres  -- can differ from job storage
    max_concurrent_workflows 50
    workflow_timeout Duration.days(30)
  end
end
```

### 2.12 Schema Validation

Schema validation is **optional**. If you define `type Args = { to: String }` and implement `fn perform`, that's all you need — you get compile-time type checking at no extra cost. Schemas add *runtime* validation for args arriving from external sources: JSON payloads from the dashboard, manually enqueued jobs, third-party webhooks, or API endpoints that feed directly into a queue.

#### The Schema API

Conduit reuses the **Depot changeset pattern**. If you already use Depot, the field validators are the same API. No new DSL to learn.

```march
mod Conduit.Schema do
  -- Build a schema for type `a`
  fn build: (fn SchemaBuilder(a) -> SchemaBuilder(a)) -> Conduit.Schema(a)

  -- The empty schema — no validation, always passes (used as default)
  fn none: () -> Conduit.Schema(a)

  -- Run a schema against a raw JSON map (for dashboard/external sources)
  fn validate: Conduit.Schema(a) -> Json -> Result(a, List(ValidationError))

  -- Run a schema against an already-typed value (for enqueue-time validation)
  fn validate_typed: Conduit.Schema(a) -> a -> Result(a, List(ValidationError))
end
```

#### Defining a Schema

```march
mod SendEmail do
  type Args = {
    to: String,
    subject: String,
    priority: Int
  }

  impl Conduit.Job(SendEmail) do
    fn schema(_self) -> Conduit.Schema(Args) do
      Conduit.Schema.build(fn s ->
        s
        |> field("to", :string,
             required: true,
             format: ~r/.+@.+\..+/)
        |> field("subject", :string,
             required: true,
             max_length: 200)
        |> field("priority", :int,
             required: true,
             inclusion: [1, 2, 3, 4, 5])
      end)
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      Mailer.send(args.to, args.subject)
      Ok(())
    end
  end
end
```

#### Available Validators

These match the Depot changeset API:

```march
-- Field type atoms: :string, :int, :float, :bool, :datetime, :date, :list, :map

field("name", :string, required: true)
field("email", :string, required: true, format: ~r/.+@.+/)
field("name", :string, min_length: 2, max_length: 100)
field("age", :int, min: 0, max: 150)
field("score", :float, min: 0.0, max: 1.0)
field("status", :string, inclusion: ["pending", "active", "cancelled"])
field("tags", :list, of: :string, max_length: 10)
field("metadata", :map)
field("scheduled_at", :datetime)
```

Custom validator functions:

```march
field("slug", :string, validate: fn value ->
  if String.matches?(value, ~r/^[a-z0-9-]+$/) do
    Ok(value)
  else
    Err("must contain only lowercase letters, digits, and hyphens")
  end
end)
```

#### Validation at Enqueue Time

When a schema is defined, `Conduit.enqueue` optionally validates at submission — fail fast before the job ever hits the queue:

```march
-- enqueue validates if the job has a schema (returns Result)
match Conduit.enqueue(SendEmail, { to: "not-an-email", subject: "Hi", priority: 1 }) do
  Ok(job_id) -> Logger.info("Enqueued #{job_id}")
  Err(ValidationErrors(errors)) ->
    Logger.warn("Invalid args", { errors: errors })
    -- e.g. errors = [{ field: "to", message: "invalid email format" }]
end

-- If you're confident in the args (typed in application code), use enqueue!
-- which raises on validation failure instead of returning Result
Conduit.enqueue!(SendEmail, { to: "alice@example.com", subject: "Hi", priority: 1 })
```

#### Validation at Execution Time

Even if enqueue-time validation is skipped (or the job was enqueued before the schema was added), Conduit validates args **before calling `perform`**. Validation failure does not retry — the error is deterministic.

```
WorkerActor picks up job
  → job.schema(_self) — check if schema is defined
  → If Conduit.Schema.none(): skip validation, call perform immediately
  → If schema defined:
      → Conduit.Schema.validate_typed(schema, args)
      → If Ok(args): call perform(self, args)
      → If Err(errors):
          → UPDATE status=failed, error="Schema validation failed: ..."
          → NO retry (validation errors are deterministic)
          → Emit JobFailed telemetry event with validation_error: true
```

#### Workflow Input Schemas

Workflow inputs follow the same pattern:

```march
mod ProcessRefundWorkflow do
  type Input = {
    order_id: String,
    amount: Float,
    reason: String
  }

  type Output = { refund_id: String }

  impl Conduit.Workflow(ProcessRefundWorkflow) do
    fn schema(_self) -> Conduit.Schema(Input) do
      Conduit.Schema.build(fn s ->
        s
        |> field("order_id", :string, required: true, format: ~r/^ord_[a-z0-9]+$/)
        |> field("amount", :float, required: true, min: 0.01)
        |> field("reason", :string, required: true, max_length: 500)
      end)
    end

    fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
      let refund_id = checkpoint!(ctx, "issue_refund", fn () ->
        PaymentGateway.refund(input.order_id, input.amount)
      end)
      Ok({ refund_id: refund_id })
    end
  end
end
```

### 2.13 Testing Helpers

Testing with Conduit is a first-class concern:

```march
-- test/jobs/send_welcome_email_test.march
mod SendWelcomeEmailTest do
  use Test
  use Conduit.Testing

  fn test_perform_sends_email() do
    -- Inline execution — job runs synchronously in test process
    let result = Conduit.Testing.perform(SendWelcomeEmail, {
      user_id: 1,
      email: "test@example.com",
      name: "Test User"
    })

    assert_ok(result)
    assert Email.Testing.was_sent_to("test@example.com")
  end

  fn test_enqueue_adds_to_queue() do
    use_fake_queue() do
      Conduit.enqueue(SendWelcomeEmail, {
        user_id: 1,
        email: "test@example.com",
        name: "Test User"
      })

      assert_enqueued(SendWelcomeEmail, { user_id: 1 })
    end
  end

  fn test_retry_behavior() do
    use_fake_queue() do
      -- Simulate failure
      Email.Testing.force_failure("test@example.com", NetworkError)

      let result = Conduit.Testing.perform_with_retries(SendWelcomeEmail, {
        user_id: 1,
        email: "test@example.com",
        name: "Test User"
      })

      -- Should have retried 3 times then moved to DLQ
      assert_discarded(SendWelcomeEmail)
    end
  end

  fn test_schema_validation_failure_does_not_retry() do
    use_fake_queue() do
      -- Enqueue with an invalid email address
      let job_id = Conduit.enqueue!(SendEmail, {
        to: "not-a-valid-email",
        subject: "Test",
        priority: 1
      })

      Conduit.Testing.drain()

      let job = Conduit.Testing.get_job(job_id)
      -- Validation failure: job is failed, never retried
      assert job.status == "failed"
      assert job.attempt == 1
      assert String.contains?(job.error, "to")
      assert String.contains?(job.error, "invalid email format")
    end
  end

  fn test_schema_validation_at_enqueue_time() do
    use_fake_queue() do
      -- enqueue/2 returns Result when schema is present
      let result = Conduit.enqueue(SendEmail, {
        to: "not-an-email",
        subject: "Hello",
        priority: 99  -- out of range: must be 1-5
      })

      assert_err(result)
      let Err(ValidationErrors(errors)) = result
      assert List.length(errors) == 2  -- 'to' and 'priority' both fail
      assert_not_enqueued(SendEmail)
    end
  end

  fn test_valid_args_pass_schema() do
    use_fake_queue() do
      let result = Conduit.enqueue(SendEmail, {
        to: "alice@example.com",
        subject: "Welcome",
        priority: 2
      })
      assert_ok(result)
      assert_enqueued(SendEmail)
    end
  end
end
```

Testing workflows:

```march
mod OnboardUserWorkflowTest do
  use Test
  use Conduit.Testing

  fn test_full_onboarding_flow() do
    use_fake_workflow_engine() do
      -- Set up mocks
      AccountService.Testing.set_response({ account_id: "acc_123" })
      Email.Testing.capture()
      ResourceProvisioner.Testing.set_response({ provisioned: true })
      BillingService.Testing.set_response({ billing_id: "bill_456" })

      let result = Conduit.Testing.run_workflow(OnboardUserWorkflow, {
        user_id: 42,
        email: "alice@example.com",
        plan: "pro"
      })

      assert_ok(result)
      let output = Result.unwrap(result)
      assert output.account_id == "acc_123"
      assert Email.Testing.was_sent_to("alice@example.com")
    end
  end

  fn test_resumes_from_checkpoint() do
    use_fake_workflow_engine() do
      -- Simulate crash after step 1
      let checkpoint_store = Conduit.Testing.FakeCheckpointStore.new()
      checkpoint_store |> Conduit.Testing.FakeCheckpointStore.set("create_account", "acc_123")
      checkpoint_store |> Conduit.Testing.FakeCheckpointStore.set_crashed_at("send_welcome_email")

      -- Resume — should NOT call AccountService again
      AccountService.Testing.track_calls()

      let result = Conduit.Testing.resume_workflow(OnboardUserWorkflow,
        checkpoint_store: checkpoint_store,
        input: { user_id: 42, email: "alice@example.com", plan: "pro" }
      )

      assert AccountService.Testing.call_count() == 0  -- was not re-called
      assert_ok(result)
    end
  end

  fn test_time_travel_approval_workflow() do
    use_fake_workflow_engine() do
      use_fake_time() do
        let handle = Conduit.Testing.start_workflow(ApprovalWorkflow, {
          request_id: 1,
          requested_by: 42,
          amount: 500.0,
          description: "Team offsite"
        })

        -- Advance time past timeout (3 days)
        FakeTime.advance(Duration.days(4))

        -- Drain — workflow should have timed out and auto-rejected
        Conduit.Testing.drain_all_workflows()

        let status = Conduit.Testing.workflow_result(handle)
        assert_ok(status)
        let output = Result.unwrap(status)
        assert output.approved == false
      end
    end
  end
end
```

### 2.14 Anti-Patterns

These patterns will bite you. Conduit tries to make them hard, but documents them explicitly.

#### Anti-Pattern: Side Effects Outside Checkpoints

```march
-- BAD: HTTP call outside a checkpoint — will re-run on resume
fn run(input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
  let result = ExternalApi.call(input.id)  -- NOT durable!
  checkpoint!(ctx, "save_result", fn () ->
    Database.save(result)
  end)
  Ok(())
end

-- GOOD: HTTP call inside checkpoint — idempotent on resume
fn run(input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
  let result = checkpoint!(ctx, "fetch_from_api", fn () ->
    ExternalApi.call(input.id)
  end)
  checkpoint!(ctx, "save_result", fn () ->
    Database.save(result)
  end)
  Ok(())
end
```

#### Anti-Pattern: Non-Deterministic Checkpoint Names

```march
-- BAD: checkpoint name depends on runtime state that may differ on resume
fn run(input: Input, ctx: WorkflowContext) do
  let items = get_items()
  items |> List.each(fn item ->
    -- The order of items may differ on resume! This causes checkpoint mismatches.
    checkpoint!(ctx, "process_#{item.id}", fn () -> process(item) end)
  end)
end

-- GOOD: sort items before iterating, or use index-based keys
fn run(input: Input, ctx: WorkflowContext) do
  let items = get_items() |> List.sort_by(fn i -> i.id end)
  items |> List.each_with_index(fn (item, i) ->
    checkpoint!(ctx, "process_item_#{i}_#{item.id}", fn () -> process(item) end)
  end)
end
```

#### Anti-Pattern: Enqueuing Jobs with Non-Serializable Types

```march
-- BAD: File handles, sockets, channels can't be serialized
mod BadJob do
  type Args = { conn: TcpConnection }  -- compile error: TcpConnection does not implement Serializable
  impl Conduit.Job(BadJob) do
    fn perform(_self, args: Args) -> Result((), ConduitError) do Ok(()) end
  end
end

-- GOOD: Use IDs or serializable data only
mod GoodJob do
  type Args = { connection_string: String, session_id: String }
  impl Conduit.Job(GoodJob) do
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      let conn = TcpConnection.connect(args.connection_string)
      -- use conn...
      Ok(())
    end
  end
end
```

#### Anti-Pattern: Long-Running Work Without Checkpoints

```march
-- BAD: 2-hour process with no checkpoints — crash = start over
fn run(input: Input, ctx: WorkflowContext) do
  let huge_dataset = load_million_rows()
  let result = process_all(huge_dataset)  -- 2 hours, no checkpoints
  save(result)
end

-- GOOD: checkpoint in batches
fn run(input: Input, ctx: WorkflowContext) do
  let total = checkpoint!(ctx, "count", fn () -> count_rows() end)
  process_in_batches(ctx, 0, total, 1000)
end
```

---

## 3. Architecture

### 3.1 Overview

Conduit has five major components:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Code                          │
│          (enqueue, start_workflow, schedule_cron)                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                      Conduit Core                                │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │  Job Queue   │  │  Workflow    │  │  Scheduler (Cron)    │   │
│  │  Engine      │  │  Engine      │  │                      │   │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘   │
│         │                 │                      │               │
│  ┌──────▼─────────────────▼──────────────────────▼───────────┐  │
│  │                    Thread Pool                             │  │
│  │         (opaque to users, managed by Conduit)              │  │
│  └──────────────────────────┬─────────────────────────────────┘  │
│                             │                                    │
│  ┌──────────────────────────▼─────────────────────────────────┐  │
│  │                  Storage Adapter                           │  │
│  │           (Postgres / SQLite / Custom)                     │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                       Database                                   │
│        (jobs, workflows, checkpoints, cron_schedules,            │
│         dead_letters, events)                                    │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 Thread Pool Execution

The thread pool is **opaque to users**. Users configure job timeouts, queue concurrency, and worker count at the application level — but they never directly create, manage, or interact with threads. Conduit owns the pool.

Internally:

```
ConduitWorkerSupervisor
├── QueuePollerActor ("default")
├── QueuePollerActor ("media_processing")
├── QueuePollerActor ("critical")
├── WorkerPoolActor
│   ├── WorkerActor(1)
│   ├── WorkerActor(2)
│   ├── ...
│   └── WorkerActor(N)
├── WorkflowSupervisor
│   ├── WorkflowRunnerActor(workflow_id_1)
│   ├── WorkflowRunnerActor(workflow_id_2)
│   └── ...
└── CronSchedulerActor
```

**QueuePollerActor**: One per active queue. Polls the storage backend on a configurable interval (default: 500ms). On Postgres, also subscribes to `LISTEN/NOTIFY` for instant wakeup when a new job is inserted. Claims a batch of jobs (configurable `claim_limit`) and dispatches them to the WorkerPoolActor.

**WorkerPoolActor**: Manages a fixed-size pool of WorkerActors. When a job arrives, it finds an available worker (or queues the job if all workers are busy). Enforces per-queue concurrency limits.

**WorkerActor**: Executes a single job at a time. Each worker is an isolated actor — a crash in one worker does not affect others. Workers are monitored by the WorkerPoolActor; a dead worker is restarted automatically.

**WorkflowRunnerActor**: Each in-flight workflow instance gets its own actor. This gives natural isolation — a misbehaving workflow can't block other workflows. The actor persists checkpoints to storage as it executes.

**CronSchedulerActor**: Maintains the cron schedule in memory (loaded from DB). On each tick, checks which schedules are due and enqueues the corresponding jobs. Uses Postgres advisory locks in multi-node mode to ensure exactly one node fires each cron.

### 3.3 Pluggable Storage Layer

Storage is an interface. Implement it to add a new backend:

```march
mod Conduit.Storage do
  type JobRecord = {
    id: String,
    queue: String,
    job_type: String,
    payload: Json,
    status: JobStatus,
    max_attempts: Int,
    attempt: Int,
    scheduled_at: DateTime,
    run_at: DateTime,
    started_at: Option(DateTime),
    completed_at: Option(DateTime),
    worker_id: Option(String),
    error: Option(String),
    meta: Json
  }

  type JobStatus = Pending | Running | Completed | Failed | Dead | Snoozed

  -- Core job operations
  interface Storage(s) do
    fn insert_job: s -> Conn -> JobRecord -> Result(JobRecord, StorageError)
    fn claim_jobs: s -> Conn -> String -> Int -> String -> Result(List(JobRecord), StorageError)
    fn complete_job: s -> Conn -> String -> Result((), StorageError)
    fn fail_job: s -> Conn -> String -> String -> Option(DateTime) -> Result((), StorageError)
    fn discard_job: s -> Conn -> String -> String -> Result((), StorageError)
    fn snooze_job: s -> Conn -> String -> DateTime -> Result((), StorageError)
    fn heartbeat_job: s -> Conn -> String -> Result((), StorageError)

    -- Checkpoint operations (for workflows)
    fn get_checkpoint: s -> Conn -> String -> String -> Result(Option(Json), StorageError)
    fn set_checkpoint: s -> Conn -> String -> String -> Json -> Result((), StorageError)

    -- Cron operations
    fn upsert_cron: s -> Conn -> CronRecord -> Result((), StorageError)
    fn get_due_crons: s -> Conn -> DateTime -> Result(List(CronRecord), StorageError)
    fn update_cron_last_run: s -> Conn -> String -> DateTime -> Result((), StorageError)

    -- Query/admin operations
    fn list_jobs: s -> Conn -> JobFilter -> Result(List(JobRecord), StorageError)
    fn count_jobs: s -> Conn -> JobFilter -> Result(Int, StorageError)
    fn get_job: s -> Conn -> String -> Result(Option(JobRecord), StorageError)
    fn retry_job: s -> Conn -> String -> Result((), StorageError)
    fn delete_job: s -> Conn -> String -> Result((), StorageError)
  end
end
```

The Postgres implementation uses `SKIP LOCKED` for job claiming and advisory locks for cron coordination:

```march
mod Conduit.Storage.Postgres do
  impl Conduit.Storage(Conduit.Storage.Postgres) do

  -- Uses SKIP LOCKED to avoid lock contention
  fn claim_jobs(_self, conn: Conn, queue: String, limit: Int, worker_id: String) do
    Depot.query(conn, """
      UPDATE conduit_jobs
      SET status = 'running',
          started_at = NOW(),
          worker_id = $4,
          attempt = attempt + 1
      WHERE id IN (
        SELECT id FROM conduit_jobs
        WHERE queue = $1
          AND status IN ('pending', 'snoozed')
          AND run_at <= NOW()
        ORDER BY priority DESC, run_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *
    """, [queue, limit, worker_id])
  end

  -- Advisory lock for cron coordination
  fn try_lock_cron(conn: Conn, cron_id: String) -> Result(Bool, StorageError) do
    let lock_key = hash_cron_id(cron_id)
    Depot.query_one(conn, "SELECT pg_try_advisory_lock($1)", [lock_key])
  end

  -- LISTEN/NOTIFY for instant job pickup
  fn subscribe_to_queue(conn: Conn, queue: String, callback: fn () -> ()) do
    Depot.listen(conn, "conduit_queue_#{queue}", fn _payload ->
      callback()
    end)
  end

  fn notify_new_job(conn: Conn, queue: String) do
    Depot.query(conn, "SELECT pg_notify($1, 'new_job')", ["conduit_queue_#{queue}"])
  end

  end  -- impl
end
```

### 3.4 Queue Internals

The queue pipeline for a single job:

```
Enqueue
  → (Optional) Schema.validate_typed(job.schema(), args)
      → If Err: return Err(ValidationErrors(...)) to caller — job never inserted
  → INSERT into conduit_jobs (status=pending, run_at=NOW() or future)
  → pg_notify("conduit_queue_{name}", "new_job")

Poll (every 500ms OR on notify)
  → UPDATE ... SKIP LOCKED ... RETURNING *
  → Dispatch to WorkerActor

Execute
  → Schema.validate_typed(job.schema(), args)
      → If Schema.none(): skip — no overhead
      → If Err(errors): UPDATE status=failed, error=validation_errors
                        NO retry — validation errors are deterministic
  → Run middleware chain
  → Call job's perform()
  → On success: UPDATE status=completed, completed_at=NOW()
  → On retry:   UPDATE status=pending, run_at=NOW()+backoff, attempt++
  → On discard: UPDATE status=dead, error=reason
  → On snooze:  UPDATE status=snoozed, run_at=snooze_until

Heartbeat (long-running jobs)
  → UPDATE heartbeat_at=NOW() every 30s
  → Stale detector: if heartbeat_at < NOW()-60s, job is orphaned → re-queue
```

### 3.5 Workflow Engine

Workflow execution is managed by `WorkflowRunnerActor`:

```
start_workflow(MyWorkflow, input)
  → INSERT into conduit_workflows (status=running, input=input)
  → INSERT into conduit_jobs (job_type=conduit.workflow_runner, payload={workflow_id})
  → Dispatch to WorkflowRunnerActor

WorkflowRunnerActor.execute(workflow_id)
  → Load workflow record
  → Schema.validate_typed(workflow.schema(), input)
      → If Schema.none(): skip
      → If Err(errors): UPDATE status=failed — no resume, no retry
  → Instantiate WorkflowContext (backed by DB checkpoint store)
  → Call workflow's run() function

During run():
  checkpoint!(ctx, "step_name", fn () -> do_work() end)
    → ctx.get_checkpoint("step_name")
      → DB: SELECT value FROM conduit_checkpoints WHERE workflow_id=$1 AND name=$2
      → If found: return stored value (skip fn)
      → If not found:
          → Execute fn()
          → DB: INSERT INTO conduit_checkpoints (workflow_id, name, value)
          → Return value

  wait_for_signal!(ctx, SignalType, timeout: dur)
    → INSERT into conduit_workflow_signals (workflow_id, status=waiting, signal_type)
    → Suspend WorkflowRunnerActor (actor goes to sleep)
    → When signal arrives OR timeout fires:
        → Actor wakes up
        → Returns signal value or timeout default

On workflow completion:
  → UPDATE conduit_workflows SET status=completed, output=result, completed_at=NOW()
  → DELETE conduit_checkpoints WHERE workflow_id=$1 (optional, configurable)

On crash (actor dies):
  → WorkflowSupervisor detects dead actor
  → Re-queues conduit.workflow_runner job
  → WorkflowRunnerActor re-runs from WorkflowContext
  → checkpoint!() calls return stored values immediately
  → Resumes from point of crash
```

### 3.6 Checkpoint System

Checkpoints are the durability backbone. Key design decisions:

**Storage:** Checkpoints are stored in `conduit_checkpoints` as JSON. The checkpoint key is `(workflow_id, step_name)`.

**Semantics:** At-least-once. A checkpoint function may execute more than once if the process crashes between execution and storage. Therefore:
- All checkpoint functions should be idempotent where possible
- Use `idempotency_key` for operations that cannot be made naturally idempotent (e.g., payment charges)

**Isolation:** Checkpoints within a single workflow instance are isolated. Two different workflow instances running the same workflow type do not share checkpoints.

**Garbage collection:** Completed workflow checkpoints are retained for a configurable period (default: 24h) for debugging, then deleted. Failed workflows retain checkpoints indefinitely until explicitly cleared.

**Nested workflows:** Sub-workflows have their own checkpoint namespace. The parent workflow checkpoints the sub-workflow's output, not its internal checkpoints.

### 3.7 Retry Engine

The retry engine runs as part of the QueuePollerActor. On each poll cycle:

1. Claim normal ready jobs (status=pending, run_at <= NOW)
2. Claim snoozed jobs that are now due (status=snoozed, run_at <= NOW)
3. Detect orphaned jobs (status=running, heartbeat_at < NOW-stale_threshold)
4. Move orphaned jobs back to pending (if attempt < max_attempts) or dead

Backoff calculation:

```march
fn calculate_run_at(config: JobConfig, attempt: Int) -> DateTime do
  let delay = match config.backoff do
    Linear(base) ->
      Duration.multiply(base, attempt)
    Exponential(base, max) ->
      let raw = Duration.multiply(base, Int.pow(2, attempt - 1))
      Duration.min(raw, max)
    Fibonacci(base) ->
      Duration.multiply(base, fib(attempt))
    Custom(f) ->
      f(attempt)
  end
  -- Add jitter: ±10% of delay to prevent thundering herd
  let jitter_range = Duration.multiply(delay, 0.1)
  let jitter = Duration.milliseconds(Random.int(
    -Duration.to_ms(jitter_range),
    Duration.to_ms(jitter_range)
  ))
  DateTime.utc_now() |> DateTime.add(Duration.add(delay, jitter))
end
```

### 3.8 Multi-Node Coordination

Conduit supports multi-node deployments from Phase 1. Each node:
- Has a unique `worker_id` (UUID, generated at startup)
- Claims jobs by setting `worker_id` in the DB
- Sends heartbeats for long-running jobs

Coordination mechanisms:

**Job claiming:** `SKIP LOCKED` ensures each job is claimed by exactly one worker.

**Cron firing:** Postgres advisory locks ensure exactly one node fires each cron schedule. The lock is acquired for the duration of the cron check, preventing duplicate fires.

**Workflow resumption:** WorkflowRunnerActors are identified by `workflow_id`. If a node dies mid-workflow, another node picks up the `conduit.workflow_runner` job and resumes from checkpoints.

**Stale job detection:** All nodes periodically scan for jobs with stale heartbeats. A job is orphaned if `heartbeat_at < NOW() - 60s` (configurable). Multiple nodes may detect the same orphaned job, but only one can UPDATE it (the first writer wins due to optimistic locking with `attempt` counter).

```march
-- Optimistic locking prevents double-pickup of stale jobs
fn rescue_stale_job(conn: Conn, job: JobRecord) -> Result(Bool, StorageError) do
  Depot.query_one(conn, """
    UPDATE conduit_jobs
    SET status = 'pending',
        run_at = NOW(),
        worker_id = NULL,
        attempt = attempt  -- don't increment, this was an orphan not a failure
    WHERE id = $1
      AND attempt = $2    -- optimistic lock
      AND status = 'running'
    RETURNING id
  """, [job.id, job.attempt])
  |> Result.map(fn row -> row != None end)
end
```

**Leader election** (optional, for future features like global rate limiting): Uses Postgres advisory locks for a "leader" lock. The node holding the lock is the cluster leader.

### 3.9 Telemetry

Conduit emits structured telemetry at every significant event:

```march
-- Events emitted
type ConduitTelemetryEvent =
  | JobEnqueued(job_id: String, job_type: String, queue: String)
  | JobStarted(job_id: String, job_type: String, worker_id: String, attempt: Int)
  | JobCompleted(job_id: String, duration_ms: Int)
  | JobFailed(job_id: String, error: String, attempt: Int, will_retry: Bool)
  | JobDiscarded(job_id: String, reason: String)
  | JobSnoozed(job_id: String, until: DateTime)
  | WorkflowStarted(workflow_id: String, workflow_type: String)
  | WorkflowCheckpointed(workflow_id: String, step: String, duration_ms: Int)
  | WorkflowCompleted(workflow_id: String, duration_ms: Int)
  | WorkflowFailed(workflow_id: String, error: String)
  | CronFired(cron_id: String, scheduled_at: DateTime)
  | QueueDepth(queue: String, pending: Int, running: Int, failed: Int)
  | WorkerPoolUtilization(active: Int, total: Int)
```

Handlers implement a simple interface:

```march
interface Conduit.TelemetryHandler(t) do
  fn handle: t -> ConduitTelemetryEvent -> ()
end

-- Built-in handlers
mod Conduit.Telemetry.Logger do
  impl Conduit.TelemetryHandler(Conduit.Telemetry.Logger) do
    fn handle(_self, event: ConduitTelemetryEvent) -> () do
      Logger.info("conduit.telemetry", { event: event })
    end
  end
end

mod Conduit.Telemetry.StatsD do
  impl Conduit.TelemetryHandler(Conduit.Telemetry.StatsD) do
    fn handle(_self, event: ConduitTelemetryEvent) -> () do
      match event do
        JobCompleted(_, duration) ->
          StatsD.histogram("conduit.job.duration", duration)
          StatsD.increment("conduit.job.completed")
        JobFailed(_, _, _, will_retry) ->
          StatsD.increment("conduit.job.failed")
          if will_retry do StatsD.increment("conduit.job.retrying") end
        QueueDepth(queue, pending, _, _) ->
          StatsD.gauge("conduit.queue.depth", pending, tags: ["queue:#{queue}"])
        _ -> ()
      end
    end
  end
end
```

### 3.10 Type System Integration

Conduit uses March's type system to enforce correctness at compile time. For correctness at runtime (args from JSON, dashboards, or external callers), it adds optional schema validation that reuses the Depot changeset API.

**Two layers of safety:**

| Layer | Mechanism | When it catches errors |
|-------|-----------|----------------------|
| Compile-time | March type checker | During `dune build` |
| Runtime | `Conduit.Schema` | At enqueue or before `perform` |

Use compile-time types always. Add a schema when args arrive from outside your type-checked code path.

**Serializable constraint:** All job and workflow payload types must implement `Serializable`. March derives this automatically for structs containing only primitive types, collections, and other `Serializable` types. Non-serializable types (channels, actors, file handles) cause a compile error.

```march
-- This is a compile error — FileHandle does not implement Serializable
mod BadJob do
  type Args = { handle: FileHandle }  -- ERROR: FileHandle does not implement Serializable
  impl Conduit.Job(BadJob) do
    fn perform(_self, args: Args) -> Result((), ConduitError) do Ok(()) end
  end
end
```

**Step result types in workflows:** The `checkpoint!` macro is type-safe. The return type of the function passed to `checkpoint!` determines the type of the result. This is checked at compile time.

```march
-- The type of `account_id` is inferred from AccountService.create's return type
let account_id = checkpoint!(ctx, "create_account", fn () ->
  AccountService.create(user_id)  -- returns String
end)
-- account_id: String — type-checked
```

**Signal types:** Workflow signals are typed variants. The `wait_for_signal!` macro returns the correct variant type.

```march
type MySignal = Approved(by: String) | Rejected(reason: String)

let decision: MySignal = wait_for_signal!(ctx, MySignal, timeout: Duration.days(1))
-- Exhaustive pattern match enforced by compiler
match decision do
  Approved(by) -> ...
  Rejected(reason) -> ...
end
```

### 3.11 Standalone vs Embedded Deployment

**Embedded in Bastion:**

```march
mod MyApp do
  use Bastion.Application

  fn start(env) do
    children [
      MyApp.Repo,
      Conduit.Worker.Supervisor.child_spec(
        queues: ["default", "email", "media"],
        worker_pool_size: 10
      ),
      MyApp.Endpoint
    ]
  end
end
```

**Standalone (no Bastion):**

```march
-- bin/worker.march
fn main() do
  Conduit.configure do
    storage Conduit.Storage.Postgres, url: System.env("DATABASE_URL")
    worker_pool_size 20
    queues ["default", "email", "media"]
  end

  Conduit.start_worker()
  -- Blocks until signal
  Signal.wait([SIGTERM, SIGINT])
  Conduit.stop_worker(graceful_timeout: Duration.seconds(30))
end
```

The core `conduit` package has zero dependency on Bastion. Integration is via the `conduit_bastion` package which provides `Conduit.Worker.Supervisor` as a Bastion-compatible child spec.

---

## 4. Data Model

### 4.1 Jobs Table

```sql
CREATE TABLE conduit_jobs (
  -- Identity
  id            TEXT        NOT NULL DEFAULT gen_random_uuid()::TEXT,
  queue         TEXT        NOT NULL DEFAULT 'default',
  job_type      TEXT        NOT NULL,

  -- Payload
  payload       JSONB       NOT NULL DEFAULT '{}',

  -- Status & lifecycle
  status        TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'running', 'completed', 'failed', 'dead', 'snoozed')),
  attempt       INTEGER     NOT NULL DEFAULT 0,
  max_attempts  INTEGER     NOT NULL DEFAULT 3,
  priority      INTEGER     NOT NULL DEFAULT 0,

  -- Scheduling
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- original requested time
  run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- when eligible to run (backoff adjusts this)

  -- Execution tracking
  started_at    TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,
  heartbeat_at  TIMESTAMPTZ,
  worker_id     TEXT,

  -- Errors
  errors        JSONB       NOT NULL DEFAULT '[]',  -- array of {attempt, error, failed_at}
  discarded_at  TIMESTAMPTZ,
  discard_reason TEXT,

  -- Workflow link (for jobs spawned by workflows)
  workflow_id   TEXT REFERENCES conduit_workflows(id) ON DELETE CASCADE,

  -- Metadata
  tags          TEXT[]      NOT NULL DEFAULT '{}',
  meta          JSONB       NOT NULL DEFAULT '{}',

  PRIMARY KEY (id)
);

-- Index for queue polling (the critical path)
CREATE INDEX idx_conduit_jobs_poll
  ON conduit_jobs (queue, status, priority DESC, run_at ASC)
  WHERE status IN ('pending', 'snoozed');

-- Index for dashboard queries
CREATE INDEX idx_conduit_jobs_status ON conduit_jobs (status, queue, inserted_at DESC);
CREATE INDEX idx_conduit_jobs_job_type ON conduit_jobs (job_type, inserted_at DESC);
CREATE INDEX idx_conduit_jobs_worker ON conduit_jobs (worker_id) WHERE worker_id IS NOT NULL;

-- Index for stale job detection
CREATE INDEX idx_conduit_jobs_heartbeat
  ON conduit_jobs (heartbeat_at, status)
  WHERE status = 'running';

-- Index for workflow jobs
CREATE INDEX idx_conduit_jobs_workflow ON conduit_jobs (workflow_id) WHERE workflow_id IS NOT NULL;
```

### 4.2 Workflows Table

```sql
CREATE TABLE conduit_workflows (
  -- Identity
  id              TEXT        NOT NULL DEFAULT gen_random_uuid()::TEXT,
  workflow_type   TEXT        NOT NULL,
  parent_id       TEXT        REFERENCES conduit_workflows(id) ON DELETE SET NULL,

  -- Input/Output
  input           JSONB       NOT NULL,
  output          JSONB,

  -- Status
  status          TEXT        NOT NULL DEFAULT 'running'
                              CHECK (status IN ('running', 'completed', 'failed', 'cancelled', 'timed_out')),

  -- Lifecycle
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  timeout_at      TIMESTAMPTZ,  -- deadline for the whole workflow

  -- Error
  error           TEXT,
  error_at        TIMESTAMPTZ,

  -- Metadata
  tags            TEXT[]      NOT NULL DEFAULT '{}',
  meta            JSONB       NOT NULL DEFAULT '{}',

  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_workflows_status ON conduit_workflows (status, started_at DESC);
CREATE INDEX idx_conduit_workflows_type   ON conduit_workflows (workflow_type, started_at DESC);
CREATE INDEX idx_conduit_workflows_parent ON conduit_workflows (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX idx_conduit_workflows_timeout
  ON conduit_workflows (timeout_at)
  WHERE status = 'running' AND timeout_at IS NOT NULL;
```

### 4.3 Checkpoints Table

```sql
CREATE TABLE conduit_checkpoints (
  -- Identity
  id          BIGSERIAL   NOT NULL,
  workflow_id TEXT        NOT NULL REFERENCES conduit_workflows(id) ON DELETE CASCADE,
  name        TEXT        NOT NULL,

  -- Value
  value       JSONB       NOT NULL,

  -- Timing
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  duration_ms INTEGER,    -- how long the checkpoint function took

  PRIMARY KEY (id),
  UNIQUE (workflow_id, name)  -- one checkpoint per step per workflow
);

CREATE INDEX idx_conduit_checkpoints_workflow ON conduit_checkpoints (workflow_id);
```

### 4.4 Workflow Signals Table

```sql
CREATE TABLE conduit_workflow_signals (
  id          BIGSERIAL   NOT NULL,
  workflow_id TEXT        NOT NULL REFERENCES conduit_workflows(id) ON DELETE CASCADE,

  -- Signal content
  signal_type TEXT        NOT NULL,
  payload     JSONB       NOT NULL DEFAULT '{}',

  -- Status
  status      TEXT        NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'delivered', 'expired')),

  -- Timing
  sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivered_at TIMESTAMPTZ,
  expires_at  TIMESTAMPTZ,

  -- Sender info
  sent_by     TEXT,       -- user ID or service name

  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_signals_workflow ON conduit_workflow_signals (workflow_id, status);
CREATE INDEX idx_conduit_signals_expiry
  ON conduit_workflow_signals (expires_at)
  WHERE status = 'pending' AND expires_at IS NOT NULL;
```

### 4.5 Cron Schedules Table

```sql
CREATE TABLE conduit_cron_schedules (
  -- Identity
  id          TEXT        NOT NULL,  -- user-defined or auto-generated
  job_type    TEXT        NOT NULL,

  -- Schedule
  schedule    TEXT        NOT NULL,  -- cron expression
  timezone    TEXT        NOT NULL DEFAULT 'UTC',

  -- Config
  queue       TEXT        NOT NULL DEFAULT 'default',
  payload     JSONB       NOT NULL DEFAULT '{}',  -- for dynamic cron with payload
  overlap     TEXT        NOT NULL DEFAULT 'skip'
                          CHECK (overlap IN ('skip', 'allow', 'replace')),
  enabled     BOOLEAN     NOT NULL DEFAULT TRUE,

  -- Tracking
  last_run_at   TIMESTAMPTZ,
  last_job_id   TEXT REFERENCES conduit_jobs(id) ON DELETE SET NULL,
  next_run_at   TIMESTAMPTZ,

  -- Metadata
  tags        TEXT[]      NOT NULL DEFAULT '{}',

  -- Lifecycle
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_cron_due
  ON conduit_cron_schedules (next_run_at)
  WHERE enabled = TRUE;
```

### 4.6 Dead Letter Table

```sql
CREATE TABLE conduit_dead_letters (
  id          TEXT        NOT NULL DEFAULT gen_random_uuid()::TEXT,
  job_id      TEXT        NOT NULL,  -- original job ID (may be deleted)
  queue       TEXT        NOT NULL,
  job_type    TEXT        NOT NULL,
  payload     JSONB       NOT NULL,
  errors      JSONB       NOT NULL,  -- full error history
  attempts    INTEGER     NOT NULL,

  -- Original timing
  first_attempted_at TIMESTAMPTZ,
  last_attempted_at  TIMESTAMPTZ,
  discarded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Admin actions
  reviewed     BOOLEAN    NOT NULL DEFAULT FALSE,
  reviewed_by  TEXT,
  reviewed_at  TIMESTAMPTZ,
  retry_count  INTEGER    NOT NULL DEFAULT 0,  -- manual retries from dashboard

  meta         JSONB      NOT NULL DEFAULT '{}',

  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_dead_letters_queue ON conduit_dead_letters (queue, discarded_at DESC);
CREATE INDEX idx_conduit_dead_letters_reviewed ON conduit_dead_letters (reviewed, discarded_at DESC);
```

### 4.7 Workers Table (for Multi-Node Visibility)

```sql
CREATE TABLE conduit_workers (
  id          TEXT        NOT NULL,  -- UUID, unique per node process
  hostname    TEXT        NOT NULL,
  pid         INTEGER     NOT NULL,

  -- Capabilities
  queues      TEXT[]      NOT NULL,
  pool_size   INTEGER     NOT NULL,

  -- Liveness
  started_at  TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Status
  status       TEXT       NOT NULL DEFAULT 'running'
                          CHECK (status IN ('running', 'stopping', 'stopped')),
  stopped_at   TIMESTAMPTZ,

  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_workers_active
  ON conduit_workers (last_seen_at)
  WHERE status = 'running';
```

---

## 5. Phased Implementation

### Phase 1: Simple Job Queue

**What we're building:** The simplest possible working queue. Enqueue a job, it runs. No fancy features. This phase validates the architecture and establishes the foundation.

**What ships:** `impl Conduit.Job(...)`, `Conduit.enqueue/2`, `Conduit.enqueue_in/3`, basic worker pool, Postgres storage, basic status tracking.

#### API (Phase 1)

```march
mod GreetingJob do
  type Args = { name: String }

  impl Conduit.Job(GreetingJob) do
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      Logger.info("Hello, #{args.name}!")
      Ok(())
    end
  end
end

-- Enqueue
Conduit.enqueue(GreetingJob, { name: "World" })
Conduit.enqueue_in(GreetingJob, Duration.minutes(5), { name: "Delayed World" })
```

#### Migration (Phase 1)

```sql
-- Generated by: forge conduit.gen.migrations
-- File: priv/migrations/20260401000001_conduit_jobs.sql

CREATE TABLE conduit_jobs (
  id           TEXT        NOT NULL DEFAULT gen_random_uuid()::TEXT,
  queue        TEXT        NOT NULL DEFAULT 'default',
  job_type     TEXT        NOT NULL,
  payload      JSONB       NOT NULL DEFAULT '{}',
  status       TEXT        NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending', 'running', 'completed', 'failed', 'dead', 'snoozed')),
  attempt      INTEGER     NOT NULL DEFAULT 0,
  max_attempts INTEGER     NOT NULL DEFAULT 3,
  priority     INTEGER     NOT NULL DEFAULT 0,
  inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  run_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at   TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  worker_id    TEXT,
  errors       JSONB       NOT NULL DEFAULT '[]',
  discarded_at TIMESTAMPTZ,
  discard_reason TEXT,
  tags         TEXT[]      NOT NULL DEFAULT '{}',
  meta         JSONB       NOT NULL DEFAULT '{}',
  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_jobs_poll
  ON conduit_jobs (queue, status, priority DESC, run_at ASC)
  WHERE status IN ('pending', 'snoozed');

CREATE INDEX idx_conduit_jobs_status ON conduit_jobs (status, inserted_at DESC);

-- LISTEN/NOTIFY function
CREATE OR REPLACE FUNCTION conduit_notify_new_job()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('conduit_queue_' || NEW.queue, NEW.id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER conduit_jobs_notify
  AFTER INSERT ON conduit_jobs
  FOR EACH ROW EXECUTE FUNCTION conduit_notify_new_job();
```

#### Test Cases (Phase 1)

```march
mod ConduitPhase1Test do
  use Test
  use Conduit.Testing

  fn test_enqueue_and_perform() do
    use_fake_queue() do
      Conduit.enqueue(GreetingJob, { name: "World" })
      assert_enqueued(GreetingJob, { name: "World" })
      Conduit.Testing.drain()
      assert_performed(GreetingJob)
    end
  end

  fn test_enqueue_in() do
    use_fake_queue() do
      use_fake_time() do
        Conduit.enqueue_in(GreetingJob, Duration.minutes(5), { name: "Future" })
        -- Should not run yet
        Conduit.Testing.drain()
        assert_not_performed(GreetingJob)
        -- Advance time
        FakeTime.advance(Duration.minutes(6))
        Conduit.Testing.drain()
        assert_performed(GreetingJob)
      end
    end
  end

  fn test_job_marked_completed() do
    use_fake_queue() do
      let job_id = Conduit.enqueue(GreetingJob, { name: "Test" })
      Conduit.Testing.drain()
      let job = Conduit.Testing.get_job(job_id)
      assert job.status == "completed"
    end
  end

  fn test_failed_job_marked_failed() do
    use_fake_queue() do
      let job_id = Conduit.enqueue(AlwaysFailsJob, {})
      Conduit.Testing.drain()
      let job = Conduit.Testing.get_job(job_id)
      assert job.status == "failed" || job.status == "dead"
    end
  end
end
```

**Definition of Done — Phase 1:**
- [ ] `impl Conduit.Job(...)` interface works
- [ ] `Conduit.enqueue/2` inserts a job to Postgres
- [ ] `Conduit.enqueue_in/3` inserts with future `run_at`
- [ ] Worker polls queue and runs jobs
- [ ] Jobs are marked `completed` or `failed`
- [ ] `LISTEN/NOTIFY` triggers immediate pickup
- [ ] Worker survives crashes and restarts
- [ ] Tests pass with fake queue

---

### Phase 2: Retries, Dead Letters & Error Control

**What we're building:** Robust retry handling. Jobs that fail retry with configurable backoff. Exhausted jobs move to a dead letter queue. Jobs can control their retry fate via `ConduitError` variants.

#### New API (Phase 2)

```march
mod UnreliableApiJob do
  type Args = { resource_id: String }

  impl Conduit.Job(UnreliableApiJob) do
    fn config(_self) -> Conduit.JobConfig do
      {
        max_attempts: 5,
        backoff: Conduit.Backoff.Exponential,
        dead_letter_queue: Some("failed_api_calls"),
        ..Conduit.JobConfig.default()
      }
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      match ExternalApi.fetch(args.resource_id) do
        Ok(data) ->
          process(data)
          Ok(())
        Err(NotFound) ->
          Err(ConduitError.Discard("Resource #{args.resource_id} not found"))
        Err(RateLimited(wait)) ->
          Err(ConduitError.Snooze(wait))
        Err(_) ->
          Err(ConduitError.Retry("Transient API failure"))
      end
    end
  end
end
```

#### Migration (Phase 2)

```sql
-- File: priv/migrations/20260401000002_conduit_dead_letters.sql

CREATE TABLE conduit_dead_letters (
  id                 TEXT        NOT NULL DEFAULT gen_random_uuid()::TEXT,
  job_id             TEXT        NOT NULL,
  queue              TEXT        NOT NULL,
  job_type           TEXT        NOT NULL,
  payload            JSONB       NOT NULL,
  errors             JSONB       NOT NULL,
  attempts           INTEGER     NOT NULL,
  first_attempted_at TIMESTAMPTZ,
  last_attempted_at  TIMESTAMPTZ,
  discarded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reviewed           BOOLEAN     NOT NULL DEFAULT FALSE,
  reviewed_by        TEXT,
  reviewed_at        TIMESTAMPTZ,
  retry_count        INTEGER     NOT NULL DEFAULT 0,
  meta               JSONB       NOT NULL DEFAULT '{}',
  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_dead_letters_queue
  ON conduit_dead_letters (queue, discarded_at DESC);

-- Add stale job detection index
CREATE INDEX idx_conduit_jobs_heartbeat
  ON conduit_jobs (heartbeat_at, status)
  WHERE status = 'running';
```

#### Test Cases (Phase 2)

```march
mod ConduitPhase2Test do
  use Test
  use Conduit.Testing

  fn test_retries_on_failure() do
    use_fake_queue() do
      let job_id = Conduit.enqueue(AlwaysFailsJob, {})
      Conduit.Testing.drain_with_retries()
      let job = Conduit.Testing.get_job(job_id)
      assert job.attempt == AlwaysFailsJob.max_attempts()
    end
  end

  fn test_discard_does_not_retry() do
    use_fake_queue() do
      let job_id = Conduit.enqueue(DiscardingJob, {})
      Conduit.Testing.drain()
      let job = Conduit.Testing.get_job(job_id)
      assert job.attempt == 1  -- only one attempt
      assert job.status == "dead"
    end
  end

  fn test_snooze_reschedules() do
    use_fake_queue() do
      use_fake_time() do
        let job_id = Conduit.enqueue(SnoozingJob, { snooze_for: Duration.minutes(10) })
        Conduit.Testing.drain()
        let job = Conduit.Testing.get_job(job_id)
        assert job.status == "snoozed"
        FakeTime.advance(Duration.minutes(11))
        Conduit.Testing.drain()
        let job2 = Conduit.Testing.get_job(job_id)
        assert job2.status == "completed"
      end
    end
  end

  fn test_exhausted_job_moves_to_dlq() do
    use_fake_queue() do
      let job_id = Conduit.enqueue(AlwaysFailsDLQJob, {})
      Conduit.Testing.drain_fully()  -- runs all retries
      let dlq = Conduit.Testing.dead_letters()
      assert List.length(dlq) == 1
      assert (List.head(dlq)).job_id == job_id
    end
  end
end
```

**Definition of Done — Phase 2:**
- [ ] Exponential, linear, fibonacci backoff work
- [ ] Custom backoff functions work
- [ ] `ConduitError.Discard` prevents retries
- [ ] `ConduitError.Snooze` reschedules to specified time
- [ ] Jobs exhausting retries move to dead letter table
- [ ] `on_dead_letter` callback fires
- [ ] Stale job detection and rescue works
- [ ] Jitter applied to backoff

---

### Phase 3: Cron Scheduling

**What we're building:** Scheduled jobs that run on cron expressions. Both static (compile-time defined) and dynamic (runtime-registered) crons.

#### New API (Phase 3)

```march
mod HourlyCleanupJob do
  impl Conduit.Cron(HourlyCleanupJob) do
    fn config(_self) -> Conduit.CronConfig do
      { schedule: "0 * * * *", timezone: "UTC", queue: "default", overlap: Conduit.CronOverlap.Skip, tags: [] }
    end

    fn perform(_self) -> Result((), ConduitError) do
      Database.delete_expired_sessions()
      Database.delete_old_logs(older_than: Duration.days(30))
      Ok(())
    end
  end
end

-- Dynamic cron (UserReminderJob implements Conduit.CronWithArgs)
Conduit.schedule_cron(UserReminderJob,
  id: "reminder_user_42",
  schedule: "0 9 * * MON",
  timezone: "America/Chicago",
  args: { user_id: 42, message: "Weekly check-in" }
)
Conduit.cancel_cron("reminder_user_42")
Conduit.pause_cron("reminder_user_42")
Conduit.resume_cron("reminder_user_42")
```

#### Migration (Phase 3)

```sql
-- File: priv/migrations/20260401000003_conduit_cron.sql

CREATE TABLE conduit_cron_schedules (
  id            TEXT        NOT NULL,
  job_type      TEXT        NOT NULL,
  schedule      TEXT        NOT NULL,
  timezone      TEXT        NOT NULL DEFAULT 'UTC',
  queue         TEXT        NOT NULL DEFAULT 'default',
  payload       JSONB       NOT NULL DEFAULT '{}',
  overlap       TEXT        NOT NULL DEFAULT 'skip'
                            CHECK (overlap IN ('skip', 'allow', 'replace')),
  enabled       BOOLEAN     NOT NULL DEFAULT TRUE,
  last_run_at   TIMESTAMPTZ,
  last_job_id   TEXT,
  next_run_at   TIMESTAMPTZ,
  tags          TEXT[]      NOT NULL DEFAULT '{}',
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_cron_due
  ON conduit_cron_schedules (next_run_at)
  WHERE enabled = TRUE;
```

#### Test Cases (Phase 3)

```march
mod ConduitPhase3Test do
  use Test
  use Conduit.Testing

  fn test_cron_fires_on_schedule() do
    use_fake_time(initial: DateTime.parse("2026-04-01T08:59:00Z")) do
      use_fake_queue() do
        Conduit.Testing.register_cron(HourlyCleanupJob)
        FakeTime.advance(Duration.minutes(2))  -- past 9:00
        Conduit.Testing.tick_crons()
        assert_enqueued(HourlyCleanupJob)
      end
    end
  end

  fn test_skip_overlap_prevents_concurrent_runs() do
    use_fake_queue() do
      -- Mark previous run as still running
      Conduit.Testing.fake_cron_running(HourlyCleanupJob)
      Conduit.Testing.tick_crons()
      assert_not_enqueued(HourlyCleanupJob)
    end
  end

  fn test_dynamic_cron_cancel() do
    use_fake_queue() do
      Conduit.schedule_cron(UserReminderJob,
        id: "test_reminder",
        schedule: "* * * * *",
        payload: { user_id: 1, message: "test" }
      )
      Conduit.cancel_cron("test_reminder")
      Conduit.Testing.tick_crons()
      assert_not_enqueued(UserReminderJob)
    end
  end

  fn test_replace_overlap_cancels_previous() do
    use_fake_queue() do
      use_replace_overlap_cron(HeavyReportCron) do
        let prev_job_id = Conduit.Testing.last_enqueued_id(HeavyReportCron)
        Conduit.Testing.tick_crons()
        let prev_job = Conduit.Testing.get_job(prev_job_id)
        assert prev_job.status == "cancelled"
      end
    end
  end
end
```

**Definition of Done — Phase 3:**
- [ ] Static crons fire on schedule
- [ ] `overlap: :skip` prevents double-fire
- [ ] `overlap: :replace` cancels previous job
- [ ] Timezone handling works
- [ ] Dynamic cron registration/cancellation works
- [ ] Multi-node: advisory lock prevents duplicate fires across nodes
- [ ] Cron schedules survive restarts (loaded from DB)

---

### Phase 4: Imperative Workflows & Checkpoints

**What we're building:** The workflow engine. Durable, resumable multi-step workflows using `checkpoint!`. This is the most complex phase.

#### New API (Phase 4)

See API Design §2.4, §2.5, §2.6.

#### Migration (Phase 4)

```sql
-- File: priv/migrations/20260401000004_conduit_workflows.sql

CREATE TABLE conduit_workflows (
  id            TEXT        NOT NULL DEFAULT gen_random_uuid()::TEXT,
  workflow_type TEXT        NOT NULL,
  parent_id     TEXT        REFERENCES conduit_workflows(id) ON DELETE SET NULL,
  input         JSONB       NOT NULL,
  output        JSONB,
  status        TEXT        NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'completed', 'failed', 'cancelled', 'timed_out')),
  started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at  TIMESTAMPTZ,
  timeout_at    TIMESTAMPTZ,
  error         TEXT,
  error_at      TIMESTAMPTZ,
  tags          TEXT[]      NOT NULL DEFAULT '{}',
  meta          JSONB       NOT NULL DEFAULT '{}',
  PRIMARY KEY (id)
);

CREATE TABLE conduit_checkpoints (
  id          BIGSERIAL   NOT NULL,
  workflow_id TEXT        NOT NULL REFERENCES conduit_workflows(id) ON DELETE CASCADE,
  name        TEXT        NOT NULL,
  value       JSONB       NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  duration_ms INTEGER,
  PRIMARY KEY (id),
  UNIQUE (workflow_id, name)
);

CREATE TABLE conduit_workflow_signals (
  id           BIGSERIAL   NOT NULL,
  workflow_id  TEXT        NOT NULL REFERENCES conduit_workflows(id) ON DELETE CASCADE,
  signal_type  TEXT        NOT NULL,
  payload      JSONB       NOT NULL DEFAULT '{}',
  status       TEXT        NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending', 'delivered', 'expired')),
  sent_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivered_at TIMESTAMPTZ,
  expires_at   TIMESTAMPTZ,
  sent_by      TEXT,
  PRIMARY KEY (id)
);

ALTER TABLE conduit_jobs
  ADD COLUMN workflow_id TEXT REFERENCES conduit_workflows(id) ON DELETE CASCADE;

CREATE INDEX idx_conduit_workflows_status ON conduit_workflows (status, started_at DESC);
CREATE INDEX idx_conduit_checkpoints_workflow ON conduit_checkpoints (workflow_id);
CREATE INDEX idx_conduit_signals_workflow ON conduit_workflow_signals (workflow_id, status);
```

#### Test Cases (Phase 4)

```march
mod ConduitPhase4Test do
  use Test
  use Conduit.Testing

  fn test_workflow_runs_all_steps() do
    use_fake_workflow_engine() do
      let result = Conduit.Testing.run_workflow(SimpleMultiStepWorkflow, { id: 1 })
      assert_ok(result)
      assert_checkpoints_completed(["step_1", "step_2", "step_3"])
    end
  end

  fn test_workflow_resumes_after_crash() do
    use_fake_workflow_engine() do
      let store = Conduit.Testing.FakeCheckpointStore.new()
      store |> Conduit.Testing.FakeCheckpointStore.set("step_1", "result_a")
      -- step_2 was never executed
      StepTwoService.Testing.track_calls()

      let result = Conduit.Testing.resume_workflow(SimpleMultiStepWorkflow,
        checkpoint_store: store,
        input: { id: 1 }
      )

      assert_ok(result)
      -- step_1 not re-called (was checkpointed)
      -- step_2 was called exactly once (new execution)
      assert StepTwoService.Testing.call_count() == 1
    end
  end

  fn test_parallel_fan_out_fan_in() do
    use_fake_workflow_engine() do
      let result = Conduit.Testing.run_workflow(ParallelWorkflow, {
        items: [1, 2, 3, 4, 5]
      })
      assert_ok(result)
      let output = Result.unwrap(result)
      assert output.processed_count == 5
    end
  end

  fn test_workflow_timeout() do
    use_fake_workflow_engine() do
      use_fake_time() do
        let handle = Conduit.Testing.start_workflow(SlowWorkflow, {}, timeout: Duration.minutes(5))
        FakeTime.advance(Duration.minutes(6))
        Conduit.Testing.tick()
        let workflow = Conduit.Testing.get_workflow(handle.id)
        assert workflow.status == "timed_out"
      end
    end
  end

  fn test_signal_received() do
    use_fake_workflow_engine() do
      let handle = Conduit.Testing.start_workflow(ApprovalWorkflow, {
        request_id: 1, requested_by: 42, amount: 100.0, description: "test"
      })
      -- Send approval signal
      Conduit.signal_workflow(handle.id, "Approved", { approver: "manager@co.com" })
      Conduit.Testing.drain_all_workflows()
      let result = Conduit.Testing.workflow_result(handle)
      assert_ok(result)
      let output = Result.unwrap(result)
      assert output.approved == true
    end
  end
end
```

**Definition of Done — Phase 4:**
- [ ] `checkpoint!` stores and retrieves results from DB
- [ ] Workflow resumes from last checkpoint after crash
- [ ] `parallel!` runs branches concurrently
- [ ] `wait_for_signal!` suspends and resumes
- [ ] `cancel_workflow/2` works
- [ ] Workflow timeout fires correctly
- [ ] Nested sub-workflows work
- [ ] Type safety enforced on checkpoint results and signals

---

### Phase 5: Multi-Node Coordination

**What we're building:** First-class multi-node support. Nodes register themselves, detect each other's failures, and redistribute work.

#### New Features (Phase 5)

```march
-- Node registration happens automatically at startup
-- Config adds node-specific options
Conduit.configure do
  node_id System.env("NODE_ID", default: Node.generate_id())
  heartbeat_interval Duration.seconds(15)
  stale_node_threshold Duration.seconds(60)

  -- Cluster coordination
  cluster do
    leader_election :postgres_advisory_lock  -- default
    cron_ownership :leader_only             -- crons fire on leader only
  end
end

-- Query cluster state (for dashboards, monitoring)
Conduit.cluster_nodes()  -- -> List(NodeInfo)
Conduit.cluster_leader() -- -> Option(NodeInfo)
```

#### Migration (Phase 5)

```sql
-- File: priv/migrations/20260401000005_conduit_multi_node.sql

CREATE TABLE conduit_workers (
  id           TEXT        NOT NULL,
  hostname     TEXT        NOT NULL,
  pid          INTEGER     NOT NULL,
  queues       TEXT[]      NOT NULL,
  pool_size    INTEGER     NOT NULL,
  started_at   TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status       TEXT        NOT NULL DEFAULT 'running'
                           CHECK (status IN ('running', 'stopping', 'stopped')),
  stopped_at   TIMESTAMPTZ,
  PRIMARY KEY (id)
);

CREATE INDEX idx_conduit_workers_active
  ON conduit_workers (last_seen_at)
  WHERE status = 'running';

-- Function to clean up stale workers
CREATE OR REPLACE FUNCTION conduit_cleanup_stale_workers(stale_threshold INTERVAL)
RETURNS INTEGER AS $$
DECLARE
  cleaned INTEGER;
BEGIN
  WITH stale AS (
    UPDATE conduit_workers
    SET status = 'stopped', stopped_at = NOW()
    WHERE status = 'running'
      AND last_seen_at < NOW() - stale_threshold
    RETURNING id
  )
  SELECT COUNT(*) INTO cleaned FROM stale;
  RETURN cleaned;
END;
$$ LANGUAGE plpgsql;
```

#### Test Cases (Phase 5)

```march
mod ConduitPhase5Test do
  use Test
  use Conduit.Testing

  fn test_stale_worker_jobs_reclaimed() do
    use_fake_cluster(nodes: 2) do
      -- Node 2 "dies" while running a job
      let job_id = Conduit.Testing.simulate_worker_death(node: 2)

      -- Advance time past stale threshold
      FakeTime.advance(Duration.seconds(90))

      -- Node 1 detects and reclaims
      Conduit.Testing.tick_stale_detector(node: 1)

      let job = Conduit.Testing.get_job(job_id)
      assert job.status == "pending"
      assert job.worker_id == None
    end
  end

  fn test_cron_fires_on_leader_only() do
    use_fake_cluster(nodes: 3) do
      Conduit.Testing.simulate_cron_tick()
      -- Should fire exactly once despite 3 nodes
      assert_enqueued_count(HourlyCleanupJob, 1)
    end
  end

  fn test_leader_election_on_node_death() do
    use_fake_cluster(nodes: 3) do
      let old_leader = Conduit.Testing.leader()
      Conduit.Testing.simulate_node_death(old_leader)
      Conduit.Testing.tick_leader_election()
      let new_leader = Conduit.Testing.leader()
      assert new_leader != old_leader
    end
  end
end
```

**Definition of Done — Phase 5:**
- [ ] Workers register on startup
- [ ] Workers heartbeat every N seconds
- [ ] Stale workers are detected and their jobs reclaimed
- [ ] Leader election with Postgres advisory lock
- [ ] Cron fires exactly once per schedule across cluster
- [ ] `Conduit.cluster_nodes()` returns accurate cluster state
- [ ] Graceful shutdown drains in-progress jobs

---

### Phase 6: Bastion Dashboard

**What we're building:** A full web dashboard for monitoring and administering Conduit. Built with Bastion/Islands. Separate `conduit_dashboard` package with no hard dependency on `conduit_bastion`.

See §6 for full dashboard spec.

**Definition of Done — Phase 6:**
- [ ] Dashboard renders in Bastion app via `forward "/conduit", Conduit.Dashboard.Router`
- [ ] Dashboard works standalone via `Conduit.Dashboard.start_standalone/1`
- [ ] All pages load with real data
- [ ] Admin actions (retry, cancel, delete) work
- [ ] Real-time updates via `LISTEN/NOTIFY` or polling
- [ ] Authentication via pluggable adapter

---

### Phase 7: Advanced Features (Rate Limiting, Priority, Unique Jobs)

**What we're building:** Production-quality features that the most demanding deployments need.

#### Rate Limiting

```march
mod ExternalApiJob do
  type Args = { user_id: Int, resource: String }

  impl Conduit.Job(ExternalApiJob) do
    fn config(_self) -> Conduit.JobConfig do
      {
        queue: "external_api",
        rate_limit: Some({
          max_per_second: 10,
          per_key: Some({ key_fn: fn args -> "user_#{args.user_id}" end, limit: 2 })
        }),
        ..Conduit.JobConfig.default()
      }
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      ExternalApi.fetch(args.resource)
    end
  end
end
```

#### Priority Queues

```march
mod CriticalAlertJob do
  type Args = { alert_id: Int }
  impl Conduit.Job(CriticalAlertJob) do
    fn config(_self) -> Conduit.JobConfig do
      { queue: "alerts", priority: 100, ..Conduit.JobConfig.default() }
    end
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      AlertService.fire(args.alert_id)
    end
  end
end

mod RoutineAlertJob do
  type Args = { alert_id: Int }
  impl Conduit.Job(RoutineAlertJob) do
    fn config(_self) -> Conduit.JobConfig do
      { queue: "alerts", priority: 0, ..Conduit.JobConfig.default() }
    end
    fn perform(_self, args: Args) -> Result((), ConduitError) do
      AlertService.log(args.alert_id)
    end
  end
end
```

#### Unique Jobs

```march
mod SendDailyDigestJob do
  type Args = { user_id: Int }

  impl Conduit.Job(SendDailyDigestJob) do
    fn config(_self) -> Conduit.JobConfig do
      {
        -- Prevent enqueueing the same job for the same user within 24h
        unique_for: Some({ duration: Duration.hours(24), by: ["user_id"] }),
        -- on_conflict: ignore (default), replace, or raise
        ..Conduit.JobConfig.default()
      }
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      DigestService.send(args.user_id)
    end
  end
end
```

#### Migration (Phase 7 additions)

```sql
-- File: priv/migrations/20260401000007_conduit_advanced.sql

-- Unique job fingerprints
ALTER TABLE conduit_jobs ADD COLUMN unique_key TEXT;
CREATE UNIQUE INDEX idx_conduit_jobs_unique_key
  ON conduit_jobs (unique_key)
  WHERE unique_key IS NOT NULL
    AND status NOT IN ('completed', 'dead');

-- Rate limit buckets (token bucket per key)
CREATE TABLE conduit_rate_limit_buckets (
  key        TEXT        NOT NULL,
  tokens     DECIMAL     NOT NULL DEFAULT 0,
  last_refill TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (key)
);
```

**Definition of Done — Phase 7:**
- [ ] Per-job-type rate limits work
- [ ] Per-key rate limits work
- [ ] Priority ordering within a queue works
- [ ] Unique jobs: duplicate enqueue is silently ignored (or replaced, or raises)
- [ ] Unique constraint released when job completes/fails

---

### Phase 8: Deterministic Replay (Late Stage, Opt-In)

**What we're building:** Full Temporal-style deterministic replay as an opt-in execution mode. This is NOT checkpoint-based resumption (which already works). This adds event sourcing: all workflow decisions are logged as events, and the workflow can be replayed deterministically from those events without re-executing side effects.

**Why this is late:** Checkpoint-based resumption handles 95% of use cases. Deterministic replay is needed only for workflows that require full auditability, time-travel debugging, or the ability to replay the *exact* history of decisions (e.g., financial workflows). The added complexity is significant and should not be borne by all users.

**Opt-in via config:**

```march
mod AuditedPaymentWorkflow do
  type Input = { order_id: Int, amount: Float }
  type Output = { charge_id: String }

  impl Conduit.Workflow(AuditedPaymentWorkflow) do
    fn config(_self) -> Conduit.WorkflowConfig do
      { execution_mode: Conduit.ExecutionMode.DeterministicReplay }
    end

  fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
    -- In replay mode, all randomness, time, and external calls are
    -- intercepted and logged to the event store on first execution.
    -- On replay, the recorded values are returned without re-executing.
    let charge_id = checkpoint!(ctx, "charge_card", fn () ->
      PaymentGateway.charge(input.order_id, input.amount)
    end)
    Ok({ charge_id: charge_id })
  end
  end  -- impl
end
```

**Event store schema:**

```sql
-- File: priv/migrations/20260401000008_conduit_event_store.sql

CREATE TABLE conduit_workflow_events (
  id            BIGSERIAL   NOT NULL,
  workflow_id   TEXT        NOT NULL REFERENCES conduit_workflows(id) ON DELETE CASCADE,
  sequence      INTEGER     NOT NULL,
  event_type    TEXT        NOT NULL,
  payload       JSONB       NOT NULL,
  recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id),
  UNIQUE (workflow_id, sequence)
);

CREATE INDEX idx_conduit_events_workflow
  ON conduit_workflow_events (workflow_id, sequence ASC);
```

**Definition of Done — Phase 8:**
- [ ] `execution_mode: :deterministic_replay` works
- [ ] First execution logs all events
- [ ] Replay reads from event store, skips side effects
- [ ] Time, randomness, and external calls are deterministic on replay
- [ ] Workflow history visible in dashboard
- [ ] Point-in-time replay works ("show me what the workflow saw at step 3")

---

## 6. Bastion Dashboard

The Conduit dashboard is a separate package (`conduit_dashboard`) that provides a full-featured admin UI. It has two deployment modes:

1. **Embedded:** Mount it inside an existing Bastion app
2. **Standalone:** Run it as its own HTTP server (no Bastion dependency required)

### 6.1 Setup

**Embedded in Bastion:**

```march
-- lib/my_app_web/router.march
mod MyAppWeb.Router do
  use Bastion.Router

  -- Mount dashboard at /conduit (protected by admin auth)
  forward "/conduit", Conduit.Dashboard.Router,
    auth: MyApp.AdminAuth
end
```

**Standalone:**

```march
-- bin/conduit_dashboard.march
fn main() do
  Conduit.Dashboard.start_standalone(
    port: 4001,
    conduit_config: Conduit.Config.from_env(),
    auth: fn req ->
      req |> Request.get_header("X-Admin-Token") == System.env("ADMIN_TOKEN")
    end
  )
end
```

### 6.2 Pages

#### Overview Page (`/conduit`)

Displays the health summary at a glance:

- **Queue Summary Cards** (one per queue): pending count, running count, failed count, throughput (jobs/min), average latency (ms)
- **Cluster Health**: node count, active workers, leader node
- **Throughput Chart**: jobs completed/failed over last 24h, sparkline per queue
- **Recent Failures**: last 10 failed jobs with error message and retry count
- **Cron Schedule Status**: next 5 scheduled crons, last run time

Real-time updates: `conduit_jobs_summary` channel via LISTEN/NOTIFY or 5-second polling fallback.

#### Queue Detail Page (`/conduit/queues/:queue`)

- **Tab: Running** — live view of in-progress jobs, worker ID, started at, duration
- **Tab: Pending** — queued jobs sorted by priority + run_at, count + pagination
- **Tab: Scheduled** — future jobs (run_at > NOW), sorted by run_at
- **Tab: Failed** — failed jobs with error details, retry button per job
- **Tab: Dead** — exhausted jobs, with retry button (re-queues with fresh attempt count)

Admin actions per job row:
- **Retry**: reset attempt count, set run_at to now, set status to pending
- **Retry with edited args**: if the job has a schema, opens a schema-aware form pre-populated with the current args; validates before re-enqueueing
- **Cancel**: set status to dead, discard_reason = "cancelled_by_admin"
- **Delete**: hard delete from DB
- **Inspect**: expand to show full payload, error history, metadata

**Schema-aware enqueue form**: The queue detail page has an "Enqueue Job" button. If the selected job type has a schema, the dashboard renders a typed form (text fields, number inputs, dropdowns for `inclusion` validators, required markers) rather than a raw JSON editor. Field-level validation runs client-side using the schema metadata, with server-side re-validation on submit.

#### Job Detail Page (`/conduit/jobs/:id`)

Full job record with:
- Current status (color coded)
- Args (pretty-printed JSON with copy button; if job has a schema, field labels and types shown)
- Validation errors (if job failed due to schema validation, errors shown per-field)
- Error history (accordion per attempt: timestamp, error message, stack trace)
- Timeline: enqueued → started → completed/failed
- Workflow link (if job is part of a workflow)
- Admin actions: Retry, Retry with edited args (schema-aware), Cancel, Delete

#### Workflows Page (`/conduit/workflows`)

- **Tab: Running** — active workflow instances, step currently executing
- **Tab: Completed** — recent completions with input/output
- **Tab: Failed** — failed workflows with error, last checkpoint

Per workflow row, expand to see:
- Checkpoint list: step name, value (truncated), duration, completed at
- Signal history: received signals with payload and delivery status
- Event history (if using deterministic replay mode)

Admin actions:
- **Cancel** running workflow
- **Inspect** checkpoints and signals

#### Cron Page (`/conduit/crons`)

- List all cron schedules with: ID, job type, schedule expression, timezone, last run, next run, status (enabled/disabled/running)
- Admin actions per row:
  - **Trigger Now** — immediately enqueues the cron job (bypasses schedule)
  - **Pause** — sets `enabled = false`
  - **Resume** — sets `enabled = true`
  - **Edit Schedule** — modal to change cron expression (with preview of next 5 fire times)
  - **Delete** — removes the cron schedule

#### Dead Letters Page (`/conduit/dead-letters`)

- List of dead letter entries: job type, queue, discarded at, attempts, last error
- Bulk actions: **Retry All**, **Delete All**, **Retry Selected**, **Delete Selected**
- Per-row actions: Retry, Inspect (shows full payload + error history), Delete
- Filter by: queue, job type, date range, reviewed status

#### Nodes Page (`/conduit/nodes`)

- Active nodes: hostname, PID, started at, last seen, queues, pool size
- Stale nodes (not seen in >60s): highlighted, with "Remove" button
- Leader node highlighted with crown icon
- Per-node active jobs list

### 6.3 Authentication

The dashboard does not ship its own auth. Instead, you provide an auth adapter:

```march
-- Bastion plug-based auth
Conduit.Dashboard.Router, auth: MyApp.AdminAuth

-- Simple token auth
Conduit.Dashboard.Router, auth: fn conn ->
  Plug.get_header(conn, "authorization") == "Bearer #{System.env("ADMIN_TOKEN")}"
end

-- No auth (development only)
Conduit.Dashboard.Router, auth: :none
```

### 6.4 Real-Time Updates

The dashboard uses a prioritized update strategy:

1. **Primary:** Postgres `LISTEN/NOTIFY` — instant updates when jobs change state
2. **Fallback:** 5-second polling if the WebSocket connection drops
3. **Manual refresh:** Refresh button on every page

The Islands architecture means only changed components re-render. A job completing updates only its row and the queue summary card, not the entire page.

---

## 7. Comparison Table

| Feature | Conduit (March) | Oban (Elixir) | Sidekiq (Ruby) | Temporal | Airflow |
|---------|----------------|---------------|----------------|----------|---------|
| **Language** | March | Elixir | Ruby | Go/Java/Python/TypeScript | Python |
| **Type Safety** | Compile-time checked payloads | None (Ecto schema optional) | None | SDK-enforced at language level | None |
| **Workflow Engine** | Imperative + checkpoints | No | No | Imperative + full event sourcing | DAG builder |
| **Durable Execution** | Checkpoint-based (Deterministic replay: Phase 8) | No | No | Full deterministic replay | Task-level retry |
| **Storage** | Pluggable (Postgres recommended) | Postgres only | Redis | External (Temporal server + DB) | DB + message broker |
| **Multi-Node** | Yes, from Phase 1 | Yes | Yes (Pro) | Yes (managed or self-hosted) | Yes (complex) |
| **Cron** | Yes, static + dynamic | Yes (Oban.Cron) | Yes (Enterprise) | Yes (schedules) | Yes (first-class) |
| **Dead Letter Queue** | Yes | Yes | Yes | Workflow history | Task instances |
| **Dashboard** | Full admin (embedded or standalone) | Oban Web (paid) | Sidekiq Web UI | Temporal Web | Airflow UI |
| **Actor Model** | Native (March actors) | Native (GenServer) | No | No | No |
| **Deployment** | Embedded or standalone | Embedded in Elixir app | Embedded or standalone | Separate Temporal server | Standalone |
| **Setup Complexity** | Low (just a Postgres migration) | Low | Low | High (separate cluster) | High |
| **Fan-Out/Fan-In** | Yes (parallel!) | Manual | Manual | Yes | Yes (DAG) |
| **Rate Limiting** | Phase 7 (per-job-type, per-key) | Yes (Oban.Pro) | Yes (Enterprise) | Yes | No |
| **Unique Jobs** | Phase 7 | Yes | No | N/A | N/A |
| **Observability** | OpenTelemetry spans, structured events | Telemetry | Metrics gem | Built-in trace UI | Task log UI |
| **Testing** | First-class fake backend | inline testing | Fake backends | Test server | Test harness |
| **Migrations** | User-owned (generated) | Auto (Ecto migrations) | None | None (Temporal manages) | Alembic |

**When to choose Conduit over alternatives:**

- **vs Oban:** Conduit if you need type-safe payloads, imperative workflows, or deployment outside of an Elixir ecosystem. Oban if you're in Elixir today and don't need workflows.
- **vs Sidekiq:** Conduit if you care about type safety or durable workflows. Sidekiq if you have a mature Ruby app and just need simple background jobs.
- **vs Temporal:** Conduit if you want simpler setup (no separate Temporal cluster), are already using March, or don't need full event sourcing yet. Temporal if you need the most battle-tested deterministic workflow replay available today.
- **vs Airflow:** Conduit if your workflows are triggered by events (not scheduled batch pipelines). Airflow if you are building data pipelines with complex dependencies between scheduled tasks.

---

## 8. Error Handling & Edge Cases

### 8.1 Worker Crashes During Job Execution

**Scenario:** A WorkerActor crashes mid-job (e.g., out of memory, OS signal).

**What happens:**
1. The job's `heartbeat_at` stops updating
2. After `stale_job_threshold` (default: 60s), the stale detector notices
3. The job is moved back to `pending` (attempt count is NOT incremented — this was a crash, not a logical failure)
4. The job is re-claimed by another worker on the next poll

**Risk:** If the job was partially through a side effect (e.g., sent half an email), it will re-run. Mitigation: idempotent job design, or use workflows with checkpoints for side effects.

### 8.2 Network Partition

**Scenario:** A node can't reach the database.

**What happens:**
1. Job claims fail (DB unreachable)
2. Heartbeats fail — jobs being run by this node appear stale to other nodes
3. Other nodes reclaim those jobs and re-execute them

**Risk:** Double execution if the original node was running the job but just couldn't heartbeat. Mitigation: idempotent job design is mandatory for production use. The `unique_for` feature (Phase 7) helps for jobs where you can define a uniqueness key.

### 8.3 Checkpoint Write Failure

**Scenario:** A workflow completes a checkpoint function, but the DB write fails.

**What happens:**
1. `checkpoint!` returns an error
2. The WorkflowRunnerActor propagates the error up
3. The workflow fails and is scheduled for retry
4. On retry, the checkpoint function re-executes (since the checkpoint was never written)

**Risk:** The function may execute twice. Mitigation: all checkpoint functions should be idempotent. For non-idempotent operations (payments), use `idempotency_key` option.

### 8.4 Workflow Signal Race

**Scenario:** A signal arrives before `wait_for_signal!` is called.

**What happens:**
Signals are stored in `conduit_workflow_signals` immediately. When `wait_for_signal!` is called, it first checks for undelivered signals. If one exists, it's returned immediately. The actor does not need to sleep.

**Implementation:**

```march
fn wait_for_signal_impl(ctx: WorkflowContext, signal_type: String, timeout: Duration) do
  -- First check for already-arrived signals
  match Storage.get_pending_signal(ctx.conn, ctx.workflow_id, signal_type) do
    Ok(Some(signal)) ->
      Storage.mark_signal_delivered(ctx.conn, signal.id)
      Ok(signal.payload)
    Ok(None) ->
      -- No signal yet — register timeout and wait
      let deadline = DateTime.utc_now() |> DateTime.add(timeout)
      Storage.register_signal_wait(ctx.conn, ctx.workflow_id, signal_type, deadline)
      -- Actor suspends here via receive
      receive_signal(ctx, signal_type, deadline)
    Err(e) ->
      Err(WorkflowError.storage(e))
  end
end
```

### 8.5 Cron Missed Fires

**Scenario:** The application is down during a scheduled cron fire time.

**What happens:**
When the node starts, the CronSchedulerActor checks `next_run_at` for all enabled crons. Jobs where `next_run_at < NOW` are considered missed.

**Configurable behavior (per cron):**

```march
config do
  on_missed_fire :fire_once     -- fire once immediately (default)
  -- on_missed_fire :skip       -- skip missed fires, wait for next scheduled time
  -- on_missed_fire :fire_all   -- fire once per missed interval (rare, use carefully)
end
```

### 8.6 Clock Skew Between Nodes

**Scenario:** Nodes have slightly different system clocks.

**Impact:** Minor scheduling imprecision (milliseconds to seconds). Jobs may run slightly early or late on different nodes.

**Mitigation:** Use `NOW()` in Postgres for all timing operations where possible, not `DateTime.utc_now()` from the application. This anchors time to the DB clock, which is authoritative.

### 8.7 Large Checkpoint Values

**Scenario:** A checkpoint stores a very large JSON blob (e.g., 50MB processed dataset).

**Impact:** Slow checkpoint reads/writes, Postgres TOAST storage pressure, slow dashboard queries.

**Mitigation:**
- Store results in object storage (S3, etc.) and checkpoint only the reference key
- Use `checkpoint_external!` helper for large values:

```march
-- checkpoint_external! stores value in S3, checkpoints the URL
let report_url = checkpoint_external!(ctx, "generated_report", fn () ->
  let data = generate_large_report()
  ObjectStorage.upload("reports/#{ctx.workflow_id}_report.json", data)
end)
```

### 8.8 Poison Pills

**Scenario:** A job crashes its worker process every time (segfault in a C extension, stack overflow, etc.).

**What happens:**
The WorkflowRunnerActor or WorkerActor is restarted by its supervisor after a crash. The job is re-attempted up to `max_attempts`. If it exhausts retries, it goes to the dead letter queue.

If a job consistently kills workers, the supervisor's restart intensity kicks in. After N crashes in T seconds, the supervisor gives up and the queue stops processing. This surfaces as an alert (telemetry event: `WorkerSupervisorGaveUp`).

**Detection:** Dashboard shows `dead` jobs with identical errors. Alert on `WorkerSupervisorGaveUp` telemetry event.

**Mitigation:** Test jobs thoroughly in staging. Use `timeout` config to prevent runaway jobs.

### 8.9 Database Schema Migration During Running Jobs

**Scenario:** You deploy a new schema migration while jobs are actively running.

**Guidance:**
- Conduit migrations are additive where possible (add columns with defaults)
- Never remove a column that job payloads reference in the same migration as the code deployment
- Follow standard deploy practices: migrate → deploy (code reads both old and new schema) → cleanup migration

---

## 9. Forge Generators & Project Templates

Conduit ships its own scaffolding API. Forge calls into it — the generators live inside the `conduit` library, not in Forge itself. This keeps Conduit self-contained: the generator logic, file templates, and `~H` sigil templates all live under `lib/conduit/gen/` and can be invoked from any tool, not just Forge.

### 9.1 How It Works

```
forge conduit <subcommand>
       │
       └─► Forge.Conduit.dispatch(subcommand, args)
               │
               └─► Conduit.Gen.run(subcommand, args)
                       │
                       ├─► Conduit.Gen.New        (new project)
                       ├─► Conduit.Gen.Job         (job module)
                       ├─► Conduit.Gen.Workflow    (workflow module)
                       ├─► Conduit.Gen.Migration   (DB tables)
                       ├─► Conduit.Gen.Cron        (cron module)
                       └─► Conduit.Gen.Dashboard   (dashboard template)
```

The `Conduit.Gen` API is a public March module. Any tooling (LSP code actions, editor plugins, CI scaffolders) can call it directly without going through Forge.

### 9.2 `forge conduit new <name>`

Scaffolds a complete Conduit project — either standalone or embedded in an existing Bastion app.

```
$ forge conduit new mailer
```

**Generates:**

```
mailer/
├── forge.march                  # project config
├── config/
│   └── conduit.march            # Conduit configuration
├── lib/
│   └── mailer/
│       ├── jobs/
│       │   └── send_welcome_email_job.march   # example job
│       ├── workflows/
│       │   └── onboard_user_workflow.march    # example workflow
│       └── worker.march         # worker entry point
├── priv/
│   └── migrations/
│       └── 20260401000001_conduit_setup.sql   # generated migration
└── test/
    └── mailer/
        ├── jobs/
        │   └── send_welcome_email_job_test.march
        └── workflows/
            └── onboard_user_workflow_test.march
```

#### Generated: `config/conduit.march`

```march
mod Mailer.ConduitConfig do
  fn configure() do
    Conduit.configure do
      storage Conduit.Storage.Postgres, url: System.env("DATABASE_URL")
      worker_pool_size 10
      poll_interval Duration.milliseconds(500)

      queue "default" do
        max_concurrency 20
        timeout Duration.seconds(30)
        max_attempts 3
        backoff :exponential
      end

      middleware [
        Conduit.Middleware.Logging,
        Conduit.Middleware.Tracing
      ]

      workflow do
        max_concurrent_workflows 25
        workflow_timeout Duration.days(7)
      end
    end
  end
end
```

#### Generated: `lib/mailer/jobs/send_welcome_email_job.march`

```march
mod Mailer.SendWelcomeEmailJob do
  type Args = {
    user_id: Int,
    email: String,
    name: String
  }

  impl Conduit.Job(Mailer.SendWelcomeEmailJob) do
    fn config(_self) -> Conduit.JobConfig do
      { queue: "default", max_attempts: 3, backoff: Conduit.Backoff.Exponential, ..Conduit.JobConfig.default() }
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      -- TODO: implement job logic
      Logger.info("Sending welcome email", { to: args.email, user_id: args.user_id })
      Ok(())
    end
  end
end
```

#### Generated: `lib/mailer/workflows/onboard_user_workflow.march`

```march
mod Mailer.OnboardUserWorkflow do
  type Input = {
    user_id: Int,
    email: String
  }

  type Output = {
    completed_at: DateTime
  }

  impl Conduit.Workflow(Mailer.OnboardUserWorkflow) do
    fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
      -- Step 1: send welcome email (durable)
      checkpoint!(ctx, "send_welcome_email", fn () ->
        -- TODO: replace with real call
        Logger.info("Sending welcome email to #{input.email}")
        Ok(())
      end)

      -- Step 2: provision account resources (durable)
      checkpoint!(ctx, "provision_resources", fn () ->
        -- TODO: replace with real call
        Logger.info("Provisioning resources for user #{input.user_id}")
        Ok(())
      end)

      Ok({ completed_at: DateTime.utc_now() })
    end
  end
end
```

#### Generated: `lib/mailer/worker.march`

```march
mod Mailer.Worker do
  fn main() do
    Mailer.ConduitConfig.configure()
    Logger.info("Starting Mailer Conduit worker")

    Conduit.start_worker(
      queues: ["default"],
      on_start: fn () ->
        Logger.info("Worker ready")
      end
    )

    Signal.wait([SIGTERM, SIGINT])
    Logger.info("Shutting down worker")
    Conduit.stop_worker(graceful_timeout: Duration.seconds(30))
  end
end
```

#### Generated: `test/mailer/jobs/send_welcome_email_job_test.march`

```march
mod Mailer.SendWelcomeEmailJobTest do
  use Test
  use Conduit.Testing

  fn test_perform_succeeds() do
    use_fake_queue() do
      let result = Conduit.Testing.perform(Mailer.SendWelcomeEmailJob, {
        user_id: 1,
        email: "test@example.com",
        name: "Test User"
      })
      assert_ok(result)
    end
  end

  fn test_enqueue_adds_to_queue() do
    use_fake_queue() do
      Conduit.enqueue(Mailer.SendWelcomeEmailJob, {
        user_id: 1,
        email: "test@example.com",
        name: "Test User"
      })
      assert_enqueued(Mailer.SendWelcomeEmailJob, { user_id: 1 })
    end
  end
end
```

**Embedded mode** (adding Conduit to an existing Bastion app):

```
$ forge conduit new --embed MyApp
```

Skips generating `forge.march` and `worker.march`. Instead adds:
- `config/conduit.march` (configuration module)
- `lib/my_app/jobs/` (empty, with `.gitkeep`)
- `lib/my_app/workflows/` (empty, with `.gitkeep`)
- `priv/migrations/TIMESTAMP_conduit_setup.sql`
- Instructions printed to stdout for adding `Conduit.Worker.Supervisor` to the app's supervision tree

### 9.3 `forge conduit gen job <Name>`

Generate a new job module. The name is CamelCase; Forge derives the filename and module path.

```
$ forge conduit gen job ProcessPayment
  create  lib/jobs/process_payment_job.march
  create  test/jobs/process_payment_job_test.march
```

With queue and schema flags:

```
$ forge conduit gen job ProcessPayment --queue billing --attempts 5 --schema
  create  lib/jobs/process_payment_job.march
  create  test/jobs/process_payment_job_test.march
```

The `--schema` flag generates a skeleton `fn schema` block. Omit it for jobs where args are always typed in application code and runtime validation is unnecessary.

#### Generated: `lib/jobs/process_payment_job.march` (without `--schema`)

```march
mod ProcessPaymentJob do
  type Args = {
    -- TODO: define your job args fields
    id: Int
  }

  impl Conduit.Job(ProcessPaymentJob) do
    fn config(_self) -> Conduit.JobConfig do
      {
        queue: "billing",
        max_attempts: 5,
        backoff: Conduit.Backoff.Exponential,
        timeout: Duration.seconds(30),
        ..Conduit.JobConfig.default()
      }
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      -- TODO: implement job logic
      Ok(())
    end
  end
end
```

#### Generated: `lib/jobs/process_payment_job.march` (with `--schema`)

```march
mod ProcessPaymentJob do
  type Args = {
    -- TODO: define your job args fields
    id: Int
  }

  impl Conduit.Job(ProcessPaymentJob) do
    fn config(_self) -> Conduit.JobConfig do
      {
        queue: "billing",
        max_attempts: 5,
        backoff: Conduit.Backoff.Exponential,
        timeout: Duration.seconds(30),
        ..Conduit.JobConfig.default()
      }
    end

    -- Optional: runtime validation before perform is called.
    -- Useful when args arrive from external sources (dashboard, APIs, webhooks).
    -- If validation fails, the job moves to failed state without retrying.
    -- Remove this function entirely if you don't need runtime validation.
    fn schema(_self) -> Conduit.Schema(Args) do
      Conduit.Schema.build(fn s ->
        s
        |> field("id", :int, required: true)
        -- TODO: add validators for each field
        -- |> field("email", :string, required: true, format: ~r/.+@.+/)
        -- |> field("amount", :float, min: 0.01)
        -- |> field("status", :string, inclusion: ["pending", "active"])
      end)
    end

    fn perform(_self, args: Args) -> Result((), ConduitError) do
      -- TODO: implement job logic
      Ok(())
    end
  end
end
```

#### Generated: `test/jobs/process_payment_job_test.march`

```march
mod ProcessPaymentJobTest do
  use Test
  use Conduit.Testing

  fn test_perform_succeeds() do
    use_fake_queue() do
      let result = Conduit.Testing.perform(ProcessPaymentJob, {
        id: 1
      })
      assert_ok(result)
    end
  end

  fn test_failed_job_is_retried() do
    use_fake_queue() do
      -- TODO: set up conditions that cause failure, then assert retry behavior
      assert true
    end
  end

  -- Generated only when --schema flag is used:
  fn test_schema_rejects_invalid_args() do
    use_fake_queue() do
      -- TODO: replace with actually invalid args for your schema
      let result = Conduit.enqueue(ProcessPaymentJob, { id: -1 })
      -- Adjust assertion based on your validation rules
      assert_err(result)
    end
  end

  fn test_schema_accepts_valid_args() do
    use_fake_queue() do
      let result = Conduit.enqueue(ProcessPaymentJob, { id: 1 })
      assert_ok(result)
    end
  end
end
```

### 9.4 `forge conduit gen workflow <Name>`

Generate a new workflow module with typed input/output and a skeleton `run` function.

```
$ forge conduit gen workflow FulfillOrder
  create  lib/workflows/fulfill_order_workflow.march
  create  test/workflows/fulfill_order_workflow_test.march
```

#### Generated: `lib/workflows/fulfill_order_workflow.march`

```march
mod FulfillOrderWorkflow do
  -- Input is the data passed when starting the workflow.
  -- All fields must implement Serializable.
  type Input = {
    -- TODO: define your workflow input fields
    id: Int
  }

  -- Output is the final result returned on successful completion.
  type Output = {
    -- TODO: define your workflow output fields
    completed_at: DateTime
  }

  impl Conduit.Workflow(FulfillOrderWorkflow) do
    fn run(_self, input: Input, ctx: WorkflowContext) -> Result(Output, WorkflowError) do
      -- Each checkpoint! call is durable: if the process crashes and restarts,
      -- completed checkpoints are skipped and their stored result is returned.

      let step_one_result = checkpoint!(ctx, "step_one", fn () ->
        -- TODO: replace with real work
        Logger.info("Running step_one for id=#{input.id}")
        Ok("step_one_complete")
      end)

      let step_two_result = checkpoint!(ctx, "step_two", fn () ->
        -- TODO: replace with real work, can use step_one_result here
        Logger.info("Running step_two, step_one produced: #{step_one_result}")
        Ok("step_two_complete")
      end)

      Ok({ completed_at: DateTime.utc_now() })
    end
  end
end
```

#### Generated: `test/workflows/fulfill_order_workflow_test.march`

```march
mod FulfillOrderWorkflowTest do
  use Test
  use Conduit.Testing

  fn test_workflow_completes() do
    use_fake_workflow_engine() do
      let result = Conduit.Testing.run_workflow(FulfillOrderWorkflow, { id: 1 })
      assert_ok(result)
    end
  end

  fn test_workflow_resumes_from_step_one() do
    use_fake_workflow_engine() do
      let store = Conduit.Testing.FakeCheckpointStore.new()
      store |> Conduit.Testing.FakeCheckpointStore.set("step_one", "step_one_complete")

      let result = Conduit.Testing.resume_workflow(FulfillOrderWorkflow,
        checkpoint_store: store,
        input: { id: 1 }
      )

      assert_ok(result)
      -- step_one was not re-executed
      assert_checkpoint_skipped(store, "step_one")
    end
  end
end
```

### 9.5 `forge conduit gen cron <Name>`

Generate a cron module.

```
$ forge conduit gen cron CleanupExpiredSessions --schedule "0 3 * * *"
  create  lib/crons/cleanup_expired_sessions_cron.march
  create  test/crons/cleanup_expired_sessions_cron_test.march
```

#### Generated: `lib/crons/cleanup_expired_sessions_cron.march`

```march
mod CleanupExpiredSessionsCron do
  impl Conduit.Cron(CleanupExpiredSessionsCron) do
    fn config(_self) -> Conduit.CronConfig do
      {
        schedule: "0 3 * * *",   -- 3am UTC daily
        timezone: "UTC",
        queue: "default",
        overlap: Conduit.CronOverlap.Skip,
        tags: []
      }
    end

    fn perform(_self) -> Result((), ConduitError) do
      -- TODO: implement cron logic
      Logger.info("Running CleanupExpiredSessionsCron")
      Ok(())
    end
  end
end
```

### 9.6 `forge conduit gen migration`

Generate the Conduit database tables migration. This is typically run once, on initial setup, but can also be run to generate additive migrations for new Conduit versions.

```
$ forge conduit gen migration
  create  priv/migrations/20260401120000_conduit_setup.sql
```

The timestamp is set to `NOW()` at generation time. The generated file is identical to the Phase 1 migration in §5 — complete with all tables, indexes, and the `LISTEN/NOTIFY` trigger. On subsequent Conduit upgrades, running `forge conduit gen migration` generates only the new columns/tables added in the upgrade, with a timestamped filename.

```
$ forge conduit gen migration --upgrade 1.0.0..1.1.0
  create  priv/migrations/20260601000001_conduit_upgrade_1_1_0.sql
```

### 9.7 `forge conduit gen dashboard`

Generate a customizable Bastion dashboard template. By default the dashboard is served directly from the `conduit_dashboard` package with no user customization needed. Use this generator when you want to override specific pages with custom `~H` templates in your own app.

```
$ forge conduit gen dashboard
  create  lib/my_app_web/conduit/
  create  lib/my_app_web/conduit/layout.march
  create  lib/my_app_web/conduit/pages/overview_page.march
  create  lib/my_app_web/conduit/pages/queue_page.march
  create  lib/my_app_web/conduit/pages/job_detail_page.march
  create  lib/my_app_web/conduit/pages/workflows_page.march
  create  lib/my_app_web/conduit/pages/cron_page.march
  create  lib/my_app_web/conduit/pages/dead_letters_page.march
  create  lib/my_app_web/conduit/components/job_row.march
  create  lib/my_app_web/conduit/components/status_badge.march
  create  lib/my_app_web/conduit/components/queue_card.march
```

Tell the router to use your custom templates instead of the defaults:

```march
forward "/conduit", Conduit.Dashboard.Router,
  auth: MyApp.AdminAuth,
  templates: MyAppWeb.Conduit  -- your generated template namespace
```

### 9.8 Dashboard Templates with `~H` Sigils

All Conduit dashboard templates use `~H`, March's HEEx-style template sigil. Templates are type-checked: component attributes are verified at compile time against their declared types.

#### `lib/my_app_web/conduit/components/status_badge.march`

```march
mod MyAppWeb.Conduit.Components.StatusBadge do
  use Bastion.Component

  -- Attribute types are compile-time checked
  attr :status, String, required: true
  attr :count, Option(Int), default: None

  fn render(assigns) do
    ~H"""
    <span class={"conduit-badge conduit-badge--#{@status}"}>
      <%= @status %>
      <%= if @count != None do %>
        <span class="conduit-badge__count"><%= Option.unwrap(@count) %></span>
      <% end %>
    </span>
    """
  end
end
```

#### `lib/my_app_web/conduit/components/queue_card.march`

```march
mod MyAppWeb.Conduit.Components.QueueCard do
  use Bastion.Component

  attr :queue_name, String, required: true
  attr :pending, Int, required: true
  attr :running, Int, required: true
  attr :failed, Int, required: true
  attr :throughput_per_min, Float, required: true
  attr :avg_latency_ms, Int, required: true

  fn render(assigns) do
    ~H"""
    <div class="conduit-queue-card">
      <div class="conduit-queue-card__header">
        <h3 class="conduit-queue-card__name"><%= @queue_name %></h3>
        <.status_badge status={queue_health(@pending, @failed)} />
      </div>
      <div class="conduit-queue-card__stats">
        <div class="conduit-queue-card__stat">
          <span class="conduit-queue-card__stat-label">Pending</span>
          <span class="conduit-queue-card__stat-value"><%= @pending %></span>
        </div>
        <div class="conduit-queue-card__stat">
          <span class="conduit-queue-card__stat-label">Running</span>
          <span class="conduit-queue-card__stat-value conduit-running"><%= @running %></span>
        </div>
        <div class="conduit-queue-card__stat">
          <span class="conduit-queue-card__stat-label">Failed</span>
          <span class={"conduit-queue-card__stat-value #{if @failed > 0 do "conduit-failed" else "" end}"}>
            <%= @failed %>
          </span>
        </div>
        <div class="conduit-queue-card__stat">
          <span class="conduit-queue-card__stat-label">Throughput</span>
          <span class="conduit-queue-card__stat-value"><%= Float.format(@throughput_per_min, 1) %>/min</span>
        </div>
        <div class="conduit-queue-card__stat">
          <span class="conduit-queue-card__stat-label">Avg Latency</span>
          <span class="conduit-queue-card__stat-value"><%= @avg_latency_ms %>ms</span>
        </div>
      </div>
      <a href={"/conduit/queues/#{@queue_name}"} class="conduit-queue-card__link">View jobs →</a>
    </div>
    """
  end

  pfn queue_health(pending, failed) do
    if failed > 0 do "warning"
    else if pending > 1000 do "degraded"
    else "healthy"
    end
  end
end
```

#### `lib/my_app_web/conduit/pages/overview_page.march`

```march
mod MyAppWeb.Conduit.Pages.OverviewPage do
  use Bastion.LivePage

  alias MyAppWeb.Conduit.Components.{QueueCard, StatusBadge}

  fn mount(params, session, socket) do
    if Bastion.connected?(socket) do
      Conduit.Dashboard.subscribe_to_summary()
      Bastion.schedule_tick(socket, :refresh, Duration.seconds(5))
    end
    let summary = Conduit.Dashboard.get_summary()
    Ok(Bastion.assign(socket,
      summary: summary,
      page_title: "Conduit — Overview"
    ))
  end

  fn handle_info(:conduit_summary_updated, socket) do
    let summary = Conduit.Dashboard.get_summary()
    Ok(Bastion.assign(socket, summary: summary))
  end

  fn handle_info(:refresh, socket) do
    let summary = Conduit.Dashboard.get_summary()
    Ok(Bastion.assign(socket, summary: summary))
  end

  fn render(assigns) do
    ~H"""
    <div class="conduit-overview">
      <div class="conduit-overview__header">
        <h1>Conduit</h1>
        <div class="conduit-overview__cluster">
          <span class="conduit-overview__node-count">
            <%= List.length(@summary.nodes) %> nodes
          </span>
          <%= if @summary.leader != None do %>
            <span class="conduit-overview__leader">
              leader: <%= (Option.unwrap(@summary.leader)).hostname %>
            </span>
          <% end %>
        </div>
      </div>

      <section class="conduit-overview__queues">
        <h2>Queues</h2>
        <div class="conduit-queue-grid">
          <%= for queue <- @summary.queues do %>
            <.queue_card
              queue_name={queue.name}
              pending={queue.pending}
              running={queue.running}
              failed={queue.failed}
              throughput_per_min={queue.throughput_per_min}
              avg_latency_ms={queue.avg_latency_ms}
            />
          <% end %>
        </div>
      </section>

      <section class="conduit-overview__recent-failures">
        <h2>Recent Failures</h2>
        <%= if List.is_empty(@summary.recent_failures) do %>
          <p class="conduit-empty">No failures in the last hour.</p>
        <% else %>
          <table class="conduit-table">
            <thead>
              <tr>
                <th>Job Type</th>
                <th>Queue</th>
                <th>Attempt</th>
                <th>Error</th>
                <th>Failed At</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <%= for job <- @summary.recent_failures do %>
                <tr class="conduit-table__row conduit-table__row--failed">
                  <td><code><%= job.job_type %></code></td>
                  <td><%= job.queue %></td>
                  <td><%= job.attempt %>/<%= job.max_attempts %></td>
                  <td class="conduit-table__error"><%= String.truncate(job.last_error, 80) %></td>
                  <td><%= DateTime.format_relative(job.failed_at) %></td>
                  <td>
                    <a href={"/conduit/jobs/#{job.id}"} class="conduit-link">Inspect</a>
                  </td>
                </tr>
              <% end %>
            </tbody>
          </table>
        <% end %>
      </section>

      <section class="conduit-overview__crons">
        <h2>Upcoming Crons</h2>
        <table class="conduit-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Schedule</th>
              <th>Last Run</th>
              <th>Next Run</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            <%= for cron <- @summary.next_crons do %>
              <tr class="conduit-table__row">
                <td><code><%= cron.id %></code></td>
                <td><code><%= cron.schedule %></code></td>
                <td>
                  <%= match cron.last_run_at do
                    None -> "Never"
                    Some(t) -> DateTime.format_relative(t)
                  end %>
                </td>
                <td><%= DateTime.format_relative(cron.next_run_at) %></td>
                <td><.status_badge status={if cron.enabled do "enabled" else "paused" end} /></td>
              </tr>
            <% end %>
          </tbody>
        </table>
      </section>
    </div>
    """
  end
end
```

#### `lib/my_app_web/conduit/pages/job_detail_page.march`

```march
mod MyAppWeb.Conduit.Pages.JobDetailPage do
  use Bastion.LivePage

  alias MyAppWeb.Conduit.Components.StatusBadge

  fn mount(params, _session, socket) do
    let job_id = Map.get(params, "id")
    match Conduit.Dashboard.get_job(job_id) do
      None ->
        Ok(Bastion.redirect(socket, to: "/conduit"))
      Some(job) ->
        Ok(Bastion.assign(socket,
          job: job,
          page_title: "Job #{String.slice(job_id, 0, 8)}…"
        ))
    end
  end

  fn handle_event("retry", _params, socket) do
    Conduit.Dashboard.retry_job(socket.assigns.job.id)
    let job = Option.unwrap(Conduit.Dashboard.get_job(socket.assigns.job.id))
    Ok(Bastion.assign(socket, job: job))
  end

  fn handle_event("cancel", _params, socket) do
    Conduit.Dashboard.cancel_job(socket.assigns.job.id)
    let job = Option.unwrap(Conduit.Dashboard.get_job(socket.assigns.job.id))
    Ok(Bastion.assign(socket, job: job))
  end

  fn handle_event("delete", _params, socket) do
    Conduit.Dashboard.delete_job(socket.assigns.job.id)
    Ok(Bastion.redirect(socket, to: "/conduit/queues/#{socket.assigns.job.queue}"))
  end

  fn render(assigns) do
    ~H"""
    <div class="conduit-job-detail">
      <div class="conduit-job-detail__header">
        <a href={"/conduit/queues/#{@job.queue}"} class="conduit-back">← <%= @job.queue %></a>
        <h1>
          <code><%= @job.job_type %></code>
          <.status_badge status={@job.status} />
        </h1>
        <div class="conduit-job-detail__id">ID: <code><%= @job.id %></code></div>
      </div>

      <div class="conduit-job-detail__actions">
        <%= if @job.status == "failed" || @job.status == "dead" do %>
          <button phx-click="retry" class="conduit-btn conduit-btn--primary">Retry</button>
        <% end %>
        <%= if @job.status == "pending" || @job.status == "running" do %>
          <button phx-click="cancel" class="conduit-btn conduit-btn--warning"
            data-confirm="Cancel this job?">Cancel</button>
        <% end %>
        <button phx-click="delete" class="conduit-btn conduit-btn--danger"
          data-confirm="Permanently delete this job?">Delete</button>
      </div>

      <section class="conduit-job-detail__section">
        <h2>Timeline</h2>
        <div class="conduit-timeline">
          <div class="conduit-timeline__event">
            <span class="conduit-timeline__label">Enqueued</span>
            <span class="conduit-timeline__time"><%= DateTime.format(@job.inserted_at) %></span>
          </div>
          <%= if @job.started_at != None do %>
            <div class="conduit-timeline__event">
              <span class="conduit-timeline__label">Started (attempt <%= @job.attempt %>)</span>
              <span class="conduit-timeline__time">
                <%= DateTime.format(Option.unwrap(@job.started_at)) %>
              </span>
            </div>
          <% end %>
          <%= if @job.completed_at != None do %>
            <div class="conduit-timeline__event conduit-timeline__event--success">
              <span class="conduit-timeline__label">Completed</span>
              <span class="conduit-timeline__time">
                <%= DateTime.format(Option.unwrap(@job.completed_at)) %>
              </span>
            </div>
          <% end %>
        </div>
      </section>

      <section class="conduit-job-detail__section">
        <h2>Payload</h2>
        <pre class="conduit-code"><%= Json.pretty_print(@job.payload) %></pre>
      </section>

      <section class="conduit-job-detail__section">
        <h2>Error History</h2>
        <%= if List.is_empty(@job.errors) do %>
          <p class="conduit-empty">No errors.</p>
        <% else %>
          <%= for {error, i} <- List.with_index(@job.errors) do %>
            <details class="conduit-error-entry">
              <summary>
                Attempt <%= i + 1 %> —
                <%= DateTime.format_relative(error.failed_at) %>:
                <%= String.truncate(error.message, 100) %>
              </summary>
              <pre class="conduit-code conduit-code--error"><%= error.message %></pre>
            </details>
          <% end %>
        <% end %>
      </section>

      <%= if @job.workflow_id != None do %>
        <section class="conduit-job-detail__section">
          <h2>Workflow</h2>
          <a href={"/conduit/workflows/#{Option.unwrap(@job.workflow_id)}"} class="conduit-link">
            View workflow → <%= Option.unwrap(@job.workflow_id) %>
          </a>
        </section>
      <% end %>
    </div>
    """
  end
end
```

#### `lib/my_app_web/conduit/pages/workflows_page.march` (workflow visualization)

```march
mod MyAppWeb.Conduit.Pages.WorkflowsPage do
  use Bastion.LivePage

  fn mount(_params, _session, socket) do
    let workflows = Conduit.Dashboard.list_workflows(status: "running", limit: 50)
    Ok(Bastion.assign(socket,
      workflows: workflows,
      tab: "running",
      page_title: "Conduit — Workflows"
    ))
  end

  fn handle_event("switch_tab", %{ "tab" => tab }, socket) do
    let workflows = Conduit.Dashboard.list_workflows(status: tab, limit: 50)
    Ok(Bastion.assign(socket, tab: tab, workflows: workflows))
  end

  fn handle_event("cancel_workflow", %{ "id" => id }, socket) do
    Conduit.cancel_workflow(id, reason: "Cancelled via dashboard")
    let workflows = Conduit.Dashboard.list_workflows(status: socket.assigns.tab, limit: 50)
    Ok(Bastion.assign(socket, workflows: workflows))
  end

  fn render(assigns) do
    ~H"""
    <div class="conduit-workflows">
      <h1>Workflows</h1>

      <div class="conduit-tabs">
        <%= for tab <- ["running", "completed", "failed", "cancelled"] do %>
          <button
            class={"conduit-tab #{if @tab == tab do "conduit-tab--active" else "" end}"}
            phx-click="switch_tab"
            phx-value-tab={tab}
          >
            <%= String.capitalize(tab) %>
          </button>
        <% end %>
      </div>

      <%= if List.is_empty(@workflows) do %>
        <p class="conduit-empty">No <%= @tab %> workflows.</p>
      <% else %>
        <div class="conduit-workflow-list">
          <%= for wf <- @workflows do %>
            <div class="conduit-workflow-card">
              <div class="conduit-workflow-card__header">
                <code class="conduit-workflow-card__type"><%= wf.workflow_type %></code>
                <span class="conduit-workflow-card__id"><%= String.slice(wf.id, 0, 8) %>…</span>
                <span class="conduit-workflow-card__age">
                  <%= DateTime.format_relative(wf.started_at) %>
                </span>
              </div>

              <%= if wf.status == "running" && wf.current_step != None do %>
                <div class="conduit-workflow-card__progress">
                  <span class="conduit-workflow-card__step-label">Current step:</span>
                  <code class="conduit-workflow-card__step">
                    <%= Option.unwrap(wf.current_step) %>
                  </code>
                </div>
              <% end %>

              <div class="conduit-workflow-card__checkpoints">
                <%= for cp <- wf.checkpoints do %>
                  <div class={"conduit-checkpoint conduit-checkpoint--completed"}>
                    <span class="conduit-checkpoint__name"><%= cp.name %></span>
                    <span class="conduit-checkpoint__duration"><%= cp.duration_ms %>ms</span>
                  </div>
                <% end %>
              </div>

              <div class="conduit-workflow-card__actions">
                <a href={"/conduit/workflows/#{wf.id}"} class="conduit-link">Inspect</a>
                <%= if wf.status == "running" do %>
                  <button
                    phx-click="cancel_workflow"
                    phx-value-id={wf.id}
                    class="conduit-btn conduit-btn--sm conduit-btn--warning"
                    data-confirm="Cancel this workflow?"
                  >
                    Cancel
                  </button>
                <% end %>
              </div>
            </div>
          <% end %>
        </div>
      <% end %>
    </div>
    """
  end
end
```

### 9.9 Generator Architecture Inside Conduit

All generators live under `lib/conduit/gen/`:

```
lib/conduit/gen/
├── gen.march              # Conduit.Gen — public dispatch API
├── new.march              # Conduit.Gen.New
├── job.march              # Conduit.Gen.Job
├── workflow.march         # Conduit.Gen.Workflow
├── cron.march             # Conduit.Gen.Cron
├── migration.march        # Conduit.Gen.Migration
├── dashboard.march        # Conduit.Gen.Dashboard
└── templates/             # EEx/HEEx template strings used by generators
    ├── job.march.eex
    ├── job_test.march.eex
    ├── workflow.march.eex
    ├── workflow_test.march.eex
    ├── cron.march.eex
    ├── config.march.eex
    └── worker.march.eex
```

The public API is:

```march
mod Conduit.Gen do
  type GenResult =
    | Created(path: String)
    | Skipped(path: String, reason: String)
    | Failed(path: String, error: String)

  fn run(subcommand: String, args: List(String)) -> Result(List(GenResult), String) do
    match subcommand do
      "new"       -> Conduit.Gen.New.run(args)
      "gen job"   -> Conduit.Gen.Job.run(args)
      "gen workflow" -> Conduit.Gen.Workflow.run(args)
      "gen cron"  -> Conduit.Gen.Cron.run(args)
      "gen migration" -> Conduit.Gen.Migration.run(args)
      "gen dashboard" -> Conduit.Gen.Dashboard.run(args)
      unknown     -> Err("Unknown subcommand: #{unknown}")
    end
  end
end
```

Forge's integration is a thin adapter:

```march
-- Inside Forge (in the forge/ directory of the March compiler repo)
mod Forge.Conduit do
  fn dispatch(subcommand: String, args: List(String)) do
    match Conduit.Gen.run(subcommand, args) do
      Ok(results) ->
        results |> List.each(fn result ->
          match result do
            Created(path) -> Forge.IO.print_created(path)
            Skipped(path, reason) -> Forge.IO.print_skipped(path, reason)
            Failed(path, error) -> Forge.IO.print_error(path, error)
          end
        end)
      Err(msg) ->
        Forge.IO.print_error("conduit gen", msg)
        Forge.exit(1)
    end
  end
end
```

This means third-party tools, editor plugins, and CI pipelines can call `Conduit.Gen.run/2` directly without going through Forge.

---

## 10. Appendix: Open Questions

These questions have been resolved. Decisions recorded below for historical context.

### Q1: Checkpoint Retention Policy

**Question:** After a workflow completes successfully, how long should checkpoints be retained?

**Options:**
- A) Delete immediately on completion (minimizes storage, loses debugging info)
- B) Retain for 24h then delete (reasonable default)
- C) Retain until explicitly cleared (simplest, but unbounded growth)
- D) Configurable per workflow type

**Decided:** D, with B as the default. Configurable per-workflow; 24h after completion by default.

### Q2: Workflow Versioning

**Question:** What happens when a workflow's code changes while instances are running?

If `OnboardUserWorkflow` is updated to add a new step between step 2 and step 3, and there are existing workflows mid-execution, what happens when they resume?

**Options:**
- A) Ignore: existing workflows resume with old logic (checkpoint names may not match)
- B) Version field on workflows: new code only applies to new instances
- C) Migration scripts: user-provided code to migrate in-flight workflows
- D) Require all workflows to complete before deploying new versions (operational constraint)

**Decided:** Use the CAS (content-addressable store) for workflow versioning. The workflow module's content hash determines the version. New instances get the current version; in-flight instances continue with their original version. This leverages March's existing CAS infrastructure rather than a manual version field.

### Q3: Workflow Sub-Step Parallelism Granularity

**Question:** In `parallel!`, should each parallel branch run as a separate job (durable across crashes of individual branches), or should parallelism be within a single actor's execution?

**Options:**
- A) Actor-level parallelism: parallel branches run in concurrent threads within one WorkflowRunnerActor. Fast, but losing the actor loses all branches.
- B) Job-level parallelism: each branch is a separate Conduit job. Durable, but more overhead.
- C) Configurable: default actor-level, opt-in to job-level for long-running branches.

**Decided:** C. Configurable — actor-level by default, job-level opt-in. Document this clearly in code.

### Q4: Checkpoint Serialization Format

**Question:** Checkpoints are currently JSON. For workflows that pass large typed structures between steps, JSON may be lossy (types lost) or verbose.

**Options:**
- A) JSON only (simple, debuggable in dashboard)
- B) Pluggable: same `Serializable` interface as job payloads, JSON default, binary opt-in
- C) MessagePack for checkpoints specifically (compact, fast, still debuggable with tooling)

**Decided:** B. Pluggable with two built-in options: JSON (default, human-readable) and March native data types (for typed round-tripping without loss).

### Q5: Dashboard Auth — Session vs Token

**Question:** Should the dashboard support cookie-based sessions (for human browser use) or only token-based auth (simpler to implement)?

**Decided:** Both session and token. The `Auth.Fn` adapter receives the full request so it can inspect cookies or headers. Already effectively implemented.

### Q6: Workflow Output Size Limit

**Question:** Workflow outputs are stored as JSONB. Should there be a size limit?

**Decided:** 1MB soft limit (log warning only, don't break working code); 10MB hard limit (reject). Larger outputs should use external storage with a reference.

### Q7: Cron Expression Validation

**Question:** Should cron expression validation be compile-time (macro) or runtime?

**Decided:** Runtime validation for now. Compile-time validation is a planned future enhancement (compile-time would require a complex macro and still can't validate dynamic crons).

### Q8: Unique Job Lock Scope

**Question:** Unique job constraints: should they be per-queue or global across all queues?

**Example:** If `SendWelcomeEmail` is unique for `user_id: 42` and the job is in queue `email`, can a second `SendWelcomeEmail { user_id: 42 }` be enqueued to queue `critical`?

**Decided:** Per-queue by default, global opt-in.

### Q9: Rate Limiter Backend

**Question:** Phase 7 rate limiting — should the rate limiter be:
- A) In-process with no cross-node coordination (fast, but per-node limits)
- B) Postgres-backed (coordinated, slightly slower)
- C) Pluggable (in-process default, Postgres or Redis for coordinated)

**Decided:** C. Pluggable — Postgres default (already implemented). In-process optimization deferred to a later phase.

### Q10: Forge Integration for Migration Generation

**Question:** The migration command is proposed as `forge conduit.gen.migrations`. This generates SQL files into the user's migration directory. What should the migration directory default be?

**Decided:** `priv/migrations/` matching Depot conventions. User can configure via Forge config:

```march
-- forge.march
Forge.configure do
  conduit do
    migration_path "priv/migrations"
    migration_prefix "conduit"
  end
end
```

---

*End of Conduit Spec*
