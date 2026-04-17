# Conduit

Conduit is a job queue and durable workflow platform for March. It provides primitives for defining, scheduling, and executing background jobs and multi-step workflows.

## Project Structure

- `lib/` — library source code
- `lib/conduit/` — submodules (error, backoff, config, schema, job, storage, queue, worker)
- `test/` — tests (run with `forge test`)
- `specs/conduit.md` — full design spec with phased implementation plan
- `.claude/skills/march-lang/SKILL.md` — **March language reference** (read this before writing any March code)

## Searching the codebase

**Use `forge search` to find modules, functions, types, and other code constructs.** This is the primary way to discover what exists in the codebase.

```
forge search "function_name"    # search for a function
forge search "ModuleName"       # search for a module
forge search "type_name"        # search for a type
```

**`forge search` is always the preferred way to search `.march` files.** Use it instead of Grep/grep whenever the target is March code — names, types, docstrings, or constructors.

## Development

This is a March library. Use `forge test` to run the test suite. The compiler is at `/Users/80197052/code/march` but files cannot be compiled directly — write correct March source and use the skill for syntax reference.

**After editing any `.march` file, run `forge check` to typecheck the whole project quickly before proceeding.** For a full validation cycle: `forge check && forge build && forge lint --strict && forge test`.

## Language

All source files use the `.march` extension. **Always read `.claude/skills/march-lang/SKILL.md` before writing March code.** Key rules:

- `mod Name do ... end` — modules (never `module`)
- `fn` / `pfn` — public / private functions (no `pub` keyword)
- `if cond do ... end` — no `then` keyword
- Lambdas: `fn x -> expr` only (no `do...end` block form)
- Zero-arg lambdas: `fn () -> expr`
- `task_spawn(fn _ -> f())` — task_spawn calls its argument with 1 arg; use `fn _ ->` not `fn () ->`
- Sum types: `type Foo = A | B(Int)` (no leading `|`)
- `interface Name(a) do ... end` / `impl Name(Type) do ... end`
- No semicolons

## Dependencies

- `depot` — March's standard data/storage library (`../depot`)

## Architecture (fully implemented)

| File | Purpose |
|------|---------|
| `lib/conduit.march` | Public API: `start`, `enqueue`, `enqueue_in`, `register`, `start_workflow`, `cancel_workflow`, `cluster_leader` |
| `lib/conduit/error.march` | `ConduitError` type |
| `lib/conduit/backoff.march` | `Conduit.Backoff` type + `delay/2` |
| `lib/conduit/config.march` | `Conduit.JobConfig`, `Conduit.Config`, defaults |
| `lib/conduit/schema.march` | `Conduit.Schema` — optional arg validation |
| `lib/conduit/job.march` | `Conduit.Job` interface |
| `lib/conduit/storage.march` | `Conduit.Storage` interface (Postgres + VaultStore impls) |
| `lib/conduit/queue.march` | Queue ops + performer registry |
| `lib/conduit/worker.march` | Task-based worker pool with heartbeat + rescue |
| `lib/conduit/node.march` | Multi-node cluster registration + leader election |
| `lib/conduit/cron.march` | `Conduit.Cron` — cron job definition |
| `lib/conduit/cron_scheduler.march` | Background cron tick loop |
| `lib/conduit/workflow.march` | `Conduit.WorkflowRow`, `WorkflowConfig` types |
| `lib/conduit/workflow_context.march` | Deterministic checkpoint execution + replay |
| `lib/conduit/workflow_registry.march` | In-process workflow handler registry |
| `lib/conduit/workflow_runner.march` | Workflow execution via job queue |
| `lib/conduit/event_store.march` | Append-only event log for workflow replay |
| `lib/conduit/dashboard/` | Web dashboard: overview, queues, jobs, workflows, crons, dead letters, nodes |
| `lib/conduit/api.march` | Top-level `Conduit.API.start` — boots workers + node + cron scheduler |

## Critical runtime notes

### `task_spawn` arity
`task_spawn` always calls its callback with **1 argument**. Always use a 1-arg lambda:
```march
task_spawn(fn _ -> my_loop(arg1, arg2))   -- correct
task_spawn(fn () -> my_loop(arg1, arg2))  -- WRONG — arity mismatch at runtime
```

### VaultStore: flat primitive keys only
When implementing `Conduit.Storage` with Vault, store each workflow/checkpoint/event field as a **separate primitive (String/Int) vault key** — never store a struct with nested DateTime fields in Vault. DateTime fields in heap-allocated structs cause GC/RC issues when stored as opaque pointers.

Pattern used in `test_conduit_app`:
```march
-- Store flat keys:
Vault.set(tbl, "wf:" ++ id ++ ":type",    row.workflow_type)
Vault.set(tbl, "wf:" ++ id ++ ":status",  row.status)
Vault.set(tbl, "wf:" ++ id ++ ":started", int_to_string(DateTime.to_timestamp(row.started_at)))
-- ...etc

-- Load flat keys:
let wtype = Vault.get(tbl, "wf:" ++ id ++ ":type") |> unwrap_or("")
```

### Vault key length collision (march runtime bug — now fixed)
The March runtime's `vault_key_cstr` previously used `march_value_to_string` on String keys, which returned `"#<tag:N>"` (where N = string length). This caused all vault keys of the **same length** to map to the same bucket. Fixed in `march_extras.c`. Always use the latest March runtime build.

## Implementation Status

- [x] **Phase 1** — Simple job queue: enqueue, execute, complete/fail
- [x] **Phase 2** — Retries, backoff, dead-letter queues
- [x] **Phase 3** — Workflows, cron, multi-node cluster
- [x] **Phase 4** — Web dashboard (overview, queues, jobs, workflows, crons, dead letters, nodes)
- [ ] **Phase 5** — Postgres storage backend (storage interface is defined; VaultStore in-memory impl complete)
- [ ] **Phase 6** — Telemetry, middleware hooks, rate limiting polish
