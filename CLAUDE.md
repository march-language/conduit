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

Always use `forge search` before grepping or manually reading files when looking for modules, functions, or types in March code.

## Development

This is a March library. Use `forge test` to run the test suite. The compiler is at `/Users/80197052/code/march` but files cannot be compiled directly — write correct March source and use the skill for syntax reference.

## Language

All source files use the `.march` extension. **Always read `.claude/skills/march-lang/SKILL.md` before writing March code.** Key rules:

- `mod Name do ... end` — modules (never `module`)
- `fn` / `pfn` — public / private functions (no `pub` keyword)
- `if cond do ... end` — no `then` keyword
- Lambdas: `fn x -> expr` only (no `do...end` block form)
- Zero-arg lambdas: `fn () -> expr`
- Sum types: `type Foo = A | B(Int)` (no leading `|`)
- `interface Name(a) do ... end` / `impl Name(Type) do ... end`
- No semicolons

## Dependencies

- `depot` — March's standard data/storage library (`../depot`)

## Architecture (Phase 1 — implemented)

| File | Purpose |
|------|---------|
| `lib/conduit.march` | Public API: `start`, `enqueue`, `enqueue_in`, `register` |
| `lib/conduit/error.march` | `ConduitError` type |
| `lib/conduit/backoff.march` | `Conduit.Backoff` type + `delay/2` |
| `lib/conduit/config.march` | `Conduit.JobConfig`, `Conduit.Config`, defaults |
| `lib/conduit/schema.march` | `Conduit.Schema` — optional arg validation |
| `lib/conduit/job.march` | `Conduit.Job` interface |
| `lib/conduit/storage.march` | `Conduit.Storage` interface + Postgres impl |
| `lib/conduit/queue.march` | Queue ops + performer registry |
| `lib/conduit/worker.march` | Task-based worker pool |

## Implementation Phases

See `specs/conduit.md` for the full phased plan. Current status:

- [x] **Phase 1** — Simple job queue: enqueue, execute, complete/fail
- [ ] **Phase 2** — Retries, backoff, dead-letter queues
- [ ] **Phase 3+** — Workflows, cron, middleware, telemetry, dashboard
