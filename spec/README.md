# `durable`: Durable Linear Pipelines with Unwind Semantics

**Status:** Draft 1.3  
**Target:** Go 1.27+  
**Persistence:** Local transactional database such as SQLite or bbolt  
**Schema and code generation:** Protocol Buffers, Buf, and `protoc-gen-durable`

## Contents

| File | Covers |
|---|---|
| [01-model.md](01-model.md) | Core identities, resource slots, Runs, scheduling, failure taxonomy, Outcome and Result |
| [02-authoring.md](02-authoring.md) | Pipeline and Step declarations, handlers, the State API, Reducers, the application-facing API, worked examples |
| [03-evolution.md](03-evolution.md) | Execution ledger, forward and unwind frontiers, pinning, reordering, retirement, removal, unwind semantics, breaking changes |
| [04-engine.md](04-engine.md) | Engine lifecycle, recovery, retries, attempts, invalid Runs, concurrency, shutdown, observability |
| [05-codegen.md](05-codegen.md) | Protobuf options, `protoc-gen-durable` outputs, generation-time validation, Buf |
| [invariants.md](invariants.md) | The core invariants checklist |
| [future-work.md](future-work.md) | Deferred design areas |
| [http-analogy.md](http-analogy.md) | Non-normative design note: the net/http analogy behind middleware and func adapters |

---

## Overview

`durable` is a Go library for executing linear pipelines whose execution state survives process crashes and restarts.

A pipeline consists of an ordered sequence of Steps:

```text
A -> B -> C -> D
```

Each Step operation uses **at-least-once execution semantics**. Step handlers therefore MUST be idempotent.

Successful forward execution automatically advances to the next applicable Step.

An ordinary error means the current operation remains unresolved and MUST be retried.

A handler explicitly declares permanent operation failure with:

```go
return durable.Fail(err)
```

During forward execution, permanent failure establishes a root failure and begins unwind.

For:

```text
A ✓ -> B ✓ -> C ✓ -> D X
```

unwind normally proceeds:

```text
C <- B <- A
```

subject to the current Pipeline definition and the Run's monotonic unwind frontier.

Pipeline definitions may evolve while Runs are active. `durable` does not persist a complete immutable Pipeline topology per Run. Instead, it persists execution facts and reconciles them against the currently registered Pipeline definition.

A Step may be marked **retired** as the intermediate stage before removal. Retirement prevents new Runs from entering that Step while allowing already-started operations to resolve.

Pipeline Output is optionally produced by a pure `Reducer` over immutable Pipeline Input and committed Step States.

---

## Design principle

`durable` intentionally keeps the authoring model small.

Correctness responsibilities are divided into three layers:

```text
Code generation
    structural API correctness

Engine configuration
    current Pipeline/Step registration correctness

Per-Run reconciliation
    compatibility between persisted execution facts
    and the current Pipeline definition
```

If an existing nonterminal Run cannot be reconciled safely against the current application definition, the Engine does not attempt increasingly complex migration semantics.

Instead:

```text
Run -> invalid for current deployment
```

The Engine:

- logs a diagnostic,
- exposes the condition through status and future observability,
- ignores the Run for execution,
- continues processing other valid Runs.

A corrected future deployment MAY make the Run valid and runnable again.

Invalidity is an operational runtime condition, not a Pipeline business failure.

---

## Package layout

The module is one package per audience plus one shared leaf, and every
import arrow points from the more specific package to the more general
one:

```text
kernel        identities, phases, outcomes, parks, failure records
   ^               ^                ^
durable       storedriver        observe
   ^          (store SPI)        (lifecycle events)
pipelinedef   type-erased definitions built by generated code
   ^
engine        the runtime: New, Bind, Schedule, Run, Result, Status
   ^
bboltstore, durabletest, contrib/durableotel, generated code
```

- `durable` is the handler contract and nothing else: `Invocation`,
  `Failure`, `Fail`, the `Await*` resolutions, `Handler` and
  `Middleware` with their classifiers, the step references, and
  aliases of the `kernel` vocabulary. Handler code imports only this.
- `engine` is the wiring side. Its exported signatures use the
  `durable` aliases, so a `Status.RunID` reads as `durable.RunID`.
- `pipelinedef` is what generated code builds and `Engine.Bind`
  validates. Application code does not import it.
- `kernel` has no audience of its own; it exists so that `storedriver`
  and `observe` do not depend on the handler contract, and the handler
  contract does not depend on the store SPI.

Normative identifiers in this specification are qualified by package
where it matters. Unqualified handler-facing names (`Invocation`,
`Fail`, `Wake`) are the `durable` package's; unqualified runtime names
(`Engine`, `Run`, `Result`, `Status`, `RunState`) are `engine`'s.

---

## Scope

The model is:

```text
linear current topology
+
typed immutable Input
+
typed immutable Step State
+
durable execution ledger
+
monotonic forward frontier
+
monotonic unwind frontier
+
Step retirement
+
automatic retry
+
explicit permanent failure
+
reverse unwind
+
pure Output reduction
+
crash recovery
```

Initial non-goals include:

- distributed execution,
- distributed consensus,
- remote workers,
- distributed queues,
- parallel branches,
- DAG execution,
- loops,
- child workflows,
- deterministic program replay,
- exactly-once external side effects,
- arbitrary workflow migration languages,
- BPMN,
- Petri nets,
- distributed transactions.

---

## Summary

The durable runtime model is:

```text
             current Pipeline definition
                        +
                Run execution ledger
                        |
                        v
              reconcile execution
                        |
          +-------------+-------------+
          |                           |
       forward                      unwind
          |                           |
 monotonic frontier         monotonic frontier
          |                           |
          v                           v
       Step.Run                  Step.Unwind
```

Forward topology evolution:

```text
insert after frontier
    -> may execute

insert before frontier
    -> skipped for that Run

unresolved Step
    -> pins Run until resolution

retired + never started
    -> bypass

retired + already started
    -> finish normally

removed while still required
    -> Run invalid
```

Unwind topology evolution:

```text
successful forward
+
currently present
+
currently unwindable
+
ahead of unwind frontier
    -> eligible

removed
    -> not eligible

never executed
    -> not eligible

becomes eligible behind frontier
    -> skipped
```

State access:

```go
state, ok := inv.State(machines.SomeStep)
```

and:

```go
state, ok := pipeline.State(machines.SomeStep)
```

Reducer:

```text
immutable Input
+
immutable committed Step States
        |
        v
      Reducer
        |
        v
immutable Pipeline Output
```

Failure semantics:

```text
ordinary error
    -> retry

durable.Fail during Run
    -> RootFailure
    -> unwind

durable.Fail during Unwind
    -> permanent UnwindFailure
    -> continue backward

runtime incompatibility
    -> RunStateInvalid
    -> diagnose
    -> ignore until repaired
```

The central persistence principle is:

> **Persist immutable execution facts, then interpret them against the currently registered linear Pipeline definition using monotonic execution frontiers.**

The central compatibility principle is:

> **Normal compatible evolution is reconciled automatically. Anything that cannot be reconciled safely is an observable runtime-invalid Run rather than a reason to complicate the workflow model or stop the entire Engine.**
