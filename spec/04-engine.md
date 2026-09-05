# Engine runtime

Part of the [`durable` specification](README.md).

## Attempt numbering

Attempt numbers are durably reserved before application invocation.

Conceptually:

```text
transaction:
    mark operation started/unresolved
    increment attempt
commit

invoke handler
```

Therefore:

```go
inv.Attempt()
```

is a monotonically increasing durable invocation reservation number.

The first reserved attempt is:

```go
1
```

A crash may happen after the attempt is durably reserved but before application handler code actually begins.

Therefore there may be gaps in observed application executions:

```text
Attempt 4 durably reserved
process crashes before handler starts

next invocation:
Attempt 5
```

Attempt numbers are never reused.

This same durable transition provides the required distinction between:

```text
never started
```

and:

```text
started but unresolved
```

without requiring a separate persistence write.

---

## Retry semantics

Ordinary errors are retried indefinitely.

There is no max-attempt exhaustion concept in v1.

Permanent semantic failure requires explicit:

```go
durable.Fail(err)
```

A future bounded retry feature would need separate exhaustion semantics and is outside v1.

---

## Retry policy

Engine-level policy MAY be configured:

```go
durable.WithRetryPolicy(
    durable.RetryPolicy{
        Initial:    100 * time.Millisecond,
        Max:        30 * time.Second,
        Multiplier: 2,
    },
)
```

Retry delays SHOULD include jitter.

Workers MUST NOT sleep while holding scheduler capacity.

After an ordinary error:

```text
record unresolved attempt
compute NextAttemptAt
record LastError / LastReason / LastErrorAt
release worker
schedule wakeup
```

The last-error fields describe the most recent ordinary-error attempt of
the current unresolved operation (handler panics included, as their
synthesized message). LastReason is extracted from the error chain via
`FailureReasoner`. They ride the same durable write as `NextAttemptAt` —
no extra persistence cost, and they survive restart — and are cleared when
the operation resolves: success wipes them, and permanent failure carries
its cause in the failure records instead. They are informational only.

---

## Retry durability

`NextAttemptAt` survives restart.

Example:

```text
failure at     10:00:00
next eligible  10:00:30
restart at     10:00:05
```

The Engine SHOULD preserve:

```text
10:00:30
```

rather than immediately retrying.

---

## Phase

```go
type Phase uint8

const (
    PhaseForward Phase = iota + 1
    PhaseUnwind
    PhaseDone
)
```

---

## RunState

Conceptually:

```go
type RunState uint8

const (
    RunStateRunnable RunState = iota + 1
    RunStateRunning
    RunStateWaitingRetry
    RunStateScheduled
    RunStateAwaiting
    RunStateThrottled
    RunStateInvalid
    RunStateDone
)
```

`RunStateScheduled` means the Run was accepted with a delayed start and no
operation attempt has been reserved yet; `RunStateWaitingRetry` means an
attempted operation is waiting for its next attempt; `RunStateAwaiting`
means the in-flight operation is parked on other Runs (see
[Awaiting other Runs](01-model.md#awaiting-other-runs)); `RunStateThrottled`
means the next operation is parked on a full concurrency class (see
[Concurrency classes](#concurrency-classes)).

`RunStateInvalid` means the current application deployment cannot safely continue the nonterminal Run.

It is not a terminal business outcome.

---

## Invalid Runs

Examples include:

- unresolved Step no longer exists,
- forward frontier cannot be reconciled,
- unwind frontier cannot be reconciled,
- persisted Step State cannot be decoded,
- state-producing handler returns `(nil, nil)`,
- successful State cannot be serialized,
- Reducer panics,
- Reducer assumptions are incompatible with persisted data,
- persisted execution facts are internally inconsistent.

For a Run-local error:

```text
log diagnostic
mark runtime state invalid
ignore Run
continue Engine
```

No:

- RootFailure,
- automatic Unwind,
- OutcomeFailure

is created.

---

## Invalidity is deployment-relative

Run invalidity SHOULD be derived from:

```text
persisted execution facts
+
current application definition
```

rather than necessarily becoming a permanent persisted flag.

Example:

```text
deployment v2 removes unresolved B
    -> Run invalid

deployment v3 restores B as retired
    -> Run valid
    -> B resumes
```

This allows operator repair through normal redeployment.

---

## Waiting on invalid Runs

`Wait` MUST NOT silently block forever once the Engine knows the Run is invalid.

Conceptually:

```go
type InvalidRunError struct {
    RunID      RunID
    PipelineID PipelineID
    Reason     string
}
```

Then:

```go
result, err := run.Wait(ctx)
```

may return:

```text
result = zero
err    = *InvalidRunError
```

because invalidity is an operational condition, not a terminal Pipeline Result.

---

## Status

Conceptually:

```go
type Status struct {
    PipelineID PipelineID
    ResourceID ResourceID
    RunID      RunID

    Phase Phase
    State RunState

    StepID  StepID
    Attempt uint64

    NextAttemptAt time.Time

    LastError   string
    LastReason  string
    LastErrorAt time.Time

    Outcome *Outcome

    CancelRequested bool
    CancelCause     string

    // RunStateAwaiting: the park.
    AwaitingRunIDs []RunID
    AwaitMode      AwaitMode // AwaitModeAll or AwaitModeAny
    AwaitDeadline  time.Time // zero without WithAwaitTimeout

    // RunStateThrottled: the class waited for.
    ThrottledClass string

    // RunStateInvalid: why.
    InvalidReason string
}
```

Invalid status MUST expose enough diagnostic information to identify why
execution is blocked.

---

## Engine lifecycle

```text
configuring
    |
    | Start
    v
running
```

During configuration:

- Pipeline definitions bind,
- Step handlers register,
- Reducers register.

After successful Start:

- registration freezes,
- recovery occurs,
- scheduling is accepted.

---

## Engine.Start granularity

`Engine.Start` validates global Engine configuration.

Engine-wide problems fail startup, such as:

- Store cannot be opened,
- exclusive ownership cannot be acquired,
- duplicate StepIDs,
- malformed current Pipeline definitions,
- invalid generated/runtime registration.

A problem isolated to one persisted Run MUST NOT fail Engine startup.

Example:

```text
1000 valid Runs
3 invalid Runs
```

results in:

```text
Engine.Start succeeds

1000 Runs recover
3 Runs logged + ignored
```

Recovery quarantine as a separate subsystem is therefore unnecessary for the basic v1 behavior.

---

## Single Engine ownership

Exactly one Engine instance may execute against a Store at once in v1.

Store implementations SHOULD enforce or detect exclusive ownership where practical.

---

## Engine recovery

Startup recovery SHOULD:

1. freeze registration,
2. discover nonterminal Runs,
3. reconstruct durable execution facts,
4. reconcile each Run independently against current definitions,
5. classify each as runnable, waiting, invalid, or otherwise nonterminal,
6. enqueue valid runnable work,
7. schedule valid retry wakeups,
8. log invalid Runs,
9. start normal execution.

---

## Engine-owned Run lifetime

The context passed to `Schedule` governs the scheduling request only.

Once accepted:

```text
caller context cancellation
    !=
Run cancellation
```

The Engine owns execution lifetime. Ending a Run early is a separate,
durable, semantic act: [cancellation](01-model.md#cancellation).

---

## Concurrency

Exactly one logical operation belonging to a Run may execute at a time.

Different Runs MAY execute concurrently.

Global concurrency:

```go
durable.WithConcurrency(32)
```

## Concurrency classes

Steps whose operations contend on a shared resource declare a named
class in the schema; the engine configures how much of that resource the
deployment has:

```proto
option (durable.v1.step) = {
  id: "snapshot/v1"
  concurrency_class: "vm-snapshots"
};
```

```go
durable.WithConcurrencyClass("vm-snapshots", 2)
```

A pipeline-level `concurrency_class` sets the default for all of its
steps; a step's own class overrides it. Declaration is schema-side
("these steps share a resource"), capacity is deployment-side — a class
with no configured capacity is unlimited, and the Engine warns at Start.

Class semantics:

- A class bounds **simultaneously executing operations** (forward and
  unwind alike) of its member steps. Tokens are held only while the
  handler runs — never across retry waits, parks, or restarts — and are
  purely in-memory: nothing is persisted.
- Acquisition never blocks a worker. A Run whose next operation finds
  its class full parks (`RunStateThrottled`, exposing the class) and is
  woken FIFO when a token releases.
- A pending cancellation bypasses the gate so the Run can resolve and
  unwind.

This is the durable form of the in-transition semaphores flyd hand-rolls
(e.g. bounding concurrent VM snapshot writes), which would starve a
bounded worker pool if ported as blocking waits.

---

## Immediate continuation

A worker MAY continue immediately through subsequent successful Steps.

It releases capacity when:

- retry is required,
- Run becomes invalid,
- Run becomes terminal,
- shutdown begins.

---

## Graceful shutdown

Shutdown:

- stops starting new operation attempts — for newly scheduled Runs, for
  the next Step of a Run whose attempt just finished, and for retries;
- lets in-flight attempts finish on their own terms for up to the drain
  timeout (`WithDrainTimeout`), their results committing normally;
- at the deadline — immediately, by default — kills the remaining
  attempt contexts, with `context.Cause` `ErrEngineStopping`;
- leaves unresolved Runs nonterminal,
- does not create RootFailure.

A future Engine resumes them. A handler that only returns on
`ctx.Done()` drains at the deadline, so the drain timeout is kept
modest.

Shutdown is operational; Run cancellation (see 01-model) is semantic and
terminal. They are unrelated mechanisms.

---

## Crash recovery and backoff

A startup-discovered unresolved operation MAY receive recovery backoff to prevent crash loops.

The system need not persist a distinction between graceful interruption and crash.

---

## Clock

The Engine SHOULD support:

```go
durable.WithClock(clock)
```

for deterministic timing tests.

Clock governs:

- retry timestamps,
- retry wakeups,
- recovery backoff,
- Failure timestamps.

---

## Handler panics

A handler panic SHOULD be recovered.

It is considered an unresolved operation, not `durable.Fail`.

The Engine SHOULD:

- capture stack diagnostics,
- record the failed invocation operationally,
- retry according to normal/recovery policy.

This differs from Reducer panic because a handler operation explicitly supports transient retry semantics.

---

## Middleware

The uniform type-erased operation is exposed as:

```go
type Handler func(ctx context.Context, inv Invocation) (proto.Message, error)

type Middleware func(next Handler) Handler
```

A forward operation returns the State to commit (nil for a stateless
Step); an unwind operation always returns `(nil, err)`.

Engine-level middleware wraps every operation, forward and unwind alike:

```go
durable.WithMiddleware(logging, metrics)
```

The first middleware is outermost. `Invocation.Phase()` distinguishes
forward from unwind operations.

Middleware contract:

- Middleware runs once per attempt, inside the durable attempt
  reservation. It inherits at-least-once semantics and MUST NOT assume
  exactly-once execution.
- Middleware participates in handler result semantics: returning the
  error unchanged preserves the retry/`Fail` classification; transforming
  an ordinary error into `durable.Fail` deliberately resolves the
  operation as permanent failure.
- Middleware MAY derive the context passed to inner handlers (timeouts,
  tracing, baggage).
- Middleware panics are recovered like handler panics and leave the
  operation unresolved.
- The Reducer is pure, not an operation, and is never wrapped.

Per-Step middleware composition is outside v1.1.

See the non-normative [net/http analogy note](http-analogy.md) for the
design rationale.

---

## Store contract

The Store persists Runs as components with distinct write cadences:

```text
meta (identity, input)        written once, at creation
step fact rows                written once per operation resolution
failures / terminal / cancel  written rarely
cursor                        rewritten on every attempt — small
```

The Cursor is the per-Run scheduling state: phase, retry/start
eligibility, last-error fields, the **single in-flight operation**
(step, attempt count), its park while awaiting other Runs (targets,
mode, deadline), and the memory of its last resolved park (targets,
done, expired) — leaning on the one-operation-per-Run invariant.
Because only the Cursor is rewritten per attempt, per-attempt write
volume is bounded by the Cursor, independent of Input and State sizes
(a park's target list is the one Cursor field that scales with
application choice).

The contract is the `storedriver` package: `Store`, `RunRecord`,
`Cursor`, and `Transition`, built from the identity, phase, outcome,
park, and failure-record vocabulary of the `kernel` package, which the
`durable` package aliases. Store implementations persist every Cursor
field; the in-memory `durabletest.MemStore` is the executable reference,
and the bbolt store is checked against it.

The engine mutates durable state exclusively through atomic transitions:

```go
Store.ApplyTransition(ctx, runID, Transition{
    Cursor:        ...,  // always applied
    Steps:         ...,  // fact rows upserted at resolution
    RootFailure:   ...,  // set once
    UnwindFailure: ...,  // appended
    Output:        ..., Outcome: ..., // terminality; releases the slot
})
```

Reads assemble the full RunRecord from the components, overlaying the
cursor's in-flight operation as an unresolved step entry. An unwind
operation displaced by topology change before resolving is flushed to its
step row so its attempt count survives (attempt numbers are never
reused).

Cancellation requests live in their own component, written only by
RequestCancel — the engine worker remains the sole writer of everything
else, with no read-modify-write races by construction.

---

## Retention

Retention is off by default: without configuration, terminal Runs
accumulate indefinitely (the Engine logs this at Start).

```go
durable.WithRetention(durable.RetentionPolicy{
    TerminalAfter: 7 * 24 * time.Hour, // keep terminal Runs this long
    Interval:      10 * time.Minute,   // jittered sweep cadence (default)
})
```

The Engine sweeps on the jittered interval, starting immediately at
Start, deleting terminal Runs in bounded batches through the Store's
`ReapTerminal(before, limit)` primitive. Each Run's components are
removed atomically; "terminal since" is the cursor timestamp stamped by
the terminality commit.

Only terminal Runs are ever reaped. Nonterminal Runs — invalid ones
included — are never touched regardless of age (force-releasing an
unrecoverable Run is the separate abandonment problem, out of v1 scope).

After reaping, lookups of the Run return `ErrRunNotFound`; a reaped Run
is no longer enumerable. Keeping a compact post-retention summary is a
possible future extension.

The Clock governs retention timing, so sweeps are deterministic under
`WithClock`.

---

## Internal type erasure

The Engine core SHOULD remain non-generic.

Generated adapters may erase typed application values into:

```text
PipelineID
ResourceID
RunID
StepID

serialized Input
serialized Step State
serialized Output

forward execution ledger
unwind execution ledger

forward frontier information
unwind frontier information

retry metadata
Failure metadata

current Pipeline descriptors
internal handler adapters
internal Reducer adapters
```

---

## Observability

Three surfaces, all optional, none affecting execution.

**Logging.** The Engine logs through a `log/slog` logger
(`WithLogger`): per-attempt progress (scheduled, retrying, succeeded,
awaiting, throttled) at Debug; once-per-Run milestones (terminal
outcome, unwind start, cancellation accepted) at Info; permanent unwind
failures at Warn; anomalies (handler panics, store errors, invalid Runs)
at Error. Lifecycle lines carry the canonical keys `pipeline`,
`resource`, and `run`; operation-scoped lines add `step`, `phase`, and
`attempt`; causes appear under `error`. `Invocation.Logger()` pre-attaches
the same keys for handler code. Invalid Runs MUST NOT be silent: their
diagnostic carries the Run identity, phase, the Step if applicable, and
the reason.

**Lifecycle events.** `observe.Observer` is a struct of optional
callbacks, installed with `WithObserver` (repeatable; every event fires on
every observer, in installation order):

```text
RunScheduled   acceptance, with any delayed start and the annotations
AttemptDone    every attempt resolution: succeeded, retrying (with the
               delay), failed, or awaiting; duration; whether it panicked
RunUnwinding   the RootFailure that started an unwind
RunTerminal    the outcome
RunInvalid     the reason
WaiterWoken    a park resolved: targets, done, expired, time parked
ClassWait      a throttled Run proceeding: class, time waited
RunsReaped     a retention sweep's count
StoreOp        every Store call: op, write-ness, duration, error
```

Callbacks run synchronously on the Engine's path and MUST be fast; a
panicking callback is recovered and logged, never allowed to affect the
Run. Middleware classifies a handler's return for its own telemetry with
`AwaitRequest` (a park) and `FailureInfo` (a permanent failure).

**Snapshots.** `Engine.Stats()` reports the occupancy of the moment —
Runs with a live worker, awaiting, throttled, delayed, invalid — and per
concurrency class its capacity, use, and queue.

The OpenTelemetry bridge (`contrib/durableotel`) is built entirely on
these seams: spans per attempt linked to the scheduling trace through
annotations, metrics from the events, gauges from the snapshot.
