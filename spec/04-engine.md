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
engine.WithRetryPolicy(
    engine.RetryPolicy{
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

Every piece of free text the Engine records on a Run — failure messages
and reasons, the last-error fields, cancel causes — is bounded to the
Engine's text limit (`WithTextLimit`, default 4096 bytes), cut at a rune
boundary and marked, so a handler error that wraps a response body
cannot grow the Run's failure record. The full text belongs in logs and
traces.

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
    RunStateQueued
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
[Concurrency classes](#concurrency-classes)); `RunStateQueued` means
the Run has not started and is in line for a token of its pipeline's
run class (see [Run classes](01-model.md#run-classes)).

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

- Failure,
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
    // Set from the start of unwind; on a terminal failure it equals
    // Result.Failure. Nil while executing forward and on success.
    Failure *Failure

    CancelRequested bool
    CancelCause     string

    // RunStateAwaiting: the park.
    AwaitingRunIDs []RunID
    AwaitMode      AwaitMode // AwaitModeAll or AwaitModeAny
    AwaitDeadline  time.Time // zero without WithAwaitTimeout

    // RunStateThrottled: the class waited for.
    ThrottledClass string

    // RunStateQueued: the run class in line for.
    QueuedClass string

    // When the first attempt was reserved; zero until then. Kept on a
    // terminal Run.
    StartedAt time.Time

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

- Pipeline definitions bind through `Engine.Bind`, which validates
  each one and rejects a malformed definition with an error,
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

A dispatched Run is read in full once, at dispatch. The worker that
reconciles it is the only writer of its cursor, operations, failure, and
outcome, and persists exactly the record it holds, so after a successful
transition the record in memory is the store's and the loop carries it
across iterations; after the dispatch's read a pass reads nothing. A
write to the record by anyone else — today only a cancel request, which
reaches the store through the engine — marks the Run dirty; an iteration
that finds the mark clears it and re-reads the record, and the mark is
taken before a read so a write after it is seen by the next iteration.
A failed transition ends the pass, and the next dispatch reads fresh.

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

Global concurrency is unbounded by default: every Run with an operation
in progress has a worker of its own, and a Run that is parked, delayed,
throttled, or queued holds none, so the worker count is the number of
Runs executing. Real resources are bounded where they are shared, by
concurrency classes and run classes, not by a global budget. A
deployment MAY bound the workers anyway:

```go
engine.WithConcurrency(32)
```

With a bound, the rules below about not sleeping while holding
scheduler capacity are what keep it from starving.

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
engine.WithConcurrencyClass("vm-snapshots", 2)
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

## Run classes

A run class bounds Runs, not operations: see
[Run classes](01-model.md#run-classes) for the model. The Engine side:

```go
engine.WithRunClass("migrations", engine.RunClass{Capacity: 4, MaxQueued: 32})
```

- The gate sits in the reconcile loop before a Run's first attempt,
  after the cancel check and the retry gate and before the
  concurrency-class gate. A started Run (`StartedAt` set) passes on a
  field check and never touches the pool.
- Acquisition never blocks a worker: a Run finding its class full is
  queued (`RunStateQueued`, exposing the class), its worker released,
  and it is redispatched by the release that frees a token. Grants go
  out in line order, to every Run that fits.
- The token is released at the terminality commit, whatever the
  outcome, and the head of the line is woken.
- An invalid Run keeps its token if it holds one and leaves the line
  if it does not, so it never holds up the Runs behind it; a corrected
  deployment puts it back in line at its own place.
- At `Start`, before any worker runs, every nonterminal head of a
  class is seeded: started ones hold, unstarted ones count as queued
  and take their place at their first dispatch.
- `MaxQueued` is checked and counted in one step at `Schedule`, before
  the store write; an admission that creates nothing is taken back.

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
- does not create Failure.

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
engine.WithClock(clock)
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

The uniform type-erased operation is exposed by the `durable` package,
since middleware is written in handler vocabulary:

```go
type Handler func(ctx context.Context, inv Invocation) (proto.Message, error)

type Middleware func(next Handler) Handler
```

A forward operation returns the State to commit (nil for a stateless
Step); an unwind operation always returns `(nil, err)`.

Engine-level middleware wraps every operation, forward and unwind alike:

```go
engine.WithMiddleware(logging, metrics)
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
meta (identity)               written once, at creation
input                         written once, at creation — the large one
step fact rows                written once per operation resolution
failure / cancel              written rarely
cursor                        rewritten on every attempt — small
terminal                      written once, replacing everything above
```

The input is its own component because it is the one large value of
the nonterminal stage: a store that keeps it apart from the small
write-once facts never rewrites it, or a page holding it, when a fact
lands.

A Run has two storage stages. Nonterminal, it is meta, its step fact
rows, the cursor, and its failure and cancel records. The terminality
commit replaces all of them with one terminal record — identity,
outcome, output, commit time, failure, cancel request, and the
permanently failed unwind operations — in the same atomic step, so a
terminal Run is one row. A store may physically free the released
components shortly after the commit, in batches, as long as no read
surfaces them: the terminal record is authoritative from the commit
on. The Input and the committed Step States have
been folded into the Output by then; nothing reachable through a terminal Run needs them, and
releasing them at terminality rather than at retention keeps the
retained history proportional to outputs. The failed unwind
operations stay because they are the durable evidence that
compensation did not happen. `InputBytes` on a terminal Run returns
`ErrRunTerminal`.

The Cursor is the per-Run scheduling state: phase, retry/start
eligibility, last-error fields, the **single in-flight operation**
(step, attempt count), its park while awaiting other Runs (targets,
mode, deadline), the memory of its last resolved park (targets, done,
expired), and the Run's start (`StartedAt`, set by the first attempt's
reservation and carried unchanged after) — leaning on the
one-operation-per-Run invariant.
Because only the Cursor is rewritten per attempt, per-attempt write
volume is bounded by the Cursor, independent of Input and State sizes
(a park's target list is the one Cursor field that scales with
application choice).

The contract is the `store/driver` package: `Store`, `RunRecord`,
`Cursor`, and `Transition`, built from the identity, phase, outcome,
park, and failure-record vocabulary of the `kernel` package, which the
`durable` package aliases. Store implementations persist every Cursor
field, answer `GetActiveRunID` from the same index `CreateRun` enforces
the resource slot with (never by scanning), answer `GetRunHead` — the
record without its blobs and operation history, which is what status,
waiting, lookups, await bookkeeping, and recovery read — in point reads
that touch no Input, State, or Output, and register a URI scheme
with `store.Register`, so applications open them through `store.Open`
and link only the drivers they import.
The in-memory `store/mem` is the executable reference, and `store/bbolt`
is checked against it by differential fuzzing.

The engine mutates durable state exclusively through atomic transitions:

```go
Store.ApplyTransition(ctx, runID, Transition{
    Cursor:      ...,  // always applied
    Ops:         ...,  // one operation row per resolution, failure included
    Failure: ...,  // set once; a cancellation has no resolving operation
    Output:      ..., Outcome: ..., // terminality; releases the slot
})
```

A Step's forward execution and its unwind are two operations with two
rows, each written once when it resolves: status, attempts, the
committed State (forward only), the permanent failure that resolved it
if any, and its resolution order within the Run. An unwind therefore
never rewrites the State, and the Run's permanent unwind failures are
the failed unwind rows read back in order. Reads assemble the full
RunRecord from the components, overlaying the cursor's in-flight
operation as an unresolved entry. An operation displaced by topology
change before resolving is flushed to its row so its attempt count
survives (attempt numbers are never reused).

Cancellation requests live in their own component, written only by
RequestCancel — the engine worker remains the sole writer of everything
else, with no read-modify-write races by construction.

The byte slices of a Run's Input, its Step States, and its Output are
immutable: a store may retain the slice it is given and return the same
backing memory on every read, from a cache of its own if it keeps one,
and no caller modifies such a slice. The engine only ever decodes them.

---

## Retention

Retention is off by default: without configuration, terminal Runs
accumulate indefinitely (the Engine logs this at Start).

```go
engine.WithRetentionPolicy(engine.RetentionPolicy{
    TerminalAfter: 7 * 24 * time.Hour, // keep terminal Runs this long
    Interval:      10 * time.Minute,   // jittered sweep cadence (default)
})
```

The Engine sweeps on the jittered interval, starting immediately at
Start, deleting terminal Runs in bounded batches through the Store's
`ReapTerminal(before, limit)` primitive. Each Run's components are
removed atomically; "terminal since" is the commit time the terminal
record carries.

Retention governs only the terminal stage (see Store contract): the
Input, the Step States, and the cursor are released by the terminality
commit itself, not by the sweep, so what a retention window keeps per
Run is its identity, outcome, output, failures, and cancel request.

Only terminal Runs are ever reaped. Nonterminal Runs — invalid ones
included — are never touched regardless of age (force-releasing an
unrecoverable Run is the separate abandonment problem, out of v1 scope).

After reaping, lookups of the Run return `ErrRunNotFound`; a reaped Run
is no longer enumerable.

The Clock governs retention timing, so sweeps are deterministic under
`WithClock`.

---

## Internal type erasure

The Engine core SHOULD remain non-generic. The erased shape is
`pipelinedef.Config`, and the Engine reads a handler's result only
through the exported classifiers middleware uses (`AwaitRequest`,
`AwaitTimeout`, `FailureInfo`, `FailureCause`, `FailureReason`), so the
`durable` package holds no engine-only plumbing.

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
RunUnwinding   the Failure that started an unwind
RunTerminal    the outcome
RunInvalid     the reason
WaiterWoken    a park resolved: targets, done, expired, time parked
ClassWait      a Run proceeding after a class wait: class, scope
               (operation or run), time waited
RunsReaped     a retention sweep's count
StoreOp        every Store call: op, write-ness, duration, error
```

Callbacks run synchronously on the Engine's path and MUST be fast; a
panicking callback is recovered and logged, never allowed to affect the
Run. Middleware classifies a handler's return for its own telemetry with
`AwaitRequest` (a park) and `FailureInfo` (a permanent failure).

**Snapshots.** `Engine.Stats()` reports the occupancy of the moment —
Runs with a live worker, awaiting, throttled, queued, delayed, invalid
— per concurrency class its capacity, use, and queue, and per run class
its capacity, holders, line, and accepted-but-unstarted count.

The OpenTelemetry bridge (`contrib/durableotel`) is built entirely on
these seams: spans per attempt linked to the scheduling trace through
annotations, metrics from the events, gauges from the snapshot.
