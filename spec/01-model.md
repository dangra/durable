# Core model

Part of the [`durable` specification](README.md).

## Core identities

```go
type PipelineID string
type ResourceID string
type RunID string
type StepID string
```

They are distinct defined types.

Their meanings are:

```text
PipelineID
    which durable Pipeline?

ResourceID
    which logical resource?

RunID
    which exact execution?

StepID
    which durable Step semantics?
```

---

## Resource slots

The scheduling slot is:

```text
(exclusion scope, ResourceID)
```

At most one nonterminal Run may occupy a slot.

By default a Pipeline's exclusion scope is private to it, so the slot is
effectively `(PipelineID, ResourceID)` and different Pipelines operate
concurrently on the same `ResourceID`.

A Pipeline MAY instead declare membership in a named **exclusion group**:

```proto
option (durable.v1.pipeline) = {
  id: "provision-machine"
  exclusion_group: "machine-lifecycle"
  ...
};
```

Pipelines sharing a group share one slot per resource: at most one
nonterminal Run may exist across the whole group for a `ResourceID`. This
models resources with multiple mutually exclusive lifecycle workflows
(provision vs decommission vs migrate on one machine), while pipelines
that deliberately coexist with them (a monitor) simply stay outside the
group.

Scopes are namespaced (`pipeline/<id>` vs `group/<name>`) so a group name
can never collide with another pipeline's default scope.

Enforcement is atomic in the Store at Run creation — there is no
check-then-schedule race.

---

## Cross-pipeline conflicts

Duplicate-scheduling equivalence (see Duplicate scheduling) applies only
within one Pipeline. A slot occupied by a Run of *another* Pipeline in
the group is always a conflict:

```go
type ScheduleConflictError struct {
    RunID      RunID
    PipelineID PipelineID
}
```

`PipelineID` identifies the blocking Run's pipeline so a caller can route
to its handle, inspect Status, or Wait. Conflicts are rejected, never
queued: reconcile-loop callers retry, which is the proven pattern for
this model.

A blocking Run's immutable Input is readable through its pipeline's
handle (typed on generated Runs, as a defensive copy), which is what
version-driven reconcile loops need to distinguish "my intent is already
in flight" from "stale work holds the slot":

```go
blocker, _ := provision.Run(ctx, conflict.RunID)
in, _ := blocker.Input(ctx)
if in.GetVersion() < desired {
    blocker.Cancel(ctx, "superseded")   // unwind cleans up, slot frees
    // reschedule the newer intent
}
```

For wait/inspect flows that know the resource but cannot reproduce the
exact Input, a pipeline exposes a read-only lookup:

```go
run, ok, err := provision.ActiveRun(ctx, resourceID)
```

Observation never claims the slot; Schedule remains the only atomic way
to do that.

---

## Run

A Run is one exact execution:

```text
PipelineID = provision-machine
ResourceID = machine-123
RunID      = 01K...
```

A resource slot may have historical Runs:

```text
Run A -> success
Run B -> failure
Run C -> active
```

but at most one nonterminal Run.

RunIDs are generated as ULIDs: time-prefixed, lexicographically
creation-ordered, with the timestamp extractable by tooling. This is an
implementation convenience for debugging, key layout, and retention
tooling — RunIDs remain opaque strings, no API compares them, and
`CreatedAt` stays authoritative for ordering.

---

## StepID

A `StepID` identifies durable Step semantics.

Example:

```text
reserve-capacity/v1
reserve-capacity/v2
```

Compatible protobuf schema evolution does not necessarily require a new StepID.

An incompatible semantic change MUST use a new StepID.

Step semantics include:

- forward behavior,
- unwind behavior,
- Step State schema.

Dynamic reads of other Step States are application behavior and are not statically declared.

---

## Step ownership

In v1, one durable Step declaration belongs to exactly one active Pipeline.

Cross-Pipeline durable Step reuse is not supported.

This permits lifecycle properties such as `retired` to live directly on the Step declaration.

Reusable application logic SHOULD live in ordinary Go helpers or services called by separate durable handlers.

---

## Pipeline Input

A Pipeline MAY declare an Input protobuf type:

```proto
message ProvisionMachineInput {
  string region = 1;
  uint64 memory_mb = 2;
  uint32 cpus = 3;
}
```

Scheduling:

```go
run, created, err := provision.Schedule(
    ctx,
    durable.ResourceID(machineID),
    &machines.ProvisionMachineInput{
        Region:   "ord",
        MemoryMb: 8192,
        Cpus:     4,
    },
)
```

Pipeline Input is immutable execution intent.

Once accepted, it MUST NOT change.

---

## Scheduling lifecycle

`Schedule` is valid only after the Engine has successfully started.

Calling it before `Engine.Start` returns:

```go
engine.ErrNotStarted
```

and MUST NOT create a Run.

For a Pipeline declaring Input, `nil` Input is invalid.

```go
provision.Schedule(ctx, resourceID, nil)
```

MUST return an input-validation error.

`nil` MUST NOT be normalized to an empty protobuf message.

For a Pipeline without Input, generated `Schedule` omits the Input argument entirely:

```go
run, created, err := cleanup.Schedule(
    ctx,
    resourceID,
)
```

---

## Delayed starts

`Schedule` accepts start options:

```go
provision.Schedule(ctx, resourceID, input, durable.StartAt(t))
provision.Schedule(ctx, resourceID, input, durable.StartAfter(d))
```

`StartAfter(d)` is sugar for `StartAt(now.Add(d))`, measured by the engine
clock at acceptance. If multiple start options are given, the last wins.

A delayed Run is created durably at acceptance and occupies its resource
slot immediately. Its first operation becomes eligible at the start time,
using the same durable eligibility mechanism as retries — the delay
therefore survives restart. Until an attempt is reserved, the Run reports
`RunStateScheduled`.

Start options are execution hints, not intent: they are not part of
duplicate-scheduling identity. An equivalent Input against an occupied
slot returns the active Run unchanged regardless of start options — the
original start time stands, and there is no expedite mechanism in v1.

A delayed Run occupies its slot until it executes or is canceled; see
Cancellation for retracting one.

---

## Cancellation

A Run may be durably canceled:

```go
err := run.Cancel(ctx, "operator retracted")
```

Cancellation reuses unwind rather than abandoning work:

```text
Cancel(runID, cause)
    -> durably record the request (first cancel wins)
    -> stop selecting new forward work
    -> RootFailure{Kind: FailureKindCanceled, Message: cause}
    -> normal unwind of successfully executed Steps
    -> OutcomeFailure, terminal
```

The cancellation RootFailure has no StepID: it is not a Step failure. The
Outcome model stays binary; `Result.Canceled()` reports a failure whose
root carries `FailureKindCanceled`.

**Started operations are never abandoned.** An unresolved operation pins
the Run as usual and continues retrying until it succeeds or returns
`Fail` — its partial effects demand resolution before unwind can be
computed correctly. Two accelerants bound the wait:

1. The in-flight attempt's context is preempted once when the request
   arrives; the interrupted attempt resolves through normal handler result
   semantics.
2. Subsequent attempts observe `Invocation.CancelRequested()` and may
   reconcile partial effects and return `Fail` fast instead of retrying
   toward an unwanted success.

The preemption arrives as the attempt context's cancellation cause:
`context.Cause(ctx)` is a `*PreemptedError` carrying the request's cause.
(An Engine shutdown kills the context with `ErrEngineStopping` instead —
operational, never semantic.) Returning `ctx.Err()` keeps the cooperative
default: the attempt is retried and the next one observes
`CancelRequested`. A handler or middleware may instead **yield**: return
`Fail` wrapping the `*PreemptedError`. The Engine attributes the resulting
RootFailure `FailureKindCanceled` with the cancellation's cause only when
its own evidence confirms the preemption — it preempted this attempt, or
the request is already durable — never on the error value alone, which a
handler could fabricate with no cancel pending. `FailFastOnCancel` is the
middleware form of that yield, for pipelines whose forward handlers are
preemption-safe; `FailFastExcept` keeps named Steps cooperative. Unwind
operations are never yielded: during a cancellation the unwind is the
work.

If the pinned operation succeeds, the Step is recorded and participates in
unwind like any other. If it permanently fails on its own, that organic
failure becomes the RootFailure — the Run terminates with unwind either
way, but `Canceled()` is false.

A never-started Run (including a delayed one) has no eligible unwind work
and terminates immediately, freeing its slot.

Additional semantics:

- The request survives restart.
- A Run remains cancelable until terminal success commits; cancellation
  arriving after forward completion but before Output commit still unwinds.
- Canceling a Run already in unwind is recorded but changes nothing:
  unwind always runs to completion.
- Canceling a terminal Run returns `ErrRunTerminal`; an unknown RunID
  returns `ErrRunNotFound`.
- Canceling an invalid Run records the request; it takes effect when a
  corrected deployment makes the Run reconcilable again. Cancellation does
  not bypass invalidity, because unwind itself requires a reconcilable
  topology.
- Cancellation is semantic and terminal; Engine shutdown remains
  operational and non-semantic — stopped Runs resume under a later Engine.

---

## Awaiting other Runs

Handlers never block on other Runs' completion: an in-handler `run.Wait`
would hold a worker slot, and enough of them exhaust the bounded pool and
deadlock the Engine. `Run.Wait` called with a handler's attempt context
(or one derived from it) therefore never blocks — it returns the Result
of an already-terminal Run and `ErrRunInProgress` otherwise. Cross-run
waiting is a resolution instead: a **park**.

```go
return durable.AwaitRun(child.ID())                        // one target
return durable.AwaitAll(ids)                               // every target
return durable.AwaitAny(ids)                               // the first target
return durable.AwaitRun(id, durable.WithAwaitTimeout(d))  // bounded
```

A park names its targets, its mode, and an optional deadline. It parks
the current operation: the operation remains unresolved (still pinning
the Run), the worker is released, no retry attempts burn, and `Status`
reports `RunStateAwaiting` with the targets, mode, and deadline. Any Run
may be a target, across pipelines; duplicate targets collapse; a park
with no targets is a contract violation and invalidates the Run.

The park **resolves** when:

- `AwaitModeAll` — every target is terminal or missing (never created,
  or reaped by retention);
- `AwaitModeAny` — the first target is;
- its deadline passes. `WithAwaitTimeout(d)` fixes an absolute deadline
  at park time that survives restart. Expiry is a wake, not a failure:
  the handler decides.

A park already satisfied when it is recorded resolves immediately. On
resolution the operation re-executes as a fresh attempt. Waking is
at-least-once re-execution, not resumption: the handler runs again from
the top.

The wake attempt's Invocation reports the park through its **memory**:

```go
type Wake struct {
    Targets []RunID // what the park was on
    Done    []RunID // Targets terminal or missing at wake time
    Expired bool    // the deadline fired first
}
func (w Wake) Pending() []RunID // Targets not in Done

if w, ok := inv.Awaited(); ok {
    // woken: inspect w.Done (Run.Wait on a done target returns at once),
    // re-park on w.Pending() to keep waiting or to extend a deadline,
    // Cancel the pending, or Fail
}
if id, ok := inv.AwaitedRunID(); ok { /* single-target parks only */ }
```

This is the durable memory that makes schedule-then-await steps safe:
without it, re-execution after the child completed would find a free
slot and spawn another child. **The memory belongs to the operation, not
to one attempt.** It is recorded when the park resolves, carried by every
later attempt of the operation — across ordinary-error retries and
Engine restarts — and cleared when the operation resolves (success or
`Fail`) or parks again. A crash after scheduling a child but before the
park commits re-executes without it — the ordinary at-least-once window,
closable with a deterministic child ResourceID where it matters. The
same caveat bounds fan-out: scheduling N children in one attempt is
idempotent across a retry only while they are nonterminal; a child that
finishes before the retry is terminal, and `Schedule` creates a fresh
one.

Awaits MUST NOT form a cycle: a park that closes a cycle of awaits marks
the parking Run invalid for the current deployment (detected after the
park is durably recorded, so concurrently formed cycles cannot escape).
Detection is conservative: a cycle through any edge is refused in every
mode, including `AwaitModeAny`, where another target might have let the
park escape — a park that can deadlock is refused, not gambled on.

A pending cancellation bypasses the park: the operation re-executes,
observes `CancelRequested`, and its `Awaited` memory reports the targets
with `Done` reflecting their state at that moment, so the handler can
cancel what it spawned before resolving. The park survives restart.

Canonical shapes:

```text
wait-for-existing:   ActiveRun -> found? AwaitRun : proceed
create-then-wait:    Awaited? proceed : Schedule -> AwaitRun
drain-then-start:    Schedule -> conflict? AwaitRun(blocker) : done
                     (waking re-executes; Schedule retries the freed slot)
fan-out:             Awaited? inspect Done : Schedule×N -> AwaitAll
select loop:         Awaited? handle Done; Pending? AwaitAny(Pending) : done
race:                Awaited? Cancel(Pending); use Done[0] : Schedule×N -> AwaitAny
deadline:            Awaited.Expired? Fail | Cancel(Pending) | re-park : proceed
```

---

## Duplicate scheduling

If no nonterminal Run occupies the slot:

```text
create Run
created = true
```

If one exists with equivalent Input:

```text
return existing Run
created = false
```

Input equivalence MUST use:

```go
proto.Equal(existing, supplied)
```

including unknown fields.

This strict unknown-field-sensitive comparison is intentional.

It favors exact durable intent identity over normalization across concurrently running binaries with different compatible protobuf schema versions.

If Input differs:

```go
type ScheduleConflictError struct {
    RunID      RunID
    PipelineID PipelineID // the blocking Run's pipeline, for routing to its handle
}
```

and:

```text
run     = zero
created = false
err     = *ScheduleConflictError
```

---

## Handler result semantics

```text
nil
    operation success

ordinary error
    unresolved operation, retry

durable.Fail(err)
    permanent semantic operation failure

durable.AwaitRun(id), AwaitAll(ids), AwaitAny(ids)
    operation parks until the park resolves (see Awaiting other Runs)

runtime contract violation
    Run invalid for current deployment
```

Runtime invalidity is not equivalent to `durable.Fail`. See [invalid Runs](04-engine.md#invalid-runs).

---

## Partial-effect responsibility

A forward Step that permanently fails was never successfully committed.

Example:

```text
A ✓
B ✓
C creates partial external effect X
C -> durable.Fail
```

Unwind does not execute C merely because C failed.

Therefore:

> A forward handler MUST reconcile its own partial uncommitted effects before returning `durable.Fail`.

---

## Failure persistence

Conceptually:

```go
type FailureRecord struct {
    StepID  StepID
    Phase   Phase
    Attempt uint64
    Message string
    At      time.Time

    Kind   FailureKind
    Reason string
}

type RootFailure struct {
    FailureRecord
}

type UnwindFailure struct {
    FailureRecord
}

type Failure struct {
    Root           RootFailure
    UnwindFailures []UnwindFailure
}
```

Arbitrary Go error chains are intentionally flattened.

The durable representation preserves:

- execution location,
- attempt,
- phase,
- timestamp,
- human-readable error message.

Wrapped error identity, Go error types, and arbitrary structured chains are not preserved in v1.

Structured durable failure details MAY be added later.

---

## Failure attribution

Permanent failures carry two informational attribution fields. They MUST
NOT affect engine scheduling, retry, or unwind behavior.

**Kind** classifies fault:

```go
type FailureKind uint8

const (
    FailureKindSystem FailureKind = iota // zero value, the default
    FailureKindUser
    FailureKindCanceled
)
```

`system` means infrastructure or environment is at fault; `user` means the
request or intent itself is. System is the default because it is the
overwhelmingly common case and the safe alerting posture. `canceled` marks
RootFailures established by Run cancellation; it is created by the engine
and reserved — handlers do not attribute their own failures with it.

**Reason** is a machine-readable slug ("invalid-image",
"insufficient-capacity") whose destiny is metrics labels and alert
routing. Reasons SHOULD be short, lowercase, and low-cardinality;
human-readable detail belongs in Message.

`Fail` is the only permanent-failure constructor; attribution attaches via
options:

```go
durable.Fail(err)                                    // kind=system
durable.Fail(err, durable.WithUserKind())            // kind=user
durable.Fail(err, durable.WithReason("invalid-image"))
```

Attribution may also be carried by any error in the handler's chain, so
domain error types classify themselves once and resolution sites stay
plain `Fail(err)`:

```go
type FailureReasoner interface { FailureReason() string }
type FailureKinder   interface { FailureKind() FailureKind }
```

Precedence, per axis: explicit `Fail` option, then the first match in the
error chain (via `errors.As`), then the default (`system` / empty reason).

Reasons are also extracted from ordinary retryable errors to populate the
Run's last-error observability fields; kind is extracted only at permanent
resolution.

---

## Failure during Unwind

`Failure.UnwindFailures` is populated incrementally.

Suppose:

```text
D.Run       -> permanent failure
C.Unwind    -> permanent failure
B.Unwind    -> permanent failure
A.Unwind    -> current
```

A receives:

```go
Failure{
    Root: root,
    UnwindFailures: []UnwindFailure{
        cFailure,
        bFailure,
    },
}
```

Permanent unwind failures appear in unwind execution order.

Ordinary retry errors are operational history and are not added to `UnwindFailures`.

---

## Outcome

```go
type Outcome uint8

const (
    OutcomeSuccess Outcome = iota + 1
    OutcomeFailure
)
```

No other terminal business outcomes exist in v1.

---

## Result

```go
type Result struct {
    Outcome Outcome

    RootFailure    *RootFailure
    UnwindFailures []UnwindFailure
}
```

Convenience methods SHOULD include:

```go
func (r Result) Succeeded() bool
func (r Result) Failed() bool
```

Success:

```text
OutcomeSuccess
RootFailure = nil
UnwindFailures = []
```

Business failure:

```text
OutcomeFailure
RootFailure != nil
```

An invalid nonterminal Run does not produce a terminal `Result`.
