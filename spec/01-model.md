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
(PipelineID, ResourceID)
```

At most one nonterminal Run may occupy a slot.

Different Pipelines MAY operate concurrently on the same `ResourceID`.

Cross-Pipeline exclusion is outside v1.

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
durable.ErrEngineNotStarted
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

Because v1 has no cancellation, a far-future delayed Run occupies its slot
until it executes; schedule distant starts only when that is acceptable.

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
    RunID RunID
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
)
```

`system` means infrastructure or environment is at fault; `user` means the
request or intent itself is. System is the default because it is the
overwhelmingly common case and the safe alerting posture.

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
