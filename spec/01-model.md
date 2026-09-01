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
