# `durable`: Durable Linear Pipelines with Unwind Semantics

**Status:** Draft 0.6  
**Target:** Go 1.27+  
**Persistence:** Local transactional database such as SQLite or bbolt  
**Schema and code generation:** Protocol Buffers, Buf, and `protoc-gen-durable`

## 1. Overview

`durable` is a Go library for executing fixed, linear pipelines whose progress survives process crashes and restarts.

A pipeline consists of an ordered sequence of steps:

```text
A -> B -> C -> D
```

Each step executes with **at-least-once semantics** and therefore MUST be idempotent.

Successful completion of a step automatically advances execution to the next step. Successful completion of the final step completes the pipeline.

Ordinary errors are considered transient and cause automatic retries.

A handler may explicitly mark the current operation as permanently failed by returning:

```go
return durable.Fail(err)
```

During forward execution, a permanent failure stops forward progress and begins unwind.

If `D` permanently fails after `A`, `B`, and `C` completed successfully:

```text
A -> B -> C -> D
              X
              |
              v
         C <- B <- A
```

Only steps that declare unwind behavior execute an `Unwind` handler.

Successfully completed steps without unwind behavior are resolved automatically while walking backward.

An unwind operation may itself fail permanently. That failure is recorded and unwind continues with the previous successfully completed step.

Every run may contain:

- an immutable pipeline definition,
- an optional immutable typed pipeline input,
- zero or more immutable per-step durable states,
- an optional immutable typed pipeline output,
- an exact durable execution identity.

Changes to pipeline declarations affect newly created runs only.

---

# 2. Design principles

`durable` follows several core principles.

### Constrained execution model

Pipelines are fixed, linear sequences.

`durable` intentionally does not attempt to model arbitrary workflow graphs.

### Explicit durable state

Execution position, retry state, input, step state, failures, and terminal output are persisted explicitly.

`durable` does not use deterministic program replay.

### Ordinary Go handlers

Application code remains ordinary Go.

Generated interfaces expose only capabilities that a step or pipeline actually declares.

### Immutable execution data

The following are immutable once committed:

```text
Pipeline definition
Pipeline input
Step state
Pipeline output
```

### Explicit permanent failure

Ordinary errors mean retry.

```go
durable.Fail(err)
```

is the explicit declaration that the current operation should no longer be retried.

---

# 3. Core identities

`durable` distinguishes four identities:

```text
PipelineID
    which pipeline?

ResourceID
    which logical resource?

RunID
    which exact execution?

StepID
    which durable step?
```

Go types:

```go
type PipelineID string
type ResourceID string
type RunID string
type StepID string
```

They MUST be distinct defined types, not aliases.

---

# 4. Pipeline resource slots

A pipeline execution occupies a logical scheduling slot identified by:

```text
(PipelineID, ResourceID)
```

At most one nonterminal run may occupy a slot at once.

For example:

```text
provision-machine / machine-123
```

may have only one active run.

Another pipeline may operate simultaneously on the same resource:

```text
provision-machine / machine-123
update-inventory  / machine-123
```

Cross-pipeline exclusion is outside the initial scope.

---

# 5. Run

A **Run** is one immutable execution of a pipeline for a resource.

Example:

```text
PipelineID = provision-machine
ResourceID = machine-123
RunID      = 01K...
```

A resource may accumulate multiple historical runs:

```text
provision-machine / machine-123

Run A
    SUCCESS

Run B
    FAILURE

Run C
    ACTIVE
```

Only one may be nonterminal at once.

---

# 6. RunID

`RunID` uniquely identifies one exact execution.

Unlike `ResourceID`, it does not identify a logical resource.

Unlike a pipeline revision number, it does not identify source code or topology.

A `RunID` is a portable durable value suitable for:

- logging,
- persistence,
- APIs,
- correlation,
- lookup after restart.

---

# 7. StepID

A `StepID` identifies durable step semantics.

Example:

```text
reserve-capacity/v1
reserve-capacity/v2
```

Changing a step incompatibly requires introducing a new `StepID`.

A `StepID` MUST NOT depend on:

- protobuf message name,
- Go package name,
- Go type name,
- display name.

Compatible protobuf schema evolution does not necessarily require a new `StepID`.

---

# 8. Protocol Buffer declarations

`durable` uses protobuf custom options to declare steps and pipelines.

Conceptually:

```proto
syntax = "proto3";

package durable.v1;

import "google/protobuf/descriptor.proto";

message StepOptions {
  string id = 1;
  bool unwind = 2;
  TombstoneOptions tombstone = 3;
}

message PipelineOptions {
  string id = 1;
  string input = 2;
  string output = 3;
  repeated string steps = 4;
}

message TombstoneOptions {
  UnwindPolicy unwind = 1;
}

enum UnwindPolicy {
  UNWIND_POLICY_UNSPECIFIED = 0;
  UNWIND_POLICY_HANDLER_REQUIRED = 1;
  UNWIND_POLICY_NOOP = 2;
}

extend google.protobuf.MessageOptions {
  StepOptions step = 51000;
  PipelineOptions pipeline = 51001;
}
```

The exact tombstone representation may evolve independently.

---

# 9. Pipeline input

A pipeline MAY declare an input protobuf type.

Example:

```proto
message ProvisionMachineInput {
  string region = 1;
  uint64 memory_mb = 2;
  uint32 cpus = 3;
}

message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"
    input: ".machines.v1.ProvisionMachineInput"

    steps: ".machines.v1.Validate"
    steps: ".machines.v1.ReserveCapacity"
    steps: ".machines.v1.CreateMachine"
  };
}
```

Scheduling:

```go
run, created, err := provision.Schedule(
    ctx,
    machineID,
    &machines.ProvisionMachineInput{
        Region:   "ord",
        MemoryMb: 8192,
        Cpus:     4,
    },
)
```

Pipeline input represents immutable run intent.

Once accepted, it MUST NOT change.

A process restart MUST observe exactly the same input.

---

# 10. Duplicate scheduling and input equality

If no nonterminal run occupies `(PipelineID, ResourceID)`:

```text
create new run
created = true
```

If an active run exists with equivalent input:

```text
return existing run
created = false
```

If an active run exists with different input:

```text
return scheduling conflict
```

The new input MUST NOT silently replace or mutate the existing run's input.

For protobuf inputs, semantic protobuf equality MAY be used.

Pipelines without input omit the input argument entirely.

---

# 11. Step declaration

A protobuf message annotated with `durable.step` declares a durable step.

Example:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v2"
    unwind: true
  };

  string reservation_id = 1;
  string host_id = 2;
}
```

A step declaration has two independent capability dimensions:

```text
protobuf fields present
    -> step establishes durable state

unwind = true
    -> step requires an Unwind operation
```

---

# 12. Step state

A successful step MAY establish immutable durable state.

The protobuf message declaring the step is also the schema of that state.

Example:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
  };

  string reservation_id = 1;
  string host_id = 2;
}
```

A successful handler might return:

```go
return &machines.ReserveCapacity{
    ReservationId: reservation.ID,
    HostId:        reservation.HostID,
}, nil
```

That value becomes the durable state established by the step.

---

# 13. Step capability matrix

The generated Go API MUST expose only capabilities actually declared by the step.

## No state, no unwind

Proto:

```proto
message Validate {
  option (durable.v1.step) = {
    id: "validate/v1"
  };
}
```

Generated:

```go
type ValidateHandler interface {
    Run(
        context.Context,
        ValidateInvocation,
    ) error
}
```

---

## State, no unwind

Proto:

```proto
message SelectHost {
  option (durable.v1.step) = {
    id: "select-host/v1"
  };

  string host_id = 1;
}
```

Generated:

```go
type SelectHostHandler interface {
    Run(
        context.Context,
        SelectHostInvocation,
    ) (*SelectHost, error)
}
```

---

## No state, with unwind

Proto:

```proto
message MarkProvisioning {
  option (durable.v1.step) = {
    id: "mark-provisioning/v1"
    unwind: true
  };
}
```

Generated:

```go
type MarkProvisioningHandler interface {
    Run(
        context.Context,
        MarkProvisioningInvocation,
    ) error

    Unwind(
        context.Context,
        MarkProvisioningInvocation,
        durable.Failure,
    ) error
}
```

---

## State and unwind

Proto:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
  };

  string reservation_id = 1;
}
```

Generated:

```go
type ReserveCapacityHandler interface {
    Run(
        context.Context,
        ReserveCapacityInvocation,
    ) (*ReserveCapacity, error)

    Unwind(
        context.Context,
        ReserveCapacityInvocation,
        state *ReserveCapacity,
        failure durable.Failure,
    ) error
}
```

The parameter is called `state` because it represents the immutable durable state established by the successful forward operation.

---

# 14. Generated API principle

The protobuf declaration is the semantic contract.

Generated Go code MUST reflect that contract exactly.

Specifically:

```text
no fields + unwind=false

    Run(...) error


fields + unwind=false

    Run(...) (*Step, error)


no fields + unwind=true

    Run(...) error
    Unwind(..., Failure) error


fields + unwind=true

    Run(...) (*Step, error)
    Unwind(..., state *Step, Failure) error
```

The framework MUST NOT require:

- dummy state objects,
- empty protobuf returns,
- synthetic no-op unwind handlers,
- marker methods purely for dispatch.

---

# 15. Step state success boundary

For state-producing steps:

```text
Run -> (state, nil)

    persist state
    mark step successful
    advance


Run -> (_, ordinary error)

    discard returned state
    retry


Run -> (_, durable.Fail(err))

    discard returned state
    permanently fail operation
    begin unwind
```

Step state exists if and only if forward execution has durably succeeded.

The state and successful transition MUST be committed as one logical atomic operation.

---

# 16. Step state immutability

Once committed, step state MUST NOT be changed.

The initial public API MUST NOT expose:

```go
SetState(...)
UpdateState(...)
MutateState(...)
```

Step state represents durable evidence of what a successful step established.

---

# 17. Explicit step-state ownership

Step state belongs to its producing `StepID`.

For:

```text
A -> B -> C
```

the durable model is:

```text
A -> A state
B -> B state
C -> C state
```

It is not:

```text
global mutable PipelineState
```

This isolates schema evolution and step retirement.

---

# 18. Downstream state access

Later steps MAY read durable state produced by previous successful steps.

Access MUST be explicit and typed.

Conceptually:

```go
reservation, ok := inv.State(
    machines.ReserveCapacityStep,
)
```

with compile-time result type:

```go
*machines.ReserveCapacity
```

Application code MUST NOT need to:

- address state by string IDs,
- manually decode protobuf values,
- type assert `any`,
- use reflection.

The generator SHOULD reject statically invalid state dependencies where practical.

---

# 19. Invocation

Handlers receive generated invocation types.

Example:

```go
func (h *reserveCapacity) Run(
    ctx context.Context,
    inv machines.ReserveCapacityInvocation,
) (*machines.ReserveCapacity, error)
```

Common invocation metadata includes:

```go
inv.PipelineID()
inv.ResourceID()
inv.RunID()
inv.StepID()
inv.Attempt()
inv.Phase()
```

If the pipeline declares input:

```go
input := inv.Input()
```

returns the generated typed pipeline input.

Typed access to previous step state MAY also be generated.

---

# 20. Operation result semantics

Handler return values have consistent meaning.

## Success

```go
return nil
```

or:

```go
return state, nil
```

means:

> The current operation completed successfully.

## Retryable failure

```go
return err
```

means:

> The current operation remains unresolved and should be retried.

## Permanent failure

```go
return durable.Fail(err)
```

means:

> The current operation should no longer be retried after this result has been durably committed.

The current phase determines the next transition.

---

# 21. Forward execution

For `Run`:

```text
success
    -> persist state if any
    -> mark step successful
    -> advance

ordinary error
    -> retry same step

durable.Fail(err)
    -> permanently fail forward operation
    -> establish root failure
    -> enter unwind
```

---

# 22. Unwind execution

For `Unwind`:

```text
nil
    -> mark unwind successful
    -> continue backward

ordinary error
    -> retry same unwind operation

durable.Fail(err)
    -> record permanent unwind failure
    -> continue backward
```

A permanent unwind failure MUST NOT terminate unwind.

---

# 23. Steps without unwind

A successful step with `unwind=false` requires no backward action.

Example:

```text
A ✓ unwind=true
B ✓ unwind=false
C ✓ unwind=true
D X
```

Unwind:

```text
C.Unwind()
    |
    v
B automatically resolved
    |
    v
A.Unwind()
```

No synthetic handler is called for `B`.

---

# 24. Unwind liveness

Every successfully completed step MUST eventually be resolved during unwind as exactly one of:

```text
no unwind required

successfully unwound

permanently failed to unwind
```

Retryable errors do not resolve an unwind operation.

---

# 25. Failure context

Every unwind handler receives a read-only `durable.Failure`.

Conceptually:

```go
type Failure struct {
    Root RootFailure

    UnwindFailures []UnwindFailure
}
```

The root failure never changes during unwind.

Permanent unwind failures already encountered SHOULD be visible to earlier unwind handlers.

Retryable attempt errors are operational history and MUST NOT become part of the semantic failure chain.

---

# 26. Pipeline output

A pipeline MAY declare a typed business output.

Example:

```proto
message ProvisionMachineOutput {
  string machine_id = 1;
  string host_id = 2;
}

message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"

    input: ".machines.v1.ProvisionMachineInput"
    output: ".machines.v1.ProvisionMachineOutput"

    steps: ".machines.v1.Validate"
    steps: ".machines.v1.SelectHost"
    steps: ".machines.v1.ReserveCapacity"
    steps: ".machines.v1.CreateMachine"
  };
}
```

Pipeline output is distinct from:

- pipeline input,
- step state,
- execution `Result`.

---

# 27. Pipeline output meaning

Pipeline output represents:

> The typed business value produced by successful completion of the entire pipeline.

It MUST NOT implicitly mean:

```text
state of final step
```

and MUST NOT use a rolling response accumulator.

---

# 28. Pipeline output projection

Pipeline output SHOULD be derived from already-durable run data:

```text
Pipeline Input
+
committed Step States
```

The projector MUST be:

- deterministic with respect to supplied durable state,
- side-effect free,
- non-blocking on external systems,
- non-failing.

If producing the output requires:

- retries,
- external I/O,
- side effects,
- failure semantics,

then that work belongs in a pipeline step instead.

---

# 29. Generated completion view

For a pipeline with output, code generation SHOULD produce a typed completion view.

Example:

```go
type ProvisionMachineCompletion struct {
    // generated
}
```

It may expose:

```go
func (c ProvisionMachineCompletion) Input() *ProvisionMachineInput

func (c ProvisionMachineCompletion) SelectHost() *SelectHost

func (c ProvisionMachineCompletion) ReserveCapacity() *ReserveCapacity

func (c ProvisionMachineCompletion) CreateMachine() *CreateMachine
```

Because completion projection only runs after successful forward execution, all declared state-producing predecessor states are guaranteed to exist.

---

# 30. Output projector API

A generated function type SHOULD be used.

Conceptually:

```go
type ProvisionMachineOutputFunc func(
    ProvisionMachineCompletion,
) *ProvisionMachineOutput
```

Usage:

```go
machines.NewProvisionMachine(
    &validate{},
    &selectHost{},
    &reserveCapacity{},
    &createMachine{},

    func(c machines.ProvisionMachineCompletion) *machines.ProvisionMachineOutput {
        return &machines.ProvisionMachineOutput{
            MachineId: c.CreateMachine().MachineId,
            HostId:    c.SelectHost().HostId,
        }
    },
)
```

The projector is pipeline configuration, not a runtime step.

---

# 31. Pipeline output durability

Pipeline output is generated only after every forward step has completed successfully.

Once committed, it is immutable.

A failed run has no pipeline output.

A successful run declaring output MUST have exactly one committed pipeline output.

If the process crashes after all steps succeed but before output is committed, recovery MAY safely execute the pure output projector again.

---

# 32. Pipeline output versus execution Result

These are distinct concepts.

`Result` answers:

> What happened to execution?

Pipeline output answers:

> What business value did successful execution produce?

Example execution result:

```text
OutcomeSuccess
```

Pipeline output:

```text
machine_id = machine-123
host_id = host-42
```

Failure example:

```text
OutcomeFailure

RootFailure:
    CreateMachine

UnwindFailures:
    ReserveCapacity
```

with no pipeline output.

---

# 33. Typed Run for output pipelines

A generic `durable.Run` cannot preserve pipeline-output type information.

Therefore a pipeline declaring output SHOULD receive a generated typed Run handle.

Example:

```go
run, created, err := provision.Schedule(...)
```

where the static type is:

```go
machines.ProvisionMachineRun
```

rather than plain:

```go
durable.Run
```

---

# 34. Typed Result for output pipelines

A generated typed run returns a generated typed result.

Conceptually:

```go
type ProvisionMachineResult struct {
    durable.Result
}

func (r ProvisionMachineResult) Output() *ProvisionMachineOutput
```

Usage:

```go
result, err := run.Wait(ctx)
if err != nil {
    return err
}

if result.Failed() {
    return fmt.Errorf(
        "provision failed: %v",
        result.RootFailure(),
    )
}

machine := result.Output()

fmt.Println(machine.MachineId)
```

No cast or protobuf reflection is required.

---

# 35. Pipelines without output

A pipeline without declared output does not need generated typed Run or Result wrappers merely for consistency.

It MAY continue to use:

```go
durable.Run
durable.Result
```

The generated API SHOULD remain minimal unless a concrete pipeline feature requires additional typing.

---

# 36. Pipeline definition

Generated code provides an unbound typed definition.

Example:

```go
definition := machines.NewProvisionMachine(
    &validate{},
    &selectHost{},
    &reserveCapacity{},
    &createMachine{},
    outputFunc,
)
```

The definition knows:

- `PipelineID`,
- input type,
- output type if any,
- immutable topology,
- handlers,
- step state capabilities,
- unwind capabilities,
- output projector if any.

---

# 37. Bind

A generated pipeline definition is associated with an Engine via:

```go
provision, err := machines.NewProvisionMachine(
    ...
).Bind(engine)
```

`Bind` means:

> Bind this pipeline definition to this Engine.

It:

1. registers the pipeline definition,
2. registers its active handlers,
3. registers generated adapters,
4. returns an Engine-bound pipeline handle.

`Bind` is valid only before `Engine.Start`.

---

# 38. Pipeline handle

A bound pipeline is resource-oriented.

Operations include:

```go
Schedule
Active
Runs
```

A subsystem SHOULD be able to depend only on the pipeline handles it needs, without receiving the whole Engine.

Example:

```go
type MachineService struct {
    provision *machines.ProvisionMachine
}
```

---

# 39. Schedule

Pipeline without input:

```go
run, created, err := pipeline.Schedule(
    ctx,
    resourceID,
)
```

Pipeline with input:

```go
run, created, err := provision.Schedule(
    ctx,
    resourceID,
    input,
)
```

`created` means:

```text
true
    this call created a new run

false
    an equivalent active run already existed
```

`Schedule` is only valid after `Engine.Start`.

---

# 40. Schedule acceptance

`Schedule` returns after the run has been durably accepted.

It does not wait for:

- worker availability,
- first handler invocation,
- first step completion,
- pipeline completion.

Conceptually:

```text
Schedule
    |
    v
create or resolve active run
    |
    v
durably accept
    |
    +---- return
    |
    v
Engine executes asynchronously
```

---

# 41. Schedule context lifetime

The context passed to `Schedule` controls only the scheduling request.

Once the run has been accepted:

```text
caller context cancellation
    !=
run cancellation
```

The run belongs to the Engine.

---

# 42. Active lookup

Pipeline handle:

```go
run, ok, err := provision.Active(
    ctx,
    machineID,
)
```

`ok` means an active nonterminal run currently occupies the slot.

This is a lookup, so `ok` is appropriate.

---

# 43. Historical runs

Pipeline handle:

```go
runs, err := provision.Runs(
    ctx,
    machineID,
)
```

returns Run handles, not just IDs.

Therefore the method is called `Runs`, not `RunIDs`.

Callers may obtain:

```go
run.ID()
```

when only the portable identifier is needed.

---

# 44. Run handle

A `durable.Run` is an Engine-bound convenience handle.

Conceptually:

```go
type Run struct {
    id     RunID
    engine *Engine
}
```

It is not the persisted run representation.

Core methods:

```go
func (r Run) ID() RunID

func (r Run) Wait(
    context.Context,
) (Result, error)

func (r Run) Status(
    context.Context,
) (Status, error)
```

Typed generated Run variants MAY override `Wait` with typed result access where pipeline output requires it.

---

# 45. Engine-level Run operations

The Engine MAY expose ID-oriented equivalents:

```go
result, err := engine.Wait(ctx, runID)

status, err := engine.Status(ctx, runID)

run := engine.Run(runID)
```

This supports callers that possess only a persisted `RunID`.

---

# 46. Wait semantics

`Wait` separates execution failure from operational waiting failure.

```go
result, err := run.Wait(ctx)
```

`err != nil` represents things such as:

- caller wait context canceled,
- lookup error,
- Engine/query failure.

Pipeline failure itself is represented by `Result`.

---

# 47. Result

Conceptually:

```go
type Result struct {
    Outcome Outcome

    RootFailure *RootFailure

    UnwindFailures []UnwindFailure
}
```

Successful run:

```text
OutcomeSuccess
RootFailure = nil
UnwindFailures = []
```

Failed run with successful cleanup:

```text
OutcomeFailure
RootFailure != nil
UnwindFailures = []
```

Failed run with incomplete cleanup:

```text
OutcomeFailure
RootFailure != nil
UnwindFailures != []
```

---

# 48. Engine lifecycle

The Engine has two main lifecycle states:

```text
configuring
    |
    | Start()
    v
running
```

During `configuring`:

- pipelines may bind,
- historical handlers may register,
- scheduling is rejected.

During `running`:

- registration is frozen,
- recovery is active,
- scheduling is accepted,
- further binding is rejected.

---

# 49. Engine.Start

`Engine.Start(ctx)` owns recovery.

Application code MUST NOT manually resume individual pipelines.

Startup SHOULD:

1. freeze registration,
2. discover all nonterminal runs,
3. validate required active and historical handlers,
4. validate historical step-state schemas where necessary,
5. reconstruct retry wakeups,
6. enqueue immediately runnable runs,
7. register future retry wakeups,
8. begin normal scheduling operation.

Startup SHOULD fail loudly if a recoverable run cannot be interpreted by the registered implementation set.

---

# 50. Engine-owned execution lifetime

Handler contexts derive from Engine lifetime, not from the original scheduling context.

Conceptually:

```text
Engine lifetime
    |
    +-> Run attempt 1
    |
    +-> Run attempt 2
    |
    +-> Unwind attempt 1
```

Each attempt receives a fresh `context.Context`.

---

# 51. Execution concurrency

Exactly one logical operation belonging to a run may execute at a time.

Different runs MAY execute concurrently.

Example:

```text
run-1 -> ReserveCapacity.Run

run-2 -> ConfigureNetwork.Unwind

run-3 -> Validate.Run
```

The Engine MUST enforce a global concurrency limit.

Conceptually:

```go
engine := durable.New(
    ...,
    durable.WithConcurrency(32),
)
```

The default remains implementation-defined.

---

# 52. Successful execution scheduling

Once a worker owns a run, immediately successful operations MAY continue synchronously through subsequent steps.

Example:

```text
A.Run succeeds
    -> B.Run

B.Run succeeds
    -> C.Run
```

Worker ownership is released when:

```text
run becomes terminal

operation requires retry

Engine begins shutdown
```

This avoids needless scheduler overhead for short linear pipelines.

---

# 53. Retryable failures

Workers MUST NOT sleep while holding execution capacity.

If an operation returns a retryable error:

```text
record failed attempt

compute next retry time

mark operation waiting-for-retry

release concurrency slot

schedule wakeup
```

---

# 54. Retry policy

The Engine SHOULD support an Engine-wide retry policy.

Conceptually:

```go
durable.WithRetryPolicy(
    durable.RetryPolicy{
        Initial:    100 * time.Millisecond,
        Max:        30 * time.Second,
        Multiplier: 2,
    },
)
```

Retries SHOULD use jitter.

Retries are unlimited by default.

The Engine MUST NOT infer permanent failure from retry count.

Only:

```go
durable.Fail(err)
```

permanently resolves a failing operation.

Per-step retry policy overrides are future work.

---

# 55. Attempt counters

Attempt counters belong to one logical operation.

Example:

```text
A.Run
    attempt 1 -> retry
    attempt 2 -> success

B.Run
    attempt 1
```

Forward and unwind operations use independent attempt sequences.

The first actual handler invocation has:

```go
inv.Attempt() == 1
```

An attempt interrupted by Engine shutdown still counts as an invocation.

---

# 56. Retry wakeup durability

The next eligible retry time MUST survive restart.

Example:

```text
attempt fails at 10:00:00
next retry = 10:00:30

process restarts at 10:00:05
```

The Engine SHOULD retain:

```text
next retry = 10:00:30
```

rather than immediately retrying.

This prevents restart-induced retry storms.

---

# 57. Recovery of retrying runs

On startup:

```text
next_retry_at <= now
    -> runnable immediately

next_retry_at > now
    -> schedule future wakeup
```

Retry state is part of durable execution state.

---

# 58. Engine shutdown

Normal shutdown is not pipeline failure.

Shutdown SHOULD:

```text
stop accepting new execution work

stop starting new handler invocations

cancel currently executing handler contexts

leave unresolved runs nonterminal
```

A later process resumes those runs.

---

# 59. Shutdown interruption semantics

If an Engine-owned context cancellation interrupts a handler:

```text
handler invocation happened

attempt counter remains incremented

operation remains unresolved

no normal retry backoff is applied merely because of shutdown
```

The interruption is not:

```text
application retryable failure
```

and not:

```text
permanent failure
```

---

# 60. Panics

The Engine SHOULD recover panics from application handlers.

A recovered panic SHOULD:

- capture diagnostic information,
- capture stack trace,
- mark the invocation unsuccessful,
- be treated as retryable by default.

A panic is not equivalent to:

```go
durable.Fail(err)
```

This allows a bug to be fixed in a later binary and the run to resume.

---

# 61. Immutable pipeline topology

Each new run receives the fully materialized pipeline topology active when it was scheduled.

Old definition:

```text
A -> B -> D
```

New definition:

```text
A -> B -> C -> D
```

Existing runs continue:

```text
A -> B -> D
```

New runs execute:

```text
A -> B -> C -> D
```

No implicit migration occurs.

---

# 62. Adding steps

Adding an intermediary step requires no migration.

Existing runs retain their immutable definitions.

New runs use the new topology.

---

# 63. Removing steps

Removing a step from new topology does not erase its historical identity.

Old:

```text
A -> B -> C
```

New:

```text
A -> C
```

Historical runs may still need to:

- execute `B`,
- retry `B`,
- read `B` state,
- unwind `B`.

Topology removal and implementation retirement are separate operations.

---

# 64. Tombstones

A historical step may remain declared as a tombstone.

A tombstone indicates:

> This `StepID` is historically valid but is no longer used by current pipeline topology.

The persisted run definition remains authoritative for historical ordering.

---

# 65. Forward tombstone safety

A tombstoned step may be skipped only if its forward operation has never been invoked.

If:

```text
attempts = 0
```

the Engine may resolve it as skipped.

If:

```text
attempts > 0
```

the Engine MUST NOT silently skip it.

A previous invocation may have produced an external side effect whose outcome remains uncertain.

---

# 66. Tombstone unwind semantics

A historical step originally declared with:

```proto
unwind: false
```

requires no historical unwind handler.

A historical step declared with:

```proto
unwind: true
```

continues to require its legacy handler unless an explicit tombstone policy declares historical unwind unnecessary.

If its unwind handler needs step state, the historical protobuf state schema MUST remain available.

---

# 67. Protobuf evolution

Compatible protobuf field evolution is allowed.

Example:

```proto
message ReserveCapacity {
  string reservation_id = 1;

  // added later
  string host_id = 2;
}
```

Compatible wire evolution does not by itself require a new `StepID`.

Incompatible semantic evolution does.

---

# 68. Code generation

`protoc-gen-durable` is responsible for generating:

- typed step handler interfaces,
- typed invocation types,
- typed state references/accessors,
- pipeline definition constructors,
- Engine binding adapters,
- bound pipeline handles,
- typed completion views,
- typed pipeline output projectors,
- typed Run/Result wrappers when output requires them.

---

# 69. Generation-time validation

`protoc-gen-durable` MUST reject:

- missing `PipelineID`,
- missing `StepID`,
- duplicate `StepID`,
- nonexistent referenced protobuf messages,
- pipeline members without `durable.step`,
- empty pipelines,
- invalid input types,
- invalid output types,
- malformed tombstones,
- tombstoned steps referenced by current topology.

Generated Go APIs SHOULD move these errors to compile time where possible:

- missing handler,
- wrong handler type,
- handler in wrong position,
- invalid `Run` signature,
- missing required `Unwind`,
- invalid typed state access,
- invalid output projector signature.

---

# 70. Internal type erasure

Generated strongly typed APIs exist at the application boundary.

Internally, generated adapters erase them into runtime concepts such as:

```text
PipelineID
ResourceID
RunID
StepID

serialized Input
serialized Step State
serialized Pipeline Output

internal handler
raw invocation metadata
```

The core Engine SHOULD remain non-generic.

---

# 71. Buf

`durable` SHOULD use Buf for protobuf development and CI.

Expected operations:

```text
buf lint
buf breaking
buf generate
```

Buf is a build-time tool.

The runtime library MUST NOT depend on Buf.

Use of the Buf Schema Registry is optional.

---

# 72. Durable compatibility

Protobuf wire compatibility alone is insufficient.

Durable breaking changes include:

```text
changing StepID semantics in place

removing a StepID still referenced by runs

removing a state schema still required by historical execution

changing unwind requirements without an explicit retirement path

changing pipeline input or output semantics incompatibly
```

A future durable-specific compatibility checker SHOULD compare current declarations against previous descriptors.

---

# 73. Preferred public API

Core package:

```go
package durable

type Engine
type Run
type Result
type Status
type Failure

type PipelineID string
type ResourceID string
type RunID string
type StepID string

func New(...Option) *Engine

func Fail(error) error
```

Engine:

```go
func (e *Engine) Start(context.Context) error

func (e *Engine) Wait(
    context.Context,
    RunID,
) (Result, error)

func (e *Engine) Status(
    context.Context,
    RunID,
) (Status, error)

func (e *Engine) Run(RunID) Run
```

Engine options MAY include:

```go
durable.WithConcurrency(n)

durable.WithRetryPolicy(policy)
```

Pipeline without input:

```go
func (p *SomePipeline) Schedule(
    context.Context,
    ResourceID,
) (Run, bool, error)
```

Pipeline with input:

```go
func (p *ProvisionMachine) Schedule(
    context.Context,
    ResourceID,
    *ProvisionMachineInput,
) (ProvisionMachineRun, bool, error)
```

Pipeline resource queries:

```go
func (p *Pipeline) Active(
    context.Context,
    ResourceID,
) (Run, bool, error)

func (p *Pipeline) Runs(
    context.Context,
    ResourceID,
) ([]Run, error)
```

Generated definition:

```go
func NewProvisionMachine(
    ValidateHandler,
    SelectHostHandler,
    ReserveCapacityHandler,
    CreateMachineHandler,
    ProvisionMachineOutputFunc,
) *ProvisionMachineDefinition

func (d *ProvisionMachineDefinition) Bind(
    *durable.Engine,
) (*ProvisionMachine, error)
```

---

# 74. Example

Proto:

```proto
message ProvisionMachineInput {
  string region = 1;
  uint64 memory_mb = 2;
}

message Validate {
  option (durable.v1.step) = {
    id: "validate/v1"
  };
}

message SelectHost {
  option (durable.v1.step) = {
    id: "select-host/v1"
  };

  string host_id = 1;
}

message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
  };

  string reservation_id = 1;
}

message CreateMachine {
  option (durable.v1.step) = {
    id: "create-machine/v1"
    unwind: true
  };

  string machine_id = 1;
}

message ProvisionMachineOutput {
  string machine_id = 1;
  string host_id = 2;
}

message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"

    input: ".machines.v1.ProvisionMachineInput"
    output: ".machines.v1.ProvisionMachineOutput"

    steps: ".machines.v1.Validate"
    steps: ".machines.v1.SelectHost"
    steps: ".machines.v1.ReserveCapacity"
    steps: ".machines.v1.CreateMachine"
  };
}
```

Application:

```go
engine := durable.New(
    durable.WithConcurrency(32),
)

provision, err := machines.NewProvisionMachine(
    &validate{},
    &selectHost{},
    &reserveCapacity{},
    &createMachine{},

    func(c machines.ProvisionMachineCompletion) *machines.ProvisionMachineOutput {
        return &machines.ProvisionMachineOutput{
            MachineId: c.CreateMachine().MachineId,
            HostId:    c.SelectHost().HostId,
        }
    },
).Bind(engine)
if err != nil {
    return err
}

if err := engine.Start(ctx); err != nil {
    return err
}

run, created, err := provision.Schedule(
    ctx,
    durable.ResourceID(machineID),
    &machines.ProvisionMachineInput{
        Region:   "ord",
        MemoryMb: 8192,
    },
)
if err != nil {
    return err
}

if !created {
    log.Printf("using existing run %s", run.ID())
}

result, err := run.Wait(ctx)
if err != nil {
    return err
}

if result.Failed() {
    return fmt.Errorf(
        "provision failed: %v",
        result.RootFailure(),
    )
}

output := result.Output()

log.Printf(
    "machine %s provisioned on %s",
    output.MachineId,
    output.HostId,
)
```

---

# 75. Core invariants

The implementation MUST maintain these invariants.

1. A run's pipeline topology never changes after creation.

2. A run's pipeline input never changes after creation.

3. A committed step state never changes.

4. A committed pipeline output never changes.

5. A stable `StepID` identifies durable step semantics.

6. A `RunID` uniquely identifies one exact execution.

7. At most one nonterminal run exists for `(PipelineID, ResourceID)`.

8. Forward and unwind operations execute at least once.

9. All handlers must therefore be idempotent.

10. Step capabilities are statically declared in protobuf.

11. Generated Go interfaces expose exactly those capabilities.

12. A step without fields establishes no durable step state.

13. A step with fields establishes state only after successful forward completion.

14. Step state and forward success are one logical commit.

15. Failed attempts never establish durable step state.

16. Steps with `unwind=false` require no backward handler.

17. Steps with `unwind=true` eventually resolve by successful unwind or permanent unwind failure.

18. `nil` means operation success.

19. Ordinary error means retry.

20. `durable.Fail(err)` means permanent operation failure.

21. Permanent forward failure establishes the root failure and begins unwind.

22. Permanent unwind failure is recorded and unwind continues.

23. Unwind follows actual successful execution history.

24. Attempted incomplete historical steps may never be silently skipped.

25. Scheduling an equivalent active intent returns the existing run.

26. Scheduling different input into an occupied resource slot returns a conflict.

27. Engine startup owns recovery.

28. `Bind` is valid only before startup.

29. `Schedule` is valid only after startup.

30. Exactly one operation for a run executes at once.

31. Different runs may execute concurrently.

32. Engine concurrency is globally bounded.

33. Immediately successful steps may continue without scheduler round trips.

34. Retryable failure releases execution capacity.

35. Retry wakeup time survives process restart.

36. Retry counters are scoped per logical operation.

37. Engine shutdown interruption is not semantic pipeline failure.

38. Panics are retryable by default.

39. Pipeline handles expose resource-oriented operations.

40. Run handles expose exact-execution-oriented operations.

41. `RunID` remains the portable identity behind a Run handle.

42. Pipeline output exists only for successful runs.

43. Pipeline output is derived only from committed immutable run data.

44. Pipeline output projection is not a durable step and performs no side effects.

45. Pipeline output is distinct from execution `Result`.

---

# 76. Future work

The following areas are intentionally left outside Draft 0.6 and should be specified separately before implementation is considered complete.

## 76.1 Store contract

Define the exact persistence abstraction required by the Engine.

Topics include:

- semantic store operations,
- transactional boundaries,
- SQLite implementation,
- bbolt implementation,
- run discovery,
- active-slot uniqueness,
- wait notification,
- historical retention.

The Store API should be designed from the state-transition requirements rather than exposing raw SQL or key/value primitives.

---

## 76.2 Persistent state and event representation

Define the exact durable representation of:

- run creation,
- immutable topology,
- pipeline input,
- step attempts,
- step states,
- retry wakeups,
- root failures,
- unwind failures,
- tombstones,
- pipeline output,
- terminal outcome.

An explicit decision remains between:

```text
current-state persistence

event journal

hybrid current-state + history
```

The execution semantics in this spec do not depend on that decision.

---

## 76.3 Cancellation semantics

Define whether application code can intentionally cancel an accepted Run.

Questions include:

- whether cancellation triggers unwind,
- whether cancellation is represented as a root failure,
- whether cancellation is permanent,
- how cancellation interacts with retry waits,
- how caller-requested cancellation differs from Engine shutdown,
- whether cancellation is addressed by `Run` or `RunID`.

Engine shutdown cancellation is already specified as non-semantic interruption and MUST remain distinct.

---

## 76.4 Observability

Define first-class observability support for:

- structured logging,
- OpenTelemetry tracing,
- metrics,
- run correlation,
- step attempt correlation,
- retry timing,
- unwind failures,
- scheduler saturation,
- pipeline duration,
- step duration,
- historical inspection.

The library should avoid requiring a specific logging framework.

---

## 76.5 Historical inspection

Potential APIs include:

```go
run.History(ctx)

run.Attempts(ctx)

run.State(ctx)
```

The terminology should distinguish:

```text
Step State
```

from:

```text
execution/event history
```

---

## 76.6 Per-step retry policy

The initial model uses Engine-wide retry defaults.

Future declarations may support step-specific policies such as:

```proto
retry: {
  initial: "1s"
  max: "1m"
}
```

This should not alter the fundamental rule that permanent failure remains explicit through:

```go
durable.Fail(err)
```

---

## 76.7 Scheduler fairness and admission control

The initial scheduler has one Engine-wide concurrency bound.

Possible future work includes:

- per-pipeline concurrency limits,
- priorities,
- resource classes,
- admission queues,
- fairness policies.

These should not change per-run serialization semantics.

---

## 76.8 Delayed and dependent scheduling

Potential future scheduling primitives include:

```text
not before time T

run after RunID X completes
```

Dependencies should reference exact `RunID` values rather than ambiguous resource identities.

---

## 76.9 Durable compatibility tooling

A future tool should compare old and new protobuf descriptors and detect durable semantic incompatibilities beyond ordinary protobuf breaking-change rules.

Potential checks include:

- StepID removed without tombstone,
- StepID changed in place,
- required historical state schema removed,
- unwind capability changed unsafely,
- pipeline input/output changed incompatibly.

---

## 76.10 Pipeline-level business output extensions

The initial output projector is pure, synchronous, deterministic, and non-failing.

Any future extension that introduces:

- I/O,
- retries,
- failure,
- side effects

should almost certainly be modeled as another step instead of expanding output projection semantics.

---

# 77. Summary

`durable` deliberately models a narrow but useful execution abstraction:

```text
immutable typed input

        |
        v

Step A
        |
        | immutable state
        v

Step B
        |
        | immutable state
        v

Step C
        |
        v

pure output projection
        |
        v

immutable typed output
```

with failure behavior:

```text
ordinary error
    -> retry

durable.Fail(err)
    -> permanently resolve operation
```

and:

```text
forward permanent failure
    -> reverse unwind

unwind permanent failure
    -> record and continue backward
```

The runtime model is:

```text
Engine
    owns lifecycle, concurrency, retry, and recovery

Pipeline
    owns resource-scoped scheduling

Run
    owns exact-execution interaction

Pipeline Input
    immutable execution intent

Step State
    immutable state established by successful steps

Pipeline Output
    immutable typed business result of successful completion

Result
    immutable execution outcome and failure information
```

The intent of `durable` is not to become a general workflow engine.

Its value comes from keeping the model constrained enough that crash recovery, evolution, retry behavior, and unwind semantics remain explicit, understandable, testable, and idiomatic in Go.