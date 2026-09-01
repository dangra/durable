# `durable`: Durable Linear Pipelines with Unwind Semantics

**Status:** Draft 0.7  
**Target:** Go 1.27+  
**Persistence:** Local transactional database such as SQLite or bbolt  
**Schema and code generation:** Protocol Buffers, Buf, and `protoc-gen-durable`

# 1. Overview

`durable` is a Go library for executing fixed, linear pipelines whose progress survives process crashes and restarts.

A pipeline consists of an ordered sequence of steps:

```text
A -> B -> C -> D
```

Each executed operation uses **at-least-once semantics** and therefore its handler MUST be idempotent.

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

Only steps declaring unwind behavior execute an `Unwind` handler.

Successfully completed steps without unwind behavior require no backward action.

An unwind operation may itself fail permanently. That failure is recorded and unwind continues with the previous successfully completed step.

Every run may contain:

- an immutable materialized pipeline definition,
- an optional immutable typed pipeline input,
- zero or more immutable per-step durable states,
- an optional typed pipeline output represented by the final step state,
- durable failure and retry state,
- one globally unique execution identity.

---

# 2. Scope

`durable` deliberately models a constrained execution system.

It supports:

```text
fixed linear topology
+
durable execution position
+
automatic retry
+
explicit permanent failure
+
reverse unwind
+
typed immutable run data
```

It does not attempt to become a general workflow engine.

Initial non-goals include:

- distributed execution,
- distributed consensus,
- remote workers,
- distributed queues,
- parallel branches,
- DAG execution,
- dynamic topology,
- loops,
- child pipelines,
- deterministic program replay,
- exactly-once external side effects,
- arbitrary in-flight workflow migration,
- BPMN semantics,
- Petri-net semantics,
- distributed transactions.

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
    which durable step semantics?
```

Go types:

```go
type PipelineID string
type ResourceID string
type RunID string
type StepID string
```

They MUST be distinct defined types rather than aliases.

---

# 4. Pipeline resource slot

A pipeline operates on a logical resource.

The scheduling slot is:

```text
(PipelineID, ResourceID)
```

At most one nonterminal run may occupy a slot at once.

For example:

```text
provision-machine / machine-123
```

may have only one active run.

Another pipeline may independently operate on the same resource:

```text
provision-machine / machine-123
update-inventory  / machine-123
```

Cross-pipeline exclusion is outside the initial scope.

---

# 5. Run

A **Run** is one exact immutable execution of a pipeline for a resource.

Example:

```text
PipelineID = provision-machine
ResourceID = machine-123
RunID      = 01K...
```

A resource slot may accumulate historical runs:

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

`RunID` identifies one exact execution.

It is not:

- the resource identity,
- a pipeline revision,
- a schema version.

A `RunID` is suitable for:

- persistence,
- logging,
- API boundaries,
- correlation,
- historical lookup,
- waiting after process restart.

---

# 7. StepID

A `StepID` identifies durable step semantics.

Example:

```text
reserve-capacity/v1
reserve-capacity/v2
```

Compatible protobuf schema evolution does not necessarily require a new `StepID`.

An incompatible semantic change MUST introduce a new `StepID`.

`StepID` MUST NOT depend on:

- protobuf message names,
- Go package names,
- generated Go type names,
- display names.

---

# 8. Protocol Buffer declarations

`durable` uses protobuf custom options.

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
  repeated string steps = 3;
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
  StepOptions step = <globally-allocated-extension-number>;
  PipelineOptions pipeline = <globally-allocated-extension-number>;
}
```

Published extension numbers MUST be allocated from the protobuf global extension registry.

Hard-coded numbers in protobuf's organizational/internal-use range MUST NOT be published as the public `durable` contract.

---

# 9. Pipeline input

A pipeline MAY declare a protobuf input type.

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

The input represents immutable execution intent.

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

Once the run is accepted, its input MUST NOT change.

A resumed run MUST observe exactly the same input.

---

# 10. Input compatibility

The pipeline input type is part of the public contract identified by `PipelineID`.

Compatible protobuf evolution is allowed.

An incompatible change to the pipeline input contract SHOULD require a new `PipelineID`.

---

# 11. Duplicate scheduling

If no nonterminal run occupies `(PipelineID, ResourceID)`:

```text
create new run
created = true
```

If a nonterminal run exists with equivalent input:

```text
return existing run
created = false
```

If a nonterminal run exists with different input:

```text
return scheduling conflict
```

For protobuf inputs, equality MUST use protobuf semantic equality.

The library MUST NOT silently replace or ignore conflicting intent.

---

# 12. ScheduleConflictError

A scheduling conflict SHOULD be represented by a typed error.

Conceptually:

```go
type ScheduleConflictError struct {
    RunID RunID
}

func (e *ScheduleConflictError) Error() string
```

The error identifies the currently occupying run.

For a conflict:

```text
run     = zero value
created = false
err     = *ScheduleConflictError
```

The caller may use the included `RunID` to inspect the existing run.

---

# 13. Step declaration

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

A step declaration has two independent capabilities:

```text
protobuf fields present
    -> successful Run establishes durable Step State

unwind = true
    -> successful step requires an Unwind operation after later failure
```

---

# 14. Step State

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

A successful handler may return:

```go
return &machines.ReserveCapacity{
    ReservationId: reservation.ID,
    HostId:        reservation.HostID,
}, nil
```

That value becomes the committed Step State for `reserve-capacity/v1` within the run.

---

# 15. Step capability matrix

Generated Go interfaces MUST expose only capabilities declared by the step.

## 15.1 No state, no unwind

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
        ctx context.Context,
        inv ValidateInvocation,
    ) error
}
```

## 15.2 State, no unwind

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
        ctx context.Context,
        inv SelectHostInvocation,
    ) (*SelectHost, error)
}
```

## 15.3 No state, with unwind

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
        ctx context.Context,
        inv MarkProvisioningInvocation,
    ) error

    Unwind(
        ctx context.Context,
        inv MarkProvisioningInvocation,
        failure durable.Failure,
    ) error
}
```

## 15.4 State and unwind

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
        ctx context.Context,
        inv ReserveCapacityInvocation,
    ) (*ReserveCapacity, error)

    Unwind(
        ctx context.Context,
        inv ReserveCapacityInvocation,
        state *ReserveCapacity,
        failure durable.Failure,
    ) error
}
```

The `state` parameter is the immutable Step State committed by the successful forward execution.

---

# 16. Generated API principle

The protobuf declaration is authoritative.

Generated code MUST expose exactly the declared capabilities.

The framework MUST NOT require:

- dummy protobuf state,
- empty protobuf results,
- synthetic no-op unwind handlers,
- marker methods solely for runtime dispatch.

---

# 17. Step State success boundary

For a state-producing step:

```text
Run -> (state, nil)

    persist state
    mark step successful
    advance
```

For retryable failure:

```text
Run -> (_, ordinary error)

    discard returned state
    retry
```

For permanent failure:

```text
Run -> (_, durable.Fail(err))

    discard returned state
    establish root failure
    begin unwind
```

Step State exists if and only if the forward step has durably succeeded.

Step success and Step State MUST be committed as one logical atomic transition.

---

# 18. Step State immutability

Once committed, Step State MUST NOT change.

The initial public API MUST NOT expose:

```go
SetState(...)
UpdateState(...)
MutateState(...)
```

Step State represents durable evidence of what a successful step established.

---

# 19. Explicit state ownership

State belongs to the step that established it.

For:

```text
A -> B -> C
```

the model is:

```text
A -> A state
B -> B state
C -> C state
```

not:

```text
global mutable PipelineState
```

---

# 20. Forward state visibility

During `Run`, an invocation may read:

```text
pipeline input
+
committed state of successful predecessor steps
```

A step MUST NOT read state from:

- a later step,
- itself before successful completion,
- a predecessor that never established state.

---

# 21. Unwind state visibility

During `Unwind`, an invocation may read:

```text
pipeline input
+
committed state of all successfully completed steps
+
current failure context
```

If the current step itself established state, that state is also passed explicitly as the named `state` argument.

---

# 22. Generated state access

Where state existence is statically guaranteed, generated accessors SHOULD return the state directly.

Example:

```go
reservation := inv.ReserveCapacityState()
```

with type:

```go
*machines.ReserveCapacity
```

No `ok` result is required where existence is guaranteed by topology and execution semantics.

An advanced dynamic lookup API MAY exist:

```go
state, ok := inv.State(machines.ReserveCapacityStep)
```

where `ok=false` may represent historical or dynamically absent state, including a skipped tombstoned step.

---

# 23. Operation result semantics

Handler returns have consistent meaning.

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

> The operation remains unresolved and should be retried.

## Permanent failure

```go
return durable.Fail(err)
```

means:

> The current operation must not be retried once that permanent result is durably committed.

The current phase determines the next state transition.

---

# 24. Forward permanent failure

For `Run`:

```text
durable.Fail(err)
    -> permanently resolve current forward operation as failed
    -> establish RootFailure
    -> enter unwind
```

The failing step itself is NOT considered successfully completed.

Therefore it is NOT included in unwind.

---

# 25. Failing-step cleanup responsibility

This is a fundamental semantic rule.

Suppose:

```text
A ✓ -> B ✓ -> C.Run
                  |
                  +-- creates partial side effect X
                  |
                  +-- returns durable.Fail(err)
```

Unwind is:

```text
B.Unwind
A.Unwind
```

NOT:

```text
C.Unwind
B.Unwind
A.Unwind
```

`C` never durably completed and therefore has no committed successful state to compensate.

Consequently:

> A `Run` handler MUST reconcile or clean up any partial effects created by its unsuccessful invocation before returning `durable.Fail`.

`Unwind` compensates previously committed successful steps. It is not cleanup for the currently failing partial step.

---

# 26. Unwind execution

For `Unwind`:

```text
nil
    -> mark unwind successful
    -> continue backward

ordinary error
    -> retry same unwind operation

durable.Fail(err)
    -> mark unwind permanently failed
    -> record UnwindFailure
    -> continue backward
```

A permanent unwind failure MUST NOT stop unwind.

---

# 27. Steps without unwind

A successful step with:

```proto
unwind: false
```

requires no backward handler.

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
B requires no action
    |
    v
A.Unwind()
```

No synthetic handler is invoked for `B`.

---

# 28. Unwind liveness

Every successfully completed step MUST eventually be resolved during unwind as exactly one of:

```text
no unwind required

successfully unwound

permanently failed to unwind
```

A retryable unwind failure does not resolve the step.

---

# 29. Phase

Pipeline phase is represented separately from scheduler state.

Conceptually:

```go
type Phase uint8

const (
    PhaseForward Phase = iota + 1
    PhaseUnwind
    PhaseDone
)
```

`Invocation.Phase()` returns this type.

---

# 30. Scheduler state

Scheduler state describes whether the run is currently eligible or executing.

Conceptually:

```go
type RunState uint8

const (
    RunStateRunnable RunState = iota + 1
    RunStateRunning
    RunStateWaitingRetry
    RunStateDone
)
```

Examples:

```text
PhaseForward + RunStateWaitingRetry

PhaseUnwind + RunStateRunning

PhaseDone + RunStateDone
```

---

# 31. Failure persistence model

Arbitrary Go `error` values are not part of the durable contract.

`durable.Fail(err)` is converted into a persistable failure representation.

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

The exact representation may gain optional structured details later.

Wrapped Go error identity is not guaranteed to survive restart.

---

# 32. Failure consistency across restart

An unwind handler SHOULD receive the same durable failure information regardless of whether unwind begins:

- in the same process that observed the failure,
- after process restart.

Therefore `Failure` represents persisted failure information rather than retaining an arbitrary in-memory Go error object.

---

# 33. Pipeline output

There is no separate output projector and no pipeline `output` option.

Instead:

> The committed Step State of the final step is the pipeline output.

If the final step has protobuf fields, the pipeline has typed output.

If the final step has no fields, the pipeline has no business output.

---

# 34. Pipeline with output

Example:

```proto
message CreateMachine {
  option (durable.v1.step) = {
    id: "create-machine/v1"
  };

  string machine_id = 1;
  string host_id = 2;
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

The pipeline output type is:

```go
*machines.CreateMachine
```

No additional output declaration is required.

---

# 35. Explicit result step

If the desired business output differs from the natural final operation, the developer SHOULD add an explicit final state-producing step.

Example:

```text
Validate
    ↓
SelectHost
    ↓
ReserveCapacity
    ↓
CreateMachine
    ↓
ProvisionResult
```

Proto:

```proto
message ProvisionResult {
  option (durable.v1.step) = {
    id: "provision-result/v1"
  };

  string machine_id = 1;
  string host_id = 2;
}
```

Its handler may derive state entirely from immutable input and predecessor states.

Because it is an ordinary step, it automatically inherits:

- retry semantics,
- StepID versioning,
- crash recovery,
- historical compatibility rules,
- state persistence semantics.

---

# 36. Final-step completion boundary

If the final step produces state:

```text
Final.Run -> (state, nil)
```

the logical durable transition establishes:

```text
final Step State
+
pipeline terminal success
```

A successful output-producing pipeline MUST therefore have final-step state.

A failed run has no pipeline output.

---

# 37. Final step and unwind

The final step MUST NOT declare:

```proto
unwind: true
```

because no later step can fail after the final step has successfully completed.

`protoc-gen-durable` SHOULD reject a pipeline whose final step declares unwind capability.

If the final step itself fails, it is not successfully completed and therefore is not unwound.

---

# 38. Pipeline output compatibility

For a stable `PipelineID`, the final step state type forms part of the pipeline's public output contract.

Compatible protobuf evolution is allowed.

An incompatible output-contract change SHOULD require a new `PipelineID`.

For example:

```text
A -> B -> ResultV1
```

changing incompatibly to:

```text
A -> B -> ResultV2
```

requires a new pipeline identity.

This ensures historical typed runs remain compatible with the generated pipeline API.

---

# 39. Typed output runs

If the final step has state, generated pipeline APIs SHOULD return a typed Run wrapper.

For example:

```go
run, created, err := provision.Schedule(...)
```

has static type:

```go
machines.ProvisionMachineRun
```

The typed wrapper embeds or internally references `durable.Run`.

---

# 40. Typed results

For an output-producing pipeline:

```go
result, err := run.Wait(ctx)
```

returns a generated typed result.

Conceptually:

```go
type ProvisionMachineResult struct {
    durable.Result
}

func (r ProvisionMachineResult) Output() *CreateMachine
```

On success:

```go
output := result.Output()
```

is statically typed.

On failure:

```text
Output() == nil
```

---

# 41. Typed lookup consistency

Output pipelines MUST preserve their typed Run wrapper across all run lookup paths.

Therefore generated methods SHOULD return:

```go
ProvisionMachineRun
```

from:

```go
Schedule(...)
Active(...)
```

and:

```go
[]ProvisionMachineRun
```

from:

```go
Runs(...)
```

A run obtained after restart must retain the same typed `Output()` access as one returned directly by `Schedule`.

---

# 42. Pipelines without output

If the final step has no state, the pipeline has no business output.

Generated methods MAY return plain:

```go
durable.Run
durable.Result
```

without unnecessary typed wrappers.

This follows the general rule:

> Generated APIs expose only capabilities that are actually present.

---

# 43. Generated type naming

Proto messages already occupy generated Go names.

For:

```proto
message ProvisionMachine { ... }
```

`protoc-gen-go` produces:

```go
machines.ProvisionMachine
```

Therefore `protoc-gen-durable` MUST use collision-free generated names.

Recommended names:

```text
ProvisionMachineDefinition
ProvisionMachinePipeline
ProvisionMachineRun
ProvisionMachineResult
```

The bound pipeline handle MUST NOT also be named `ProvisionMachine`.

---

# 44. Generated pipeline construction

Example:

```go
definition := machines.NewProvisionMachine(
    &validate{},
    &reserveCapacity{},
    &createMachine{},
)
```

The static return type is conceptually:

```go
*machines.ProvisionMachineDefinition
```

The definition contains:

- `PipelineID`,
- input type,
- ordered topology,
- active handlers,
- state capabilities,
- unwind capabilities,
- output type implied by the final step.

---

# 45. Bind

A generated definition binds to an Engine:

```go
provision, err := machines.NewProvisionMachine(
    &validate{},
    &reserveCapacity{},
    &createMachine{},
).Bind(engine)
```

The return type is:

```go
*machines.ProvisionMachinePipeline
```

`Bind` means:

> Bind this pipeline definition to this Engine.

It:

1. registers the pipeline definition,
2. registers its handlers,
3. registers generated runtime adapters,
4. returns an Engine-bound pipeline handle.

`Bind` is allowed only before `Engine.Start`.

---

# 46. Pipeline handle

A bound pipeline handle is resource-oriented.

It exposes operations such as:

```go
Schedule
Active
Runs
```

A subsystem SHOULD be able to depend on only the pipeline handles it needs.

Example:

```go
type MachineService struct {
    provision *machines.ProvisionMachinePipeline
}
```

The subsystem need not receive `*durable.Engine`.

---

# 47. Schedule

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
    this call created a new Run

false
    an equivalent nonterminal Run already existed
```

`Schedule` is valid only after `Engine.Start`.

---

# 48. Active

For an output-producing pipeline:

```go
run, ok, err := provision.Active(
    ctx,
    resourceID,
)
```

returns:

```go
machines.ProvisionMachineRun
```

For a pipeline without output it MAY return plain:

```go
durable.Run
```

`ok` indicates whether a nonterminal run occupies the resource slot.

---

# 49. Runs

For an output-producing pipeline:

```go
runs, err := provision.Runs(
    ctx,
    resourceID,
)
```

returns:

```go
[]machines.ProvisionMachineRun
```

For a pipeline without output it MAY return:

```go
[]durable.Run
```

The method is called `Runs`, not `RunIDs`, because it returns Run handles.

Identifiers remain available through:

```go
run.ID()
```

---

# 50. Run handle

A plain `durable.Run` is conceptually:

```go
type Run struct {
    id     RunID
    engine *Engine
}
```

It is an in-process convenience handle, not persisted run state.

Core operations:

```go
func (r Run) ID() RunID

func (r Run) Wait(
    ctx context.Context,
) (Result, error)

func (r Run) Status(
    ctx context.Context,
) (Status, error)
```

Generated typed runs wrap this behavior where necessary.

---

# 51. Engine-level run access

The Engine MAY expose:

```go
result, err := engine.Wait(ctx, runID)

status, err := engine.Status(ctx, runID)

run := engine.Run(runID)
```

These are useful when only the portable `RunID` is available.

---

# 52. Wait semantics

`Wait` separates operational waiting errors from pipeline execution outcome.

```go
result, err := run.Wait(ctx)
```

`err != nil` means an operational condition such as:

- wait context cancellation,
- lookup failure,
- Engine/query failure.

Pipeline failure is represented by `Result`, not by `Wait`'s error.

---

# 53. Result

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

Failed run:

```text
OutcomeFailure
RootFailure != nil
```

Cleanup may be complete:

```text
UnwindFailures = []
```

or incomplete:

```text
UnwindFailures != []
```

Pipeline business output remains separate from these execution semantics.

---

# 54. Status

`Status` SHOULD expose both pipeline and scheduler state.

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

    Outcome *Outcome
}
```

For states where no current step or next retry exists, their corresponding values may be zero.

The exact representation may use accessors rather than exported fields.

---

# 55. Engine lifecycle

An Engine has two main states:

```text
configuring
    |
    | Start()
    v
running
```

During configuration:

- pipelines may bind,
- historical step handlers may register,
- scheduling is rejected.

During running:

- registration is frozen,
- recovery is active,
- scheduling is accepted,
- further binding is rejected.

---

# 56. Single Engine ownership

Exactly one Engine instance may own and execute against a given store at a time.

This is an explicit v1 assumption.

The library does not provide multi-process coordination.

A storage backend SHOULD enforce or detect exclusive ownership where possible.

---

# 57. Engine.Start

`Engine.Start(ctx)` owns recovery.

Application code does not manually resume individual pipelines.

Startup SHOULD:

1. freeze registration,
2. discover every nonterminal run,
3. verify its pipeline definition is interpretable,
4. verify required active and historical handlers exist,
5. verify required historical protobuf state schemas exist,
6. reconstruct retry eligibility,
7. enqueue runnable runs,
8. schedule future retry wakeups,
9. begin normal scheduling.

---

# 58. Startup failure policy

Draft 0.7 uses fail-fast startup.

If any nonterminal run cannot be safely interpreted or resumed, `Engine.Start` fails.

This establishes the invariant:

> If the Engine is running, every nonterminal durable run is interpretable by the registered binary.

This favors correctness and operational visibility over partial availability.

Per-run recovery quarantine is future work.

---

# 59. Historical handlers

A step removed from current topology may still require a historical implementation.

Generated durable step descriptors SHOULD support explicit historical binding.

Conceptually:

```go
err := machines.ReserveCapacityStep.BindHistorical(
    engine,
    &reserveCapacityV1{},
)
```

Historical binding:

- does not add the step to current pipeline topology,
- makes its handler available to old runs,
- is valid only before `Engine.Start`.

The exact generated API may evolve, but historical-handler registration MUST be possible without restoring the step to current pipelines.

---

# 60. Engine execution lifetime

The context passed to `Schedule` governs only scheduling acceptance.

Once durably accepted:

```text
caller context cancellation
    !=
run cancellation
```

The run belongs to the Engine.

Each actual handler invocation receives a fresh context derived from Engine execution lifetime.

---

# 61. Execution concurrency

Exactly one logical operation belonging to a particular run may execute at once.

Different runs MAY execute concurrently.

Example:

```text
run-1 -> ReserveCapacity.Run

run-2 -> ConfigureNetwork.Unwind

run-3 -> Validate.Run
```

The Engine MUST enforce a global concurrency bound.

Conceptually:

```go
engine := durable.New(
    ...,
    durable.WithConcurrency(32),
)
```

---

# 62. Immediate continuation

Once a worker owns a run, successful operations MAY continue immediately through subsequent steps.

Example:

```text
A.Run succeeds
    -> B.Run

B.Run succeeds
    -> C.Run
```

Worker ownership is released when:

- the run reaches terminal state,
- the current operation requires retry,
- Engine shutdown begins.

---

# 63. Retryable failure scheduling

Workers MUST NOT sleep while holding execution capacity.

After a retryable error:

```text
record attempt failure

compute next retry eligibility

mark run waiting for retry

release execution capacity

schedule wakeup
```

---

# 64. Retry policy

The Engine SHOULD provide an Engine-wide retry policy.

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

Retries SHOULD include jitter.

Retries are unlimited by default.

The Engine MUST NOT infer permanent failure from retry count.

Permanent failure is always explicit:

```go
durable.Fail(err)
```

---

# 65. Attempt counters

Attempt counters belong to a logical operation.

Example:

```text
A.Run
    attempt 1 -> retry
    attempt 2 -> success

B.Run
    attempt 1
```

Forward and unwind attempts are distinct.

The first handler invocation has:

```go
inv.Attempt() == 1
```

An invocation interrupted by Engine shutdown still counts as an invocation.

---

# 66. Retry wakeup durability

Retry eligibility MUST survive process restart.

Example:

```text
attempt fails at 10:00:00
next retry = 10:00:30

process restarts at 10:00:05
```

The next retry remains approximately:

```text
10:00:30
```

rather than restarting immediately.

This prevents restart-induced retry storms.

---

# 67. Crash recovery backoff

A process may crash while a handler invocation is in progress, leaving an unresolved attempted operation.

On startup, such an operation SHOULD NOT necessarily execute immediately in a tight crash loop.

The Engine SHOULD apply recovery retry/backoff semantics to unresolved attempted operations so repeated process crashes cannot produce uncontrolled hot restart loops.

The exact recovery-backoff policy is part of scheduler implementation design.

---

# 68. Clock

The Engine SHOULD support an injectable clock for deterministic testing.

Conceptually:

```go
durable.WithClock(clock)
```

The clock abstraction is used for:

- retry eligibility,
- retry wakeups,
- timestamps,
- startup recovery timing.

The production default uses the system clock.

---

# 69. Engine shutdown

Normal shutdown is not pipeline failure.

Shutdown SHOULD:

```text
stop accepting new execution work

stop starting new handler invocations

cancel currently executing handler contexts

leave unresolved runs nonterminal
```

A future process resumes unresolved runs.

---

# 70. Shutdown interruption

If Engine shutdown interrupts an active invocation:

```text
the invocation occurred

attempt count remains incremented

the logical operation remains unresolved

shutdown itself does not create normal retry backoff

shutdown is not RootFailure
```

An Engine-owned `context.Canceled` MUST be distinguishable from an application-produced retryable error.

---

# 71. Panics

The Engine SHOULD recover panics from application handlers.

A recovered panic SHOULD:

- capture diagnostic information,
- capture the stack trace,
- mark the invocation unsuccessful,
- be treated as retryable by default.

A panic is not equivalent to:

```go
durable.Fail(err)
```

This allows a code bug to be fixed and the persisted run resumed by a later binary.

---

# 72. Immutable topology

Every run captures its exact materialized pipeline topology at scheduling time.

Old topology:

```text
A -> B -> D
```

new source topology:

```text
A -> B -> C -> D
```

Existing runs retain:

```text
A -> B -> D
```

New runs receive:

```text
A -> B -> C -> D
```

No implicit in-flight migration occurs.

---

# 73. Adding intermediary steps

Adding a step requires no migration.

Old runs keep their previous topology.

New runs receive the new topology.

---

# 74. Removing intermediary steps

Removing a step from current topology does not erase its historical durable identity.

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

Topology removal and implementation retirement are separate lifecycle operations.

---

# 75. Tombstones

A removed historical step MAY remain declared as a tombstone.

A tombstone means:

> This `StepID` is historically valid but is no longer used in current topology.

Historical run topology remains the source of truth for step ordering.

---

# 76. Tombstone forward skipping

A tombstoned step may be skipped only when its forward operation has never been invoked.

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

An earlier invocation may have produced an external side effect whose completion remains uncertain.

---

# 77. Tombstone unwind

A historical step originally declared with:

```proto
unwind: false
```

requires no historical unwind handler.

A historical step declared with:

```proto
unwind: true
```

continues to require its historical handler unless an explicit tombstone policy declares historical unwind unnecessary.

If historical unwind requires Step State, the corresponding protobuf schema must remain decodable.

---

# 78. At-least-once invariant

The at-least-once guarantee applies to operations resolved through handler execution.

Specifically:

> Any operation resolved by a handler success or permanent handler failure was invoked at least once and may have been invoked multiple times.

Operations explicitly resolved without handler execution, such as safe tombstone skipping or steps requiring no unwind action, are exempt.

---

# 79. Protobuf evolution

Compatible protobuf field evolution is allowed.

Example:

```proto
message ReserveCapacity {
  string reservation_id = 1;

  // Added compatibly later.
  string host_id = 2;
}
```

Compatible wire evolution does not alone require a new `StepID`.

Incompatible semantic evolution does.

The pipeline input and final-step output contracts additionally form part of `PipelineID` compatibility.

---

# 80. Code generation

`protoc-gen-durable` is responsible for generating:

- typed step handler interfaces,
- typed invocation types,
- typed state descriptors and accessors,
- pipeline definition constructors,
- bound pipeline handles,
- typed Run wrappers where final-step state requires them,
- typed Result wrappers where final-step state requires them,
- historical handler binding support,
- runtime adapters.

---

# 81. Generation-time validation

`protoc-gen-durable` MUST reject:

- missing `PipelineID`,
- missing `StepID`,
- duplicate stable `StepID`,
- nonexistent referenced protobuf messages,
- pipeline members without `durable.step`,
- empty pipelines,
- invalid input types,
- malformed tombstones,
- tombstoned steps referenced by current topology,
- final pipeline step declaring `unwind=true`.

Generated APIs SHOULD make these compile-time failures:

- missing handlers,
- wrong handler types,
- handler in wrong position,
- invalid `Run` signature,
- missing required `Unwind`,
- invalid typed state dependencies,
- invalid historical handler type.

---

# 82. Internal type erasure

Generated strongly typed APIs live at the application boundary.

Internally, adapters erase them into runtime concepts such as:

```text
PipelineID
ResourceID
RunID
StepID

serialized Pipeline Input
serialized Step States

Failure records
retry metadata

internal handler
raw invocation metadata
```

The core Engine SHOULD remain non-generic.

---

# 83. Buf

`durable` SHOULD use Buf for:

```text
buf lint
buf breaking
buf generate
```

Buf is build-time and CI tooling.

The runtime MUST NOT depend on Buf.

Use of the Buf Schema Registry is optional.

---

# 84. Durable compatibility

Ordinary protobuf wire compatibility is insufficient.

Durable breaking changes include:

- changing `StepID` semantics in place,
- removing a historical `StepID` still referenced by runs,
- removing a required historical state schema,
- changing unwind semantics without a retirement path,
- incompatibly changing pipeline input while retaining `PipelineID`,
- incompatibly changing final-step output contract while retaining `PipelineID`.

A future compatibility checker SHOULD compare current declarations against previous descriptors.

---

# 85. Minimal observability requirements

Full observability design remains future work, but unlimited retries MUST NOT be completely opaque.

At minimum:

- `Status` exposes current `Attempt`,
- `Status` exposes `NextAttemptAt`,
- failure records expose durable failure metadata,
- the Engine SHOULD provide some diagnostic hook or logging integration for repeated retry and panic conditions.

The library SHOULD NOT require a particular logging framework.

---

# 86. Preferred public API

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

type Phase uint8
type RunState uint8

func New(...Option) *Engine

func Fail(error) error
```

Engine:

```go
func (e *Engine) Start(
    context.Context,
) error

func (e *Engine) Wait(
    context.Context,
    RunID,
) (Result, error)

func (e *Engine) Status(
    context.Context,
    RunID,
) (Status, error)

func (e *Engine) Run(
    RunID,
) Run
```

Options MAY include:

```go
durable.WithConcurrency(n)

durable.WithRetryPolicy(policy)

durable.WithClock(clock)
```

---

# 87. Generated API example

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
  };

  string machine_id = 1;
  string host_id = 2;
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

Generated definition:

```go
func NewProvisionMachine(
    validate ValidateHandler,
    reserveCapacity ReserveCapacityHandler,
    createMachine CreateMachineHandler,
) *ProvisionMachineDefinition
```

Binding:

```go
func (d *ProvisionMachineDefinition) Bind(
    engine *durable.Engine,
) (*ProvisionMachinePipeline, error)
```

Bound output-producing pipeline:

```go
func (p *ProvisionMachinePipeline) Schedule(
    ctx context.Context,
    resourceID durable.ResourceID,
    input *ProvisionMachineInput,
) (ProvisionMachineRun, bool, error)

func (p *ProvisionMachinePipeline) Active(
    ctx context.Context,
    resourceID durable.ResourceID,
) (ProvisionMachineRun, bool, error)

func (p *ProvisionMachinePipeline) Runs(
    ctx context.Context,
    resourceID durable.ResourceID,
) ([]ProvisionMachineRun, error)
```

Typed Run:

```go
func (r ProvisionMachineRun) ID() durable.RunID

func (r ProvisionMachineRun) Status(
    context.Context,
) (durable.Status, error)

func (r ProvisionMachineRun) Wait(
    context.Context,
) (ProvisionMachineResult, error)
```

Typed result:

```go
type ProvisionMachineResult struct {
    durable.Result
}

func (r ProvisionMachineResult) Output() *CreateMachine
```

---

# 88. Typical application flow

```go
engine := durable.New(
    durable.WithConcurrency(32),
)

provision, err := machines.NewProvisionMachine(
    &validate{},
    &reserveCapacity{},
    &createMachine{},
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

machine := result.Output()

log.Printf(
    "machine %s provisioned on host %s",
    machine.MachineId,
    machine.HostId,
)
```

---

# 89. Core invariants

The implementation MUST maintain the following invariants.

1. A run's materialized pipeline topology never changes after creation.

2. A run's pipeline input never changes after creation.

3. A committed Step State never changes.

4. A stable `StepID` identifies durable step semantics.

5. A `RunID` uniquely identifies one exact execution.

6. At most one nonterminal run exists for `(PipelineID, ResourceID)`.

7. Operations resolved through handler execution use at-least-once semantics.

8. All handlers must therefore be idempotent.

9. Step capabilities are declared statically in protobuf.

10. Generated Go APIs expose exactly those capabilities.

11. A step without protobuf fields establishes no Step State.

12. A step with fields establishes Step State only after successful forward completion.

13. Step State and forward success are one logical durable transition.

14. Failed attempts never establish Step State.

15. A committed Step State is never overwritten.

16. A step with `unwind=false` requires no backward handler.

17. A step with `unwind=true` must eventually resolve by successful unwind or permanent unwind failure after a later step fails.

18. The final pipeline step may not declare unwind.

19. `nil` means operation success.

20. Ordinary error means retry.

21. `durable.Fail(err)` means permanent operation failure.

22. Permanent forward failure establishes the RootFailure and begins unwind.

23. A forward step that returns `durable.Fail` is never unwound itself.

24. A failing `Run` handler is responsible for cleaning up its own partial uncommitted effects before returning `durable.Fail`.

25. Permanent unwind failure is recorded and unwind continues.

26. Unwind follows committed successful execution history.

27. A step resolved through handler execution was invoked at least once.

28. Safe tombstone skipping is explicitly exempt from handler at-least-once execution.

29. Attempted incomplete historical steps may never be silently skipped.

30. Scheduling equivalent active intent returns the existing Run.

31. Scheduling conflicting input into an occupied resource slot returns a typed conflict.

32. Engine startup owns recovery.

33. `Bind` is valid only before Engine startup.

34. Historical handlers register only before Engine startup.

35. `Schedule` is valid only after Engine startup.

36. Exactly one operation belonging to a Run executes at once.

37. Different Runs may execute concurrently.

38. Engine concurrency is globally bounded.

39. Immediately successful steps may continue without a scheduler round trip.

40. Retryable failure releases execution capacity.

41. Retry eligibility survives process restart.

42. Retry counters are scoped to individual logical operations.

43. Engine shutdown interruption is not semantic pipeline failure.

44. Panics are retryable by default.

45. Exactly one Engine may own a store at a time.

46. Pipeline handles expose resource-oriented operations.

47. Run handles expose exact-execution-oriented operations.

48. `RunID` remains the portable identity behind a Run handle.

49. The final step's committed Step State is the pipeline output.

50. A pipeline whose final step has no state has no business output.

51. Failed runs have no pipeline output.

52. The pipeline input and final-step output contracts must remain compatible for a stable `PipelineID`.

53. Output-producing pipelines preserve typed Run access through `Schedule`, `Active`, and `Runs`.

54. If the Engine is running, every nonterminal Run is interpretable by the registered binary.

---

# 90. Future work

The following areas are intentionally left for later design.

## 90.1 Store contract

Define:

- semantic persistence operations,
- transactional boundaries,
- SQLite implementation,
- bbolt implementation,
- run discovery,
- active-slot uniqueness,
- wait notification,
- retention.

The Store API should be driven by durable state transitions rather than raw SQL or key/value primitives.

## 90.2 Persistent representation

Choose between:

```text
current-state persistence

event journal

hybrid state + history
```

and define the exact representation of:

- run creation,
- materialized topology,
- pipeline input,
- attempts,
- Step States,
- retry wakeups,
- RootFailure,
- UnwindFailures,
- tombstones,
- terminal outcome.

## 90.3 Cancellation semantics

Specify explicit application cancellation of accepted Runs:

- whether cancellation triggers unwind,
- whether it creates RootFailure,
- cancellation during retry wait,
- cancellation versus Engine shutdown,
- Run-level API.

## 90.4 Recovery quarantine

Consider allowing an Engine to start while quarantining uninterpretable historical Runs.

This would trade the current strong fail-fast startup invariant for partial availability and requires explicit administrative semantics.

## 90.5 Observability

Design first-class support for:

- structured diagnostics,
- OpenTelemetry,
- metrics,
- run correlation,
- step attempt correlation,
- retry timing,
- unwind failures,
- scheduler saturation,
- pipeline and step duration.

## 90.6 Historical inspection

Potential APIs:

```go
run.History(ctx)

run.Attempts(ctx)
```

Execution history terminology must remain distinct from immutable Step State.

## 90.7 Per-step retry policy

Potential protobuf declarations for step-specific retry behavior.

Permanent failure MUST remain explicit through:

```go
durable.Fail(err)
```

regardless of retry policy.

## 90.8 Scheduler fairness and admission control

Potential additions:

- per-pipeline concurrency limits,
- priorities,
- resource classes,
- admission queues,
- fairness policies.

These must preserve one-operation-per-run serialization.

## 90.9 Delayed and dependent scheduling

Potential scheduling primitives:

```text
not before time T

run after RunID X reaches terminal state
```

Dependencies should reference exact `RunID` values.

## 90.10 Durable compatibility tooling

A future tool should compare prior and current protobuf descriptors and identify durable semantic incompatibilities beyond normal protobuf wire compatibility.

---

# 91. Summary

The data model is:

```text
immutable Pipeline Input
          |
          v
       Step A
          |
          +---- immutable State A
          |
          v
       Step B
          |
          +---- immutable State B
          |
          v
     Final Step
          |
          +---- immutable Final State
                         |
                         v
                 Pipeline Output
```

Failure semantics are:

```text
ordinary error
    -> retry

durable.Fail(err) during Run
    -> permanent forward failure
    -> unwind committed predecessors

durable.Fail(err) during Unwind
    -> permanent unwind failure
    -> record
    -> continue backward
```

Runtime ownership is:

```text
Engine
    lifecycle, scheduling, retry, recovery

Pipeline
    resource-scoped scheduling and lookup

Run
    exact-execution interaction

Pipeline Input
    immutable execution intent

Step State
    immutable state established by a successful step

Pipeline Output
    final step's immutable state

Result
    execution success/failure semantics
```

The value proposition of `durable` comes from remaining deliberately constrained: a small execution model with explicit durable semantics, strong generated typing, straightforward crash recovery, and ordinary Go application handlers.