# `durable`: Durable Linear Pipelines with Unwind Semantics

**Status:** Draft 0.8  
**Target:** Go 1.27+  
**Persistence:** Local transactional database such as SQLite or bbolt  
**Schema and code generation:** Protocol Buffers, Buf, and `protoc-gen-durable`

# 1. Overview

`durable` is a Go library for executing fixed, linear pipelines whose progress survives process crashes and restarts.

A pipeline consists of an ordered sequence of steps:

```text
A -> B -> C -> D
```

Each operation resolved through handler execution uses **at-least-once semantics** and therefore its handler MUST be idempotent.

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

Only successfully completed steps declaring unwind behavior execute an `Unwind` handler.

Successfully completed steps without unwind behavior require no backward action.

An unwind operation may itself fail permanently. That failure is recorded and unwind continues with the previous successfully completed step.

Every run may contain:

- an immutable materialized pipeline definition,
- an optional immutable typed pipeline input,
- zero or more immutable per-step durable states,
- an optional typed pipeline output represented by the final Step State,
- durable retry and failure state,
- one globally unique execution identity.

---

# 2. Scope

`durable` deliberately implements a constrained execution model:

```text
fixed linear topology
+
typed immutable input
+
typed immutable Step State
+
automatic retry
+
explicit permanent failure
+
reverse unwind
+
durable recovery
```

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

`durable` distinguishes four durable identities:

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

At most one nonterminal run may occupy that slot at once.

For example:

```text
provision-machine / machine-123
```

may have only one active run.

Different pipelines may independently operate on the same resource:

```text
provision-machine / machine-123
update-inventory  / machine-123
```

Cross-pipeline exclusion is outside v1.

---

# 5. Run

A **Run** is one exact execution of one pipeline for one resource.

Example:

```text
PipelineID = provision-machine
ResourceID = machine-123
RunID      = 01K...
```

A resource slot may accumulate historical runs:

```text
Run A -> SUCCESS
Run B -> FAILURE
Run C -> ACTIVE
```

Only one may be nonterminal at once.

---

# 6. RunID

`RunID` uniquely identifies one exact execution.

It is not:

- a resource identifier,
- a pipeline revision,
- a schema version.

A `RunID` is suitable for:

- persistence,
- logging,
- APIs,
- correlation,
- historical lookup,
- waiting after restart.

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

The following are part of Step semantics:

- forward behavior,
- unwind behavior,
- Step State schema,
- declared Step State dependencies.

Changing those incompatibly requires a new `StepID`.

`StepID` MUST NOT depend on:

- protobuf message names,
- Go package names,
- generated Go type names,
- display names.

---

# 8. Global StepID uniqueness

A `StepID` MUST uniquely identify one durable Step declaration across the Engine's registered durable descriptors.

Two different Step declarations MUST NOT use the same stable `StepID`.

This keeps historical resolution simple:

```text
StepID -> declaration/schema/handler
```

without requiring `PipelineID` as additional disambiguation.

---

# 9. Step declaration ownership

In v1, one durable Step declaration may belong to exactly one active Pipeline declaration.

Cross-pipeline reuse of the same Step declaration is not supported.

This is rejected:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
  };
}

message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"
    steps: ".machines.v1.ReserveCapacity"
  };
}

message ResizeMachine {
  option (durable.v1.pipeline) = {
    id: "resize-machine"
    steps: ".machines.v1.ReserveCapacity"
  };
}
```

The reason is that generated Step invocations are pipeline-contextual. They may expose:

- the pipeline's typed input,
- declared predecessor Step State dependencies,
- pipeline identity.

Reusable application logic SHOULD live in ordinary Go helpers or services called by separate durable Step handlers.

Cross-pipeline durable Step reuse may be reconsidered in future versions.

---

# 10. Protocol Buffer declarations

`durable` uses protobuf custom options.

Conceptually:

```proto
syntax = "proto3";

package durable.v1;

import "google/protobuf/descriptor.proto";

message StepOptions {
  string id = 1;
  bool unwind = 2;

  // Step declarations whose committed state this step requires.
  repeated string state = 3;

  TombstoneOptions tombstone = 4;
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

Published extension numbers MUST be globally allocated from the protobuf extension registry.

---

# 11. Pipeline marker messages

A Pipeline declaration is represented by a protobuf message carrying the `durable.pipeline` option.

For example:

```proto
message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"
    ...
  };
}
```

`protoc-gen-go` will also generate the ordinary Go message type:

```go
machines.ProvisionMachine
```

Application code normally does not instantiate this marker type.

Generated durable APIs instead use types such as:

```text
ProvisionMachineDefinition
ProvisionMachinePipeline
ProvisionMachineRun
ProvisionMachineResult
```

---

# 12. Pipeline input

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

The input represents immutable run intent.

Once accepted, it MUST NOT change.

A resumed run MUST observe exactly the same input.

---

# 13. Pipeline input compatibility

Pipeline input is part of the public contract identified by `PipelineID`.

Compatible protobuf evolution is allowed.

An incompatible input contract change SHOULD require a new `PipelineID`.

---

# 14. Duplicate scheduling

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

Input equivalence MUST use:

```go
proto.Equal(existing, supplied)
```

semantics.

This includes protobuf unknown fields. Inputs that differ in preserved unknown fields are not equivalent.

The library MUST NOT silently replace, mutate, or ignore conflicting intent.

---

# 15. ScheduleConflictError

Scheduling conflict SHOULD use a typed error.

Conceptually:

```go
type ScheduleConflictError struct {
    RunID RunID
}

func (e *ScheduleConflictError) Error() string
```

For a conflict:

```text
run     = zero
created = false
err     = *ScheduleConflictError
```

The included `RunID` identifies the currently occupying run.

---

# 16. Step declaration

A protobuf message annotated with `durable.step` declares a durable Step.

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

A Step declaration has three relevant dimensions:

```text
protobuf fields present
    -> Step establishes durable Step State

unwind = true
    -> successful Step may require Unwind after later failure

state dependencies
    -> Step requires committed state from specific predecessors
```

---

# 17. Step State

A successful Step MAY establish immutable durable state.

The protobuf message declaring the Step is also the schema of that state.

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

Successful handler:

```go
return &machines.ReserveCapacity{
    ReservationId: reservation.ID,
    HostId:        reservation.HostID,
}, nil
```

That value becomes committed Step State for `reserve-capacity/v1`.

---

# 18. Step State dependencies

A Step MUST explicitly declare any predecessor Step States its handler depends on.

Example:

```proto
message CreateMachine {
  option (durable.v1.step) = {
    id: "create-machine/v2"

    state: ".machines.v1.SelectHost"
    state: ".machines.v1.ReserveCapacity"
  };

  string machine_id = 1;
}
```

This means `CreateMachine/v2` requires committed state from:

```text
SelectHost
ReserveCapacity
```

These dependencies are part of the durable Step semantics.

Adding, removing, or changing them incompatibly requires a new `StepID`.

---

# 19. State dependency validation

At generation time, every declared state dependency MUST:

1. reference a valid `durable.step`,
2. reference a Step that produces state,
3. belong to the same active Pipeline,
4. appear before the dependent Step in pipeline topology.

Invalid dependencies MUST cause generation failure.

---

# 20. Historical dependency validation

A resumed historical run executes against its own materialized topology.

Therefore `Engine.Start` MUST validate every remaining historical Step's persisted dependencies against that run.

For each remaining Step:

- each required dependency must exist in the run's materialized topology,
- it must precede the dependent Step,
- it must be a state-producing Step,
- if forward execution has advanced beyond it, required state must be available when expected.

If current registered handler semantics cannot be satisfied by the historical run definition, startup MUST fail under the v1 fail-fast policy.

A Step dependency change that breaks historical runs is a durable breaking change.

---

# 21. Step capability matrix

Generated Go interfaces expose only declared capabilities.

## 21.1 No state, no unwind

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

## 21.2 State, no unwind

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

## 21.3 No state, with unwind

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

## 21.4 State and unwind

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

---

# 22. Generated API principle

The protobuf declaration is authoritative.

Generated Go APIs MUST expose exactly the capabilities declared.

The framework MUST NOT require:

- dummy state values,
- synthetic empty protobuf returns,
- synthetic no-op unwind methods,
- marker methods solely for dispatch.

---

# 23. Step State success boundary

For a state-producing Step:

```text
Run -> (state, nil)

    commit Step State
    mark Step successful
    advance
```

Retryable failure:

```text
Run -> (_, ordinary error)

    discard returned state
    retry
```

Permanent failure:

```text
Run -> (_, durable.Fail(err))

    discard returned state
    establish RootFailure
    begin unwind
```

Step State exists if and only if forward execution has durably succeeded.

Step success and Step State MUST be one logical durable transition.

---

# 24. Step State immutability

Once committed, Step State MUST NOT change.

The initial public API MUST NOT expose mutation operations such as:

```go
SetState(...)
UpdateState(...)
MutateState(...)
```

---

# 25. Invocation

Generated handlers receive generated Invocation types.

Every generated Invocation MUST expose the core durable execution metadata:

```go
inv.PipelineID()
inv.ResourceID()
inv.RunID()
inv.StepID()
inv.Attempt()
inv.Phase()
```

If the Pipeline declares input:

```go
input := inv.Input()
```

returns the concrete generated pipeline input type.

Declared Step State dependencies produce typed state accessors.

Example:

```go
host := inv.SelectHostState()
reservation := inv.ReserveCapacityState()
```

These methods return the concrete Step State pointer directly.

---

# 26. Forward invocation visibility

During `Run`, an Invocation may read:

```text
Pipeline Input
+
committed state of explicitly declared predecessor dependencies
```

It MUST NOT access:

- later Step State,
- its own uncommitted state,
- undeclared predecessor state through generated guaranteed accessors.

An optional advanced dynamic lookup API MAY expose additional historical/dynamic inspection.

---

# 27. Unwind invocation visibility

During `Unwind`, an Invocation may read:

```text
Pipeline Input
+
committed state of all successfully completed steps allowed by the runtime API
+
Failure context
```

The current Step's own committed state, if any, is passed explicitly as:

```go
state *StepType
```

Declared dependency accessors remain available.

---

# 28. Dynamic Step State lookup

An advanced generic lookup MAY exist:

```go
state, ok := inv.State(machines.ReserveCapacityStep)
```

Unlike generated guaranteed accessors, this API may return:

```text
ok = false
```

Examples include:

- a historical tombstoned Step skipped before execution,
- state not present in an older run topology,
- explicitly dynamic inspection.

Generated required dependency accessors MUST NOT use `(value, ok)` when startup and topology validation guarantee existence.

---

# 29. Operation result semantics

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

> Current operation completed successfully.

## Retryable failure

```go
return err
```

means:

> Current operation remains unresolved and should be retried.

## Permanent failure

```go
return durable.Fail(err)
```

means:

> Current operation must not be retried once this result is durably committed.

---

# 30. Forward permanent failure

For `Run`:

```text
durable.Fail(err)
    -> permanently fail current forward operation
    -> establish RootFailure
    -> enter Unwind
```

The failing Step itself is not successfully completed and therefore is not unwound.

---

# 31. Failing-Step cleanup responsibility

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

Therefore:

> A `Run` handler MUST reconcile or clean up partial effects created by its unsuccessful invocation before returning `durable.Fail`.

`Unwind` compensates committed successful Steps, not the currently failing partial Step.

---

# 32. Unwind execution

For `Unwind`:

```text
nil
    -> mark unwind successful
    -> continue backward

ordinary error
    -> retry same unwind

durable.Fail(err)
    -> permanently fail current unwind
    -> record UnwindFailure
    -> continue backward
```

A permanent unwind failure MUST NOT stop unwind.

---

# 33. Steps without unwind

A successful Step with:

```proto
unwind: false
```

requires no backward action.

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

# 34. Unwind liveness

Every successfully completed Step MUST eventually resolve during unwind as one of:

```text
no unwind required

successfully unwound

permanently failed to unwind
```

Retryable unwind errors do not resolve the Step.

---

# 35. Phase

Pipeline phase is distinct from scheduler state.

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

# 36. Scheduler state

Scheduler state describes run execution eligibility.

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

# 37. Durable failure representation

Arbitrary Go `error` values are not durable API state.

`durable.Fail(err)` is converted into a persisted representation.

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

Wrapped Go error identity is not guaranteed to survive restart.

Structured failure details may be added later.

---

# 38. Failure consistency across restart

An unwind handler SHOULD receive equivalent `Failure` information whether unwind begins:

- immediately in the current process,
- after process restart.

`Failure` therefore represents persisted execution information, not an arbitrary in-memory Go error object.

---

# 39. Pipeline output

There is no separate pipeline output projector.

Instead:

> The committed Step State of the final Step is the Pipeline Output.

If the final Step has fields, the Pipeline has typed business output.

If the final Step has no fields, the Pipeline has no business output.

---

# 40. Pipeline with output

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

Pipeline output type:

```go
*machines.CreateMachine
```

---

# 41. Explicit final result Step

If the desired business output differs from the natural final operation, the developer SHOULD add an explicit final state-producing Step.

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

`ProvisionResult` may derive its state from immutable Pipeline Input and explicitly declared predecessor Step States.

Because it is an ordinary Step, it inherits:

- retry semantics,
- StepID versioning,
- state persistence,
- crash recovery,
- historical compatibility semantics.

---

# 42. Final Step completion boundary

If the final Step produces state:

```text
Final.Run -> (state, nil)
```

the logical successful terminal transition establishes:

```text
Final Step State
+
Pipeline terminal success
```

A failed Run has no Pipeline Output.

---

# 43. Final Step unwind rule

A final Step MUST NOT declare:

```proto
unwind: true
```

because there is no later Step whose failure could trigger compensation of a successfully completed final Step.

`protoc-gen-durable` MUST reject such a Pipeline.

If the final Step itself fails, it never successfully completed and is not unwound.

---

# 44. Stable final Step contract

For a stable `PipelineID`, the final `StepID` MUST remain stable.

Compatible protobuf evolution of that final Step's state schema is allowed.

Changing the final `StepID` requires a new `PipelineID`.

Therefore appending a new trailing Step changes the Pipeline's output contract and requires a new `PipelineID`.

This is distinct from adding an intermediary Step.

Example allowed under stable `PipelineID`:

```text
v1:
A -> B -> Result/v1

v2:
A -> X -> B -> Result/v1
```

Example requiring new `PipelineID`:

```text
A -> B -> Result/v1 -> Notify/v1
```

because `Notify/v1` becomes the new final Step.

---

# 45. Intermediary Step evolution

Adding, removing, or reordering intermediary Steps MAY be compatible with a stable `PipelineID`, provided:

- the final Step remains the same,
- Pipeline Input remains compatible,
- Step dependency contracts remain satisfiable for historical runs,
- historical handlers remain available where required.

Existing Runs retain their materialized topology.

New Runs receive the new topology.

---

# 46. Typed output Runs

If the final Step has state, generated Pipeline APIs MUST preserve typed Run access.

Example:

```go
run, created, err := provision.Schedule(...)
```

returns:

```go
machines.ProvisionMachineRun
```

rather than plain `durable.Run`.

---

# 47. Typed Result

For an output-producing Pipeline:

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
result.Output() != nil
```

On failure:

```go
result.Output() == nil
```

---

# 48. Typed lookup consistency

Output-producing Pipelines MUST preserve typed Run wrappers across:

```text
Schedule
Active
Runs
Run(RunID)
```

A typed Run obtained after restart must have the same output access as one returned directly by `Schedule`.

---

# 49. Typed Run recovery by RunID

A bound generated Pipeline SHOULD expose:

```go
run, err := provision.Run(
    ctx,
    runID,
)
```

returning:

```go
machines.ProvisionMachineRun
```

The method MUST verify the `RunID` belongs to the bound Pipeline's `PipelineID`.

Pipeline mismatch SHOULD return a typed error.

Conceptually:

```go
type PipelineMismatchError struct {
    RunID      RunID
    Expected   PipelineID
    Actual     PipelineID
}
```

This allows persisted `RunID` values to recover fully typed Run handles after restart.

---

# 50. Pipelines without output

If the final Step has no state, generated APIs MAY use plain:

```go
durable.Run
durable.Result
```

without unnecessary typed wrappers.

Generated APIs expose only capabilities that exist.

---

# 51. Generated type naming

Proto marker messages already occupy Go names.

Therefore `protoc-gen-durable` MUST use collision-free names.

Recommended:

```text
ProvisionMachineDefinition
ProvisionMachinePipeline
ProvisionMachineRun
ProvisionMachineResult
```

The bound Pipeline handle MUST NOT be named `ProvisionMachine`.

---

# 52. Generated pipeline construction

Example:

```go
definition := machines.NewProvisionMachine(
    &validate{},
    &reserveCapacity{},
    &createMachine{},
)
```

returns:

```go
*machines.ProvisionMachineDefinition
```

The definition knows:

- `PipelineID`,
- typed input,
- ordered topology,
- active handlers,
- state capabilities,
- unwind capabilities,
- declared state dependencies,
- output type implied by final Step.

---

# 53. Bind

Generated definition:

```go
provision, err := machines.NewProvisionMachine(
    &validate{},
    &reserveCapacity{},
    &createMachine{},
).Bind(engine)
```

returns:

```go
*machines.ProvisionMachinePipeline
```

`Bind`:

1. registers the Pipeline definition,
2. registers active Step handlers,
3. registers generated adapters,
4. returns the bound Pipeline handle.

`Bind` is allowed only before `Engine.Start`.

---

# 54. Pipeline handle

A bound Pipeline is resource-oriented.

It exposes:

```text
Schedule
Active
Runs
Run
```

A subsystem SHOULD depend on only the Pipeline handles it needs.

Example:

```go
type MachineService struct {
    provision *machines.ProvisionMachinePipeline
}
```

---

# 55. Schedule

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
    this call created the Run

false
    an equivalent nonterminal Run already existed
```

`Schedule` is valid only after `Engine.Start`.

---

# 56. Active

Example:

```go
run, ok, err := provision.Active(
    ctx,
    resourceID,
)
```

For output-producing Pipelines this returns the generated typed Run.

`ok` indicates whether a nonterminal Run currently occupies the slot.

---

# 57. Runs

Example:

```go
runs, err := provision.Runs(
    ctx,
    resourceID,
)
```

For output-producing Pipelines:

```go
[]machines.ProvisionMachineRun
```

is returned.

Identifiers remain available through:

```go
run.ID()
```

---

# 58. Run handle

Plain `durable.Run` is conceptually:

```go
type Run struct {
    id     RunID
    engine *Engine
}
```

It is an in-process convenience handle, not persisted state.

Core methods:

```go
func (r Run) ID() RunID

func (r Run) Wait(
    ctx context.Context,
) (Result, error)

func (r Run) Status(
    ctx context.Context,
) (Status, error)
```

Generated typed Run wrappers provide pipeline-specific typed behavior.

---

# 59. Engine-level Run access

The Engine MAY expose untyped access:

```go
result, err := engine.Wait(ctx, runID)

status, err := engine.Status(ctx, runID)

run := engine.Run(runID)
```

Callers requiring typed output SHOULD use the generated Pipeline's:

```go
pipeline.Run(ctx, runID)
```

method.

---

# 60. Wait semantics

`Wait` separates operational wait failure from Pipeline execution failure.

```go
result, err := run.Wait(ctx)
```

`err != nil` represents:

- caller wait-context cancellation,
- lookup failure,
- Engine/query failure.

Pipeline failure is represented by `Result`.

---

# 61. Result

Conceptually:

```go
type Result struct {
    Outcome Outcome

    RootFailure *RootFailure

    UnwindFailures []UnwindFailure
}
```

Successful Run:

```text
OutcomeSuccess
RootFailure = nil
UnwindFailures = []
```

Failed Run:

```text
OutcomeFailure
RootFailure != nil
```

Unwind may be complete:

```text
UnwindFailures = []
```

or incomplete:

```text
UnwindFailures != []
```

---

# 62. Status

`Status` exposes pipeline and scheduler state.

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

Zero values may be used where fields do not apply.

The exact representation may later use accessors rather than exported fields.

---

# 63. Engine lifecycle

Engine lifecycle:

```text
configuring
    |
    | Start()
    v
running
```

During configuration:

- Pipelines may bind,
- historical handlers may register,
- scheduling is rejected.

During running:

- registration is frozen,
- recovery is active,
- scheduling is accepted,
- further binding is rejected.

---

# 64. Single Engine ownership

Exactly one Engine instance may own and execute against a given Store at once.

This is an explicit v1 assumption.

No multi-process execution coordination is provided.

A Store SHOULD enforce or detect exclusive ownership where practical.

---

# 65. Engine.Start

`Engine.Start(ctx)` owns recovery.

It SHOULD:

1. freeze registration,
2. discover every nonterminal Run,
3. validate Pipeline definitions,
4. validate historical topology,
5. validate Step State dependency satisfiability,
6. verify required current and historical handlers,
7. verify required historical protobuf state schemas,
8. reconstruct retry eligibility,
9. enqueue runnable Runs,
10. schedule future retry wakeups,
11. begin normal scheduling.

---

# 66. Startup fail-fast policy

Draft 0.8 uses fail-fast recovery.

If any nonterminal Run cannot be safely interpreted or resumed:

```go
engine.Start(ctx)
```

fails.

This preserves:

> If the Engine is running, every nonterminal durable Run is interpretable and resumable by the registered binary.

Per-Run recovery quarantine is future work.

---

# 67. Historical handlers

Removed Steps may still require historical implementations.

Generated Step descriptors SHOULD provide explicit historical binding.

Conceptually:

```go
err := machines.ReserveCapacityStep.BindHistorical(
    engine,
    &reserveCapacityV1{},
)
```

Historical binding:

- does not add the Step to current topology,
- makes the implementation available to historical Runs,
- is valid only before `Engine.Start`.

---

# 68. Engine-owned execution lifetime

The context passed to `Schedule` controls scheduling acceptance only.

Once accepted:

```text
caller context cancellation
    !=
Run cancellation
```

Handler invocations receive contexts derived from Engine lifetime.

---

# 69. Execution concurrency

Exactly one logical operation for a given Run may execute at once.

Different Runs MAY execute concurrently.

Example:

```text
run-1 -> ReserveCapacity.Run
run-2 -> Network.Unwind
run-3 -> Validate.Run
```

The Engine MUST enforce a global concurrency bound.

Conceptually:

```go
durable.WithConcurrency(32)
```

---

# 70. Immediate continuation

Once a worker owns a Run, successful operations MAY continue immediately through subsequent Steps.

Example:

```text
A.Run succeeds
    -> B.Run

B.Run succeeds
    -> C.Run
```

Worker ownership is released when:

- the Run becomes terminal,
- an operation requires retry,
- shutdown begins.

---

# 71. Retryable failure scheduling

Workers MUST NOT sleep while holding execution capacity.

After retryable failure:

```text
record failed attempt
compute retry eligibility
mark Run waiting
release capacity
schedule wakeup
```

---

# 72. Retry policy

Engine SHOULD support an Engine-wide RetryPolicy.

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

Permanent failure MUST remain explicit through:

```go
durable.Fail(err)
```

---

# 73. Attempt counters

Attempt counters belong to one logical operation.

Example:

```text
A.Run
    attempt 1 -> retry
    attempt 2 -> success

B.Run
    attempt 1
```

Forward and unwind attempt sequences are independent.

The first invocation has:

```go
inv.Attempt() == 1
```

An invocation interrupted by shutdown still counts as an invocation.

---

# 74. Retry wakeup durability

Retry eligibility MUST survive restart.

Example:

```text
attempt fails at 10:00:00
next retry = 10:00:30

restart at 10:00:05
```

The Engine SHOULD preserve:

```text
next retry = 10:00:30
```

rather than retry immediately.

---

# 75. Graceful shutdown versus crash recovery

During graceful shutdown:

- Engine-owned cancellation is not recorded as application failure,
- no normal application retry backoff is advanced merely because of shutdown.

After a later restart, an operation that had previously been invoked but never durably resolved is treated as an unresolved attempted operation.

The new Engine MAY apply recovery backoff before invoking it again regardless of whether the previous process ended through:

- graceful shutdown,
- crash,
- forced termination.

No persistent distinction between graceful interruption and crash is required in v1.

---

# 76. Recovery crash-loop protection

An unresolved attempted operation discovered at startup SHOULD NOT necessarily run immediately.

The Engine SHOULD apply recovery backoff to avoid repeated process crash loops causing hot re-execution.

The exact recovery-backoff policy is an implementation detail but MUST be testable and observable.

---

# 77. Clock

The Engine SHOULD support an injectable clock.

Conceptually:

```go
durable.WithClock(clock)
```

Clock controls:

- retry timestamps,
- retry wakeups,
- recovery timing,
- failure timestamps.

Production defaults to the system clock.

---

# 78. Engine shutdown

Normal Engine shutdown is not Pipeline failure.

Shutdown SHOULD:

```text
stop accepting new execution work
stop starting new handlers
cancel active handler contexts
leave unresolved Runs nonterminal
```

A future Engine resumes them.

---

# 79. Shutdown interruption

If shutdown interrupts a handler:

```text
invocation occurred
attempt count remains incremented
logical operation remains unresolved
shutdown does not create RootFailure
```

Engine-owned cancellation MUST be distinguishable from ordinary application error handling within the runtime.

---

# 80. Panics

The Engine SHOULD recover application panics.

A recovered panic SHOULD:

- record diagnostic information,
- capture stack trace,
- mark invocation unsuccessful,
- remain retryable by default.

A panic does not imply:

```go
durable.Fail(err)
```

---

# 81. Immutable topology

Each Run captures its materialized topology at scheduling time.

Old:

```text
A -> B -> D
```

New:

```text
A -> B -> C -> D
```

Old Runs retain:

```text
A -> B -> D
```

New Runs receive:

```text
A -> B -> C -> D
```

No implicit migration occurs.

---

# 82. Adding intermediary Steps

Adding an intermediary Step can be compatible with a stable `PipelineID` when:

- the final Step remains unchanged,
- existing StepIDs retain semantics,
- historical state dependencies remain satisfiable,
- required historical handlers remain available.

Example:

```text
v1:
A -> B -> Result/v1

v2:
A -> X -> B -> Result/v1
```

---

# 83. Appending trailing Steps

Appending a trailing Step changes the final Step.

Because final Step State defines Pipeline Output:

> Appending a new Step at the end requires a new `PipelineID`.

Example:

```text
A -> B -> Result/v1
```

to:

```text
A -> B -> Result/v1 -> Notify/v1
```

requires a new Pipeline identity.

---

# 84. Removing intermediary Steps

Removing a Step from current topology does not erase its historical identity.

Historical Runs may still need to:

- execute it,
- retry it,
- read its state,
- unwind it.

Current topology and historical implementation retirement are separate concerns.

---

# 85. Tombstones

A removed historical Step MAY remain declared as a tombstone.

Tombstone means:

> This `StepID` remains historically valid but is no longer used by active topology.

Historical materialized Run topology remains authoritative.

---

# 86. Forward tombstone skipping

A tombstoned Step may be skipped only when its forward operation has never been invoked.

If:

```text
attempts = 0
```

safe skipping may occur.

If:

```text
attempts > 0
```

it MUST NOT be silently skipped.

External partial effects may exist.

---

# 87. Tombstone unwind semantics

A historical Step originally declared:

```proto
unwind: false
```

requires no historical Unwind handler.

A historical Step with:

```proto
unwind: true
```

requires its legacy handler unless an explicit tombstone policy retires that requirement.

Historical Step State schemas needed by legacy Unwind MUST remain decodable.

---

# 88. At-least-once invariant

The at-least-once guarantee applies to operations resolved through handler execution.

Specifically:

> Any operation resolved by handler success or permanent handler failure was invoked at least once and may have been invoked multiple times.

Operations resolved without handler execution are exempt, including:

- safe tombstone skipping,
- backward traversal over Steps declaring no Unwind.

---

# 89. Protobuf evolution

Compatible protobuf field evolution is allowed.

Example:

```proto
message ReserveCapacity {
  string reservation_id = 1;

  // Added compatibly.
  string host_id = 2;
}
```

Compatible wire evolution does not alone require a new `StepID`.

Incompatible semantic changes do.

Pipeline Input and final Step State additionally form the external type contract of `PipelineID`.

---

# 90. Durable breaking changes

Examples include:

- changing Step semantics while retaining `StepID`,
- changing declared Step State dependencies while retaining `StepID`,
- removing a historical `StepID` still referenced by Runs,
- removing required historical state schemas,
- changing unwind semantics without retirement,
- changing Pipeline Input incompatibly while retaining `PipelineID`,
- changing final `StepID` while retaining `PipelineID`,
- changing historical topology such that remaining Step dependencies become unsatisfiable.

A future compatibility checker SHOULD detect these from descriptor history where possible.

---

# 91. Code generation

`protoc-gen-durable` is responsible for generating:

- typed Step handler interfaces,
- typed Invocation types,
- typed dependency state accessors,
- Step descriptors,
- Pipeline definition constructors,
- bound Pipeline handles,
- typed Run wrappers,
- typed Result wrappers,
- typed RunID lookup methods,
- historical handler binding support,
- runtime adapters.

---

# 92. Generation-time validation

`protoc-gen-durable` MUST reject:

- missing `PipelineID`,
- missing `StepID`,
- duplicate global `StepID`,
- nonexistent referenced messages,
- pipeline members without `durable.step`,
- empty pipelines,
- invalid input types,
- invalid state dependencies,
- dependency on a Step outside the same Pipeline,
- dependency on a later Step,
- dependency on a Step with no state,
- one active Step declaration reused across multiple Pipelines,
- malformed tombstones,
- tombstoned Steps referenced by active topology,
- final Step declaring `unwind=true`.

Generated Go APIs SHOULD make these compile-time failures:

- missing handlers,
- wrong handler types,
- handlers in wrong position,
- invalid `Run` signatures,
- missing required `Unwind`,
- invalid historical handler type.

---

# 93. Internal type erasure

Generated typed APIs live at the boundary.

Internally, adapters erase into runtime concepts:

```text
PipelineID
ResourceID
RunID
StepID

serialized Pipeline Input
serialized Step States

materialized topology
persisted dependency metadata
failure records
retry metadata

internal handler
raw invocation metadata
```

The Engine core SHOULD remain non-generic.

---

# 94. Buf

`durable` SHOULD use Buf for:

```text
buf lint
buf breaking
buf generate
```

Buf is build-time and CI tooling.

Runtime MUST NOT depend on Buf.

Use of the Buf Schema Registry is optional.

---

# 95. Minimal observability requirements

Full observability remains future work, but retries MUST NOT be opaque.

At minimum:

- `Status` exposes `Attempt`,
- `Status` exposes `NextAttemptAt`,
- failure records expose durable failure metadata,
- panic diagnostics retain stack information somewhere observable,
- Engine SHOULD provide a logging or diagnostic hook for repeated retries and recovery failures.

No specific logging framework is required.

---

# 96. Preferred core API

Conceptually:

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

# 97. Generated API example

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

    state: ".machines.v1.SelectHost"
  };

  string reservation_id = 1;
}

message CreateMachine {
  option (durable.v1.step) = {
    id: "create-machine/v1"

    state: ".machines.v1.SelectHost"
    state: ".machines.v1.ReserveCapacity"
  };

  string machine_id = 1;
  string host_id = 2;
}

message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"
    input: ".machines.v1.ProvisionMachineInput"

    steps: ".machines.v1.Validate"
    steps: ".machines.v1.SelectHost"
    steps: ".machines.v1.ReserveCapacity"
    steps: ".machines.v1.CreateMachine"
  };
}
```

Generated definition:

```go
func NewProvisionMachine(
    validate ValidateHandler,
    selectHost SelectHostHandler,
    reserveCapacity ReserveCapacityHandler,
    createMachine CreateMachineHandler,
) *ProvisionMachineDefinition
```

Bind:

```go
func (d *ProvisionMachineDefinition) Bind(
    engine *durable.Engine,
) (*ProvisionMachinePipeline, error)
```

Typed Pipeline:

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

func (p *ProvisionMachinePipeline) Run(
    ctx context.Context,
    runID durable.RunID,
) (ProvisionMachineRun, error)
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

Typed Result:

```go
type ProvisionMachineResult struct {
    durable.Result
}

func (r ProvisionMachineResult) Output() *CreateMachine
```

---

# 98. Generated Invocation example

For:

```proto
message CreateMachine {
  option (durable.v1.step) = {
    id: "create-machine/v1"

    state: ".machines.v1.SelectHost"
    state: ".machines.v1.ReserveCapacity"
  };

  string machine_id = 1;
  string host_id = 2;
}
```

generated Invocation SHOULD expose:

```go
type CreateMachineInvocation struct {
    // generated/internal
}

func (i CreateMachineInvocation) PipelineID() durable.PipelineID
func (i CreateMachineInvocation) ResourceID() durable.ResourceID
func (i CreateMachineInvocation) RunID() durable.RunID
func (i CreateMachineInvocation) StepID() durable.StepID
func (i CreateMachineInvocation) Attempt() uint64
func (i CreateMachineInvocation) Phase() durable.Phase

func (i CreateMachineInvocation) Input() *ProvisionMachineInput

func (i CreateMachineInvocation) SelectHostState() *SelectHost
func (i CreateMachineInvocation) ReserveCapacityState() *ReserveCapacity
```

Application handler:

```go
func (h *createMachine) Run(
    ctx context.Context,
    inv machines.CreateMachineInvocation,
) (*machines.CreateMachine, error) {
    host := inv.SelectHostState()
    reservation := inv.ReserveCapacityState()

    machine, err := h.create(
        ctx,
        host.HostId,
        reservation.ReservationId,
    )
    if err != nil {
        return nil, err
    }

    return &machines.CreateMachine{
        MachineId: machine.ID,
        HostId:    host.HostId,
    }, nil
}
```

---

# 99. Typical application flow

```go
engine := durable.New(
    durable.WithConcurrency(32),
)

provision, err := machines.NewProvisionMachine(
    &validate{},
    &selectHost{},
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

# 100. Core invariants

The implementation MUST maintain these invariants.

1. A Run's materialized topology never changes after creation.

2. A Run's Pipeline Input never changes after creation.

3. A committed Step State never changes.

4. A globally unique `StepID` identifies one durable semantic Step declaration.

5. A Step declaration belongs to exactly one active Pipeline in v1.

6. Declared Step State dependencies are part of Step semantics.

7. Changing Step State dependencies incompatibly requires a new `StepID`.

8. A `RunID` uniquely identifies one exact execution.

9. At most one nonterminal Run exists for `(PipelineID, ResourceID)`.

10. Operations resolved through handler execution use at-least-once semantics.

11. All handlers must therefore be idempotent.

12. Step capabilities are declared statically in protobuf.

13. Generated Go APIs expose exactly those capabilities.

14. A Step without protobuf fields establishes no Step State.

15. A Step with fields establishes Step State only after successful forward completion.

16. Step State and forward success form one logical durable transition.

17. Failed attempts never establish Step State.

18. A Step with `unwind=false` requires no backward handler.

19. A Step with `unwind=true` must eventually resolve through successful unwind or permanent unwind failure after a later Step fails.

20. The final Pipeline Step may not declare unwind.

21. `nil` means operation success.

22. Ordinary error means retry.

23. `durable.Fail(err)` means permanent operation failure.

24. Permanent forward failure establishes RootFailure and begins unwind.

25. A forward Step returning `durable.Fail` is never unwound itself.

26. A failing `Run` handler is responsible for cleaning up its own partial uncommitted effects before returning `durable.Fail`.

27. Permanent unwind failure is recorded and unwind continues.

28. Unwind follows committed successful execution history.

29. Any operation resolved through handler execution was invoked at least once.

30. Safe tombstone skipping is exempt from handler execution.

31. Attempted incomplete historical Steps may never be silently skipped.

32. Required Step State dependencies must be satisfiable for every recoverable historical Run.

33. Scheduling equivalent active intent returns the existing Run.

34. Scheduling conflicting input returns a typed conflict.

35. Engine startup owns recovery.

36. `Bind` is valid only before Engine startup.

37. Historical handlers register only before Engine startup.

38. `Schedule` is valid only after Engine startup.

39. Exactly one operation belonging to a Run executes at once.

40. Different Runs may execute concurrently.

41. Engine concurrency is globally bounded.

42. Immediately successful Steps may continue without scheduler round trips.

43. Retryable failure releases execution capacity.

44. Retry eligibility survives process restart.

45. Retry counters are scoped to individual logical operations.

46. Engine shutdown interruption is not semantic Pipeline failure.

47. A later startup may apply recovery backoff to unresolved attempted operations regardless of prior shutdown cause.

48. Panics are retryable by default.

49. Exactly one Engine may own a Store at a time.

50. Pipeline handles expose resource-oriented operations.

51. Run handles expose exact-execution-oriented operations.

52. `RunID` remains the portable identity behind a Run handle.

53. A bound Pipeline can recover a typed Run from a valid `RunID`.

54. The final Step's committed Step State is the Pipeline Output.

55. A Pipeline whose final Step has no state has no business output.

56. Failed Runs have no Pipeline Output.

57. A stable `PipelineID` has a stable final `StepID`.

58. Compatible final Step state schema evolution is allowed.

59. Changing the final `StepID`, including appending a trailing Step, requires a new `PipelineID`.

60. Pipeline Input must remain compatible for a stable `PipelineID`.

61. Output-producing Pipelines preserve typed Run access through `Schedule`, `Active`, `Runs`, and `Run(RunID)`.

62. If the Engine is running, every nonterminal Run is interpretable and resumable by the registered binary.

---

# 101. Future work

The following remain intentionally outside Draft 0.8.

## 101.1 Store contract

Define:

- semantic persistence operations,
- transactional boundaries,
- SQLite implementation,
- bbolt implementation,
- active-slot uniqueness,
- run discovery,
- waiter notification,
- retention.

## 101.2 Persistent representation

Choose between:

```text
current-state persistence
event journal
hybrid state + history
```

and define exact representation of:

- run creation,
- materialized topology,
- persisted Step dependency metadata,
- Pipeline Input,
- attempts,
- Step States,
- retry wakeups,
- failures,
- tombstones,
- terminal outcome.

## 101.3 Cancellation semantics

Specify application cancellation of an accepted Run:

- whether cancellation triggers unwind,
- whether cancellation creates RootFailure,
- cancellation during retry wait,
- cancellation versus Engine shutdown,
- Run-level cancellation API.

## 101.4 Recovery quarantine

Consider starting the Engine while isolating uninterpretable historical Runs instead of failing the whole Engine.

## 101.5 Full observability

Design:

- OpenTelemetry,
- metrics,
- structured diagnostics,
- retry instrumentation,
- scheduler saturation,
- Step duration,
- Run duration,
- unwind-failure visibility.

## 101.6 Historical inspection

Potential APIs:

```go
run.History(ctx)
run.Attempts(ctx)
```

Execution history terminology must remain distinct from immutable Step State.

## 101.7 Per-Step retry policy

Allow Step-specific retry policies while preserving:

```go
durable.Fail(err)
```

as the only application-declared permanent failure mechanism.

## 101.8 Scheduler fairness and admission control

Potential:

- per-Pipeline concurrency,
- priorities,
- admission queues,
- resource classes,
- fairness policies.

## 101.9 Delayed and dependent scheduling

Potential:

```text
not before time T
run after RunID X
```

Exact dependencies should use `RunID`.

## 101.10 Cross-Pipeline Step reuse

v1 explicitly forbids one durable Step declaration from belonging to multiple active Pipelines.

Future work may explore whether occurrence-specific generated handlers can support safe Step reuse without weakening typing or historical compatibility.

## 101.11 Durable compatibility tooling

A future compatibility checker should compare current and historical protobuf descriptors and detect durable semantic incompatibilities beyond normal protobuf wire compatibility.

---

# 102. Summary

The durable data model is:

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

Step State dependencies are explicit:

```text
SelectHost State ---------+
                          |
ReserveCapacity State ----+--> CreateMachine
```

Failure semantics:

```text
ordinary error
    -> retry

durable.Fail(err) during Run
    -> permanent forward failure
    -> unwind successful predecessors

durable.Fail(err) during Unwind
    -> permanent unwind failure
    -> record
    -> continue backward
```

Runtime ownership:

```text
Engine
    lifecycle, scheduling, retry, recovery

Pipeline
    resource-scoped scheduling and typed lookup

Run
    exact-execution interaction

Pipeline Input
    immutable execution intent

Step State
    immutable state established by successful Steps

Pipeline Output
    immutable state of the final Step

Result
    execution outcome and failure information
```

`durable` intentionally remains narrow.

Its objective is to provide rigorous crash-safe linear execution, strong compile-time typing, explicit evolution rules, and predictable recovery semantics without introducing the complexity of a general-purpose workflow runtime.