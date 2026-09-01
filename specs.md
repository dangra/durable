# `durable`: Durable Linear Pipelines with Unwind Semantics

**Status:** Draft 1.0  
**Target:** Go 1.27+  
**Persistence:** Local transactional database such as SQLite or bbolt  
**Schema and code generation:** Protocol Buffers, Buf, and `protoc-gen-durable`

# 1. Overview

`durable` is a Go library for executing fixed, linear pipelines whose execution state survives process crashes and restarts.

A pipeline consists of an ordered sequence of steps:

```text
A -> B -> C -> D
```

Each step operation uses **at-least-once execution semantics**. Step handlers therefore MUST be idempotent.

Successful completion automatically advances execution to the next applicable step.

Ordinary errors are considered transient and cause automatic retries.

A handler may explicitly mark the current operation as permanently failed by returning:

```go
return durable.Fail(err)
```

During forward execution, permanent failure establishes the run's root failure and begins unwind.

If `D` permanently fails after `A`, `B`, and `C` successfully executed:

```text
A -> B -> C -> D
              X
              |
              v
         C <- B <- A
```

Only steps that:

1. successfully executed forward for this run,
2. remain present in the current pipeline definition,
3. currently declare unwind behavior,

participate in unwind.

Unwind occurs in reverse current-pipeline order.

A step may be **retired** as an intermediate stage before being removed from a pipeline. Retirement prevents new forward executions from entering that step while allowing already-started executions to resolve.

Pipeline output is produced by an optional pure `Reducer` over immutable Pipeline Input and committed Step States.

---

# 2. Scope

`durable` deliberately implements a constrained execution model:

```text
linear topology
+
typed immutable input
+
typed immutable Step State
+
durable execution ledger
+
mutable pipeline definitions
+
automatic retry
+
explicit permanent failure
+
reverse unwind
+
pure output reduction
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
- child pipelines,
- deterministic program replay,
- exactly-once external side effects,
- arbitrary workflow-language semantics,
- BPMN,
- Petri nets,
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

They MUST be distinct defined types.

---

# 4. Pipeline resource slot

The scheduling slot is:

```text
(PipelineID, ResourceID)
```

At most one nonterminal run may occupy a slot at once.

Different pipelines MAY concurrently operate on the same `ResourceID`.

Cross-pipeline exclusion is outside v1.

---

# 5. Run

A Run is one exact execution of one pipeline against one resource.

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

Only one may be nonterminal.

---

# 6. StepID

A `StepID` identifies durable Step semantics.

Example:

```text
reserve-capacity/v1
reserve-capacity/v2
```

Compatible protobuf evolution does not necessarily require a new StepID.

An incompatible semantic change MUST introduce a new StepID.

Step semantics include:

- forward behavior,
- unwind behavior,
- Step State schema.

Dynamic reads of other Step States are application implementation behavior and are not declared in protobuf.

---

# 7. Step declaration ownership

In v1, one durable Step declaration belongs to exactly one active Pipeline.

Cross-pipeline reuse of a durable Step declaration is not supported.

Reusable business logic SHOULD live in ordinary Go helpers or services called by separate durable handlers.

This restriction also allows lifecycle properties such as `retired` to live directly on the Step declaration.

---

# 8. Protocol Buffer declarations

Conceptually:

```proto
syntax = "proto3";

package durable.v1;

import "google/protobuf/descriptor.proto";

message StepOptions {
  string id = 1;
  bool unwind = 2;
  bool retired = 3;
}

message PipelineOptions {
  string id = 1;
  string input = 2;
  string output = 3;
  repeated string steps = 4;
}

extend google.protobuf.MessageOptions {
  StepOptions step = <globally-allocated-extension-number>;
  PipelineOptions pipeline = <globally-allocated-extension-number>;
}
```

Published extension numbers MUST use globally allocated protobuf extension numbers.

---

# 9. Pipeline declaration

A Pipeline declaration is represented by a protobuf marker message.

Example:

```proto
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

Pipeline marker messages SHOULD NOT declare ordinary protobuf fields.

The generated Go type:

```go
machines.ProvisionMachine
```

is additionally used as the generated read-only Reducer input view.

---

# 10. Pipeline input

A Pipeline MAY declare a protobuf Input type.

Example:

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

# 11. Duplicate scheduling

If no active Run occupies `(PipelineID, ResourceID)`, create one:

```text
created = true
```

If an active Run exists with equivalent input, return it:

```text
created = false
```

Equivalence MUST use:

```go
proto.Equal(existing, supplied)
```

including unknown fields.

Different input produces:

```go
type ScheduleConflictError struct {
    RunID durable.RunID
}
```

and:

```text
run     = zero
created = false
err     = *ScheduleConflictError
```

---

# 12. Step declaration

A protobuf message annotated with `durable.step` declares a durable Step.

Example:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
  };

  string reservation_id = 1;
  string host_id = 2;
}
```

The Step declaration determines:

```text
fields present
    -> successful forward execution produces Step State

unwind=true
    -> successful forward execution may later participate in unwind

retired=true
    -> no new forward operation may begin for this Step
```

---

# 13. Step State

The protobuf Step message is also its durable Step State schema.

Example:

```proto
message SelectHost {
  option (durable.v1.step) = {
    id: "select-host/v1"
  };

  string host_id = 1;
}
```

Successful handler:

```go
return &machines.SelectHost{
    HostId: host.ID,
}, nil
```

commits immutable Step State.

Step State exists only after successful forward execution.

---

# 14. Step references

A state-producing Step receives a generated typed reference:

```go
var SelectHostStep durable.StateStepRef[*SelectHost]
```

A stateless Step receives:

```go
var ValidateStep durable.StepRef
```

Therefore:

```go
inv.State(machines.ValidateStep)
```

SHOULD fail at compile time.

---

# 15. Step handler capability matrix

## No State, no Unwind

```go
type ValidateHandler interface {
    Run(
        context.Context,
        ValidateInvocation,
    ) error
}
```

## State, no Unwind

```go
type SelectHostHandler interface {
    Run(
        context.Context,
        SelectHostInvocation,
    ) (*SelectHost, error)
}
```

## No State, with Unwind

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

## State and Unwind

```go
type ReserveCapacityHandler interface {
    Run(
        context.Context,
        ReserveCapacityInvocation,
    ) (*ReserveCapacity, error)

    Unwind(
        context.Context,
        ReserveCapacityInvocation,
        durable.Failure,
    ) error
}
```

`Unwind` does NOT receive its own State as a separate parameter.

It uses:

```go
state, ok := inv.State(machines.ReserveCapacityStep)
```

like any other state lookup.

---

# 16. Generated API principle

The protobuf declaration is authoritative.

Generated Go APIs MUST expose exactly the declared capabilities.

The framework MUST NOT require:

- dummy state,
- empty synthetic protobuf results,
- synthetic no-op Unwind methods,
- explicit State dependency declarations.

---

# 17. Step State success boundary

For a state-producing Step:

```text
Run -> (state, nil)

    commit Step State
    mark forward operation successful
    advance
```

Retry:

```text
Run -> (_, ordinary error)

    discard returned state
    persist unresolved attempt
    retry
```

Permanent failure:

```text
Run -> (_, durable.Fail(err))

    discard returned state
    mark forward operation permanently failed
    establish RootFailure
    begin unwind
```

Step State and successful forward resolution form one logical durable transition.

---

# 18. Step State immutability

Committed Step State MUST NOT change.

No public mutation API is provided.

---

# 19. Invocation

Generated Step handlers receive generated Invocation types.

Every Invocation exposes:

```go
inv.PipelineID()
inv.ResourceID()
inv.RunID()
inv.StepID()
inv.Attempt()
inv.Phase()
```

If the Pipeline has Input:

```go
inv.Input()
```

returns its concrete Pipeline Input type.

---

# 20. Dynamic typed State access

Invocation exposes:

```go
func (inv SomeInvocation) State[T proto.Message](
    step durable.StateStepRef[T],
) (T, bool)
```

Example:

```go
host, ok := inv.State(machines.SelectHostStep)

reservation, ok := inv.State(
    machines.ReserveCapacityStep,
)
```

The static result types are inferred.

No:

- `any`,
- protobuf `Any`,
- application reflection,
- manual type assertion

is required.

---

# 21. Meaning of `State(...)(value, ok)`

`ok == true` means the referenced Step successfully executed forward for this Run and committed State.

`ok == false` means no committed State exists.

Reasons include:

- the Step has not run yet,
- the Step was retired before the Run entered it,
- the Step was introduced after this Run passed its position,
- the Step was removed,
- the Step was attempted but never successfully completed,
- historical topology evolution caused it not to execute.

Historical compatibility of dynamic State reads belongs to application code.

---

# 22. Dynamic State compatibility

A handler MAY explicitly support multiple pipeline generations:

```go
network, ok := inv.State(machines.ConfigureNetworkStep)
if ok {
    return h.createWithNetwork(ctx, network)
}

return h.createLegacy(ctx)
```

If changing a handler's State assumptions makes it incompatible with existing Runs, the application SHOULD use a new StepID or otherwise retain compatible behavior.

The Engine cannot infer arbitrary Go-level State dependencies.

---

# 23. Handler result semantics

Success:

```go
return nil
```

or:

```go
return state, nil
```

means the operation resolved successfully.

Ordinary error:

```go
return err
```

means the operation remains unresolved and MUST be retried.

Permanent failure:

```go
return durable.Fail(err)
```

means the operation permanently resolves as failure.

---

# 24. Forward permanent failure

During `Run`:

```text
durable.Fail(err)
    -> resolve current forward operation as permanent failure
    -> establish RootFailure
    -> begin unwind
```

The failing Step is not successfully completed.

It therefore does not become eligible for unwind solely because it failed.

---

# 25. Partial-effect responsibility

A Step that returns `durable.Fail` is responsible for its own partial uncommitted effects.

Example:

```text
A ✓
B ✓
C creates partial X
C -> durable.Fail
```

Unwind considers successful Steps, not C's incomplete operation.

So C must reconcile X before permanently failing.

---

# 26. Failure representation

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

Arbitrary Go error object identity is not durable.

---

# 27. Outcome

Terminal Run Outcome is intentionally small:

```go
type Outcome uint8

const (
    OutcomeSuccess Outcome = iota + 1
    OutcomeFailure
)
```

A failure may have zero or more permanent unwind failures.

---

# 28. Result

Conceptually:

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

---

# 29. Pipeline Output

A Pipeline MAY declare an Output protobuf type.

Example:

```proto
message ProvisionMachineOutput {
  string machine_id = 1;
  string host_id = 2;
}
```

Output is distinct from:

- Input,
- Step State,
- execution Result.

---

# 30. Reducer

Pipeline Output is produced through a pure Reducer:

```text
Pipeline Input
+
committed Step States
        |
        v
      Reducer
        |
        v
 Pipeline Output
```

For:

```proto
message ProvisionMachine {
  option (durable.v1.pipeline) = {
    ...
    output: ".machines.v1.ProvisionMachineOutput"
  };
}
```

the generated Reducer is conceptually:

```go
type ProvisionMachineReducer func(
    *ProvisionMachine,
) *ProvisionMachineOutput
```

---

# 31. Pipeline marker as Reducer input

The ordinary generated pipeline type:

```go
*machines.ProvisionMachine
```

is used as the read-only Reducer input.

`protoc-gen-durable` adds methods to it:

```go
func (p *ProvisionMachine) Input() *ProvisionMachineInput

func (p *ProvisionMachine) State[T proto.Message](
    step durable.StateStepRef[T],
) (T, bool)
```

Example:

```go
func reduceProvisionMachine(
    p *machines.ProvisionMachine,
) *machines.ProvisionMachineOutput {
    machine, ok := p.State(machines.CreateMachineStep)
    if !ok {
        panic("create-machine state missing")
    }

    host, _ := p.State(machines.SelectHostStep)

    return &machines.ProvisionMachineOutput{
        MachineId: machine.MachineId,
        HostId:    host.HostId,
    }
}
```

No separate `ProvisionMachineReduceInput` type is generated.

---

# 32. Reducer contract

A Reducer MUST be:

- deterministic relative to durable input/state,
- side-effect free,
- synchronous,
- non-failing by contract.

A Reducer MUST NOT:

- perform external I/O,
- schedule work,
- mutate durable state,
- return ordinary errors,
- return `durable.Fail`.

External/retryable work belongs in a Step.

---

# 33. Reducer compatibility

Reducers use the currently registered implementation.

Historical Runs may contain different sets of committed Step States.

Reducer code is therefore responsible for interpreting:

```go
state, ok := p.State(step)
```

compatibly across Runs associated with the same PipelineID.

An incompatible Output or Reducer contract SHOULD use a new PipelineID.

---

# 34. Reducer execution and durability

The Reducer runs only after forward execution has successfully reached the end of the applicable current topology.

Reducer output is persisted as immutable Pipeline Output.

If the process crashes before output is committed, the Reducer MAY execute again.

This is safe because the Reducer is pure.

A Reducer panic is recovered and treated as unresolved internal execution, not semantic pipeline failure.

---

# 35. Execution ledger

A Run does NOT persist an immutable materialized pipeline topology.

Instead it persists durable execution facts.

Conceptually, the Run contains an **execution ledger** recording forward operations that actually began and how they resolved.

For example:

```text
A:
    forward succeeded

B:
    forward attempted
    currently unresolved
    attempt = 4

C:
    no ledger entry
```

The exact storage representation is implementation-defined.

---

# 36. Forward execution states

Conceptually, a Step may be in one of these Run-relative forward conditions:

```text
NotStarted

Unresolved
    handler has executed at least once
    no successful or permanent resolution yet

Succeeded

PermanentlyFailed
```

The implementation does not have to expose this exact enum.

The distinction between:

```text
never executed
```

and:

```text
executed but unresolved
```

MUST be durable.

---

# 37. Mutable topology

Pipeline definitions MAY change while Runs remain nonterminal.

The current registered Pipeline definition determines which Steps are structurally present for future execution.

The execution ledger determines which operations have already happened for a particular Run.

Forward scheduling reconciles:

```text
current topology
+
Run execution ledger
=
next applicable forward operation
```

---

# 38. Forward execution frontier

Successfully resolved execution establishes a monotonic forward frontier.

Suppose the Run has successfully executed:

```text
A
B
```

and current topology changes from:

```text
A -> B -> C
```

to:

```text
A -> B -> X -> C
```

Then X is after the frontier and executes next:

```text
A ✓ -> B ✓ -> X -> C
```

If topology becomes:

```text
A -> X -> B -> C
```

then X lies before the already-crossed frontier and MUST NOT retroactively execute:

```text
A ✓ -> X skipped historically -> B ✓ -> C
```

---

# 39. Adding Steps

A new Step inserted after the Run's forward frontier MAY execute for that existing Run.

Example:

```text
old:
A -> B -> D

run:
A ✓
B ✓

new:
A -> B -> C -> D
```

Existing Run proceeds:

```text
C -> D
```

A new Step inserted before the forward frontier is not retroactively executed.

---

# 40. Retired Steps

A Step MAY be marked:

```proto
retired: true
```

Example:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
    retired: true
  };

  string reservation_id = 1;
}
```

A retired Step remains structurally present in the Pipeline but no **new forward operation** may begin for it.

Retirement is the intermediate lifecycle stage before removal.

---

# 41. Retired forward semantics

If a retired Step has never begun forward execution for the Run:

```text
NotStarted + retired
```

the Engine skips it.

No handler invocation occurs.

No successful-forward ledger record is fabricated.

No State is created.

If the Step's forward handler has already executed at least once and remains unresolved:

```text
Unresolved + retired
```

the Engine MUST continue retrying it normally until it:

```text
succeeds
```

or:

```text
returns durable.Fail
```

Retirement MUST NOT abandon already-started operations.

---

# 42. Retired Step example

Initial topology:

```text
A -> B -> C
```

Run 1:

```text
A ✓
B attempt 4, unresolved
```

B becomes retired:

```text
A -> B(retired) -> C
```

Run 1 continues:

```text
B attempt 5
B attempt 6
...
```

until B resolves.

Another Run that has not entered B:

```text
A ✓
B not started
```

skips B and proceeds directly to C.

---

# 43. Step removal lifecycle

Step removal SHOULD occur in two phases.

Initial:

```text
A -> B -> C
```

Retirement:

```text
A -> B(retired) -> C
```

Removal after relevant Runs have drained:

```text
A -> C
```

This preserves B as a positional anchor while already-started forward operations resolve.

There is no separate Tombstone abstraction.

---

# 44. Removing an unstarted Step

If B is retired before a given Run ever executes it:

```text
A ✓
B not started
```

the Run skips B.

Once B is later removed entirely:

```text
A -> C
```

the Run is already compatible with that topology.

---

# 45. Removing an unresolved Step

A Step with an unresolved forward operation MUST NOT be removed directly if existing Runs may still require that operation.

It should first be retired.

Example:

```text
A ✓
B attempt 3 unresolved
```

Deploy:

```text
A -> B(retired) -> C
```

B continues retrying.

Only after B resolves and no active Run requires B as a forward anchor should the Step be removed.

---

# 46. No tombstones

Draft 1.0 removes tombstones.

The lifecycle is instead:

```text
active
    |
    v
retired
    |
    | relevant active executions drain
    v
removed
```

Retirement communicates:

> Keep this Step in structural topology for compatibility, but do not begin new forward executions.

---

# 47. Unwind eligibility

A Step participates in unwind for a Run only if ALL are true:

1. its forward operation successfully completed for that Run,
2. the Step is present in the current Pipeline definition,
3. it currently declares `unwind: true`,
4. its unwind operation has not already durably resolved.

Therefore unwind is determined by:

```text
current pipeline topology
+
successful-forward execution ledger
+
resolved-unwind ledger
=
current unwind work
```

---

# 48. Retired Steps during unwind

`retired` affects forward entry only.

It does NOT itself suppress unwind.

If:

```text
B(retired)
unwind=true
```

and B successfully executed forward for this Run, B participates in unwind.

If B was retired before its forward operation ever executed for this Run, B does NOT participate in unwind.

---

# 49. Retired unwind examples

B executed before retirement:

```text
run:
A ✓
B ✓
D fails

current:
A -> B(retired) -> D
```

Unwind includes:

```text
B.Unwind
A.Unwind
```

B never executed because it was already retired:

```text
run:
A ✓
B skipped
C ✓
D fails

current:
A -> B(retired) -> C -> D
```

Unwind:

```text
C.Unwind
A.Unwind
```

B.Unwind is skipped.

---

# 50. Newly added Steps during unwind

A Step added to the current Pipeline after a Run passed that position does NOT unwind unless it successfully executed forward for that Run.

Example:

```text
old execution:
A ✓ -> B ✓ -> D fails
```

Current topology:

```text
A -> X -> B -> C -> D
```

Run ledger contains successful:

```text
A
B
```

but not:

```text
X
C
```

Therefore unwind candidates are only:

```text
B
A
```

assuming both currently declare `unwind: true`.

Dynamic topology alone does not fabricate historical execution.

---

# 51. Removed Steps during unwind

If a Step successfully executed for a Run but is later removed from the current Pipeline, it no longer participates in unwind.

Example:

```text
historical forward:
A ✓
B ✓
C ✓
D fails
```

Current topology:

```text
A -> C -> D
```

Unwind considers:

```text
C
A
```

B is absent and therefore skipped.

No persisted `UnwindRequired` flag is needed.

The current Pipeline definition controls whether an existing successful Step remains unwindable.

---

# 52. Unwind ordering

Eligible unwind Steps execute in reverse order according to the **current Pipeline topology**.

Example successful execution ledger:

```text
A
B
C
```

Current topology:

```text
A -> B -> X -> C -> D
```

X was never successful.

Eligible:

```text
A
B
C
```

Reverse current topology order:

```text
C
B
A
```

---

# 53. Unwind operation semantics

For an eligible Step:

```go
Unwind(ctx, inv, failure)
```

returning:

```text
nil
    -> unwind succeeds

ordinary error
    -> retry same unwind operation

durable.Fail(err)
    -> permanently fail this unwind operation
    -> record UnwindFailure
    -> continue to earlier eligible Step
```

A permanent unwind failure does not stop the unwind process.

---

# 54. Unwind State access

Unwind handlers use:

```go
state, ok := inv.State(machines.ReserveCapacityStep)
```

No own-State argument is passed separately.

For a state-producing Step eligible for unwind, own State should normally exist because eligibility requires successful forward execution.

Dynamic State access is also available for other successful Step States.

---

# 55. Unwind liveness

Every Step that becomes eligible for unwind MUST eventually resolve as:

```text
unwind success
```

or:

```text
permanent unwind failure
```

Ordinary unwind errors leave it unresolved and retryable.

---

# 56. Unwind ledger

The Run persists unwind execution facts independently from forward execution.

Conceptually:

```text
C:
    unwind succeeded

B:
    unwind permanently failed

A:
    unwind unresolved
```

The Engine recomputes eligible current-topology unwind candidates and excludes already-resolved operations.

---

# 57. Topology changes during unwind

Because unwind uses the current topology intersected with successful forward execution history, topology changes may affect remaining unwind work.

Removing a Step before its unwind begins causes it to be skipped.

Changing `unwind:true` to `unwind:false` causes it to stop participating if its unwind has not already resolved.

Changing `unwind:false` to `unwind:true` MAY cause it to participate if:

- it successfully executed forward for this Run,
- it remains before the failure boundary,
- its unwind has not resolved.

These changes are application semantic decisions.

---

# 58. Root failure boundary

The Step that permanently failed forward establishes the RootFailure and marks the transition from forward execution to unwind.

Its `StepID` is persisted as part of `RootFailure`.

The failing Step itself is not eligible for unwind because it did not successfully complete.

Retirement/removal procedures SHOULD avoid removing structural anchors still needed by nonterminal Runs until those Runs have drained.

---

# 59. Phase

Conceptually:

```go
type Phase uint8

const (
    PhaseForward Phase = iota + 1
    PhaseUnwind
    PhaseDone
)
```

`Invocation.Phase()` returns the current phase.

---

# 60. Scheduler state

Scheduler state is distinct:

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
PhaseForward + WaitingRetry
PhaseUnwind + Running
PhaseDone + Done
```

---

# 61. Status

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

Exact exported representation may evolve.

---

# 62. Pipeline construction

Example:

```go
definition := machines.NewProvisionMachine(
    &validate{},
    &selectHost{},
    &reserveCapacity{},
    &createMachine{},
    reduceProvisionMachine,
)
```

The Definition knows:

- PipelineID,
- Input type,
- Output type,
- current ordered Step topology,
- current retirement flags,
- current unwind capabilities,
- active handlers,
- Reducer.

---

# 63. Bind

```go
provision, err := machines.NewProvisionMachine(
    ...,
).Bind(engine)
```

returns:

```go
*machines.ProvisionMachinePipeline
```

Binding is allowed only before `Engine.Start`.

---

# 64. Bound Pipeline API

A bound Pipeline exposes:

```text
Schedule
Active
Runs
Run
```

Example:

```go
run, created, err := provision.Schedule(
    ctx,
    resourceID,
    input,
)
```

---

# 65. Typed Run

Output-producing Pipelines use generated typed Runs:

```go
type ProvisionMachineRun struct {
    // wraps durable.Run
}
```

Methods:

```go
func (r ProvisionMachineRun) ID() durable.RunID

func (r ProvisionMachineRun) Status(
    context.Context,
) (durable.Status, error)

func (r ProvisionMachineRun) Wait(
    context.Context,
) (ProvisionMachineResult, error)
```

---

# 66. Typed Result

```go
type ProvisionMachineResult struct {
    durable.Result
}

func (r ProvisionMachineResult) Output() *ProvisionMachineOutput
```

Successful Run has non-nil Output.

Failed Run has no Output.

---

# 67. Typed Run recovery

```go
run, err := provision.Run(ctx, runID)
```

verifies the Run belongs to `provision-machine`.

Mismatch SHOULD return:

```go
type PipelineMismatchError struct {
    RunID    RunID
    Expected PipelineID
    Actual   PipelineID
}
```

---

# 68. Run handle

Plain `durable.Run` is an in-process handle:

```go
type Run struct {
    id     RunID
    engine *Engine
}
```

Core methods:

```go
func (r Run) ID() RunID
func (r Run) Wait(context.Context) (Result, error)
func (r Run) Status(context.Context) (Status, error)
```

The handle itself is not persistent state.

---

# 69. Wait semantics

```go
result, err := run.Wait(ctx)
```

`err != nil` means operational waiting/query failure such as:

- context cancellation,
- Engine failure,
- lookup failure.

Pipeline semantic failure is represented by:

```go
result.Outcome == durable.OutcomeFailure
```

---

# 70. Engine lifecycle

```text
configuring
    |
    | Start()
    v
running
```

Before `Start`:

- definitions bind,
- handlers register.

After `Start`:

- registration freezes,
- recovery occurs,
- scheduling is accepted.

---

# 71. Single Engine ownership

Exactly one Engine may execute against a Store at a time in v1.

Store implementations SHOULD enforce exclusive ownership where practical.

---

# 72. Engine.Start

`Engine.Start(ctx)` SHOULD:

1. freeze registration,
2. discover nonterminal Runs,
3. validate Pipeline identities,
4. validate Step identities required by unresolved forward operations,
5. validate committed Step State schemas,
6. validate current Reducers,
7. reconstruct execution ledgers,
8. reconcile each Run against current topology,
9. reconstruct retry eligibility,
10. enqueue runnable work.

The Engine does not require historical full topology reconstruction.

---

# 73. Recovery requirements

A nonterminal Run may refer to a Step that no longer accepts new forward execution.

If that Run already has an unresolved forward operation for that Step, an executable handler for the Step MUST remain available until that operation resolves.

This is why retirement precedes physical removal.

---

# 74. Engine-owned execution lifetime

The context supplied to `Schedule` controls only acceptance of the scheduling request.

After durable acceptance, Run execution belongs to the Engine.

Caller context cancellation does not cancel the accepted Run.

---

# 75. Execution concurrency

Exactly one logical operation per Run may execute at once.

Different Runs MAY execute concurrently.

Engine concurrency is globally bounded:

```go
durable.WithConcurrency(32)
```

---

# 76. Immediate continuation

After successful operation resolution, a worker MAY immediately continue the same Run to its next applicable operation.

It releases capacity when:

- retry is required,
- Run becomes terminal,
- shutdown begins,
- Reducer cannot currently complete.

---

# 77. Retry policy

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

Ordinary errors retry indefinitely by default.

Permanent failure is explicit through:

```go
durable.Fail(err)
```

---

# 78. Attempts

Attempt counters belong to one logical operation.

Example:

```text
B.Run
    attempt 1 -> retry
    attempt 2 -> retry
    attempt 3 -> success
```

Retiring B after attempt 2 does not reset or terminate the operation.

It continues with attempt 3.

A Step skipped because it was retired before first execution has no attempt.

---

# 79. Durable retry timing

Retry eligibility survives restart.

If retry is scheduled for 10:00:30 and the Engine restarts at 10:00:05, it SHOULD preserve the 10:00:30 eligibility time.

---

# 80. Shutdown

Graceful shutdown:

- stops new execution work,
- cancels active handler contexts,
- leaves unresolved Runs nonterminal,
- does not establish semantic failure.

A later Engine resumes them.

---

# 81. Crash-loop protection

Unresolved operations discovered after restart MAY receive recovery backoff to prevent repeated process failures from becoming hot loops.

---

# 82. Clock

The Engine SHOULD support:

```go
durable.WithClock(clock)
```

for deterministic tests.

Clock governs:

- retries,
- failure timestamps,
- retry wakeups,
- recovery timing.

---

# 83. Panics

Handler panics SHOULD be recovered.

They are retryable by default and SHOULD record diagnostic stack information.

Reducer panics similarly leave output reduction unresolved.

A panic does not imply `durable.Fail`.

---

# 84. Protobuf evolution

Compatible protobuf field evolution is allowed without automatically changing StepID or PipelineID.

Incompatible semantics require durable identity changes as appropriate.

Pipeline Input and Output are contracts associated with PipelineID.

---

# 85. Durable breaking changes

Examples include:

- changing Step behavior incompatibly under the same StepID,
- changing Step State incompatibly,
- changing Pipeline Input incompatibly under the same PipelineID,
- changing Pipeline Output incompatibly,
- changing dynamic State assumptions incompatibly,
- removing a Step while Runs still have unresolved forward executions requiring it,
- changing Reducer semantics incompatibly with existing Runs.

Some changes are statically detectable. Arbitrary Go semantic compatibility is not.

---

# 86. Code generation

`protoc-gen-durable` generates:

- typed Step handler interfaces,
- typed Invocation types,
- typed Step references,
- Pipeline definition constructors,
- Pipeline runtime methods on marker types,
- typed Reducer functions,
- typed Pipeline handles,
- typed Runs,
- typed Results,
- typed Run lookup,
- runtime adapters.

---

# 87. Generation-time validation

Generation MUST reject:

- missing PipelineID,
- missing StepID,
- duplicate StepID,
- nonexistent referenced Step,
- non-Step message in Pipeline topology,
- empty Pipeline,
- invalid Input type,
- invalid Output type,
- Step reused across active Pipelines,
- invalid handler capability combinations.

Generated Go types SHOULD statically reject:

- missing handlers,
- wrong handler signatures,
- missing required Unwind,
- invalid Reducer signature,
- `State()` on stateless Step references.

---

# 88. Internal type erasure

Generated APIs remain strongly typed at the boundary.

The Engine core may internally operate on:

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
retry metadata
failure metadata

current Pipeline descriptors
internal handlers
internal Reducer
```

The Engine core SHOULD remain non-generic.

---

# 89. Buf

`durable` SHOULD use:

```text
buf lint
buf breaking
buf generate
```

Buf is build/CI tooling only.

Runtime does not depend on Buf.

---

# 90. Example declarations

```proto
message ProvisionMachineInput {
  string region = 1;
  uint64 memory_mb = 2;
}

message ProvisionMachineOutput {
  string machine_id = 1;
  string host_id = 2;
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
  };

  string machine_id = 1;
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

---

# 91. Example handler

```go
func (h *createMachine) Run(
    ctx context.Context,
    inv machines.CreateMachineInvocation,
) (*machines.CreateMachine, error) {
    host, ok := inv.State(machines.SelectHostStep)
    if !ok {
        return nil, durable.Fail(
            errors.New("select-host state unavailable"),
        )
    }

    reservation, ok := inv.State(
        machines.ReserveCapacityStep,
    )
    if !ok {
        return nil, durable.Fail(
            errors.New("reservation state unavailable"),
        )
    }

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
    }, nil
}
```

---

# 92. Example Unwind

```go
func (h *reserveCapacity) Unwind(
    ctx context.Context,
    inv machines.ReserveCapacityInvocation,
    failure durable.Failure,
) error {
    reservation, ok := inv.State(
        machines.ReserveCapacityStep,
    )
    if !ok {
        return nil
    }

    err := h.release(
        ctx,
        reservation.ReservationId,
    )
    if err != nil {
        return err
    }

    return nil
}
```

---

# 93. Example Reducer

```go
func reduceProvisionMachine(
    p *machines.ProvisionMachine,
) *machines.ProvisionMachineOutput {
    machine, ok := p.State(machines.CreateMachineStep)
    if !ok {
        panic("successful pipeline missing machine state")
    }

    host, ok := p.State(machines.SelectHostStep)
    if !ok {
        return &machines.ProvisionMachineOutput{
            MachineId: machine.MachineId,
        }
    }

    return &machines.ProvisionMachineOutput{
        MachineId: machine.MachineId,
        HostId:    host.HostId,
    }
}
```

---

# 94. Example retirement

Initial declaration:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
  };

  string reservation_id = 1;
}
```

First deployment for removal:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
    retired: true
  };

  string reservation_id = 1;
}
```

Pipeline topology remains:

```text
Validate
SelectHost
ReserveCapacity(retired)
CreateMachine
```

Existing Runs already executing `ReserveCapacity` finish it.

Runs that have not entered `ReserveCapacity` bypass it.

Once relevant nonterminal Runs have drained, remove it from the Pipeline definition.

---

# 95. Core invariants

1. `RunID` identifies one exact execution.

2. At most one nonterminal Run exists for `(PipelineID, ResourceID)`.

3. Pipeline Input is immutable.

4. Committed Step State is immutable.

5. Pipeline Output is immutable.

6. Step operations use at-least-once invocation semantics.

7. Handlers MUST be idempotent.

8. Ordinary error means retry.

9. `durable.Fail` means permanent operation failure.

10. Step State exists only after successful forward execution.

11. Failed attempts never establish Step State.

12. Dynamic State lookup is strongly typed.

13. Missing Step State is represented by `ok=false`.

14. Dynamic State compatibility belongs to application code.

15. The failing forward Step is not unwound merely because it failed.

16. A permanently failing handler is responsible for its own partial uncommitted effects.

17. The Run persists execution facts, not an immutable complete topology.

18. Current Pipeline topology determines future structural execution.

19. Forward progress is monotonic.

20. Newly inserted Steps after the frontier may execute.

21. Newly inserted Steps before the frontier do not execute retroactively.

22. A retired Step does not begin new forward operations.

23. An already-started operation continues retrying even after its Step becomes retired.

24. A retired unstarted Step is skipped without fabricating forward success.

25. Retirement is the compatibility phase before Step removal.

26. No tombstone abstraction exists.

27. Unwind eligibility requires successful forward execution for the Run.

28. Unwind eligibility also requires current presence in Pipeline topology.

29. Unwind eligibility also requires current `unwind=true`.

30. Retirement does not by itself disable unwind.

31. A retired Step that never executed forward for a Run is not unwound for that Run.

32. A removed Step is not unwound even if it executed historically.

33. Unwind order follows reverse current Pipeline order among eligible successfully executed Steps.

34. Permanent Unwind failure is recorded and unwind continues.

35. Unwind operations themselves use at-least-once semantics.

36. Forward and unwind execution facts are separately durable.

37. Pipeline Output is produced only after successful forward completion.

38. Failed Runs have no Pipeline Output.

39. Reducers are pure and deterministic relative to durable data.

40. Reducers use the ordinary generated Pipeline marker type as input.

41. Reducers and handlers use the same typed `State()` lookup model.

42. Engine startup owns recovery.

43. Exactly one operation per Run executes at once.

44. Different Runs may execute concurrently.

45. Retry timing survives restart.

46. Shutdown is not semantic Run failure.

47. Handler panics are retryable by default.

48. Exactly one Engine owns a Store in v1.

---

# 96. Future work

## Store contract

Specify:

- transactions,
- active-slot uniqueness,
- run discovery,
- execution-ledger persistence,
- waiters,
- retention,
- SQLite,
- bbolt.

## Persistent representation

Determine exact storage of:

- Input,
- forward ledger,
- unwind ledger,
- attempts,
- State,
- RootFailure,
- UnwindFailures,
- retry timing,
- Output.

## Pipeline evolution validation

Potential tooling to detect:

- unsafe direct removal of Steps still required by active Runs,
- retirement drain status,
- incompatible Step identity reuse,
- output/input incompatibility.

Potential API:

```text
durable inspect retirement reserve-capacity/v1
```

or equivalent programmatic status.

## Cancellation

Define explicit application cancellation separately from Engine shutdown.

## Recovery quarantine

Allow Engine startup while isolating unrecoverable Runs.

## Observability

Add:

- OpenTelemetry,
- retry metrics,
- scheduler metrics,
- Step duration,
- Run duration,
- retirement/drain visibility,
- permanent unwind failure metrics.

## Historical inspection

Potential:

```go
run.History(ctx)
run.Attempts(ctx)
```

## Per-Step retry policy

Support local retry policies while preserving `durable.Fail` as explicit permanent failure.

## Cross-Pipeline Step reuse

Reconsider the v1 one-Step-one-Pipeline restriction.

## Scheduler fairness

Potential:

- priorities,
- per-Pipeline limits,
- resource classes,
- admission policies.

---

# 97. Summary model

Forward execution:

```text
Current Pipeline
       +
Run Execution Ledger
       |
       v
Reconcile forward frontier
       |
       v
next applicable Step
```

Retirement:

```text
active Step
    |
    v
retired Step

never-started Runs
    -> bypass

already-started Runs
    -> continue retrying until resolved

    |
    v
remove after drain
```

Unwind:

```text
Current Pipeline
       +
Successful Forward Ledger
       +
Resolved Unwind Ledger
       |
       v
eligible unwind Steps
       |
       v
reverse current order
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
committed Step States
       |
       v
    Reducer
       |
       v
immutable Pipeline Output
```

Failure:

```text
ordinary error
    -> retry

durable.Fail during Run
    -> RootFailure
    -> unwind

durable.Fail during Unwind
    -> record permanent UnwindFailure
    -> continue backward
```

The core durable representation is therefore no longer an immutable workflow topology.

It is:

> **immutable execution facts interpreted against the currently registered linear Pipeline definition.**

That allows pipeline definitions to evolve naturally while retaining enough durable history to distinguish work that never started, work already in progress, successful work that may need unwind, and work that has permanently failed.