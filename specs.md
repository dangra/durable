# `durable`: Durable Linear Pipelines with Unwind Semantics

**Status:** Draft 1.1  
**Target:** Go 1.27+  
**Persistence:** Local transactional database such as SQLite or bbolt  
**Schema and code generation:** Protocol Buffers, Buf, and `protoc-gen-durable`

# 1. Overview

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

# 2. Design principle

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

# 3. Scope

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

# 4. Core identities

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

# 5. Resource slots

The scheduling slot is:

```text
(PipelineID, ResourceID)
```

At most one nonterminal Run may occupy a slot.

Different Pipelines MAY operate concurrently on the same `ResourceID`.

Cross-Pipeline exclusion is outside v1.

---

# 6. Run

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

# 7. StepID

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

# 8. Step ownership

In v1, one durable Step declaration belongs to exactly one active Pipeline.

Cross-Pipeline durable Step reuse is not supported.

This permits lifecycle properties such as `retired` to live directly on the Step declaration.

Reusable application logic SHOULD live in ordinary Go helpers or services called by separate durable handlers.

---

# 9. Protobuf declarations

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

Published protobuf extensions MUST use globally allocated extension numbers.

---

# 10. Pipeline declaration

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

Pipeline marker messages SHOULD NOT contain ordinary protobuf fields.

The generated protobuf type:

```go
*machines.ProvisionMachine
```

also acts as the read-only input to the Pipeline Reducer.

---

# 11. Pipeline Input

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

# 12. Scheduling lifecycle

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

# 13. Duplicate scheduling

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

# 14. Step declaration

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

Step capabilities are determined by:

```text
protobuf fields present
    -> successful Run produces Step State

unwind=true
    -> successful forward execution may later unwind

retired=true
    -> no new forward operation may begin
```

---

# 15. Step State

The Step protobuf message is also its durable State schema.

Example:

```proto
message SelectHost {
  option (durable.v1.step) = {
    id: "select-host/v1"
  };

  string host_id = 1;
}
```

A successful handler:

```go
return &machines.SelectHost{
    HostId: host.ID,
}, nil
```

commits immutable `SelectHost` Step State.

Step State exists only after successful forward completion.

---

# 16. Step references

A state-producing Step receives a typed generated reference:

```go
var SelectHostStep durable.StateStepRef[*SelectHost]
```

A stateless Step receives:

```go
var ValidateStep durable.StepRef
```

Only `StateStepRef[T]` is accepted by State lookup.

Therefore:

```go
inv.State(machines.ValidateStep)
```

MUST fail to compile.

---

# 17. Generic State API

`durable` targets Go 1.27+ and uses generic methods on generated concrete types.

Example:

```go
func (inv CreateMachineInvocation) State[T proto.Message](
    step durable.StateStepRef[T],
) (T, bool)
```

and:

```go
func (p *ProvisionMachine) State[T proto.Message](
    step durable.StateStepRef[T],
) (T, bool)
```

The library does NOT require generic methods on interfaces.

Application usage:

```go
host, ok := inv.State(machines.SelectHostStep)
```

infers:

```go
host // *machines.SelectHost
```

No:

- `any`,
- protobuf `Any`,
- manual reflection,
- application type assertions

are required.

---

# 18. State lookup semantics

```go
state, ok := inv.State(step)
```

returns `ok == true` only when the referenced Step successfully completed forward for this Run and committed State.

`ok == false` may mean:

- Step has not executed,
- Step was retired before this Run entered it,
- Step was inserted behind the Run's forward frontier,
- Step was removed,
- Step was attempted but never successfully completed.

Historical compatibility of dynamic State reads belongs to application code.

Example:

```go
network, ok := inv.State(machines.ConfigureNetworkStep)
if ok {
    return h.createWithNetwork(ctx, network)
}

return h.createLegacy(ctx)
```

---

# 19. Defensive-copy semantics

Pipeline Input and Step State are immutable durable values.

Any value returned through:

```go
inv.Input()
inv.State(...)
pipeline.Input()
pipeline.State(...)
```

MUST be caller-owned.

Mutating the returned protobuf value MUST NOT affect:

- persisted durable data,
- other handlers,
- subsequent lookups,
- Reducer inputs.

A fresh unmarshal or equivalent defensive copy is the natural implementation.

Likewise, after a handler returns a State value, later mutation of the original Go pointer MUST NOT alter the committed State.

---

# 20. Step handler capability matrix

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

An Unwind handler obtains its own State through:

```go
state, ok := inv.State(machines.ReserveCapacityStep)
```

No separate State parameter is passed.

---

# 21. Successful State boundary

For a state-producing handler:

```text
(state != nil, nil)
    -> serialize/copy State
    -> atomically commit State + forward success
    -> advance
```

Ordinary error:

```text
(_, err)
    -> discard returned State
    -> retry
```

Permanent failure:

```text
(_, durable.Fail(err))
    -> discard State
    -> RootFailure
    -> unwind
```

A state-producing handler returning:

```go
return nil, nil
```

violates the generated runtime contract.

This MUST NOT commit success.

The Run becomes invalid for the current deployment.

A corrected deployment MAY retry the unresolved operation.

Serialization failure of a supposedly successful State is treated similarly as runtime invalidity.

---

# 22. Handler result semantics

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

Runtime invalidity is not equivalent to `durable.Fail`.

---

# 23. Partial-effect responsibility

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

# 24. Failure persistence

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

# 25. Failure during Unwind

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

# 26. Outcome

```go
type Outcome uint8

const (
    OutcomeSuccess Outcome = iota + 1
    OutcomeFailure
)
```

No other terminal business outcomes exist in v1.

---

# 27. Result

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

---

# 28. Pipeline Output

A Pipeline MAY declare Output:

```proto
message ProvisionMachineOutput {
  string machine_id = 1;
  string host_id = 2;
}
```

Output is distinct from:

- Pipeline Input,
- Step State,
- execution Result.

---

# 29. Reducer

Pipeline Output is produced by a pure Reducer:

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

Generated type:

```go
type ProvisionMachineReducer func(
    *ProvisionMachine,
) *ProvisionMachineOutput
```

---

# 30. Pipeline marker as Reducer input

The generated protobuf Pipeline type becomes a read-only reduction view:

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

---

# 31. Reducer contract

A Reducer MUST be:

- deterministic relative to durable Input and State,
- side-effect free,
- synchronous,
- non-failing by contract.

It MUST NOT:

- perform external I/O,
- mutate durable state,
- schedule work,
- return ordinary errors,
- return `durable.Fail`.

External or retryable work belongs in a normal Step.

---

# 32. Reducer runtime failure

Reducer implementation code is not durably versioned with the Run.

If a Reducer:

- panics,
- encounters an impossible runtime contract condition,
- cannot interpret persisted data under the current application definition,

the Run becomes invalid for the current deployment.

It is not retried continuously.

The Engine logs the condition and stops scheduling the Run.

A corrected deployment may later reconcile the same nonterminal Run and execute the new Reducer successfully.

Example:

```text
bad reducer deployed
    -> Run invalid

corrected reducer deployed
    -> Run becomes valid
    -> reduce
    -> commit Output
    -> terminal success
```

---

# 33. Reducer durability

Reducer Output becomes immutable Pipeline Output only after durable commit.

If the process crashes after Reducer execution but before commit, reduction MAY execute again.

Purity makes this safe.

---

# 34. Terminality boundary

A Run remains subject to current-topology reconciliation until terminal success is durably committed.

For an Output-producing Pipeline:

```text
forward Steps complete
Reducer executes
Output + OutcomeSuccess commit
    -> terminal
```

For an Output-less Pipeline:

```text
forward Steps complete
OutcomeSuccess commit
    -> terminal
```

Until that commit, the Run remains extendable by compatible Pipeline changes.

Example:

```text
A -> B -> C
A ✓ B ✓ C ✓
process crashes before Output commit
```

New topology:

```text
A -> B -> C -> E
```

On recovery, E is after the forward frontier and MAY execute before reduction.

After terminal success commits, later Pipeline changes never alter the completed Run.

---

# 35. Execution ledger

A Run does NOT persist an immutable full Pipeline topology.

It persists execution facts.

Conceptually:

```text
A:
    forward succeeded

B:
    forward attempt reserved
    unresolved
    attempt = 4

C:
    no execution record
```

The exact physical representation is implementation-defined.

---

# 36. Forward operation states

Conceptually:

```text
NotStarted

Unresolved
    at least one durable invocation reservation exists
    no successful or permanent resolution

Succeeded

PermanentlyFailed
```

The distinction between:

```text
never started
```

and:

```text
started but unresolved
```

MUST survive restart.

---

# 37. Current topology reconciliation

Forward scheduling combines:

```text
current Pipeline topology
+
Run execution ledger
+
monotonic forward frontier
=
next applicable operation
```

Reconciliation selects new work only when there is no unresolved forward operation.

---

# 38. Unresolved operation pinning

An unresolved forward operation pins the Run.

If:

```text
A ✓
B unresolved
```

the Engine MUST continue B until B resolves.

Topology reconciliation MUST NOT select another forward Step in the meantime.

Example:

```text
old:
A -> B -> C

Run:
A ✓
B attempt 3 unresolved

new:
A -> X -> B -> C
```

B remains current:

```text
B attempt 4
B attempt 5
...
```

After B succeeds, X lies behind the effective frontier and does not execute.

---

# 39. Effective pinned frontier

The current position of a pinned unresolved Step acts as the effective forward frontier for topology evolution.

A Step inserted at or before that position MUST NOT subsequently execute for this Run.

Example:

```text
A ✓ -> B unresolved | future
```

New:

```text
A -> X -> B -> C
```

X is historical for this Run.

New:

```text
A -> B -> X -> C
```

X remains future work and may execute after B resolves.

If the unresolved Step no longer exists in the current Pipeline definition, the Run is invalid.

---

# 40. Forward frontier

When there is no unresolved operation, the forward frontier is the maximum current-topology position occupied by successfully executed Steps that can be reconciled against the current Pipeline.

Example ledger:

```text
A ✓
B ✓
```

Current:

```text
A -> X -> B -> C
```

B is the greatest-position successful Step.

Therefore:

```text
A ✓ -> X skipped -> B ✓ | C
```

C is next.

Current:

```text
A -> B -> X -> C
```

X is after the frontier and executes.

If no safe frontier can be reconciled from persisted execution facts and the current Pipeline definition, the Run becomes invalid.

---

# 41. Adding Steps

A Step inserted after the forward frontier MAY execute for an existing nonterminal Run.

A Step inserted at or before the frontier MUST NOT execute retroactively.

Example:

```text
old:
A -> B -> D

Run:
A ✓
B ✓

new:
A -> B -> C -> D
```

C executes.

But:

```text
new:
A -> C -> B -> D
```

C is behind B's frontier and is skipped for this Run.

---

# 42. Reordering Steps

Existing Steps MAY be reordered.

Reordering affects only work that remains ahead of the Run's monotonic execution frontier.

Example:

```text
ledger:
A ✓
B ✓

old:
A -> B -> C -> D

new:
A -> C -> B -> D
```

B is the maximum-position successful Step.

C is now behind the frontier and does not execute for that existing Run.

Reordering MUST NOT cause forward execution to move backward across the frontier.

Reordering is not inherently a durable breaking change.

If a particular nonterminal Run cannot be safely reconciled after reordering, that Run becomes invalid.

Deploy-time validation of reorder compatibility is state-dependent and belongs to future evolution tooling.

---

# 43. Retired Steps

A Step MAY declare:

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

Retirement means:

> Keep this Step structurally present, but do not begin new forward operations.

It is the intermediate stage before Step removal.

---

# 44. Retired forward semantics

If the Step never started for a Run:

```text
NotStarted + retired
    -> bypass
```

No handler invocation occurs.

No State is created.

No synthetic success ledger entry is fabricated.

If it has already started:

```text
Unresolved + retired
    -> continue retrying
```

until it:

```text
succeeds
```

or:

```text
returns durable.Fail
```

Retirement MUST NOT abandon an already-started operation.

---

# 45. Retirement observability consequence

A retired Step that is bypassed without invocation leaves no forward execution ledger entry.

Therefore absence alone may not distinguish:

- Step did not yet exist,
- Step was retired and bypassed,
- Step was behind the frontier,
- Step was otherwise never executed.

v1 stores execution facts, not synthetic traversal/skip events.

Future `History()` APIs may expose only that the Step did not execute unless richer traversal events are introduced.

---

# 46. Step removal lifecycle

Normal removal:

```text
active
    |
    v
retired
    |
    | relevant Runs drain
    v
removed
```

Example:

```text
A -> B -> C
```

then:

```text
A -> B(retired) -> C
```

then:

```text
A -> C
```

No tombstone abstraction exists.

---

# 47. Removing an unresolved Step

Direct removal of an unresolved Step may invalidate existing Runs.

Example:

```text
Run:
A ✓
B unresolved

current topology:
A -> C
```

The Engine cannot safely resume B.

That Run becomes invalid.

Correct procedure:

```text
A -> B(retired) -> C
```

allow B to resolve, then remove B after relevant Runs drain.

---

# 48. StepID migration recipe

Suppose `reserve-capacity/v1` must be replaced by incompatible `v2`.

First introduce v2 while retiring v1:

```proto
message ReserveCapacityV1 {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
    retired: true
  };

  string reservation_id = 1;
}

message ReserveCapacityV2 {
  option (durable.v1.step) = {
    id: "reserve-capacity/v2"
    unwind: true
  };

  string reservation_id = 1;
  string host_id = 2;
}
```

Temporary topology:

```text
... ->
ReserveCapacityV1(retired) ->
ReserveCapacityV2 ->
...
```

Runs already executing v1 continue v1 until it resolves.

Runs that never entered v1 bypass it and execute v2.

After no relevant nonterminal Run requires v1, remove v1:

```text
... -> ReserveCapacityV2 -> ...
```

---

# 49. Unwind eligibility

A Step participates in unwind only when ALL are true:

1. it successfully completed forward for this Run,
2. it remains present in the current Pipeline definition,
3. it currently declares `unwind: true`,
4. its position has not been crossed by the monotonic unwind frontier,
5. its unwind operation has not already resolved.

So:

```text
current topology
+
successful forward ledger
+
unwind ledger
+
unwind frontier
=
next unwind work
```

---

# 50. Retired Steps during unwind

`retired` affects only new forward entry.

It does not suppress unwind.

If a retired Step successfully executed forward, it may unwind.

If it was retired and bypassed before ever running, it does not unwind.

Example:

```text
A ✓
B skipped because retired
C ✓
D fails
```

Unwind candidates do not include B.

---

# 51. Removed Steps during unwind

A successfully executed Step that is removed from the current Pipeline no longer participates in future unwind work.

Example historical ledger:

```text
A ✓
B ✓
C ✓
D failed
```

Current Pipeline:

```text
A -> C -> D
```

Unwind considers:

```text
C
A
```

B is absent.

No persisted `UnwindRequired` flag is used.

---

# 52. Newly added Steps during unwind

A newly added Step does not unwind merely because it exists in the current Pipeline.

It must have successfully executed forward for this Run.

Example:

```text
ledger:
A ✓
B ✓
D failed
```

Current topology:

```text
A -> X -> B -> C -> D
```

X and C were never successfully executed.

They do not unwind.

---

# 53. Unwind ordering

Eligible Steps unwind in reverse **current Pipeline order**, constrained by a monotonic backward frontier.

Example successful ledger:

```text
A
B
C
```

Current:

```text
A -> C -> B -> D
```

If unwind has not yet traversed those positions, eligible order is:

```text
B
C
A
```

Current topology intentionally determines future unwind ordering.

---

# 54. Monotonic unwind frontier

Unwind progress is monotonic backward.

Once traversal passes a Pipeline position, work that later becomes eligible at or beyond that already-traversed position MUST NOT execute for the Run.

Example:

```text
A -> C -> D
```

C unwinds, then A unwinds.

Later topology becomes:

```text
A -> B -> C -> D
```

B does not retroactively unwind because the unwind frontier has already crossed its position.

Similarly, changing:

```proto
unwind: false
```

to:

```proto
unwind: true
```

only affects a Step if its position has not yet been crossed.

---

# 55. Unwind frontier reconciliation

The Engine persists enough unwind progress to reconcile the monotonic backward frontier against the current topology.

Pipeline evolution may affect only work still ahead of that frontier.

If the frontier cannot be safely reconciled against the current Pipeline, the Run becomes invalid.

No complex historical migration algorithm is required.

---

# 56. Unwind operation semantics

For an eligible Step:

```text
Unwind -> nil
    success, advance backward

Unwind -> ordinary error
    unresolved, retry

Unwind -> durable.Fail(err)
    permanent UnwindFailure
    record
    continue backward
```

A permanent unwind failure does not stop remaining unwind.

---

# 57. Unwind State access

Unwind receives:

```go
Unwind(
    ctx,
    inv,
    failure,
)
```

State is obtained dynamically:

```go
reservation, ok := inv.State(
    machines.ReserveCapacityStep,
)
```

For a state-producing Step that is eligible for unwind, own State normally exists because eligibility requires successful forward completion.

---

# 58. Forward and unwind monotonicity

Pipeline evolution MUST NOT cause execution to move backward across either frontier.

Symmetry:

```text
new Step at/before forward frontier
    -> do not Run retroactively

newly unwind-eligible Step behind unwind frontier
    -> do not Unwind retroactively
```

---

# 59. Attempt numbering

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

# 60. Retry semantics

Ordinary errors are retried indefinitely.

There is no max-attempt exhaustion concept in v1.

Permanent semantic failure requires explicit:

```go
durable.Fail(err)
```

A future bounded retry feature would need separate exhaustion semantics and is outside v1.

---

# 61. Retry policy

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
release worker
schedule wakeup
```

---

# 62. Retry durability

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

# 63. Phase

```go
type Phase uint8

const (
    PhaseForward Phase = iota + 1
    PhaseUnwind
    PhaseDone
)
```

---

# 64. RunState

Conceptually:

```go
type RunState uint8

const (
    RunStateRunnable RunState = iota + 1
    RunStateRunning
    RunStateWaitingRetry
    RunStateInvalid
    RunStateDone
)
```

`RunStateInvalid` means the current application deployment cannot safely continue the nonterminal Run.

It is not a terminal business outcome.

---

# 65. Invalid Runs

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

# 66. Invalidity is deployment-relative

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

# 67. Waiting on invalid Runs

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

# 68. Status

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

Invalid status SHOULD expose enough diagnostic information to identify why execution is blocked.

The exact public shape may use methods or a structured invalid-reason type.

---

# 69. Engine lifecycle

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

# 70. Engine.Start granularity

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

# 71. Single Engine ownership

Exactly one Engine instance may execute against a Store at once in v1.

Store implementations SHOULD enforce or detect exclusive ownership where practical.

---

# 72. Engine recovery

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

# 73. Engine-owned Run lifetime

The context passed to `Schedule` governs the scheduling request only.

Once accepted:

```text
caller context cancellation
    !=
Run cancellation
```

The Engine owns execution lifetime.

Explicit Run cancellation is outside v1.

---

# 74. Concurrency

Exactly one logical operation belonging to a Run may execute at a time.

Different Runs MAY execute concurrently.

Global concurrency:

```go
durable.WithConcurrency(32)
```

---

# 75. Immediate continuation

A worker MAY continue immediately through subsequent successful Steps.

It releases capacity when:

- retry is required,
- Run becomes invalid,
- Run becomes terminal,
- shutdown begins.

---

# 76. Graceful shutdown

Shutdown:

- stops scheduling new operations,
- cancels active handler contexts,
- leaves unresolved Runs nonterminal,
- does not create RootFailure.

A future Engine resumes them.

---

# 77. Crash recovery and backoff

A startup-discovered unresolved operation MAY receive recovery backoff to prevent crash loops.

The system need not persist a distinction between graceful interruption and crash.

---

# 78. Clock

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

# 79. Handler panics

A handler panic SHOULD be recovered.

It is considered an unresolved operation, not `durable.Fail`.

The Engine SHOULD:

- capture stack diagnostics,
- record the failed invocation operationally,
- retry according to normal/recovery policy.

This differs from Reducer panic because a handler operation explicitly supports transient retry semantics.

---

# 80. Typed Pipeline construction

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

The positional constructor is type-safe because each position expects a distinct generated handler interface.

Accidentally swapping two unrelated Step handlers fails to compile.

---

# 81. Bind

```go
provision, err := machines.NewProvisionMachine(
    ...,
).Bind(engine)
```

returns:

```go
*machines.ProvisionMachinePipeline
```

`Bind` is allowed only before `Engine.Start`.

---

# 82. Bound Pipeline API

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

# 83. Typed Run

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

# 84. Typed Result

```go
type ProvisionMachineResult struct {
    durable.Result
}

func (r ProvisionMachineResult) Output() *ProvisionMachineOutput
```

Successful Run:

```text
Output != nil
```

Failed Run:

```text
Output == nil
```

---

# 85. Typed Run recovery

```go
run, err := provision.Run(ctx, runID)
```

MUST verify the Run belongs to the expected Pipeline.

Mismatch:

```go
type PipelineMismatchError struct {
    RunID    RunID
    Expected PipelineID
    Actual   PipelineID
}
```

---

# 86. Plain Run handle

Conceptually:

```go
type Run struct {
    id     RunID
    engine *Engine
}
```

Methods:

```go
func (r Run) ID() RunID
func (r Run) Wait(context.Context) (Result, error)
func (r Run) Status(context.Context) (Status, error)
```

The handle itself is not durable state.

---

# 87. Wait semantics

```go
result, err := run.Wait(ctx)
```

`err != nil` means operational inability to produce a terminal Result, such as:

- caller context cancellation,
- Run lookup failure,
- Engine failure,
- Run invalidity.

Pipeline semantic failure is represented by:

```go
result.Outcome == OutcomeFailure
```

---

# 88. Protobuf evolution

Compatible protobuf wire evolution is allowed.

Compatible Step State changes do not automatically require a new StepID.

Compatible Pipeline Input/Output evolution does not automatically require a new PipelineID.

Semantic compatibility remains the application's responsibility.

---

# 89. Durable breaking changes

Examples include:

- incompatible Step behavior under the same StepID,
- incompatible Step State schema under the same StepID,
- incompatible Pipeline Input under the same PipelineID,
- incompatible Pipeline Output under the same PipelineID,
- dynamic State assumptions incompatible with existing Runs,
- directly removing a Step still required by unresolved operations,
- incompatible Reducer semantics.

A particular topology reorder is not inherently a durable breaking change.

If it cannot be reconciled for an existing Run, that Run becomes invalid.

---

# 90. Code generation

`protoc-gen-durable` generates:

- typed Step handler interfaces,
- typed concrete Invocation types,
- typed Step references,
- generic concrete `State` methods,
- Pipeline constructors,
- Reducer function types,
- runtime methods on Pipeline marker types,
- bound Pipeline handles,
- typed Runs,
- typed Results,
- runtime adapters.

---

# 91. Generation-time validation

Generation MUST reject:

- missing PipelineID,
- missing StepID,
- duplicate StepID,
- nonexistent Step references,
- non-Step messages in Pipeline topology,
- empty Pipelines,
- invalid Input/Output references,
- Step reuse across active Pipelines,
- malformed capability declarations.

Generated APIs MUST make these compile-time errors where possible:

- missing handler,
- wrong handler signature,
- missing required `Unwind`,
- invalid Reducer signature,
- passing a stateless `StepRef` to `State`.

---

# 92. Internal type erasure

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

# 93. Buf

Use Buf for:

```text
buf lint
buf breaking
buf generate
```

Buf is build/CI tooling only.

Runtime does not depend on Buf.

---

# 94. Minimal observability

Full observability is future work, but invalid Runs MUST NOT be silent.

At minimum the Engine SHOULD emit structured diagnostics containing:

```text
RunID
PipelineID
ResourceID
Phase
StepID if applicable
reason
```

Future metrics SHOULD expose values such as:

```text
durable_invalid_runs
durable_invalid_runs_total
durable_invalid_runs{pipeline, reason}
```

Retry observability SHOULD expose:

```text
Attempt
NextAttemptAt
```

---

# 95. Example declarations

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

# 96. Example forward handler

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

# 97. Example Unwind handler

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

    if err := h.release(
        ctx,
        reservation.ReservationId,
    ); err != nil {
        return err
    }

    return nil
}
```

---

# 98. Example Reducer

```go
func reduceProvisionMachine(
    p *machines.ProvisionMachine,
) *machines.ProvisionMachineOutput {
    machine, ok := p.State(machines.CreateMachineStep)
    if !ok {
        panic("create-machine state unavailable")
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

# 99. Core invariants

1. `RunID` identifies one exact execution.

2. At most one nonterminal Run exists for `(PipelineID, ResourceID)`.

3. Pipeline Input is immutable.

4. Committed Step State is immutable.

5. Pipeline Output is immutable.

6. Handler operations use at-least-once invocation semantics.

7. Handlers MUST be idempotent.

8. Ordinary error means retry indefinitely.

9. `durable.Fail` is the explicit semantic permanent-failure mechanism.

10. Runtime contract violations do not become business failures.

11. State exists only after successful forward completion.

12. Successful state-producing handlers MUST return non-nil State.

13. `State(...)(value, true)` implies a non-nil committed durable State exists.

14. State lookup accepts only `StateStepRef[T]`.

15. State lookup is strongly typed through concrete generic methods.

16. State and Input lookups return defensive caller-owned copies.

17. Dynamic State compatibility belongs to application code.

18. The failing forward Step is not automatically unwound.

19. A permanently failing handler owns cleanup of its own partial uncommitted effects.

20. Runs persist execution facts rather than immutable full topology.

21. Current Pipeline topology determines future structural work.

22. Forward progress is monotonic.

23. An unresolved forward operation pins the Run until resolution.

24. No new forward work is selected while an unresolved operation exists.

25. A Step inserted at or before a pinned Step's current position does not execute for that Run.

26. A Step inserted after the forward frontier may execute.

27. A Step inserted at or before the forward frontier does not execute retroactively.

28. Reordering never moves forward execution backward across its frontier.

29. A retired Step does not begin a new forward operation.

30. An already-started Step continues retrying after retirement.

31. A retired unstarted Step leaves no synthetic success record.

32. Retirement is the intermediate lifecycle stage before removal.

33. Directly removing a required unresolved Step may invalidate Runs.

34. No tombstone abstraction exists.

35. Unwind requires successful forward completion.

36. Unwind also requires current presence in Pipeline topology.

37. Unwind also requires current `unwind=true`.

38. Retirement does not itself disable unwind.

39. A retired Step that never executed forward does not unwind.

40. A removed Step does not newly unwind.

41. Unwind ordering follows reverse current Pipeline order.

42. Unwind progress is monotonic backward.

43. A Step becoming unwind-eligible behind the unwind frontier does not execute retroactively.

44. Reordering never moves unwind execution backward across its frontier.

45. Permanent Unwind failure is recorded and unwind continues.

46. `Failure.UnwindFailures` contains permanent unwind failures accumulated so far in execution order.

47. Attempt numbers are durably reserved before handler invocation.

48. Attempt numbers are monotonic and never reused.

49. A reserved attempt may exist even if the process crashes before application code begins.

50. Retry eligibility survives restart.

51. A successful Run remains extendable until terminal success is durably committed.

52. Terminal Runs never change due to later Pipeline evolution.

53. Reducers are pure and deterministic relative to durable data.

54. Reducers and handlers use the same typed State lookup model.

55. Reducer/runtime incompatibility invalidates the Run rather than causing infinite retry.

56. Invalidity is not a Pipeline business outcome.

57. Invalid Runs do not prevent Engine startup.

58. Invalid Runs are ignored for execution and surfaced operationally.

59. A corrected deployment may make an invalid Run runnable again.

60. `Wait` does not silently block forever on known-invalid Runs.

61. Engine startup fails only for Engine-wide configuration/storage problems.

62. Exactly one Engine owns a Store in v1.

63. Exactly one logical operation per Run executes at once.

64. Different Runs may execute concurrently.

65. Shutdown does not create Pipeline failure.

66. Input-declaring Pipelines reject nil scheduling Input.

67. Input-less Pipelines generate `Schedule` without an Input argument.

68. Scheduling before `Engine.Start` fails without creating a Run.

69. Duplicate scheduling uses exact `proto.Equal`, including unknown fields.

70. Positional generated constructors remain compile-time type-safe because every Step position has a distinct generated interface.

---

# 100. Future work

## Store contract

Specify transactional persistence operations for:

- Runs,
- resource-slot uniqueness,
- Input,
- forward execution ledger,
- unwind execution ledger,
- State,
- retry scheduling,
- Failure,
- Output,
- terminal outcome,
- waiters.

Implementations:

- SQLite,
- bbolt.

## Historical inspection

Potential:

```go
run.History(ctx)
run.Attempts(ctx)
```

Because v1 does not persist synthetic skip events, richer traversal observability may require additional event data.

## Administrative cancellation or abandonment

Explicit operator-controlled Run termination remains outside v1.

Its relationship with:

- unwind,
- RootFailure,
- Outcome

must be designed independently.

## Evolution validation tooling

Potential deploy-time tooling could inspect active Runs and identify:

- Steps unsafe to remove,
- retired Steps ready for deletion,
- state-dependent reorder hazards,
- unresolved Step references,
- incompatible current definitions.

## Structured failure details

Future Failure records may include durable structured metadata beyond `Message`.

## Per-Step retry policy

Step-specific retry configuration may be added later.

Any bounded retry mechanism must define exhaustion semantics separately from `durable.Fail`.

## Scheduler fairness

Potential:

- per-Pipeline concurrency,
- priorities,
- admission policies,
- resource classes.

## Cross-Pipeline Step reuse

The one-Step-one-Pipeline restriction may be revisited.

## Full observability

Potential:

- OpenTelemetry,
- scheduler metrics,
- retry metrics,
- invalid-run metrics,
- Step duration,
- Run duration,
- unwind failure metrics,
- retirement drain metrics.

---

# 101. Summary

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