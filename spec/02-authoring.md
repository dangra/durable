# Authoring Pipelines

Part of the [`durable` specification](README.md).

## Pipeline declaration

Example:

```proto
message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"
    input: ".machines.v1.ProvisionMachineInput"
    output: ".machines.v1.ProvisionMachineOutput"
  };

  message Validate { option (durable.v1.step) = {id: "validate/v1"}; }
  message SelectHost { /* ... */ }
  message ReserveCapacity { /* ... */ }
  message CreateMachine { /* ... */ }
}
```

The Steps are the messages nested directly in the pipeline message,
and their declaration order is the topology: inserting, reordering,
or retiring a Step is an edit in one place. A Step belongs to its
pipeline by construction, and two pipelines may name their Steps alike.

Pipeline marker messages SHOULD NOT contain ordinary protobuf fields.

The generated protobuf type:

```go
*machines.ProvisionMachine
```

also acts as the read-only input to the Pipeline Reducer.

---

## Step declaration

A Step is a message nested in its pipeline message. Example:

```proto
message ProvisionMachine {
  option (durable.v1.pipeline) = { /* ... */ };

  message ReserveCapacity {
    option (durable.v1.step) = {
      id: "reserve-capacity/v1"
      unwind: true
    };

    string reservation_id = 1;
    string host_id = 2;
  }
}
```

Its Go type is protoc-gen-go's nested name, `ProvisionMachine_ReserveCapacity`;
its handler method and its StepID are its own.

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

## Step State

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
return &machines.ProvisionMachine_SelectHost{
    HostId: host.ID,
}, nil
```

commits immutable `SelectHost` Step State.

Step State exists only after successful forward completion.

---

## Step references

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
inv.State(machines.ProvisionMachine_ValidateStep)
```

MUST fail to compile.

---

## Generic State API

`durable` targets Go 1.27+ and uses generic methods on generated concrete types.

Example:

```go
func (inv durable.TypedInvocation[In]) State[T proto.Message](
    step durable.StateStepRef[T],
) (T, bool)
```

(`machines.ProvisionMachineInvocation` is the generated alias of
`durable.TypedInvocation[*machines.ProvisionMachineInput]`.)

and:

```go
func (p *ProvisionMachine) State[T proto.Message](
    step durable.StateStepRef[T],
) (T, bool)
```

The library does NOT require generic methods on interfaces.

Application usage:

```go
host, ok := inv.State(machines.ProvisionMachine_SelectHostStep)
```

infers:

```go
host // *machines.ProvisionMachine_SelectHost
```

No:

- `any`,
- protobuf `Any`,
- manual reflection,
- application type assertions

are required.

---

## State lookup semantics

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
network, ok := inv.State(machines.ProvisionMachine_ConfigureNetworkStep)
if ok {
    return h.createWithNetwork(ctx, network)
}

return h.createLegacy(ctx)
```

---

## Defensive-copy semantics

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

## The pipeline handler interface

A pipeline's handlers are one interface: a method per Step, named after
the Step message, plus `Unwind<Step>` for each Step that unwinds, plus
the reducers the pipeline declares. Every method takes the pipeline's
Invocation; a State-producing Step returns its State message.

```go
type ProvisionMachineHandlers interface {
    // no State, no Unwind
    Validate(context.Context, ProvisionMachineInvocation) error
    // State, no Unwind
    SelectHost(context.Context, ProvisionMachineInvocation) (*SelectHost, error)
    // no State, with Unwind
    MarkProvisioning(context.Context, ProvisionMachineInvocation) error
    UnwindMarkProvisioning(context.Context, ProvisionMachineInvocation) error
    // State and Unwind
    ReserveCapacity(context.Context, ProvisionMachineInvocation) (*ReserveCapacity, error)
    UnwindReserveCapacity(context.Context, ProvisionMachineInvocation) error

    ReduceOutput(*ProvisionMachine) *ProvisionMachineOutput
}

func NewProvisionMachine(h ProvisionMachineHandlers, opts ...pipelinedef.Option) *ProvisionMachineDefinition
```

The options configure what the proto cannot declare;
`pipelinedef.WithMiddleware(mw...)` installs pipeline-level middleware,
wrapping this pipeline's operations alone inside any engine-level chain
(see [04-engine](04-engine.md#middleware)).

One type implements the whole pipeline, so its dependencies are declared
once; a Step the implementor lacks, a Step added to the proto included,
is a compile error naming the method.

An Unwind handler obtains its own State through:

```go
state, ok := inv.State(machines.ProvisionMachine_ReserveCapacityStep)
```

and the failure it is unwinding through:

```go
failure := inv.Failure() // *durable.Failure; never nil during unwind
```

Neither is passed as a parameter: an unwind handler is a handler that
observes `PhaseUnwind`, and everything the attempt knows is on the
Invocation. `Failure` is nil during forward attempts, which is how
middleware tells the two apart without a separate handler type.

---

## Invocation accessors

Every handler method receives the pipeline's Invocation,
`durable.TypedInvocation[*Input]`: the core `durable.Invocation` — an
interface the Engine implements and application code never does — with
the Input typed. Beyond typed `Input()` and `State(...)`, it exposes:

```text
PipelineID, ResourceID, RunID, StepID   identity
Phase                                   forward or unwind
Attempt                                 the durable reservation number
Awaited, AwaitedRunID                   the resolved park, if this attempt
                                        follows one (see 01-model)
Annotations                             the Run's acceptance-time metadata
Failure                                 the failure being unwound; nil in forward
Logger                                  a slog.Logger with the canonical keys
```

All return caller-owned copies; none can be used to reach durable state
of other Runs except through the public `Run` handle.

Because the core is an interface, a handler is unit-testable without an
Engine or a Store: `durabletest.NewInvocation` builds a fake from an
`InvocationConfig` (identity, Input, committed States keyed by StepID,
annotations, park memory) that satisfies both `durable.Invocation` and
`durable.ReduceView`, and records the contract violations a real Engine
would invalidate the Run for. Generated code wraps it the same way it
wraps the Engine's: `NewProvisionMachineInvocation(fake)` is what a
`ProvisionMachineHandlers` method takes, and
`ReduceProvisionMachineOutput(h, fake)` folds the reducer over it.

```go
inv := durabletest.NewInvocation(durabletest.InvocationConfig{
    Phase:   durable.PhaseUnwind,
    State:   map[durable.StepID]proto.Message{
        machines.ProvisionMachine_ReserveCapacityStep.ID(): &machines.ProvisionMachine_ReserveCapacity{ReservationId: "res-1"},
    },
    Failure: &durable.Failure{ /* ... */ },
})
err := h.UnwindReserveCapacity(ctx, machines.NewProvisionMachineInvocation(inv))
```

---

## Successful State boundary

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
    -> Failure
    -> unwind
```

A state-producing handler returning:

```go
return nil, nil
```

violates the generated runtime contract.

This MUST NOT commit success.

The Run becomes [invalid for the current deployment](04-engine.md#invalid-runs).

A corrected deployment MAY retry the unresolved operation.

Serialization failure of a supposedly successful State is treated similarly as runtime invalidity.

---

## Pipeline Output

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

## Reducer

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

A Pipeline MAY also declare a failure Output, produced by a pure
**failure Reducer** when a failed Run's unwind completes. The failure
Output relates to the Run's `Failure` and the Step States the way the
Output relates to the States:

```text
Pipeline Input
+
committed Step States
+
the Run's Failure
+
per-step unwind failures
        |
        v
  failure Reducer
        |
        v
 failure Output
```

```proto
option (durable.v1.pipeline) = {
  output:         ".machines.v1.ProvisionMachineOutput"
  failure_output: ".machines.v1.ProvisionMachineFailure"
  ...
};
```

```go
type ProvisionMachineFailureReducer func(
    *ProvisionMachine,
) *ProvisionMachineFailure
```

It runs once, in the transition that commits the failed Outcome, for
every failed Run including canceled ones; the typed `Result` carries its
value through `FailureOutput()`, non-nil exactly when the Run failed.
Either reducer may be declared without the other. Both are positional
arguments of the generated constructor, after the step handlers.

---

## Pipeline marker as Reducer input

The generated protobuf Pipeline type becomes a read-only reduction view,
the same for both reducers:

```go
func (p *ProvisionMachine) Failure() *durable.Failure          // nil on success
func (p *ProvisionMachine) UnwindFailure(step durable.StepIdentifier) (durable.Failure, bool)
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
    machine, ok := p.State(machines.ProvisionMachine_CreateMachineStep)
    if !ok {
        panic("create-machine state missing")
    }

    host, _ := p.State(machines.ProvisionMachine_SelectHostStep)

    return &machines.ProvisionMachineOutput{
        MachineId: machine.MachineId,
        HostId:    host.HostId,
    }
}
```

---

`UnwindFailure` answers per step, keyed like `State`: a failure reducer
pairs the two to say what a failed Run left behind, as in a compensation
that failed permanently for a step whose State names the resource it
created.

---

## Reducer contract

Everything in this section and the next applies to both reducers.

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

## Reducer runtime failure

Reducer implementation code is not durably versioned with the Run.

If a Reducer:

- panics,
- encounters an impossible runtime contract condition,
- cannot interpret persisted data under the current application definition,

the Run becomes [invalid for the current deployment](04-engine.md#invalid-runs).

It is not retried continuously.

The Engine logs the condition and stops scheduling the Run.

A corrected deployment may later reconcile the same nonterminal Run and execute the new Reducer successfully.

The failure Reducer follows the same rule: a fault in it leaves the Run
nonterminal and invalid rather than committing a failed Outcome without
its failure Output. A reducer bug is a definition bug either way, and a
silent drop would be the harder one to notice.

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

## Reducer durability

Reducer Output becomes immutable Pipeline Output only after durable commit.

If the process crashes after Reducer execution but before commit, reduction MAY execute again.

Purity makes this safe.

---

## Typed Pipeline construction

Example:

```go
definition := machines.NewProvisionMachine(&handlers{cloud: c})
```

The constructor takes one implementation of the pipeline's handler
interface: a missing or mis-typed Step method, a Step added to the proto
included, fails to compile. Middleware for this pipeline alone is an
option: `machines.NewProvisionMachine(&handlers{cloud: c}, pipelinedef.WithMiddleware(notFoundIsPermanent))`.

---

## Bind

```go
provision, err := machines.NewProvisionMachine(&handlers{cloud: c}).Bind(eng)
```

returns:

```go
*machines.ProvisionMachinePipeline
```

`Bind` is allowed only before `Engine.Start` (`engine.ErrStarted`
afterwards). The generated `Bind` delegates to `engine.Bind`, which is
the single validator of a definition: an empty or malformed identifier,
a pipeline with no steps or a duplicated step, a step with no forward
adapter, or an unwind declaration that disagrees with its adapter is a
`Bind` error. For generated code such an error indicates a
code-generation bug; construction itself never panics.

---

## Untyped definitions

Beneath every generated constructor is a `pipelinedef.Definition`: the
type-erased description of a pipeline (`pipelinedef.Config`, one
`pipelinedef.Step` per step, the `NewInput` and `Reduce` adapters).
Generated code builds it and hands it to `engine.Bind`; hand-rolled
pipelines — tests, mostly — build the same value directly:

```go
def := pipelinedef.New(pipelinedef.Config{
    ID: "p",
    Steps: []pipelinedef.Step{{
        ID:  "s/v1",
        Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { ... },
    }},
})
pipe, err := eng.Bind(def)
```

`pipelinedef.New` validates nothing; it copies the step list and applies
the pipeline-level concurrency class. The typed step references generated
packages export are built the same way (`pipelinedef.StepRef`,
`pipelinedef.StateStepRef`) and are plain values with exported fields.

---

## Bound Pipeline API

A bound Pipeline exposes:

```text
Schedule
GetRun
GetActiveRun
```

`GetActiveRun` is one indexed read against the store's slot index (the
structure `CreateRun` enforces mutexes with), never a scan. There is
deliberately no listing or history API on a Pipeline: the engine is not
a dashboard, and fleet-wide or historical views belong to the
application's own records.

Example:

```go
run, created, err := provision.Schedule(
    ctx,
    resourceID,
    input,
)
```

`Schedule` accepts options: `StartAfter` / `StartAt` for a
[delayed start](01-model.md#delayed-starts), and `WithAnnotations` for
caller-supplied propagation metadata (trace contexts, tenant tags) —
immutable for the life of the Run, readable through `Run.Annotations` and
`Invocation.Annotations`, and never part of duplicate-scheduling identity.
`WithScheduleAnnotator` on the Engine derives annotations from the
scheduling context for every call, so propagation intent is declared once.

---

## Typed Run

Pipelines declaring an Input or an Output use generated typed Runs — a
typed `Input(ctx)` accessor comes with Input, a typed `Wait` result with
Output:

```go
type ProvisionMachineRun struct {
    // wraps engine.Run
}
```

Methods:

```go
func (r ProvisionMachineRun) ID() durable.RunID

func (r ProvisionMachineRun) Status(
    context.Context,
) (engine.Status, error)

func (r ProvisionMachineRun) Wait(
    context.Context,
) (ProvisionMachineResult, error)
```

---

## Typed Result

```go
type ProvisionMachineResult struct {
    engine.Result
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

## Typed Run recovery

```go
run, err := provision.GetRun(ctx, runID)
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

## Plain Run handle

Conceptually (in package `engine`):

```go
type Run struct {
    id     durable.RunID
    engine *Engine
}
```

Methods:

```go
func (r Run) ID() RunID
func (r Run) Wait(context.Context) (Result, error)
func (r Run) Status(context.Context) (Status, error)
func (r Run) Cancel(context.Context, cause string) error
func (r Run) Annotations(context.Context) (map[string]string, error)
func (r Run) InputBytes(context.Context) ([]byte, error)   // for generated code
func (r Run) OutputBytes(context.Context) ([]byte, error)  // for generated code
```

A handle is obtained from a bound Pipeline (`Schedule`, `GetRun`,
`GetActiveRun`) or from the Engine, for callers that hold only a
`RunID` — an API client polling an operation id:

```go
func (e *Engine) GetRun(context.Context, durable.RunID) (Run, error)
```

`Engine.GetRun` returns the untyped handle whatever pipeline owns the
Run; `Status.PipelineID` names the typed Pipeline to route to when the
typed Output is needed. `Pipeline.GetRun` on any other pipeline returns
`*PipelineMismatchError`.

The handle itself is not durable state.

---

## Wait semantics

```go
result, err := run.Wait(ctx)
```

`err != nil` means operational inability to produce a terminal Result, such as:

- caller context cancellation,
- Run lookup failure,
- Engine shutdown or failure,
- [Run invalidity](04-engine.md#waiting-on-invalid-runs),
- `ErrRunInProgress`: `Wait` was called from inside a handler, with the
  attempt context, on a nonterminal Run. Inside a handler `Wait` never
  blocks — see [Awaiting other Runs](01-model.md#awaiting-other-runs).

Pipeline semantic failure is represented by:

```go
result.Outcome == OutcomeFailure
```

---

## Example declarations

```proto
message ProvisionMachineInput {
  string region = 1;
  uint64 memory_mb = 2;
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
  };

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
}
```

---

## Example forward handler

```go
func (h *handlers) CreateMachine(
    ctx context.Context,
    inv machines.ProvisionMachineInvocation,
) (*machines.ProvisionMachine_CreateMachine, error) {
    host, ok := inv.State(machines.ProvisionMachine_SelectHostStep)
    if !ok {
        return nil, durable.Fail(
            errors.New("select-host state unavailable"),
        )
    }

    reservation, ok := inv.State(
        machines.ProvisionMachine_ReserveCapacityStep,
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

    return &machines.ProvisionMachine_CreateMachine{
        MachineId: machine.ID,
    }, nil
}
```

---

## Example Unwind handler

```go
func (h *reserveCapacity) Unwind(
    ctx context.Context,
    inv machines.ReserveCapacityInvocation,
) error {
    reservation, ok := inv.State(
        machines.ProvisionMachine_ReserveCapacityStep,
    )
    if !ok {
        return nil
    }

    inv.Logger().Info("releasing reservation",
        "root_step", inv.Failure().StepID)

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

## Example Reducer

```go
func reduceProvisionMachine(
    p *machines.ProvisionMachine,
) *machines.ProvisionMachineOutput {
    machine, ok := p.State(machines.ProvisionMachine_CreateMachineStep)
    if !ok {
        panic("create-machine state unavailable")
    }

    host, ok := p.State(machines.ProvisionMachine_SelectHostStep)
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
