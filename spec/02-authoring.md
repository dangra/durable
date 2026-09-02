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

## Step declaration

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
return &machines.SelectHost{
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
inv.State(machines.ValidateStep)
```

MUST fail to compile.

---

## Generic State API

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
network, ok := inv.State(machines.ConfigureNetworkStep)
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

## Step handler capability matrix

### No State, no Unwind

```go
type ValidateHandler interface {
    Run(
        context.Context,
        ValidateInvocation,
    ) error
}
```

### State, no Unwind

```go
type SelectHostHandler interface {
    Run(
        context.Context,
        SelectHostInvocation,
    ) (*SelectHost, error)
}
```

### No State, with Unwind

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

### State and Unwind

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

## Handler func adapters

Each handler interface receives a generated adapter in the style of
`http.HandlerFunc`.

A single-method interface gets a func type:

```go
type ValidateFunc func(context.Context, ValidateInvocation) error

func (f ValidateFunc) Run(ctx context.Context, inv ValidateInvocation) error
```

An unwind-bearing interface has two methods, which a func type cannot
implement, so it gets a struct of funcs:

```go
type ReserveCapacityFuncs struct {
    RunFunc    func(context.Context, ReserveCapacityInvocation) (*ReserveCapacity, error)
    UnwindFunc func(context.Context, ReserveCapacityInvocation, durable.Failure) error
}
```

Adapters are conveniences for tests and small handlers; the interfaces
remain the authoritative contract.

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
    -> RootFailure
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

---

## Pipeline marker as Reducer input

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

## Reducer contract

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

## Bind

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

## Bound Pipeline API

A bound Pipeline exposes:

```text
Schedule
Active
ActiveRun
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

## Typed Run

Pipelines declaring an Input or an Output use generated typed Runs — a
typed `Input(ctx)` accessor comes with Input, a typed `Wait` result with
Output:

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

## Typed Result

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

## Typed Run recovery

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

## Plain Run handle

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

## Wait semantics

```go
result, err := run.Wait(ctx)
```

`err != nil` means operational inability to produce a terminal Result, such as:

- caller context cancellation,
- Run lookup failure,
- Engine failure,
- [Run invalidity](04-engine.md#waiting-on-invalid-runs).

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

## Example forward handler

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

## Example Unwind handler

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

## Example Reducer

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
