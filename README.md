# durable

Durable linear pipelines with unwind semantics for Go.

`durable` executes fixed, linear pipelines whose execution state survives
process crashes and restarts. Pipelines and steps are declared in Protocol
Buffers; `protoc-gen-durable` compiles them into fully typed Go APIs. Step
operations run with at-least-once semantics: ordinary errors retry with
backoff, permanent failure is declared explicitly with `durable.Fail`, and a
permanent forward failure unwinds previously successful steps in reverse
order. Pipeline definitions may evolve while runs are active — the runtime
persists immutable execution facts and reconciles them against the current
definition using monotonic forward and unwind frontiers.

The full specification lives in [spec/](spec/README.md).

**Requires Go 1.27+** (the generated `State` API uses generic methods).

## Example

Declare a pipeline ([full example](examples/machines/)):

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
  };

  string reservation_id = 1;
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

Implement the generated handler interfaces:

```go
func (h *createMachine) Run(
    ctx context.Context,
    inv machinespb.CreateMachineInvocation,
) (*machinespb.CreateMachine, error) {
    reservation, ok := inv.State(machinespb.ReserveCapacityStep)
    if !ok {
        return nil, durable.Fail(errors.New("reservation state unavailable"))
    }
    // ... at-least-once: must be idempotent; plain errors are retried.
    return &machinespb.CreateMachine{MachineId: id}, nil
}
```

Run it:

```go
store, _ := bboltstore.Open("machines.db")
engine := durable.NewEngine(store)

provision, _ := machinespb.NewProvisionMachine(
    validate{}, &selectHost{}, &reserveCapacity{}, &createMachine{},
    reduceProvisionMachine,
).Bind(engine)

engine.Start(ctx)

run, created, _ := provision.Schedule(ctx, "machine-123", input)
result, _ := run.Wait(ctx)
if result.Succeeded() {
    fmt.Println(result.Output().GetMachineId())
}
```

Cross-cutting concerns use net/http-style middleware over the uniform
type-erased operation layer (see
[the design note](spec/http-analogy.md)):

```go
engine := durable.NewEngine(store, durable.WithMiddleware(logging, metrics))
```

## Layout

| Path | Contents |
|---|---|
| `spec/` | The specification |
| `.` (`package durable`) | Public runtime API: engine, identities, `Fail`, results, typed step references, `LookupState` |
| `internal/ledger` | Pure reconciliation core: frontiers, pinning, unwind eligibility |
| `proto/durable/v1` + `durablepb/` | The `durable.v1.step` / `durable.v1.pipeline` protobuf options |
| `cmd/protoc-gen-durable` | The code generator |
| `bboltstore/` | bbolt-backed `Store` (SQLite: future work) |
| `durabletest/` | In-memory `Store` and fake `Clock` for tests |
| `examples/machines` | The provision-machine example with committed generated code |

## Development

```sh
buf lint && buf generate   # regenerate durablepb and example code
go test ./...
```

`buf generate` runs `protoc-gen-go` (via `go tool`, version-locked to
go.mod's protobuf runtime) and `protoc-gen-durable` (via `go run`) — no
plugin installs needed.
