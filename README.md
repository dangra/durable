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

Start with the [guided tour](docs/tour.md) — every feature introduced
as an operational need, with runnable
[godoc examples](https://pkg.go.dev/github.com/dangra/durable/engine#pkg-examples).
The full specification lives in [spec/](spec/README.md).

The module is split by audience. Handler code imports only `durable`,
the handler contract: what a step receives, how it resolves, and the
middleware layer. Wiring code imports `engine`, which runs pipelines:
opening a store, binding the generated definitions, scheduling and
waiting on runs. Generated code additionally imports `pipelinedef`, the
type-erased definition it builds; the shared vocabulary lives in
`kernel` and is aliased into `durable`. Stores are opened by URI through
`store` with a blank import of the driver (`store/bbolt` persistent,
`store/mem` for ephemeral runs); implementers use `store/driver`,
telemetry adapters `observe`.

**Requires Go 1.27+** (the typed `State` lookup is a generic method).

## Example

Declare a pipeline in protobuf (condensed from
[examples/machines](examples/machines/), which has a validation step and
a second pipeline sharing a mutex). Each step is a message whose fields
are the state it commits; the pipeline message lists the steps in
order:

```proto
message SelectHost {
  option (durable.v1.step) = {id: "select-host/v1"};
  string host_id = 1;
}

message ReserveCapacity {
  option (durable.v1.step) = {id: "reserve-capacity/v1" unwind: true};
  string reservation_id = 1;
}

message CreateMachine {
  option (durable.v1.step) = {id: "create-machine/v1"};
  string machine_id = 1;
}

message ProvisionMachine {
  option (durable.v1.pipeline) = {
    id: "provision-machine"
    input: ".machines.v1.ProvisionMachineInput"
    output: ".machines.v1.ProvisionMachineOutput"

    steps: ".machines.v1.SelectHost"
    steps: ".machines.v1.ReserveCapacity"
    steps: ".machines.v1.CreateMachine"
  };
}
```

`protoc-gen-durable` turns that into one interface, `ProvisionMachineHandlers`:
a method per step, `Unwind<Step>` for each step that unwinds, and
`Reduce` for the output. One type implements the pipeline, so its
dependencies are declared once, and a step it lacks — a step added to
the proto included — is a compile error naming the method:

```go
type handlers struct{ cloud *cloud }

func (h *handlers) SelectHost(ctx context.Context, inv machinespb.ProvisionMachineInvocation) (*machinespb.SelectHost, error) {
    return &machinespb.SelectHost{HostId: "host-" + inv.Input().GetRegion() + "-1"}, nil
}

func (h *handlers) ReserveCapacity(ctx context.Context, inv machinespb.ProvisionMachineInvocation) (*machinespb.ReserveCapacity, error) {
    host, _ := inv.State(machinespb.SelectHostStep) // typed, committed state
    id, err := h.cloud.Reserve(ctx, host.GetHostId())
    if err != nil {
        return nil, err // an ordinary error: retried with backoff
    }
    return &machinespb.ReserveCapacity{ReservationId: id}, nil
}

func (h *handlers) UnwindReserveCapacity(ctx context.Context, inv machinespb.ProvisionMachineInvocation) error {
    r, ok := inv.State(machinespb.ReserveCapacityStep)
    if !ok {
        return nil // never committed: nothing to release
    }
    return h.cloud.Release(ctx, r.GetReservationId())
}

func (h *handlers) CreateMachine(ctx context.Context, inv machinespb.ProvisionMachineInvocation) (*machinespb.CreateMachine, error) {
    r, _ := inv.State(machinespb.ReserveCapacityStep)
    id, err := h.cloud.Create(ctx, r.GetReservationId(), inv.Input().GetRegion())
    if errors.Is(err, errNoCapacity) {
        // A decision, not an error class: the run unwinds from here,
        // and ReserveCapacity's unwind releases the reservation.
        return nil, durable.Fail(err, durable.WithReason("insufficient-capacity"))
    }
    if err != nil {
        return nil, err
    }
    return &machinespb.CreateMachine{MachineId: id}, nil
}

func (h *handlers) Reduce(p *machinespb.ProvisionMachine) *machinespb.ProvisionMachineOutput {
    m, _ := p.State(machinespb.CreateMachineStep)
    return &machinespb.ProvisionMachineOutput{MachineId: m.GetMachineId()}
}
```

Handlers run at least once, so they are idempotent; the invocation's
`ctx` dies only for engine shutdown or a cancel of the run, and
returning `ctx.Err()` is the right answer to both.

Wire it up. This side of an application imports `engine`; handler
files never do:

```go
st, _ := store.Open("bbolt:///var/lib/app/machines.db") // import _ ".../store/bbolt"
eng := engine.New(st)
provision, _ := machinespb.NewProvisionMachine(&handlers{cloud: c}).Bind(eng)
eng.Start(ctx)

run, _, _ := provision.Schedule(ctx, "machine-123", &machinespb.ProvisionMachineInput{Region: "ams"})
result, _ := run.Wait(ctx)
switch {
case result.Succeeded():
    fmt.Println(result.Output().GetMachineId())
case result.Canceled():
    // run.Cancel(ctx, "operator retracted") on any handle, from any process
default:
    fmt.Println(result.Failure.Reason) // "insufficient-capacity"
}
```

A run survives the process: stop the engine mid-step, start another on
the same store, and `provision.GetRun(ctx, id)` continues where the
facts left off — under a newer pipeline definition if one shipped in
between. Handlers unit-test without an engine: hand a method
`machinespb.NewProvisionMachineInvocation(durabletest.NewInvocation(cfg))`.

For the whole story in one runnable demo — a release surviving a daemon
crash, a pipeline definition that evolves mid-flight, parent runs
awaiting children, and a cascading cancellation that rolls everything
back — run [examples/release-train](examples/release-train/).

## Observability

The engine logs through `log/slog`, emits typed lifecycle events
(`observe.Observer`), and snapshots occupancy (`Engine.Stats`); cross-cutting
concerns use net/http-style middleware over the uniform type-erased
operation layer (see [the design note](spec/http-analogy.md)). The core
never depends on a telemetry library —
[`contrib/durableotel`](contrib/durableotel/), a separate module,
packages the OpenTelemetry integration: a span per attempt linked (not
parented) to the trace that scheduled the Run, metrics with
durable-scale histogram buckets, `trace_id`/`span_id` log correlation,
and an opt-in W3C Baggage relay. Everything is declared once, at engine
construction:

```go
obs, _ := durableotel.NewObserver()
eng := engine.New(store,
    engine.WithMiddleware(durableotel.Middleware()),
    engine.WithObserver(obs),
    engine.WithScheduleAnnotator(durableotel.Annotator()))

// anywhere, by any subsystem, with the provision pipeline from above —
// propagation rides the request's ctx:
run, _, _ := provision.Schedule(reqCtx, "machine-123", input)
```

[examples/tracing-otel](examples/tracing-otel/) demonstrates the
complete shape against the real OpenTelemetry SDK.

## Development

```sh
buf lint && buf generate   # regenerate durablepb and example code
go test ./...
```

`buf generate` runs `protoc-gen-go` (via `go tool`, version-locked to
go.mod's protobuf runtime) and `protoc-gen-durable` (via `go run`) — no
plugin installs needed.

## License

[Apache-2.0](LICENSE)
