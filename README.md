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

Wire it up — this side of an application imports `engine`, handler files never do:

```go
st, _ := store.Open("bbolt:///var/lib/app/machines.db") // import _ ".../store/bbolt"
eng := engine.New(st)

provision, _ := machinespb.NewProvisionMachine(
    validate{}, &selectHost{}, &reserveCapacity{}, &createMachine{},
    reduceProvisionMachine,
).Bind(eng)

eng.Start(ctx)

run, created, _ := provision.Schedule(ctx, "machine-123", input)
result, _ := run.Wait(ctx)
if result.Succeeded() {
    fmt.Println(result.Output().GetMachineId())
}
```

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
