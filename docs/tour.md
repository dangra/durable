# A tour of durable

This is the middle layer between the [README](../README.md) and the
[specification](../spec/README.md): every feature, introduced as an
operational need, with enough code to use it and links into the spec
for the full rules. The runnable versions of most snippets live in the
[godoc examples](https://pkg.go.dev/github.com/dangra/durable#pkg-examples).

The running example is a deployment platform: a `deploy-service`
pipeline that provisions an environment, runs database migrations, and
shifts traffic — work that takes minutes to hours, must survive the
deploy daemon restarting, and must be able to roll back.

- [The model in five sentences](#the-model-in-five-sentences)
- [Declaring a pipeline](#declaring-a-pipeline)
- [Handlers: at-least-once, retries, permanent failure](#handlers-at-least-once-retries-permanent-failure)
- [Unwind: rollback as a first-class phase](#unwind-rollback-as-a-first-class-phase)
- [Durability: crash, restart, continue](#durability-crash-restart-continue)
- [Cancellation](#cancellation)
- [Composing runs: AwaitRun](#composing-runs-awaitrun)
- [Contention: dedup, exclusion groups, concurrency classes](#contention-dedup-exclusion-groups-concurrency-classes)
- [Time: delayed starts and retention](#time-delayed-starts-and-retention)
- [Evolving a pipeline with runs in flight](#evolving-a-pipeline-with-runs-in-flight)
- [Observability](#observability)

## The model in five sentences

A pipeline is a fixed, linear sequence of steps. Executing a run never
mutates state in place: the engine appends immutable execution facts
(attempt reserved, state committed, permanently failed) to a
[store](../spec/04-engine.md), and everything else — what runs next,
what unwinds, what the outcome is — is *reconciled* from those facts
against the current pipeline definition. Handlers run with
at-least-once semantics, so they must be idempotent. A permanent
forward failure flips the run into the unwind phase, compensating
previously successful steps in reverse order. Because the facts are
durable and the reconciliation is pure, a process crash loses nothing:
the next engine — even one running a *newer definition* — picks every
run up exactly where the facts left it.

The full model: [spec/01-model.md](../spec/01-model.md).

## Declaring a pipeline

Pipelines and steps are protobuf messages carrying `durable.v1`
options; `protoc-gen-durable` compiles them into a fully typed Go API
(see [spec/05-codegen.md](../spec/05-codegen.md) and the committed
generated code in [examples/machines](../examples/machines/)):

```proto
message ProvisionEnv {
  option (durable.v1.step) = {
    id: "provision-env/v1"
    unwind: true              // has a rollback
  };
  string env_id = 1;          // step state: committed on success
}

message RunMigrations {
  option (durable.v1.step) = {
    id: "run-migrations/v1"
    unwind: true
  };
  string schema_version = 1;
}

message ShiftTraffic {
  option (durable.v1.step) = { id: "shift-traffic/v1" };
  string lb_generation = 1;
}

message DeployService {
  option (durable.v1.pipeline) = {
    id: "deploy-service"
    input: ".deploy.v1.DeployServiceInput"
    output: ".deploy.v1.DeployServiceOutput"

    steps: ".deploy.v1.ProvisionEnv"
    steps: ".deploy.v1.RunMigrations"
    steps: ".deploy.v1.ShiftTraffic"
  };
}
```

Each step message's *fields* are its **state**: what the forward
handler returns is committed durably on success, and later steps read
it. The generated package gives you typed handler interfaces, a
constructor, and a pipeline handle:

```go
store, _ := bboltstore.Open("deploys.db")
engine := durable.NewEngine(store)

deploy, err := deploypb.NewDeployService(
    &provisionEnv{}, &runMigrations{}, &shiftTraffic{},
    reduceDeployService, // pure: folds step states into the Output
).Bind(engine)
// bind every pipeline, then:
engine.Start(ctx)

run, created, err := deploy.Schedule(ctx, "service-web", &deploypb.DeployServiceInput{
    Image: "registry.example.com/web:v42",
})
result, err := run.Wait(ctx)
```

`"service-web"` is the **resource**: the thing the run is about. At
most one run of a pipeline is active per resource at a time — more on
that [below](#contention-dedup-exclusion-groups-concurrency-classes).

## Handlers: at-least-once, retries, permanent failure

A forward handler receives a typed invocation: the pipeline input,
committed state of earlier steps, and attempt metadata.

```go
func (h *runMigrations) Run(ctx context.Context, inv deploypb.RunMigrationsInvocation) (*deploypb.RunMigrations, error) {
    env, ok := inv.State(deploypb.ProvisionEnvStep) // typed, committed state
    if !ok {
        return nil, durable.Fail(errors.New("env state unavailable"))
    }
    version, err := h.db.Migrate(ctx, env.GetEnvId(), inv.Input().GetImage())
    if err != nil {
        return nil, err // plain error: retried with backoff
    }
    return &deploypb.RunMigrations{SchemaVersion: version}, nil
}
```

The three ways out of a handler:

- **Success** — return the step's state message; it is committed
  durably, the run advances.
- **Retry** — return any ordinary error; the attempt is retried with
  backoff (`WithRetryPolicy`), forever, because the engine cannot know
  whether a migration timeout means "failed" or "still applying".
  At-least-once means the handler may run again *after* succeeding
  (crash between effect and commit), so make it idempotent: check
  whether the migration is already applied before applying it.
- **Permanent failure** — declare it explicitly:

```go
return nil, durable.Fail(errors.New("image not in registry"),
    durable.WithUserKind(),               // the request was wrong, not the infra
    durable.WithReason("image-not-found")) // low-cardinality slug for metrics/alerts
```

`Fail` is a decision, not an error class: it flips the run to unwind.
Kind and reason are informational attribution — `user` (no retry
anywhere would have helped) vs the default `system` (page someone) —
and surface on the `Result`, on log lines, and as metric labels.
Errors anywhere in the chain can also carry them via the
`FailureReasoner`/`FailureKinder` interfaces. Runnable:
[`ExampleFail`](https://pkg.go.dev/github.com/dangra/durable#example-Fail).

## Unwind: rollback as a first-class phase

When `shift-traffic/v1` fails permanently, the successful steps before
it unwind in reverse order — migrations roll back, then the environment
is torn down. A step opts in with `unwind: true` and an `Unwind`
handler:

```go
func (h *runMigrations) Unwind(ctx context.Context, inv deploypb.RunMigrationsInvocation, f durable.Failure) error {
    m, ok := inv.State(deploypb.RunMigrationsStep) // what forward committed
    if !ok {
        return nil // forward never committed; nothing to undo
    }
    return h.db.Rollback(ctx, m.GetSchemaVersion()) // plain error: retried
}
```

`f.Root` carries the failure that started the unwind (step, message,
kind, reason). Unwind handlers have the same at-least-once/retry
semantics as forward ones; a *permanent* unwind failure (a `Fail` from
an unwind handler) is recorded on the result as an `UnwindFailure` and
does **not** stop the remaining unwind — the environment still gets
torn down even if the migration rollback is beyond saving. The run
terminates with `OutcomeFailure` and full attribution.

Rules: [spec/03-evolution.md § unwind eligibility](../spec/03-evolution.md#unwind-eligibility).

## Durability: crash, restart, continue

This is the reason the library exists. Kill the deploy daemon between
`run-migrations` and `shift-traffic` — or during them — and nothing is
lost: the store holds the committed facts, and the next `engine.Start`
recovers every nonterminal run and continues it. There is no separate
"recovery mode"; startup recovery *is* ordinary reconciliation.

```go
// After restart: same store, same binds, and the run is still there.
engine := durable.NewEngine(store)
deploy, _ := deploypb.NewDeployService(...).Bind(engine)
engine.Start(ctx)

run, err := deploy.Run(ctx, runID) // recover a handle by RunID
result, err := run.Wait(ctx)
```

The contract this rests on is the one from the handler section: an
attempt that succeeded right before the crash may execute again,
because the crash may have landed between the effect and the commit.
Idempotent handlers make re-execution invisible.

The engine's crash-restart convergence is model-checked in CI against
randomized kill/restart schedules (`crash_restart_test.go`).

## Cancellation

Aborting a deploy is not deleting it — the environment it provisioned
must still be torn down. `Cancel` reuses unwind:

```go
err := run.Cancel(ctx, "bad canary metrics")
result, _ := run.Wait(ctx)
result.Canceled() // true: failed with FailureKindCanceled
```

The first request wins, it is durable (survives restart), and it stops
the run from *selecting new forward work*. A **started** operation is
never abandoned: its in-flight attempt context is preempted once, and
the re-executed attempt is expected to observe
`inv.CancelRequested()` and resolve promptly — the run stays cancelable
until terminal success commits, so the engine takes over and unwinds as
soon as the operation resolves:

```go
func (h *shiftTraffic) Run(ctx context.Context, inv deploypb.ShiftTrafficInvocation) (*deploypb.ShiftTraffic, error) {
    if inv.CancelRequested() {
        return nil, durable.Fail(errors.New("deploy canceled"))
        // or resolve successfully; either way, the engine unwinds next
    }
    // ...
}
```

Runnable: [`ExampleRun_Cancel`](https://pkg.go.dev/github.com/dangra/durable#example-Run_Cancel).

## Composing runs: AwaitRun

A release train deploys many services: a parent run schedules child
runs and waits for them. Handlers must never block on other runs
(`run.Wait` inside a handler can exhaust the worker pool and deadlock);
instead, a handler *parks*:

```go
func (h *shipServices) Run(ctx context.Context, inv releasepb.ShipServicesInvocation) (*releasepb.ShipServices, error) {
    if childID, woken := inv.AwaitedRunID(); woken {
        // The awaited run reached terminality; inspect and continue.
        _ = childID
        return &releasepb.ShipServices{}, nil
    }
    child, _, err := deploy.Schedule(ctx, "service-web", input)
    if err != nil {
        return nil, err
    }
    return nil, durable.AwaitRun(child.ID())
}
```

Like `Fail`, `AwaitRun` is a resolution, not an error: the operation
stays unresolved, the worker is released immediately (no goroutine or
token is held while parked — a park can outlive many restarts), and the
moment the target terminates the operation re-executes as a fresh
attempt. `AwaitedRunID` is the memory that distinguishes "woken" from
"first execution", so the handler doesn't re-schedule the child.
Awaits must not form a cycle; a cycle-closing park makes the run
invalid. Runnable:
[`ExampleAwaitRun`](https://pkg.go.dev/github.com/dangra/durable#example-AwaitRun).

## Contention: dedup, exclusion groups, concurrency classes

Three different problems, three different tools.

**Duplicate scheduling.** `Schedule` is an idempotent entry point:
while a run is active on a resource, scheduling the same input again
returns the *existing* run with `created=false` — safe under
at-least-once callers like message consumers. Scheduling a *different*
input on a busy resource is a `*ScheduleConflictError`. Runnable:
[`ExamplePipeline_Schedule`](https://pkg.go.dev/github.com/dangra/durable#example-Pipeline_Schedule).

**Exclusion groups: one run per resource, across pipelines.** A
`deploy-service` and a `rollback-service` pipeline must not both act on
`service-web` at once:

```proto
option (durable.v1.pipeline) = {
  id: "deploy-service"
  exclusion_group: "service-lifecycle"
  // ...
};
```

Pipelines sharing a group allow at most one nonterminal run per
resource across the whole group.

**Concurrency classes: bounded parallel work.** At most three deploys
per cluster touching the load balancer, regardless of how many runs are
active:

```proto
option (durable.v1.step) = { id: "shift-traffic/v1" concurrency_class: "lb" };
```

```go
engine := durable.NewEngine(store, durable.WithConcurrencyClass("lb", 3))
```

Tokens are execution-scoped: held only while a handler runs, never
across retry waits, parks, or restarts — a parked release train
consumes nothing. Runnable:
[`ExampleWithConcurrencyClass`](https://pkg.go.dev/github.com/dangra/durable#example-WithConcurrencyClass).

## Time: delayed starts and retention

A deploy scheduled into a release window occupies its resource slot
immediately but attempts nothing until the hour:

```go
run, _, err := deploy.Schedule(ctx, "service-web", input,
    durable.StartAt(window))          // or durable.StartAfter(4 * time.Hour)
```

The delay is durable — it survives restarts, like retry backoffs do.
Terminal runs accumulate by default (they are the audit trail); opt
into reaping with a policy:

```go
engine := durable.NewEngine(store,
    durable.WithRetention(durable.RetentionPolicy{TerminalAfter: 30 * 24 * time.Hour}))
```

Runnable: [`ExampleStartAfter`](https://pkg.go.dev/github.com/dangra/durable#example-StartAfter).

## Evolving a pipeline with runs in flight

Deploy pipelines change more often than deploys finish. durable's
answer is the part of the model that looks least like other workflow
engines: the ledger records *facts*, and every dispatch reconciles them
against the *current* definition using a forward frontier — the
furthest current-topology position the run's successful steps reach.

**Adding a step.** The platform team adds canary analysis:

```proto
steps: ".deploy.v1.ProvisionEnv"
steps: ".deploy.v1.RunMigrations"
steps: ".deploy.v1.CanaryAnalysis"   // new
steps: ".deploy.v1.ShiftTraffic"
```

A run whose frontier is `run-migrations/v1` executes the new step —
it is ahead of the frontier. A run already past `shift-traffic/v1`
never executes it retroactively — it is behind the frontier and is
skipped. Deploy the new definition, restart, and in-flight runs simply
follow the new topology where it is still ahead of them.

**Retiring a step.** Replacing `run-migrations/v1` with an
incompatible v2 is a two-phase recipe: introduce v2 while marking v1
`retired: true` — structurally present (old runs still reconcile, its
unwind still works) but never starting new forward work — then remove
v1 entirely once no nonterminal run references it. The full recipe:
[spec/03-evolution.md § StepID migration](../spec/03-evolution.md#stepid-migration-recipe).

**Invalid runs.** If no safe frontier can be reconciled — a definition
change that contradicts the recorded facts — the run does not guess and
does not crash the engine: it becomes *invalid for this deployment*,
parked and reported (`Status`, observer events, `Stats`), and resumes
automatically under a corrected definition. Bad deploys of the
*pipeline definitions themselves* are recoverable by rolling forward.

Full rules: [spec/03-evolution.md](../spec/03-evolution.md).

## Observability

The engine logs through `log/slog` with canonical keys, emits typed
lifecycle events (`Observer`), and snapshots occupancy
(`Engine.Stats`) — see the [README](../README.md#observability). The
[contrib/durableotel](../contrib/durableotel/) module packages the
OpenTelemetry integration (per-attempt spans linked to the scheduling
trace, metrics, log correlation, W3C Baggage relay), declared once at
engine construction; [examples/tracing-otel](../examples/tracing-otel/)
demonstrates it end to end.

## Where next

- [Godoc examples](https://pkg.go.dev/github.com/dangra/durable#pkg-examples) — runnable versions of this tour's snippets
- [examples/machines](../examples/machines/) — a complete generated pipeline
- [examples/tracing-otel](../examples/tracing-otel/) — the observability story, end to end
- [The specification](../spec/README.md) — the full rules this tour summarizes
