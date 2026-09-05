# A tour of durable

This is the middle layer between the [README](../README.md) and the
[specification](../spec/README.md): every feature, introduced as an
operational need, with enough code to use it and links into the spec
for the full rules. The runnable versions of most snippets live in the
[godoc examples](https://pkg.go.dev/github.com/dangra/durable/engine#pkg-examples).

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
eng := engine.New(store)

deploy, err := deploypb.NewDeployService(
    &provisionEnv{}, &runMigrations{}, &shiftTraffic{},
    reduceDeployService, // pure: folds step states into the Output
).Bind(eng)
// bind every pipeline, then:
eng.Start(ctx)

run, created, err := deploy.Schedule(ctx, "service-web", &deploypb.DeployServiceInput{
    Image: "registry.example.com/web:v42",
})
result, err := run.Wait(ctx)
```

Two packages appear here and they stay apart throughout: `durable` is
the handler contract (`durable.Fail`, `durable.AwaitRun`, the
`Invocation` a handler receives), and `engine` is the wiring side
(`engine.New`, the `With*` options, `Bind`, `Schedule`, `Wait`). A file
that implements steps imports `durable`; the file that starts the
daemon imports `engine`; nothing else imports the generated package's
dependencies.

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
  `ctx.Err()` is an ordinary error too: `context.Canceled` says the
  ctx died, not *why*, so the engine never infers durable intent from
  the error value — it classifies only await/permanent/nil/other. When
  your ctx dies, return `ctx.Err()` as-is; whatever killed it is already
  tracked on its own channel (a pending cancel gates the next dispatch,
  a shutdown resumes via recovery), so don't wrap it in `Fail` — a
  preempted attempt is not necessarily a permanent failure.
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
eng := engine.New(store)
deploy, _ := deploypb.NewDeployService(...).Bind(eng)
eng.Start(ctx)

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

### What about context.Context?

Context is a delivery mechanism here, never the source of truth. Four
contexts are in play, deliberately unrelated:

1. **`Schedule`'s ctx** governs only the store write that accepts the
   Run. It never reaches handlers — the Run outlives the request — and
   no values flow from it; annotations are the only bridge.
2. **The attempt ctx** a handler receives is derived fresh per attempt
   from the *engine's* context. It dies for exactly two reasons, and
   `context.Cause(ctx)` names which: engine shutdown
   (`durable.ErrEngineStopping`) or the one-time preemption after
   `Cancel` (`*durable.PreemptedError`, carrying the cancel's cause).
3. **`Cancel`'s own ctx** governs only the store write recording the
   request. The durable record is the real signal; the preemption is a
   courtesy wake-up for a blocked handler.
4. **Post-cancel attempts get a fresh, live ctx.** Cancellation is not
   delivered as a permanently dead context: the re-executed attempt
   sees `inv.CancelRequested()` on a working ctx, and unwind handlers
   run under live contexts too — compensation must be able to do real
   work, and a poisoned ctx would make rollback impossible. The actual
   stop is enforced by the engine (a pending cancel gates dispatch
   from selecting new forward work), not by handlers honoring a dead
   ctx.

Engine **shutdown is not cancellation**: `Stop` kills every in-flight
attempt ctx but leaves unresolved Runs nonterminal, with no failure
recorded — the next `Start` resumes them (that is the
[durability chapter](#durability-crash-restart-continue)). Shutdown is
ephemeral and process-scoped; `Cancel` is durable intent that survives
restart and drives the Run to a terminal `Canceled()` outcome via
unwind. By default `Stop` preempts in-flight attempts immediately;
`WithDrainTimeout` makes it graceful — no new attempts start while
in-flight ones finish with live contexts and commit their results,
with preemption only for stragglers at the deadline.

### Opting out of the cooperative loop

When every forward handler is **preemption-safe** — idempotent, no
partial external effects, or cleanup keyed off the input rather than
committed state — the per-handler `CancelRequested` check can be
replaced by one middleware:

```go
eng := engine.New(store,
    engine.WithMiddleware(durable.FailFastOnCancel()))
```

A canceled Run's forward operations are then resolved by the
middleware: the preempted attempt converts its ctx death into a yield
in the same attempt (the `*PreemptedError` cause proves it was the
cancel, so shutdowns and unrelated wrapped `context.Canceled` errors
pass through), a cancel landing between retries short-circuits before
the handler runs, and the engine — after verifying the preemption with
its own evidence — attributes the outcome `FailureKindCanceled`, so
`Result.Canceled()` still reports true. Unwind operations are never
touched: during a cancellation, the unwind *is* the work.

Understand the trade before opting in: an abandoned attempt commits no
state, and a step that never commits state is invisible to unwind —
partial external effects (a charge that landed, a half-created
resource) get no compensation hook. The cooperative default lets each
handler finish or clean up first; `FailFastOnCancel` trades that
safety for immediacy. For a mixed pipeline, `FailFastExcept` keeps the
steps that can't make the preemption-safety claim on the cooperative
path:

```go
durable.FailFastOnCancel(durable.FailFastExcept(deploypb.RunMigrationsStep))
```

Steps are named by the references generated code exports (or a bare
`durable.StepID`), so the exception list is typo-proof at compile time.

## Composing runs: AwaitRun

A release train deploys many services: a parent run schedules child
runs and waits for them. Handlers cannot block on other runs: a
blocked handler holds a worker slot and can deadlock the pool, so
`run.Wait` called with the attempt context fails fast with
`engine.ErrRunInProgress` when the target is still running (it still
returns the `Result` of an already-terminal run). Instead, a handler
*parks*:

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
"first execution", so the handler doesn't re-schedule the child. The
memory belongs to the operation, not to one attempt: if the woken
attempt returns an ordinary error, or the process restarts while it is
running, the retry still sees it.
Awaits must not form a cycle; a cycle-closing park makes the run
invalid. Runnable:
[`ExampleAwaitRun`](https://pkg.go.dev/github.com/dangra/durable#example-AwaitRun).

**Fan-out.** A step can park on several runs at once. `AwaitAll` wakes
once, when the last of them is terminal; `AwaitAny` wakes as soon as
the first is. Either way the woken attempt reads the park back through
`Awaited()` — a `Wake` with `Targets` (what was parked on), `Done` (the
targets terminal or missing at wake time), and `Pending()` for the rest
— so the handler never has to remember its children itself:

```go
func (h *shipServices) Run(ctx context.Context, inv releasepb.ShipServicesInvocation) (*releasepb.ShipServices, error) {
    if w, woken := inv.Awaited(); woken {
        for _, id := range w.Done {              // all of them, under AwaitAll
            res, err := deploy.Run(ctx, id).Wait(ctx) // terminal → returns immediately
            if err != nil { return nil, err }
            if !res.Succeeded() { return nil, durable.Fail(fmt.Errorf("deploy %s failed", id)) }
        }
        return &releasepb.ShipServices{}, nil
    }
    var ids []durable.RunID
    for _, svc := range inv.Input().GetServices() {
        run, _, err := deploy.Schedule(ctx, durable.ResourceID(svc), input)
        if err != nil { return nil, err }
        ids = append(ids, run.ID())
    }
    return nil, durable.AwaitAll(ids)
}
```

`AwaitAny` is the select loop — handle `w.Done`, then
`return durable.AwaitAny(w.Pending())` to keep waiting — or the race:
act on the winner and `Cancel` the rest.

**Deadlines.** A park has no deadline unless you give it one:
`durable.AwaitRun(id, durable.WithAwaitTimeout(10*time.Minute))`. Expiry
is a wake, not a failure: the attempt runs with `w.Expired` set and
`w.Done` listing whatever had finished, and the handler decides — `Fail`
with a reason, `Cancel` the pending children, or park again on
`w.Pending()` to extend. The deadline is stored as an absolute time, so
it survives restarts, and `Status.AwaitDeadline` shows it. Two things to
know. Scheduling
N children in one attempt is safe against a crash halfway only because
`Schedule` is idempotent on (pipeline, resource, input) *while the child
is nonterminal*; a child that finishes before the retry is terminal, and
the retry creates a fresh one, so keep child resource IDs deterministic
and don't let a scheduling attempt do slow work after scheduling. And
a cancel that bypasses a park still produces a `Wake`, with `Done`
reflecting the children's state at that moment, so "on freeze, cancel my
pending children" is the same loop under every mode. Cycle detection is
conservative: a cycle through any edge invalidates the run, even under
`AwaitAny` where another target might have let it escape.

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
eng := engine.New(store, engine.WithConcurrencyClass("lb", 3))
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
    engine.StartAt(window))          // or engine.StartAfter(4 * time.Hour)
```

The delay is durable — it survives restarts, like retry backoffs do.
Terminal runs accumulate by default (they are the audit trail); opt
into reaping with a policy:

```go
eng := engine.New(store,
    engine.WithRetention(engine.RetentionPolicy{TerminalAfter: 30 * 24 * time.Hour}))
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
lifecycle events (`observe.Observer`), and snapshots occupancy
(`Engine.Stats`) — see the [README](../README.md#observability). The
[contrib/durableotel](../contrib/durableotel/) module packages the
OpenTelemetry integration (per-attempt spans linked to the scheduling
trace, metrics, log correlation, W3C Baggage relay), declared once at
engine construction; [examples/tracing-otel](../examples/tracing-otel/)
demonstrates it end to end.

## Where next

- [Godoc examples](https://pkg.go.dev/github.com/dangra/durable/engine#pkg-examples) — runnable versions of this tour's snippets
- [examples/release-train](../examples/release-train/) — the flagship demo: one release living through a crash, a definition change, awaited children, and a cascading cancellation
- [examples/machines](../examples/machines/) — a complete generated pipeline
- [examples/tracing-otel](../examples/tracing-otel/) — the observability story, end to end
- [The specification](../spec/README.md) — the full rules this tour summarizes
