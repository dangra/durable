# release-train

The flagship durable demo: one release run that lives through
everything the library exists for — a daemon crash, an interrupted
attempt re-executed idempotently, a pipeline definition that evolves
while the run is in flight, parent runs awaiting children, and a
cascading cancellation that rolls back exactly what it should.

```sh
go run ./examples/release-train
```

## The pipelines

Two pipelines, composed: a `release-train` parent ships each service by
scheduling a `deploy-service` child run and parking on it with
`AwaitRun` (no worker or token is held while parked):

```mermaid
flowchart TB
    subgraph train["release-train (parent run)"]
        direction TB
        P[plan/v1] --> SW[ship-web/v1] --> SA[ship-api/v1]
    end

    subgraph deploy["deploy-service (one child run per service)"]
        direction LR
        A[provision-env/v1] --> B[run-migrations/v1] --> C[canary-analysis/v1] --> D[shift-traffic/v1]
    end

    SW -. "schedules + AwaitRun (web)" .-> A
    SA -. "schedules + AwaitRun (api)" .-> A
```

`provision-env` and `run-migrations` declare `unwind: true` — their
rollbacks (environment teardown, migration rollback) run in reverse
order when a deploy fails permanently or is canceled.

The deploy pipeline also exists in a second, older build without the
canary step: `legacyproto/` vs `proto/`, separate buf modules sharing
one pipeline id, carried in one binary only to simulate a daemon
restart onto a new deployment (yesterday's and today's builds never
coexist in a real one). The flow below shows the web deploy crossing
that definition change mid-run.

## The flow

One process, two engine generations, one bbolt store:

```mermaid
sequenceDiagram
    participant Op as operator (main)
    participant T as release-train run
    participant W as web deploy run
    participant A as api deploy run

    rect rgb(245,245,245)
        note over Op,W: yesterday's build (no canary step)
        Op->>T: Schedule(image v42)
        T->>T: plan/v1
        T->>W: ship-web schedules child
        note over T: AwaitRun(web) — parked, no worker held
        W->>W: provision-env ✓
        W->>W: run-migrations (applied, commit lost)
        note over Op,W: 💥 daemon crashes mid-migration
    end

    rect rgb(232,245,233)
        note over Op,A: today's build (canary-analysis added)
        W->>W: run-migrations attempt 2 — already applied, idempotent skip
        W->>W: canary-analysis ✓ — step added while this run was in flight
        W->>W: shift-traffic ✓ → terminal success
        W-->>T: park resolves, ship-web woken
        T->>A: ship-api schedules child
        note over T: AwaitRun(api) — parked again
        A->>A: provision-env ✓, run-migrations ✓
        A->>A: canary-analysis running...
        Op->>T: Cancel("incident declared")
        note over T: awaiting op woken with CancelRequested
        T->>A: cancels child (cascade), resolves
        A->>A: canary preempted, yields
        A->>A: unwind: migrations rolled back, env torn down
        note over T,A: both terminal: Canceled() = true — web stays shipped
    end
```

## What to look for in the output

| chapter | line |
|---|---|
| durability | `---- daemon crashes; restarting with today's build ----` — only the store survives |
| at-least-once | `[web] migrations already applied — idempotent re-execution` |
| evolution | `[web] canary analysis: score 98 — a step added while this run was in flight` |
| composition | `[train] web deploy scheduled; parking until it lands` |
| cancellation | `[train] release frozen — canceling api deploy` → `[api] migrations rolled back (unwind)` |

## The cast

| file | role |
|---|---|
| `main.go` | the story: schedule, crash, restart, freeze, verdict |
| `world.go` | the fake platform backend — what "survives" the crash |
| `deploy.go` | today's `deploy-service` handlers (`releasepb`) |
| `legacy.go` | yesterday's handlers (`legacypb`), including the crash-mid-migration one |
| `train.go` | the parent orchestration: schedule child, `AwaitRun`, cascade cancel |
| `builds.go` | the two daemon generations wired to their pipelines |
| `main_test.go` | asserts the durable facts, not print interleaving |

For the concepts behind each chapter, see the
[guided tour](../../docs/tour.md); for the full rules,
[the specification](../../spec/README.md).
