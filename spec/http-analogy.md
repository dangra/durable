# Design note: the net/http analogy

Part of the [`durable` specification](README.md). **Non-normative** — this
note explains the layering behind the middleware and func-adapter APIs by
analogy with Go's `net/http`; the normative contracts live in
[02-authoring.md](02-authoring.md) and [04-engine.md](04-engine.md).

## The mapping

| net/http | durable |
|---|---|
| `http.Handler` | `durable.Handler` — the uniform type-erased operation `func(ctx, *Invocation) (proto.Message, error)` |
| `http.HandlerFunc` | generated func adapters (`XFunc`, `XFuncs`) |
| middleware `func(http.Handler) http.Handler` | `durable.Middleware`, installed with `WithMiddleware` |
| `ServeMux` and route patterns | the protobuf Pipeline topology; `StepID` plays the route pattern |
| `*http.Request` | `*durable.Invocation` — identity, attempt, phase, input, State lookup |
| `http.ResponseWriter` | — (deliberately absent; see below) |
| `http.Server` / `Server.Shutdown` | `Engine` / `Engine.Stop` |

## The layering insight

Application code sees fully typed generated handler interfaces, but every
one of them erases into the same operation shape before reaching the
Engine — exactly as typed routers and handler wrappers in the Go ecosystem
ultimately sit on `http.Handler`:

```text
typed generated handlers  :  durable.Handler
        =
typed router ecosystems   :  http.Handler
```

That erased seam is where cross-cutting behavior belongs. Middleware
wraps it uniformly — logging, metrics, tracing spans, per-operation
timeouts — without touching the typed authoring surface or the durable
execution model.

## What deliberately does not transfer

**ResponseWriter.** An `http.ResponseWriter` is a progressive output
channel: headers, then body bytes, possibly interleaved with failures.
Durable Step State must not work that way — committed State and forward
success form one atomic durable transition. Handlers therefore *return*
their State, and the Engine commits it with the success record in a single
write. A writer-style API would reintroduce partial output.

**The mux.** Routing exists, but it is declarative: the protobuf Pipeline
definition is the routing table, resolved by reconciliation against the
execution ledger rather than by matching an incoming request. There is
nothing to register at runtime.

**The goroutine-per-request model.** An `http.Server` runs each request
concurrently and forgets it when the connection closes. The Engine owns
execution lifetime, runs exactly one operation per Run at a time, bounds
global concurrency, and resumes Runs after restarts. Handlers are invoked
*by* the scheduler, at-least-once, rather than per event.
