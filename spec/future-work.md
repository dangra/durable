# Future work

Part of the [`durable` specification](README.md).

## Store contract remainder

The transactional store contract is now specified (see 04-engine, Store
contract): component persistence, atomic transitions, the cursor, slot
uniqueness, and terminal-run retention. Remaining:

- waiters (currently in-process only),
- a compact post-retention summary tier (terminal Runs currently delete
  entirely at retention),
- a SQLite implementation.

## Historical inspection

Potential:

```go
run.History(ctx)
run.Attempts(ctx)
```

Because v1 does not persist synthetic skip events, richer traversal observability may require additional event data.

## Run abandonment

Cancellation (terminate through unwind) is specified in 01-model. A
separate, more drastic operation — abandoning a Run terminally *without*
unwind, e.g. to force-release the slot of an unreconcilable invalid Run —
remains outside v1. It is deliberately dangerous (partial effects are left
unresolved) and would need its own confirmation semantics.

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

## Observability beyond the event surface

Logging, lifecycle events, snapshots, and the OpenTelemetry bridge are
specified (see 04-engine, Observability). Remaining:

- retirement drain metrics,
- trace grouping across a Run's attempts (today attempts are linked to
  the scheduling trace, not parented under a Run span),
- an indexed read model for introspection (list Runs by state, class,
  or await target without scanning).

## Parks at scale

A park's target list rides the Cursor, so a very wide fan-out makes
per-attempt writes scale with it; moving the park into its own storage
component, and a documented fan-out cap, remain open.
