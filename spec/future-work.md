# Future work

Part of the [`durable` specification](README.md).

## Store contract remainder

The transactional store contract is now specified (see 04-engine, Store
contract): component persistence, atomic transitions, the cursor, and
slot uniqueness. Remaining:

- waiters (currently in-process only),
- retention/archival of terminal runs,
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

## Full observability

Potential:

- OpenTelemetry,
- scheduler metrics,
- retry metrics,
- invalid-run metrics,
- Step duration,
- Run duration,
- unwind failure metrics,
- retirement drain metrics.
