# Future work

Part of the [`durable` specification](README.md).

## Store contract

Specify transactional persistence operations for:

- Runs,
- resource-slot uniqueness,
- Input,
- forward execution ledger,
- unwind execution ledger,
- State,
- retry scheduling,
- Failure,
- Output,
- terminal outcome,
- waiters.

Implementations:

- SQLite,
- bbolt.

## Historical inspection

Potential:

```go
run.History(ctx)
run.Attempts(ctx)
```

Because v1 does not persist synthetic skip events, richer traversal observability may require additional event data.

## Administrative cancellation or abandonment

Explicit operator-controlled Run termination remains outside v1.

Its relationship with:

- unwind,
- RootFailure,
- Outcome

must be designed independently.

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
