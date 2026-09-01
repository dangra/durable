# Topology evolution and unwind

Part of the [`durable` specification](README.md).

## Terminality boundary

A Run remains subject to current-topology reconciliation until terminal success is durably committed.

For an Output-producing Pipeline:

```text
forward Steps complete
Reducer executes
Output + OutcomeSuccess commit
    -> terminal
```

For an Output-less Pipeline:

```text
forward Steps complete
OutcomeSuccess commit
    -> terminal
```

Until that commit, the Run remains extendable by compatible Pipeline changes.

Example:

```text
A -> B -> C
A ✓ B ✓ C ✓
process crashes before Output commit
```

New topology:

```text
A -> B -> C -> E
```

On recovery, E is after the forward frontier and MAY execute before reduction.

After terminal success commits, later Pipeline changes never alter the completed Run.

---

## Execution ledger

A Run does NOT persist an immutable full Pipeline topology.

It persists execution facts.

Conceptually:

```text
A:
    forward succeeded

B:
    forward attempt reserved
    unresolved
    attempt = 4

C:
    no execution record
```

The exact physical representation is implementation-defined.

---

## Forward operation states

Conceptually:

```text
NotStarted

Unresolved
    at least one durable invocation reservation exists
    no successful or permanent resolution

Succeeded

PermanentlyFailed
```

The distinction between:

```text
never started
```

and:

```text
started but unresolved
```

MUST survive restart.

---

## Current topology reconciliation

Forward scheduling combines:

```text
current Pipeline topology
+
Run execution ledger
+
monotonic forward frontier
=
next applicable operation
```

Reconciliation selects new work only when there is no unresolved forward operation.

---

## Unresolved operation pinning

An unresolved forward operation pins the Run.

If:

```text
A ✓
B unresolved
```

the Engine MUST continue B until B resolves.

Topology reconciliation MUST NOT select another forward Step in the meantime.

Example:

```text
old:
A -> B -> C

Run:
A ✓
B attempt 3 unresolved

new:
A -> X -> B -> C
```

B remains current:

```text
B attempt 4
B attempt 5
...
```

After B succeeds, X lies behind the effective frontier and does not execute.

---

## Effective pinned frontier

The current position of a pinned unresolved Step acts as the effective forward frontier for topology evolution.

A Step inserted at or before that position MUST NOT subsequently execute for this Run.

Example:

```text
A ✓ -> B unresolved | future
```

New:

```text
A -> X -> B -> C
```

X is historical for this Run.

New:

```text
A -> B -> X -> C
```

X remains future work and may execute after B resolves.

If the unresolved Step no longer exists in the current Pipeline definition, the Run is [invalid](04-engine.md#invalid-runs).

---

## Forward frontier

When there is no unresolved operation, the forward frontier is the maximum current-topology position occupied by successfully executed Steps that can be reconciled against the current Pipeline.

Example ledger:

```text
A ✓
B ✓
```

Current:

```text
A -> X -> B -> C
```

B is the greatest-position successful Step.

Therefore:

```text
A ✓ -> X skipped -> B ✓ | C
```

C is next.

Current:

```text
A -> B -> X -> C
```

X is after the frontier and executes.

If no safe frontier can be reconciled from persisted execution facts and the current Pipeline definition, the Run becomes [invalid](04-engine.md#invalid-runs).

---

## Adding Steps

A Step inserted after the forward frontier MAY execute for an existing nonterminal Run.

A Step inserted at or before the frontier MUST NOT execute retroactively.

Example:

```text
old:
A -> B -> D

Run:
A ✓
B ✓

new:
A -> B -> C -> D
```

C executes.

But:

```text
new:
A -> C -> B -> D
```

C is behind B's frontier and is skipped for this Run.

---

## Reordering Steps

Existing Steps MAY be reordered.

Reordering affects only work that remains ahead of the Run's monotonic execution frontier.

Example:

```text
ledger:
A ✓
B ✓

old:
A -> B -> C -> D

new:
A -> C -> B -> D
```

B is the maximum-position successful Step.

C is now behind the frontier and does not execute for that existing Run.

Reordering MUST NOT cause forward execution to move backward across the frontier.

Reordering is not inherently a durable breaking change.

If a particular nonterminal Run cannot be safely reconciled after reordering, that Run becomes [invalid](04-engine.md#invalid-runs).

Deploy-time validation of reorder compatibility is state-dependent and belongs to future evolution tooling.

---

## Retired Steps

A Step MAY declare:

```proto
retired: true
```

Example:

```proto
message ReserveCapacity {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
    retired: true
  };

  string reservation_id = 1;
}
```

Retirement means:

> Keep this Step structurally present, but do not begin new forward operations.

It is the intermediate stage before Step removal.

---

## Retired forward semantics

If the Step never started for a Run:

```text
NotStarted + retired
    -> bypass
```

No handler invocation occurs.

No State is created.

No synthetic success ledger entry is fabricated.

If it has already started:

```text
Unresolved + retired
    -> continue retrying
```

until it:

```text
succeeds
```

or:

```text
returns durable.Fail
```

Retirement MUST NOT abandon an already-started operation.

---

## Retirement observability consequence

A retired Step that is bypassed without invocation leaves no forward execution ledger entry.

Therefore absence alone may not distinguish:

- Step did not yet exist,
- Step was retired and bypassed,
- Step was behind the frontier,
- Step was otherwise never executed.

v1 stores execution facts, not synthetic traversal/skip events.

Future `History()` APIs may expose only that the Step did not execute unless richer traversal events are introduced.

---

## Step removal lifecycle

Normal removal:

```text
active
    |
    v
retired
    |
    | relevant Runs drain
    v
removed
```

Example:

```text
A -> B -> C
```

then:

```text
A -> B(retired) -> C
```

then:

```text
A -> C
```

No tombstone abstraction exists.

---

## Removing an unresolved Step

Direct removal of an unresolved Step may invalidate existing Runs.

Example:

```text
Run:
A ✓
B unresolved

current topology:
A -> C
```

The Engine cannot safely resume B.

That Run becomes [invalid](04-engine.md#invalid-runs).

Correct procedure:

```text
A -> B(retired) -> C
```

allow B to resolve, then remove B after relevant Runs drain.

---

## StepID migration recipe

Suppose `reserve-capacity/v1` must be replaced by incompatible `v2`.

First introduce v2 while retiring v1:

```proto
message ReserveCapacityV1 {
  option (durable.v1.step) = {
    id: "reserve-capacity/v1"
    unwind: true
    retired: true
  };

  string reservation_id = 1;
}

message ReserveCapacityV2 {
  option (durable.v1.step) = {
    id: "reserve-capacity/v2"
    unwind: true
  };

  string reservation_id = 1;
  string host_id = 2;
}
```

Temporary topology:

```text
... ->
ReserveCapacityV1(retired) ->
ReserveCapacityV2 ->
...
```

Runs already executing v1 continue v1 until it resolves.

Runs that never entered v1 bypass it and execute v2.

After no relevant nonterminal Run requires v1, remove v1:

```text
... -> ReserveCapacityV2 -> ...
```

---

## Unwind eligibility

A Step participates in unwind only when ALL are true:

1. it successfully completed forward for this Run,
2. it remains present in the current Pipeline definition,
3. it currently declares `unwind: true`,
4. its position has not been crossed by the monotonic unwind frontier,
5. its unwind operation has not already resolved.

So:

```text
current topology
+
successful forward ledger
+
unwind ledger
+
unwind frontier
=
next unwind work
```

---

## Retired Steps during unwind

`retired` affects only new forward entry.

It does not suppress unwind.

If a retired Step successfully executed forward, it may unwind.

If it was retired and bypassed before ever running, it does not unwind.

Example:

```text
A ✓
B skipped because retired
C ✓
D fails
```

Unwind candidates do not include B.

---

## Removed Steps during unwind

A successfully executed Step that is removed from the current Pipeline no longer participates in future unwind work.

Example historical ledger:

```text
A ✓
B ✓
C ✓
D failed
```

Current Pipeline:

```text
A -> C -> D
```

Unwind considers:

```text
C
A
```

B is absent.

No persisted `UnwindRequired` flag is used.

---

## Newly added Steps during unwind

A newly added Step does not unwind merely because it exists in the current Pipeline.

It must have successfully executed forward for this Run.

Example:

```text
ledger:
A ✓
B ✓
D failed
```

Current topology:

```text
A -> X -> B -> C -> D
```

X and C were never successfully executed.

They do not unwind.

---

## Unwind ordering

Eligible Steps unwind in reverse **current Pipeline order**, constrained by a monotonic backward frontier.

Example successful ledger:

```text
A
B
C
```

Current:

```text
A -> C -> B -> D
```

If unwind has not yet traversed those positions, eligible order is:

```text
B
C
A
```

Current topology intentionally determines future unwind ordering.

---

## Monotonic unwind frontier

Unwind progress is monotonic backward.

Once traversal passes a Pipeline position, work that later becomes eligible at or beyond that already-traversed position MUST NOT execute for the Run.

Example:

```text
A -> C -> D
```

C unwinds, then A unwinds.

Later topology becomes:

```text
A -> B -> C -> D
```

B does not retroactively unwind because the unwind frontier has already crossed its position.

Similarly, changing:

```proto
unwind: false
```

to:

```proto
unwind: true
```

only affects a Step if its position has not yet been crossed.

---

## Unwind frontier reconciliation

The Engine persists enough unwind progress to reconcile the monotonic backward frontier against the current topology.

Pipeline evolution may affect only work still ahead of that frontier.

If the frontier cannot be safely reconciled against the current Pipeline, the Run becomes [invalid](04-engine.md#invalid-runs).

No complex historical migration algorithm is required.

---

## Unwind operation semantics

For an eligible Step:

```text
Unwind -> nil
    success, advance backward

Unwind -> ordinary error
    unresolved, retry

Unwind -> durable.Fail(err)
    permanent UnwindFailure
    record
    continue backward
```

A permanent unwind failure does not stop remaining unwind.

---

## Unwind State access

Unwind receives:

```go
Unwind(
    ctx,
    inv,
    failure,
)
```

State is obtained dynamically:

```go
reservation, ok := inv.State(
    machines.ReserveCapacityStep,
)
```

For a state-producing Step that is eligible for unwind, own State normally exists because eligibility requires successful forward completion.

---

## Forward and unwind monotonicity

Pipeline evolution MUST NOT cause execution to move backward across either frontier.

Symmetry:

```text
new Step at/before forward frontier
    -> do not Run retroactively

newly unwind-eligible Step behind unwind frontier
    -> do not Unwind retroactively
```

---

## Protobuf evolution

Compatible protobuf wire evolution is allowed.

Compatible Step State changes do not automatically require a new StepID.

Compatible Pipeline Input/Output evolution does not automatically require a new PipelineID.

Semantic compatibility remains the application's responsibility.

---

## Durable breaking changes

Examples include:

- incompatible Step behavior under the same StepID,
- incompatible Step State schema under the same StepID,
- incompatible Pipeline Input under the same PipelineID,
- incompatible Pipeline Output under the same PipelineID,
- dynamic State assumptions incompatible with existing Runs,
- directly removing a Step still required by unresolved operations,
- incompatible Reducer semantics.

A particular topology reorder is not inherently a durable breaking change.

If it cannot be reconciled for an existing Run, that Run becomes [invalid](04-engine.md#invalid-runs).
