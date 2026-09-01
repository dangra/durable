# Core invariants

Part of the [`durable` specification](README.md). This list is append-only; invariant numbers are stable references.

1. `RunID` identifies one exact execution.

2. At most one nonterminal Run exists for `(PipelineID, ResourceID)`.

3. Pipeline Input is immutable.

4. Committed Step State is immutable.

5. Pipeline Output is immutable.

6. Handler operations use at-least-once invocation semantics.

7. Handlers MUST be idempotent.

8. Ordinary error means retry indefinitely.

9. `durable.Fail` is the explicit semantic permanent-failure mechanism.

10. Runtime contract violations do not become business failures.

11. State exists only after successful forward completion.

12. Successful state-producing handlers MUST return non-nil State.

13. `State(...)(value, true)` implies a non-nil committed durable State exists.

14. State lookup accepts only `StateStepRef[T]`.

15. State lookup is strongly typed through concrete generic methods.

16. State and Input lookups return defensive caller-owned copies.

17. Dynamic State compatibility belongs to application code.

18. The failing forward Step is not automatically unwound.

19. A permanently failing handler owns cleanup of its own partial uncommitted effects.

20. Runs persist execution facts rather than immutable full topology.

21. Current Pipeline topology determines future structural work.

22. Forward progress is monotonic.

23. An unresolved forward operation pins the Run until resolution.

24. No new forward work is selected while an unresolved operation exists.

25. A Step inserted at or before a pinned Step's current position does not execute for that Run.

26. A Step inserted after the forward frontier may execute.

27. A Step inserted at or before the forward frontier does not execute retroactively.

28. Reordering never moves forward execution backward across its frontier.

29. A retired Step does not begin a new forward operation.

30. An already-started Step continues retrying after retirement.

31. A retired unstarted Step leaves no synthetic success record.

32. Retirement is the intermediate lifecycle stage before removal.

33. Directly removing a required unresolved Step may invalidate Runs.

34. No tombstone abstraction exists.

35. Unwind requires successful forward completion.

36. Unwind also requires current presence in Pipeline topology.

37. Unwind also requires current `unwind=true`.

38. Retirement does not itself disable unwind.

39. A retired Step that never executed forward does not unwind.

40. A removed Step does not newly unwind.

41. Unwind ordering follows reverse current Pipeline order.

42. Unwind progress is monotonic backward.

43. A Step becoming unwind-eligible behind the unwind frontier does not execute retroactively.

44. Reordering never moves unwind execution backward across its frontier.

45. Permanent Unwind failure is recorded and unwind continues.

46. `Failure.UnwindFailures` contains permanent unwind failures accumulated so far in execution order.

47. Attempt numbers are durably reserved before handler invocation.

48. Attempt numbers are monotonic and never reused.

49. A reserved attempt may exist even if the process crashes before application code begins.

50. Retry eligibility survives restart.

51. A successful Run remains extendable until terminal success is durably committed.

52. Terminal Runs never change due to later Pipeline evolution.

53. Reducers are pure and deterministic relative to durable data.

54. Reducers and handlers use the same typed State lookup model.

55. Reducer/runtime incompatibility invalidates the Run rather than causing infinite retry.

56. Invalidity is not a Pipeline business outcome.

57. Invalid Runs do not prevent Engine startup.

58. Invalid Runs are ignored for execution and surfaced operationally.

59. A corrected deployment may make an invalid Run runnable again.

60. `Wait` does not silently block forever on known-invalid Runs.

61. Engine startup fails only for Engine-wide configuration/storage problems.

62. Exactly one Engine owns a Store in v1.

63. Exactly one logical operation per Run executes at once.

64. Different Runs may execute concurrently.

65. Shutdown does not create Pipeline failure.

66. Input-declaring Pipelines reject nil scheduling Input.

67. Input-less Pipelines generate `Schedule` without an Input argument.

68. Scheduling before `Engine.Start` fails without creating a Run.

69. Duplicate scheduling uses exact `proto.Equal`, including unknown fields.

70. Positional generated constructors remain compile-time type-safe because every Step position has a distinct generated interface.
