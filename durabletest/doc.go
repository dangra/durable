// Package durabletest provides engine-free test doubles: a deterministic
// fake Clock for the engine's time, and a fake Invocation (NewInvocation)
// for unit-testing handlers without an engine or a store. Generated code
// accepts the fake through its NewXxxInvocation constructors and folds it
// as a reducer view through XxxReducer.Reduce. The in-memory store lives
// in store/mem: it is a real driver, not a test double.
package durabletest
