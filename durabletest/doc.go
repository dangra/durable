// Package durabletest provides engine-free test doubles: a deterministic
// fake Clock for the engine's time, and a fake Invocation (NewInvocation)
// for unit-testing handlers without an engine or a store. The in-memory
// store lives in store/mem: it is a real driver, not a test double.
package durabletest
