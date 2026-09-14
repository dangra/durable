package durable

import "google.golang.org/protobuf/proto"

// TypedInvocation is an Invocation whose pipeline Input is typed. It is
// what a generated pipeline's handler methods receive: each generated
// package aliases it to its Input type, so a handler of the
// "stop-machine" pipeline takes a stoppb.StopMachineInvocation and reads
// inv.Input() as a *stoppb.StopMachineInput. Everything else on it is
// the core Invocation, promoted. Middleware and hand-rolled pipelines
// keep using Invocation itself.
type TypedInvocation[In any] struct {
	Invocation
}

// Typed wraps a core Invocation for a pipeline whose Input is In. The
// engine wraps its own invocations through generated adapters; a
// handler unit test wraps durabletest.NewInvocation the same way.
func Typed[In any](core Invocation) TypedInvocation[In] {
	return TypedInvocation[In]{Invocation: core}
}

// Input returns a defensive caller-owned copy of the immutable pipeline
// Input; the zero In when the pipeline declares none.
func (inv TypedInvocation[In]) Input() In {
	msg, _ := inv.InputMessage().(In)
	return msg
}

// State returns the committed State of the referenced Step for this Run.
// ok is false when no committed State exists.
func (inv TypedInvocation[In]) State[T proto.Message](step StateStepRef[T]) (T, bool) {
	return LookupState(inv.Invocation, step)
}

// NoInput is the Input type of a TypedInvocation for a pipeline that
// declares no Input: Input() returns its zero value.
type NoInput struct{}
