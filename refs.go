package durable

import "google.golang.org/protobuf/proto"

// StepIdentifier names a Step. It is satisfied by the generated step
// references (StepRef and StateStepRef) and by a bare StepID, so APIs
// selecting steps — FailFastExcept, say — accept the typed constants
// generated code exports without stringly-typed ids at call sites.
type StepIdentifier interface {
	ID() StepID
}

// StepRef is a generated reference to a stateless Step. It is not accepted
// by State lookup; passing it to a generated State method fails to compile.
type StepRef struct {
	id StepID
}

// NewStepRef constructs a StepRef. It is intended for generated code.
func NewStepRef(id StepID) StepRef { return StepRef{id: id} }

func (r StepRef) ID() StepID { return r.id }

// StateStepRef is a generated typed reference to a state-producing Step.
// It carries the Step identity and the concrete State type, so dynamic
// State lookups are strongly typed with no assertions or reflection in
// application code.
type StateStepRef[T proto.Message] struct {
	id  StepID
	new func() T
}

// NewStateStepRef constructs a StateStepRef. It is intended for generated
// code.
func NewStateStepRef[T proto.Message](id StepID, new func() T) StateStepRef[T] {
	return StateStepRef[T]{id: id, new: new}
}

func (r StateStepRef[T]) ID() StepID { return r.id }
