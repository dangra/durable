package durable

import "google.golang.org/protobuf/proto"

// StepIdentifier names a Step. It is satisfied by the generated step
// references (StepRef and StateStepRef) and by a bare StepID, so APIs
// selecting steps — FailFastExcept, say — accept the typed constants
// generated code exports without stringly-typed ids at call sites.
type StepIdentifier interface {
	ID() StepID
}

// StepRef is a reference to a stateless Step; generated packages export
// one per stateless Step. It is not accepted by State lookup: passing it
// to a generated State method fails to compile.
type StepRef struct {
	Step StepID
}

func (r StepRef) ID() StepID { return r.Step }

// StateStepRef is a typed reference to a state-producing Step; generated
// packages export one per such Step. It carries the Step identity and the
// concrete State type, so dynamic State lookups are strongly typed with
// no assertions or reflection in application code. New allocates the
// message LookupState decodes into; a ref with a nil New is unusable.
type StateStepRef[T proto.Message] struct {
	Step StepID
	New  func() T
}

func (r StateStepRef[T]) ID() StepID { return r.Step }
