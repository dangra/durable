package durable

import "errors"

// Fail marks err as a permanent operation failure.
//
// Returned from a forward handler, it resolves the current operation as
// permanently failed, establishes the Run's RootFailure, and begins unwind.
// Returned from an Unwind handler, it records a permanent UnwindFailure and
// unwind continues with earlier eligible Steps.
//
// Any other non-nil error means the operation remains unresolved and is
// retried.
func Fail(err error) error {
	if err == nil {
		err = errors.New("unspecified permanent failure")
	}
	return &permanentError{err: err}
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string {
	return "durable: permanent failure: " + e.err.Error()
}

func (e *permanentError) Unwrap() error { return e.err }

// asPermanent reports whether err declares permanent failure via Fail and,
// if so, returns the wrapped cause.
func asPermanent(err error) (error, bool) {
	var pe *permanentError
	if errors.As(err, &pe) {
		return pe.err, true
	}
	return nil, false
}
