package durable

import "errors"

// FailureReasoner may be implemented by any error in a handler's error
// chain to carry a machine-readable failure reason. Reasons should be
// short, low-cardinality slugs ("invalid-image", "insufficient-capacity"):
// their destiny is metrics labels and alert routing. Human-readable detail
// belongs in the error message.
//
// The engine extracts the reason with errors.As at resolution time — for
// permanent failures (recorded on the FailureRecord) and for ordinary
// retryable errors (recorded as the Run's LastReason). An explicit
// WithReason on Fail takes precedence over the chain.
type FailureReasoner interface {
	FailureReason() string
}

// FailureKinder may be implemented by any error in a handler's error chain
// to attribute permanent failures at the point where the error is created,
// keeping resolution sites down to a plain Fail(err). An explicit
// WithUserKind on Fail takes precedence over the chain.
type FailureKinder interface {
	FailureKind() FailureKind
}

// FailOption annotates a permanent failure declared with Fail.
type FailOption func(*permanentError)

// WithUserKind attributes the permanent failure to the request or intent
// (FailureKindUser). Without it, the kind comes from the first
// FailureKinder in the error chain, defaulting to FailureKindSystem.
func WithUserKind() FailOption {
	return func(e *permanentError) {
		e.kind = FailureKindUser
		e.kindSet = true
	}
}

// WithReason sets the machine-readable failure reason, overriding any
// FailureReasoner in the error chain. Keep reasons short, lowercase,
// low-cardinality slugs.
func WithReason(reason string) FailOption {
	return func(e *permanentError) {
		e.reason = reason
	}
}

// Fail marks err as a permanent operation failure. It is the only
// permanent-failure mechanism; options attach attribution.
//
// Returned from a forward handler, it resolves the current operation as
// permanently failed, establishes the Run's RootFailure, and begins unwind.
// Returned from an Unwind handler, it records a permanent UnwindFailure and
// unwind continues with earlier eligible Steps.
//
// Any other non-nil error means the operation remains unresolved and is
// retried.
func Fail(err error, opts ...FailOption) error {
	if err == nil {
		err = errors.New("unspecified permanent failure")
	}
	pe := &permanentError{err: err}
	for _, o := range opts {
		o(pe)
	}
	return pe
}

type permanentError struct {
	err     error
	kind    FailureKind
	kindSet bool
	reason  string
}

func (e *permanentError) Error() string {
	return "durable: permanent failure: " + e.err.Error()
}

func (e *permanentError) Unwrap() error { return e.err }

// failureKind resolves the attribution: explicit option, then the error
// chain, then the system default.
func (e *permanentError) failureKind() FailureKind {
	if e.kindSet {
		return e.kind
	}
	// Not errors.AsType: FailureKinder is a plain interface, not an error,
	// and AsType's constraint requires E to implement error.
	var fk FailureKinder
	if errors.As(e.err, &fk) {
		return fk.FailureKind()
	}
	return FailureKindSystem
}

// failureReason resolves the reason: explicit option, then the error chain.
func (e *permanentError) failureReason() string {
	if e.reason != "" {
		return e.reason
	}
	return reasonOf(e.err)
}

// reasonOf extracts a FailureReasoner reason from an error chain.
func reasonOf(err error) string {
	var fr FailureReasoner
	if err != nil && errors.As(err, &fr) {
		return fr.FailureReason()
	}
	return ""
}

// FailureInfo reports whether a handler's returned error declares
// permanent failure via Fail and, if so, the attribution it resolves
// to — the kind and reason that will reach the Run's RootFailure.
// Middleware wrapping handlers use it to label spans and metrics with
// the same attribution the engine will commit.
func FailureInfo(err error) (kind FailureKind, reason string, ok bool) {
	pe, ok := asPermanent(err)
	if !ok {
		return FailureKindSystem, "", false
	}
	return pe.failureKind(), pe.failureReason(), true
}

// asPermanent reports whether err declares permanent failure via Fail.
func asPermanent(err error) (*permanentError, bool) {
	if pe, ok := errors.AsType[*permanentError](err); ok {
		return pe, true
	}
	return nil, false
}
