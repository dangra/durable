package durable

import (
	"context"
	"errors"
	"slices"

	"google.golang.org/protobuf/proto"
)

// Handler is the uniform type-erased operation the Engine executes —
// durable's analog of http.Handler. Every generated step adapter erases
// into this shape. A forward operation returns the State to commit (nil for
// a stateless Step); an unwind operation always returns (nil, err).
//
// Result semantics are those of any handler: nil error resolves the
// operation successfully, an ordinary error leaves it unresolved and
// retried, and Fail resolves it as permanent failure.
type Handler func(ctx context.Context, inv *Invocation) (proto.Message, error)

// Middleware wraps a Handler — durable's analog of
// func(http.Handler) http.Handler. Use it for cross-cutting concerns such
// as logging, metrics, tracing spans, or per-operation timeouts.
//
// Middleware runs once per attempt, inside the durable attempt
// reservation: it inherits the operation's at-least-once semantics and
// must not assume exactly-once execution. It participates in handler
// result semantics — returning the error unchanged preserves the
// retry/Fail classification, while transforming an ordinary error into
// Fail deliberately changes durable behavior. The Reducer is pure, not an
// operation, and is never wrapped.
type Middleware func(next Handler) Handler

// FailFastOnCancel returns a Middleware that opts forward handlers out
// of cooperative cancellation: instead of each handler observing
// Invocation.CancelRequested and resolving, a canceled Run's forward
// operations are resolved by the middleware — as a Fail wrapping
// *PreemptedError, which the engine attributes as FailureKindCanceled
// (Result.Canceled() reports true) once its own evidence confirms the
// cancellation. Unwind operations are never touched: during a
// cancellation the unwind is the work.
//
// Install it only when every forward handler is preemption-safe:
// abandoning an attempt mid-flight forfeits the step's completion, and
// a step that never commits state is invisible to unwind — partial
// external effects (a charge that landed, a half-created resource) get
// no compensation hook. The cooperative default lets each handler
// finish or clean up before the Run unwinds; this middleware trades
// that safety for immediacy. Engine shutdown is unaffected: a ctx
// killed by Stop carries ErrEngineStopping, not *PreemptedError, and
// passes through as an ordinary retryable resolution.
func FailFastOnCancel() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, inv *Invocation) (proto.Message, error) {
			if inv.Phase() != PhaseForward {
				return next(ctx, inv)
			}
			// A retry dispatched with the cancel already visible: yield
			// without invoking the handler. The engine fills the cause
			// from the durable cancel request.
			if inv.CancelRequested() {
				return nil, Fail(&PreemptedError{})
			}
			out, err := next(ctx, inv)
			// The attempt the cancel preempted mid-flight: convert its
			// ctx death into a yield, but only when the cause proves a
			// cancellation — shutdown (ErrEngineStopping) or unrelated
			// errors pass through untouched.
			if err != nil && errors.Is(err, context.Canceled) {
				if pe, ok := errors.AsType[*PreemptedError](context.Cause(ctx)); ok {
					return nil, Fail(pe)
				}
			}
			return out, err
		}
	}
}

// WithMiddleware installs middleware around every operation the Engine
// executes, forward and unwind alike; use Invocation.Phase to distinguish
// them. The first middleware is the outermost, following the net/http
// convention: WithMiddleware(a, b) yields a(b(handler)).
func WithMiddleware(mw ...Middleware) Option {
	return func(e *Engine) {
		e.middleware = append(e.middleware, mw...)
	}
}

// wrap composes the engine's middleware chain around h, first middleware
// outermost.
func (e *Engine) wrap(h Handler) Handler {
	for _, v := range slices.Backward(e.middleware) {
		h = v(h)
	}
	return h
}
