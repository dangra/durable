package durable

import (
	"context"

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
	for i := len(e.middleware) - 1; i >= 0; i-- {
		h = e.middleware[i](h)
	}
	return h
}
