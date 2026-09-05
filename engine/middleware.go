package engine

import (
	"slices"

	"github.com/dangra/durable"
)

// WithMiddleware installs middleware around every operation the Engine
// executes, forward and unwind alike; use Invocation.Phase to distinguish
// them. The first middleware is the outermost, following the net/http
// convention: WithMiddleware(a, b) yields a(b(handler)).
func WithMiddleware(mw ...durable.Middleware) Option {
	return func(e *Engine) {
		e.middleware = append(e.middleware, mw...)
	}
}

// wrap composes the engine's middleware chain around h, first middleware
// outermost.
func (e *Engine) wrap(h durable.Handler) durable.Handler {
	for _, v := range slices.Backward(e.middleware) {
		h = v(h)
	}
	return h
}
