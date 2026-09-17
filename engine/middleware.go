package engine

import (
	"slices"

	"github.com/dangra/durable"
)

// WithMiddleware installs middleware around every operation the Engine
// executes, forward and unwind alike; use Invocation.Phase to distinguish
// them. The first middleware is the outermost, following the net/http
// convention: WithMiddleware(a, b) yields a(b(handler)). Pipeline-level
// middleware (pipelinedef.WithMiddleware on a generated constructor, or
// pipelinedef.Config.Middleware) composes inside this chain: engine
// middleware is outermost.
func WithMiddleware(mw ...durable.Middleware) Option {
	return func(e *Engine) {
		e.middleware = append(e.middleware, mw...)
	}
}

// wrap composes middleware chains around h, outermost first across chains
// and within each: wrap(h, engine, pipeline) yields
// engine[0](…engine[n](pipeline[0](…pipeline[m](h)))). It runs once, at
// Bind; the result runs once per attempt.
func wrap(h durable.Handler, chains ...[]durable.Middleware) durable.Handler {
	for _, chain := range slices.Backward(chains) {
		for _, mw := range slices.Backward(chain) {
			h = mw(h)
		}
	}
	return h
}
