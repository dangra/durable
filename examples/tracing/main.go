// Command tracing demonstrates durable's trace-propagation shape, end to
// end, without any tracing library — the same shape works verbatim with
// OpenTelemetry (the comments mark where its calls go):
//
//  1. The scheduling side INJECTS its W3C traceparent into the Run at
//     acceptance with durable.WithAnnotations. The Run outlives this
//     process and its trace; the annotation is the durable carrier.
//  2. A durable.WithMiddleware tracing middleware EXTRACTS the
//     traceparent from Invocation.Annotations on every attempt — hours
//     or restarts later — and emits one span per attempt.
//  3. Each attempt span is LINKED to the originating span rather than
//     parented under it: a parent span held open across retries,
//     parks, and restarts is an anti-pattern; links are the
//     recommended shape for asynchronous, long-lived work.
//
// The engine core never touches a tracing library: everything below is
// application wiring over Annotations and Middleware.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
)

// traceContext is a parsed W3C traceparent: 00-<traceID>-<spanID>-<flags>.
type traceContext struct {
	traceID string // 32 hex chars
	spanID  string // 16 hex chars
}

func (tc traceContext) traceparent() string {
	return fmt.Sprintf("00-%s-%s-01", tc.traceID, tc.spanID)
}

func parseTraceparent(s string) (traceContext, bool) {
	parts := strings.Split(s, "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		return traceContext{}, false
	}
	return traceContext{traceID: parts[1], spanID: parts[2]}, true
}

func randHex(n int) string {
	b := make([]byte, n/2)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// span is a minimal stand-in for a tracer's span: with OpenTelemetry,
// collector.startAttemptSpan becomes tracer.Start(ctx, name,
// trace.WithLinks(trace.Link{SpanContext: origin})).
type span struct {
	TraceID  string        // the span's own trace
	SpanID   string        //
	Name     string        // step/phase/attempt identity
	LinkedTo traceContext  // the originating trace, via span link
	Duration time.Duration //
}

// collector records finished spans; a real adapter exports them.
type collector struct {
	mu    sync.Mutex
	spans []span
}

// middleware is the whole integration: extract the durable trace
// context, wrap the attempt in a span linked to it.
func (c *collector) middleware(next durable.Handler) durable.Handler {
	return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
		origin, ok := parseTraceparent(inv.Annotations()["traceparent"])
		if !ok {
			return next(ctx, inv) // untraced Run: pass through
		}
		// With OpenTelemetry: ctx, sp := tracer.Start(ctx, name,
		// trace.WithLinks(link(origin))); defer sp.End().
		sp := span{
			TraceID:  randHex(32), // each attempt is its own trace root...
			SpanID:   randHex(16),
			Name:     fmt.Sprintf("%s/%s attempt=%d", inv.StepID(), inv.Phase(), inv.Attempt()),
			LinkedTo: origin, // ...linked to the trace that scheduled the Run
		}
		start := time.Now()
		out, err := next(ctx, inv)
		sp.Duration = time.Since(start)
		c.mu.Lock()
		c.spans = append(c.spans, sp)
		c.mu.Unlock()
		return out, err
	}
}

// demoPipeline: a retry and an unwind, so the span stream shows multiple
// attempts and both phases carrying the same origin.
func demoPipeline() *durable.Definition {
	return durable.NewDefinition(durable.DefinitionConfig{
		ID: "traced-provision",
		Steps: []durable.StepConfig{
			{
				ID:     "reserve/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					if inv.Attempt() == 1 {
						return nil, errors.New("transient: capacity check raced")
					}
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return nil // release the reservation
				},
			},
			{
				ID: "create/v1",
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, durable.Fail(errors.New("image rejected"), durable.WithUserKind())
				},
			},
		},
	})
}

func run() (*collector, traceContext, durable.Result, error) {
	c := &collector{}
	engine := durable.NewEngine(durabletest.NewMemStore(),
		durable.WithRetryPolicy(durable.RetryPolicy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2}),
		durable.WithLogger(slog.New(slog.DiscardHandler)),
		durable.WithMiddleware(c.middleware))
	pipe, err := demoPipeline().Bind(engine)
	if err != nil {
		return nil, traceContext{}, durable.Result{}, err
	}
	if err := engine.Start(context.Background()); err != nil {
		return nil, traceContext{}, durable.Result{}, err
	}
	defer engine.Stop(context.Background())

	// The scheduling side is mid-request, inside its own trace — with
	// OpenTelemetry this comes from trace.SpanContextFromContext(ctx).
	origin := traceContext{traceID: randHex(32), spanID: randHex(16)}

	run, _, err := pipe.Schedule(context.Background(), "machine-42", nil,
		durable.WithAnnotations(map[string]string{"traceparent": origin.traceparent()}))
	if err != nil {
		return nil, traceContext{}, durable.Result{}, err
	}
	res, err := run.Wait(context.Background())
	return c, origin, res, err
}

func main() {
	c, origin, res, err := run()
	if err != nil {
		panic(err)
	}
	fmt.Printf("origin trace   %s (the request that scheduled the run)\n", origin.traceID)
	fmt.Printf("run outcome    %v\n\n", res.Outcome)
	for _, sp := range c.spans {
		fmt.Printf("span %s  %-32s  link -> %s\n", sp.SpanID, sp.Name, sp.LinkedTo.traceID)
	}
}
