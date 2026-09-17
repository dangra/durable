// Middleware: ordering, phases, escalation to a permanent failure, and
// context propagation into handlers.
package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
	"google.golang.org/protobuf/proto"
)

func TestMiddlewareOrderingAndPhases(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}
	mw := func(name string) durable.Middleware {
		return func(next durable.Handler) durable.Handler {
			return func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				record(fmt.Sprintf("%s:in:%v:%s:%d", name, inv.Phase(), inv.StepID(), inv.Attempt()))
				// The failure being unwound is visible from the chain,
				// non-nil exactly in the unwind phase.
				switch f := inv.Failure(); {
				case inv.Phase() == durable.PhaseUnwind && (f == nil || f.StepID != "b/v1"):
					t.Errorf("%s: unwind of %s sees Failure %+v; want root b/v1", name, inv.StepID(), f)
				case inv.Phase() == durable.PhaseForward && f != nil:
					t.Errorf("%s: forward %s sees Failure %+v; want nil", name, inv.StepID(), f)
				}
				state, err := next(ctx, inv)
				record(name + ":out")
				return state, err
			}
		}
	}

	store := mem.New()
	e := engine.New(store, fastRetry, engine.WithMiddleware(mw("outer"), mw("inner")))
	def := pipelinedef.New(pipelinedef.Config{
		ID: "mw",
		Steps: []pipelinedef.Step{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
					return nil
				},
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.Attempt() == 1 {
					return errors.New("transient")
				}
				return durable.Fail(errors.New("permanent"))
			}),
		},
	})
	p, err := e.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run, _, _ := p.Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v; want failure", res, err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Four operations, each an onion of outer(inner(handler)):
	// a.Run, b.Run attempt 1 (retry), b.Run attempt 2 (Fail), a.Unwind.
	want := []string{
		"outer:in:forward:a/v1:1", "inner:in:forward:a/v1:1", "inner:out", "outer:out",
		"outer:in:forward:b/v1:1", "inner:in:forward:b/v1:1", "inner:out", "outer:out",
		"outer:in:forward:b/v1:2", "inner:in:forward:b/v1:2", "inner:out", "outer:out",
		"outer:in:unwind:a/v1:1", "inner:in:unwind:a/v1:1", "inner:out", "outer:out",
	}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events[%d] = %q, want %q (all: %v)", i, events[i], want[i], events)
		}
	}
}

func TestMiddlewareCanEscalateToFail(t *testing.T) {
	escalate := func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
			state, err := next(ctx, inv)
			if err != nil {
				return state, durable.Fail(err)
			}
			return state, err
		}
	}
	var attempts atomic.Uint64
	def := pipelinedef.New(pipelinedef.Config{
		ID: "escalating",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				attempts.Store(inv.Attempt())
				return errors.New("would ordinarily retry")
			}),
		},
	})
	e := engine.New(mem.New(), fastRetry, engine.WithMiddleware(escalate))
	p, _ := e.Bind(def)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run, _, _ := p.Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v; want permanent failure via middleware", res, err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry after escalation)", attempts.Load())
	}
}

type ctxKey struct{}

func TestMiddlewareContextReachesHandlers(t *testing.T) {
	inject := func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
			return next(context.WithValue(ctx, ctxKey{}, "present"), inv)
		}
	}
	def := pipelinedef.New(pipelinedef.Config{
		ID: "ctxpipe",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				if ctx.Value(ctxKey{}) != "present" {
					return durable.Fail(errors.New("middleware context value missing"))
				}
				return nil
			}),
		},
	})
	e := engine.New(mem.New(), fastRetry, engine.WithMiddleware(inject))
	p, _ := e.Bind(def)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run, _, _ := p.Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success", res, err)
	}
}

// recordingMiddleware records name:in/out around every operation it
// wraps, keyed by pipeline so a chain's reach is visible.
func recordingMiddleware(mu *sync.Mutex, events map[durable.PipelineID][]string, name string) durable.Middleware {
	return func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
			mu.Lock()
			events[inv.PipelineID()] = append(events[inv.PipelineID()], name+":in")
			mu.Unlock()
			state, err := next(ctx, inv)
			mu.Lock()
			events[inv.PipelineID()] = append(events[inv.PipelineID()], name+":out")
			mu.Unlock()
			return state, err
		}
	}
}

// TestPipelineMiddlewareScopeAndOrder pins that a pipeline's own chain
// wraps only that pipeline's operations, forward and unwind, inside the
// engine chain, first listed outermost.
func TestPipelineMiddlewareScopeAndOrder(t *testing.T) {
	var mu sync.Mutex
	events := map[durable.PipelineID][]string{}
	mw := func(name string) durable.Middleware { return recordingMiddleware(&mu, events, name) }

	own := pipelinedef.New(pipelinedef.Config{
		ID:         "own",
		Middleware: []durable.Middleware{mw("p1"), mw("p2")},
		Steps: []pipelinedef.Step{
			{
				ID:         "a/v1",
				Unwind:     true,
				Run:        func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error { return nil },
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.Attempt() == 1 {
					return errors.New("transient")
				}
				return durable.Fail(errors.New("permanent"))
			}),
		},
	})
	other := pipelinedef.New(pipelinedef.Config{
		ID:    "other",
		Steps: []pipelinedef.Step{stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error { return nil })},
	})
	e := engine.New(mem.New(), fastRetry, engine.WithLogger(discardTestLogger()),
		engine.WithMiddleware(mw("engine")))
	pOwn, err := e.Bind(own)
	if err != nil {
		t.Fatalf("Bind own: %v", err)
	}
	pOther, err := e.Bind(other)
	if err != nil {
		t.Fatalf("Bind other: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	rOwn, _, _ := pOwn.Schedule(context.Background(), "r", nil)
	rOther, _, _ := pOther.Schedule(context.Background(), "r", nil)
	if res, err := rOwn.Wait(context.Background()); err != nil || !res.Failed() {
		t.Fatalf("own Wait = %+v, %v; want failure", res, err)
	}
	if res, err := rOther.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("other Wait = %+v, %v; want success", res, err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Four operations on own — a.Run, b.Run twice, a.Unwind — each the
	// onion engine(p1(p2(handler))).
	onion := []string{"engine:in", "p1:in", "p2:in", "p2:out", "p1:out", "engine:out"}
	var want []string
	for i := 0; i < 4; i++ {
		want = append(want, onion...)
	}
	if got := events["own"]; !slices.Equal(got, want) {
		t.Fatalf("own events = %v, want %v", got, want)
	}
	if got := events["other"]; !slices.Equal(got, []string{"engine:in", "engine:out"}) {
		t.Fatalf("other events = %v; want the engine chain alone", got)
	}
}

// TestPipelineMiddlewareEscalatesForItsPipelineOnly is the flyd case: a
// store's not-found error is a permanent failure of one pipeline's
// forward operations and an ordinary error of everyone else's.
func TestPipelineMiddlewareEscalatesForItsPipelineOnly(t *testing.T) {
	errGone := errors.New("machine not found")
	gone := func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
			out, err := next(ctx, inv)
			if err != nil && inv.Phase() == durable.PhaseForward && errors.Is(err, errGone) {
				return nil, durable.Fail(err, durable.WithReason("machine-gone"))
			}
			return out, err
		}
	}
	var strictAttempts, lenientAttempts atomic.Int32
	strict := pipelinedef.New(pipelinedef.Config{
		ID:         "strict",
		Middleware: []durable.Middleware{gone},
		Steps: []pipelinedef.Step{stateless("strict/v1", func(ctx context.Context, inv durable.Invocation) error {
			strictAttempts.Add(1)
			return errGone
		})},
	})
	lenient := pipelinedef.New(pipelinedef.Config{
		ID: "lenient",
		Steps: []pipelinedef.Step{stateless("lenient/v1", func(ctx context.Context, inv durable.Invocation) error {
			if lenientAttempts.Add(1) == 1 {
				return errGone
			}
			return nil
		})},
	})
	_, pipes := startEngine(t, mem.New(), strict, lenient)
	rs, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	rl, _, _ := pipes[1].Schedule(context.Background(), "r", nil)
	res, err := rs.Wait(context.Background())
	if err != nil || !res.Failed() || res.Failure.Reason != "machine-gone" {
		t.Fatalf("strict Wait = %+v, %v; want a machine-gone failure", res, err)
	}
	if n := strictAttempts.Load(); n != 1 {
		t.Errorf("strict attempts = %d, want 1", n)
	}
	if res, err := rl.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("lenient Wait = %+v, %v; want success after a retry", res, err)
	}
	if n := lenientAttempts.Load(); n != 2 {
		t.Errorf("lenient attempts = %d, want 2", n)
	}
}

// TestMiddlewareComposedOnceAtBind pins that the wrapper factories run
// once per step and phase, however many attempts the operations take.
func TestMiddlewareComposedOnceAtBind(t *testing.T) {
	var factories atomic.Int32
	counting := func(next durable.Handler) durable.Handler {
		factories.Add(1)
		return next
	}
	def := pipelinedef.New(pipelinedef.Config{
		ID:         "once",
		Middleware: []durable.Middleware{counting},
		Steps: []pipelinedef.Step{
			{
				ID:         "a/v1",
				Unwind:     true,
				Run:        func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error { return nil },
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.Attempt() < 3 {
					return errors.New("transient")
				}
				return durable.Fail(errors.New("permanent"))
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	if res, err := run.Wait(context.Background()); err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v; want failure", res, err)
	}
	if n := factories.Load(); n != 3 {
		t.Fatalf("factory ran %d times, want 3: a forward, b forward, a unwind", n)
	}
}
