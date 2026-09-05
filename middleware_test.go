// Middleware: ordering, phases, escalation to a permanent failure, and
// context propagation into handlers.
package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
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
			return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				record(fmt.Sprintf("%s:in:%v:%s:%d", name, inv.Phase(), inv.StepID(), inv.Attempt()))
				state, err := next(ctx, inv)
				record(name + ":out")
				return state, err
			}
		}
	}

	store := durabletest.NewMemStore()
	e := durable.NewEngine(store, fastRetry, durable.WithMiddleware(mw("outer"), mw("inner")))
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "mw",
		Steps: []durable.StepConfig{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return nil
				},
			},
			stateless("b/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if inv.Attempt() == 1 {
					return errors.New("transient")
				}
				return durable.Fail(errors.New("permanent"))
			}),
		},
	})
	p, err := def.Bind(e)
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
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			state, err := next(ctx, inv)
			if err != nil {
				return state, durable.Fail(err)
			}
			return state, err
		}
	}
	var attempts atomic.Uint64
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "escalating",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				attempts.Store(inv.Attempt())
				return errors.New("would ordinarily retry")
			}),
		},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry, durable.WithMiddleware(escalate))
	p, _ := def.Bind(e)
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
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			return next(context.WithValue(ctx, ctxKey{}, "present"), inv)
		}
	}
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "ctxpipe",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if ctx.Value(ctxKey{}) != "present" {
					return durable.Fail(errors.New("middleware context value missing"))
				}
				return nil
			}),
		},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry, durable.WithMiddleware(inject))
	p, _ := def.Bind(e)
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
