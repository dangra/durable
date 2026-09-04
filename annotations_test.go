package durable_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
)

// TestTracePropagationPattern is the documented tracing shape end to
// end: a trace context injected at Schedule via WithAnnotations must
// reach every attempt of every operation — forward and unwind, before
// and after an engine restart — through a tracing middleware reading
// Invocation.Annotations, and surface on the scheduled/terminal
// observer events for span links and labels.
func TestTracePropagationPattern(t *testing.T) {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	var (
		mu   sync.Mutex
		seen []string // one traceparent observation per middleware pass
	)
	tracing := func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			mu.Lock()
			seen = append(seen, inv.Annotations()["traceparent"])
			mu.Unlock()
			return next(ctx, inv)
		}
	}

	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "traced",
		Steps: []durable.StepConfig{
			{
				ID:     "prepare/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					if inv.Attempt() == 1 {
						return nil, errors.New("transient")
					}
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return nil
				},
			},
			stateless("explode/v1", func(ctx context.Context, inv *durable.Invocation) error {
				return durable.Fail(errors.New("boom"))
			}),
		},
	})

	store := durabletest.NewMemStore()
	var scheduled durable.RunEvent
	var terminal durable.RunTerminalEvent
	obs := durable.Observer{
		RunScheduled: func(ev durable.RunEvent) { mu.Lock(); scheduled = ev; mu.Unlock() },
		RunTerminal:  func(ev durable.RunTerminalEvent) { mu.Lock(); terminal = ev; mu.Unlock() },
	}
	boot := func() (*durable.Engine, *durable.Pipeline) {
		e := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0),
			durable.WithLogger(discardTestLogger()),
			durable.WithMiddleware(tracing), durable.WithObserver(obs))
		pipe, err := def.Bind(e)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		return e, pipe
	}

	// Schedule with the trace context and a delayed start, then restart
	// the engine before the first attempt can run: the annotations must
	// come back from the store, not from process memory.
	e, pipe := boot()
	run, _, err := pipe.Schedule(context.Background(), "res-1", nil,
		durable.WithAnnotations(map[string]string{"traceparent": traceparent}),
		durable.WithAnnotations(map[string]string{"tenant": "acme"}), // options merge
		durable.StartAfter(30*time.Millisecond))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := e.Stop(sctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	cancel()

	e2, pipe2 := boot()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e2.Stop(ctx)
	})
	run2, err := pipe2.Run(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if res, err := run2.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeFailure {
		t.Fatalf("Wait = %+v, %v", res, err)
	}

	mu.Lock()
	defer mu.Unlock()
	// prepare retry + success, explode fail, prepare unwind: 4 passes,
	// every one carrying the traceparent from the store.
	if len(seen) < 4 {
		t.Fatalf("middleware passes = %d, want >= 4", len(seen))
	}
	for i, tp := range seen {
		if tp != traceparent {
			t.Fatalf("pass %d traceparent = %q", i, tp)
		}
	}
	if scheduled.Annotations["traceparent"] != traceparent || scheduled.Annotations["tenant"] != "acme" {
		t.Fatalf("RunScheduled annotations = %v", scheduled.Annotations)
	}
	if terminal.Annotations["tenant"] != "acme" {
		t.Fatalf("RunTerminal annotations = %v", terminal.Annotations)
	}

	// Run-level accessor returns a caller-owned copy.
	ann, err := run2.Annotations(context.Background())
	if err != nil || ann["traceparent"] != traceparent {
		t.Fatalf("Run.Annotations = %v, %v", ann, err)
	}
	ann["traceparent"] = "mutated"
	again, _ := run2.Annotations(context.Background())
	if again["traceparent"] != traceparent {
		t.Fatal("Run.Annotations returned a shared map")
	}
}

// TestAnnotationsDedupAndValidation pins the identity and contract
// rules: annotations never affect duplicate-scheduling identity (the
// active Run keeps its own), absent annotations read as nil, and
// invalid UTF-8 is rejected at Schedule.
func TestAnnotationsDedupAndValidation(t *testing.T) {
	hold := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "annotated",
		Steps: []durable.StepConfig{
			stateless("hold/v1", func(ctx context.Context, inv *durable.Invocation) error {
				select {
				case <-hold:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	pipe := pipes[0]

	first, created, err := pipe.Schedule(context.Background(), "res-1", nil,
		durable.WithAnnotations(map[string]string{"origin": "first"}))
	if err != nil || !created {
		t.Fatalf("Schedule: created=%v err=%v", created, err)
	}
	// Same input, different annotations: dedup returns the active Run,
	// whose annotations win.
	dup, created, err := pipe.Schedule(context.Background(), "res-1", nil,
		durable.WithAnnotations(map[string]string{"origin": "second"}))
	if err != nil || created || dup.ID() != first.ID() {
		t.Fatalf("dedup Schedule = %v created=%v err=%v", dup.ID(), created, err)
	}
	ann, err := dup.Annotations(context.Background())
	if err != nil || ann["origin"] != "first" {
		t.Fatalf("dedup annotations = %v, %v (active Run's must win)", ann, err)
	}
	close(hold)
	if _, err := first.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	// No annotations reads as nil, run- and invocation-side.
	bare, _, err := pipe.Schedule(context.Background(), "res-2", nil)
	if err != nil {
		t.Fatalf("Schedule bare: %v", err)
	}
	if ann, err := bare.Annotations(context.Background()); err != nil || ann != nil {
		t.Fatalf("bare annotations = %v, %v", ann, err)
	}

	// Invalid UTF-8 keys or values are rejected upfront.
	for name, m := range map[string]map[string]string{
		"key":   {"k\xff": "v"},
		"value": {"k": "v\xff"},
	} {
		if _, _, err := pipe.Schedule(context.Background(), "res-3", nil, durable.WithAnnotations(m)); err == nil {
			t.Fatalf("Schedule accepted invalid UTF-8 annotation %s", name)
		}
	}
}
