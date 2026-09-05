package engine_test

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/observe"
)

type tenantKey struct{}

// TestScheduleAnnotator pins the engine-wide annotation seam: annotators
// derive annotations from the caller's ctx on every Schedule, compose in
// installation order, and lose to explicit call-site options on key
// conflicts.
func TestScheduleAnnotator(t *testing.T) {
	var (
		mu   sync.Mutex
		seen map[string]string
	)
	obs := observe.Observer{RunScheduled: func(ev observe.RunEvent) {
		mu.Lock()
		seen = ev.Annotations
		mu.Unlock()
	}}
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "annotated",
		Steps: []engine.StepConfig{{
			ID: "noop/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		}},
	})
	e := engine.New(durabletest.NewMemStore(), fastRetry,
		engine.WithLogger(discardTestLogger()), engine.WithObserver(obs),
		engine.WithScheduleAnnotator(func(ctx context.Context) map[string]string {
			tenant, _ := ctx.Value(tenantKey{}).(string)
			if tenant == "" {
				return nil // contributes nothing
			}
			return map[string]string{"tenant": tenant, "shared": "from-first"}
		}),
		engine.WithScheduleAnnotator(func(context.Context) map[string]string {
			return map[string]string{"second": "yes", "shared": "from-second"}
		}),
		engine.WithScheduleAnnotator(nil), // ignored
	)
	pipe, err := def.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	ctx := context.WithValue(context.Background(), tenantKey{}, "acme")
	run, _, err := pipe.Schedule(ctx, "res-1", nil,
		engine.WithAnnotations(map[string]string{"shared": "explicit"}))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if _, err := run.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	want := map[string]string{
		"tenant": "acme",     // from the ctx via the first annotator
		"second": "yes",      // annotators compose
		"shared": "explicit", // the call site wins over both annotators
	}
	mu.Lock()
	for k, v := range want {
		if seen[k] != v {
			mu.Unlock()
			t.Fatalf("annotations[%q] = %q, want %q (all: %v)", k, seen[k], v, seen)
		}
	}
	mu.Unlock()

	// A ctx without the value: the first annotator contributes nothing,
	// the second still does.
	run2, _, err := pipe.Schedule(context.Background(), "res-2", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if _, err := run2.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if _, ok := seen["tenant"]; ok || seen["second"] != "yes" {
		t.Fatalf("annotations without ctx value = %v, want only the second annotator's", seen)
	}
}

// TestScheduleAnnotatorInvalidUTF8 pins that annotator output passes
// through the same validation as call-site annotations.
func TestScheduleAnnotatorInvalidUTF8(t *testing.T) {
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "annotated-bad",
		Steps: []engine.StepConfig{{
			ID: "noop/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		}},
	})
	e := engine.New(durabletest.NewMemStore(), fastRetry,
		engine.WithLogger(discardTestLogger()),
		engine.WithScheduleAnnotator(func(context.Context) map[string]string {
			return map[string]string{"bad": "\xff\xfe"}
		}))
	pipe, err := def.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	if _, _, err := pipe.Schedule(context.Background(), "res-1", nil); err == nil {
		t.Fatal("Schedule accepted invalid UTF-8 from an annotator")
	}
}
