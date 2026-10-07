package badger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
)

// The benchmarks model the flyd machine-start shape, as the bbolt
// driver's do: a fat input (32KB config) through an 8-step pipeline
// committing modest per-step states.

const (
	benchInputSize = 32 << 10
	benchStateSize = 1 << 10
	benchSteps     = 8
)

func benchEngine(b *testing.B, def *pipelinedef.Definition) (*Store, *engine.Pipeline) {
	b.Helper()
	s, err := Open(filepath.Join(b.TempDir(), "bench"))
	if err != nil {
		b.Fatal(err)
	}
	e := engine.New(s,
		engine.WithRetryPolicy(engine.RetryPolicy{Initial: time.Microsecond, Max: time.Microsecond, Multiplier: 1}),
		engine.WithConcurrency(64),
		engine.WithLogger(slog.New(slog.DiscardHandler)),
	)
	p, err := e.Bind(def)
	if err != nil {
		b.Fatal(err)
	}
	if err := e.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		e.Stop(context.Background())
		s.Close()
	})
	return s, p
}

func BenchmarkRunWrites(b *testing.B) {
	state := make([]byte, benchStateSize)
	var steps []pipelinedef.Step
	for i := 0; i < benchSteps; i++ {
		steps = append(steps, pipelinedef.Step{
			ID:       durable.StepID(fmt.Sprintf("step-%d/v1", i)),
			HasState: true,
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return wrapperspb.Bytes(state), nil
			},
		})
	}
	def := pipelinedef.New(pipelinedef.Config{
		ID:       "bench-run",
		Steps:    steps,
		NewInput: func() proto.Message { return &wrapperspb.BytesValue{} },
	})
	_, p := benchEngine(b, def)
	input := wrapperspb.Bytes(make([]byte, benchInputSize))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run, _, err := p.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("r-%d", i)), input)
		if err != nil {
			b.Fatal(err)
		}
		if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
			b.Fatalf("run failed: %+v %v", res, err)
		}
	}
}

func BenchmarkRetryWrites(b *testing.B) {
	const retries = 10
	def := pipelinedef.New(pipelinedef.Config{
		ID: "bench-retry",
		Steps: []pipelinedef.Step{{
			ID: "flaky/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if inv.Attempt() <= retries {
					return nil, errors.New("transient")
				}
				return nil, nil
			},
		}},
		NewInput: func() proto.Message { return &wrapperspb.BytesValue{} },
	})
	_, p := benchEngine(b, def)
	input := wrapperspb.Bytes(make([]byte, benchInputSize))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run, _, err := p.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("r-%d", i)), input)
		if err != nil {
			b.Fatal(err)
		}
		if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
			b.Fatalf("run failed: %+v %v", res, err)
		}
	}
}

// BenchmarkConcurrentRuns measures transition throughput when many runs
// execute in parallel. Small payloads isolate commit costs from byte
// costs.
func BenchmarkConcurrentRuns(b *testing.B) {
	const parallel = 32
	var steps []pipelinedef.Step
	for i := 0; i < 4; i++ {
		steps = append(steps, pipelinedef.Step{
			ID: durable.StepID(fmt.Sprintf("step-%d/v1", i)),
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		})
	}
	def := pipelinedef.New(pipelinedef.Config{ID: "bench-conc", Steps: steps})
	_, p := benchEngine(b, def)

	b.ResetTimer()
	var wg sync.WaitGroup
	sem := make(chan struct{}, parallel)
	for i := 0; i < b.N; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			run, _, err := p.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("c-%d", i)), nil)
			if err != nil {
				b.Error(err)
				return
			}
			if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
				b.Errorf("run: %+v %v", res, err)
			}
		}(i)
	}
	wg.Wait()
}
