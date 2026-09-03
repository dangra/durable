package bboltstore

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
)

// The benchmarks model the flyd machine-start shape: a fat input (32KB
// config) through an 8-step pipeline committing modest per-step states.
// diskB/op is bbolt page-allocation bytes per run — the write
// amplification under measurement.

const (
	benchInputSize = 32 << 10
	benchStateSize = 1 << 10
	benchSteps     = 8
)

func benchEngine(b *testing.B, def *durable.Definition) (*Store, *durable.Pipeline) {
	b.Helper()
	s, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	e := durable.NewEngine(s,
		durable.WithRetryPolicy(durable.RetryPolicy{Initial: time.Microsecond, Max: time.Microsecond, Multiplier: 1}),
		durable.WithConcurrency(64),
		durable.WithLogger(slog.New(slog.DiscardHandler)),
	)
	p, err := def.Bind(e)
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
	var steps []durable.StepConfig
	for i := 0; i < benchSteps; i++ {
		steps = append(steps, durable.StepConfig{
			ID:       durable.StepID(fmt.Sprintf("step-%d/v1", i)),
			HasState: true,
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				return wrapperspb.Bytes(state), nil
			},
		})
	}
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID:       "bench-run",
		Steps:    steps,
		NewInput: func() proto.Message { return &wrapperspb.BytesValue{} },
	})
	s, p := benchEngine(b, def)
	input := wrapperspb.Bytes(make([]byte, benchInputSize))

	b.ResetTimer()
	before := s.db.Stats()
	for i := 0; i < b.N; i++ {
		run, _, err := p.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("r-%d", i)), input)
		if err != nil {
			b.Fatal(err)
		}
		if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
			b.Fatalf("run failed: %+v %v", res, err)
		}
	}
	after := s.db.Stats()
	d := after.TxStats.Sub(&before.TxStats)
	b.ReportMetric(float64(d.GetPageAlloc())/float64(b.N), "diskB/op")
	b.ReportMetric(float64(d.GetWrite())/float64(b.N), "txwrites/op")
}

func BenchmarkRetryWrites(b *testing.B) {
	const retries = 10
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "bench-retry",
		Steps: []durable.StepConfig{{
			ID: "flaky/v1",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				if inv.Attempt() <= retries {
					return nil, errors.New("transient")
				}
				return nil, nil
			},
		}},
		NewInput: func() proto.Message { return &wrapperspb.BytesValue{} },
	})
	s, p := benchEngine(b, def)
	input := wrapperspb.Bytes(make([]byte, benchInputSize))

	b.ResetTimer()
	before := s.db.Stats()
	for i := 0; i < b.N; i++ {
		run, _, err := p.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("r-%d", i)), input)
		if err != nil {
			b.Fatal(err)
		}
		if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
			b.Fatalf("run failed: %+v %v", res, err)
		}
	}
	after := s.db.Stats()
	d := after.TxStats.Sub(&before.TxStats)
	b.ReportMetric(float64(d.GetPageAlloc())/float64(b.N), "diskB/op")
	b.ReportMetric(float64(d.GetPageAlloc())/float64(b.N)/(retries+1), "diskB/attempt")
}

// BenchmarkConcurrentRuns measures transition throughput when many runs
// execute in parallel — the single-writer serialization question. Small
// payloads isolate commit costs from byte costs.
func BenchmarkConcurrentRuns(b *testing.B) {
	const parallel = 32
	var steps []durable.StepConfig
	for i := 0; i < 4; i++ {
		steps = append(steps, durable.StepConfig{
			ID: durable.StepID(fmt.Sprintf("step-%d/v1", i)),
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		})
	}
	def := durable.NewDefinition(durable.DefinitionConfig{ID: "bench-conc", Steps: steps})
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
