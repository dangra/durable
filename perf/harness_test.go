// Package perf holds the performance regression suite: engine-level
// workload scenarios modeled on the flyd machine-orchestration use case.
//
// Metrics come in two tiers. Deterministic counters gate tightly in CI:
// transitions/run (logical store writes observed via a durable.Observer,
// exactly deterministic, gated two-sided at 0.1%) and the
// near-deterministic byte and allocation metrics (diskB/*, B/op,
// allocs/op, gated at 10%). Every scenario runs with the observer
// installed, so observer-path overhead is itself gated. Wall-clock metrics
// (p50-ms, p99-ms, runs/sec, ns/op) are gated loosely at 25% on the best
// of the -count samples, since shared-runner noise only adds time; the
// internal/perfcompare tool applies the per-metric-class thresholds.
//
// Every scenario is write-deterministic: retries are bounded by attempt
// number, never by timing, so the logical write counts are identical
// across runs and machines.
package perf

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
)

// The flyd machine-start shape: a fat config input through a pipeline of
// several state-committing steps.
const (
	inputSize = 32 << 10
	stateSize = 1 << 10
	numSteps  = 8
)

// scale shrinks scenario populations under -short (and in -race runs,
// which multiply costs without changing the counters' meaning per run).
func scale(t testing.TB, full int) int {
	if testing.Short() {
		return max(full/5, 2)
	}
	return full
}

var fatInput = wrapperspb.Bytes(make([]byte, inputSize))

type env struct {
	store  *bboltstore.Store
	writes *atomic.Int64
	engine *durable.Engine
	pipe   *durable.Pipeline
}

// countingObserver counts logical write calls through the StoreOp
// observer. Unlike bbolt-level transaction/page counts — which the
// adaptive group commit makes timing-dependent — these are exactly
// deterministic per scenario, so they gate at effectively zero
// tolerance. Installing it in every scenario also makes observer-path
// overhead part of every gated metric: a regression in the emit path
// shows up in the wall-clock and allocation gates.
func countingObserver(writes *atomic.Int64) durable.Observer {
	return durable.Observer{
		StoreOp: func(ev durable.StoreOpEvent) {
			switch ev.Op {
			case "CreateRun", "ApplyTransition", "RequestCancel":
				writes.Add(1)
			}
		},
		// One logical delete per reaped run.
		RunsReaped: func(count int) { writes.Add(int64(count)) },
	}
}

// newEnv builds a store and engine with the given definition bound, not
// yet started.
func newEnv(b *testing.B, def *durable.Definition, opts ...durable.Option) *env {
	b.Helper()
	store, err := bboltstore.Open(filepath.Join(b.TempDir(), "perf.db"))
	if err != nil {
		b.Fatal(err)
	}
	writes := new(atomic.Int64)
	opts = append([]durable.Option{
		durable.WithConcurrency(32),
		durable.WithRetryPolicy(durable.RetryPolicy{
			Initial: 500 * time.Microsecond, Max: 2 * time.Millisecond, Multiplier: 2,
		}),
		durable.WithRecoveryBackoff(0),
		durable.WithLogger(discardLogger()),
		durable.WithObserver(countingObserver(writes)),
	}, opts...)
	e := durable.NewEngine(store, opts...)
	pipe, err := def.Bind(e)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
		_ = store.Close()
	})
	return &env{store: store, writes: writes, engine: e, pipe: pipe}
}

func (v *env) start(b *testing.B) {
	b.Helper()
	if err := v.engine.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
}

// stepConfig builds one state-committing step; failAt, when > 0, makes the
// step return an ordinary error while attempt < failAt (bounded,
// deterministic retries), and permanentAt makes it Fail at that attempt.
type stepSpec struct {
	unwind    bool
	retries   uint64 // succeed on attempt retries+1
	permanent bool   // durable.Fail instead of succeeding
	class     string // concurrency class, if any
}

func machinePipeline(id durable.PipelineID, specs [numSteps]stepSpec) *durable.Definition {
	state := wrapperspb.Bytes(make([]byte, stateSize))
	var steps []durable.StepConfig
	for i, spec := range specs {
		spec := spec
		sc := durable.StepConfig{
			ID:               durable.StepID(fmt.Sprintf("%s-step-%d/v1", id, i)),
			HasState:         true,
			Unwind:           spec.unwind,
			ConcurrencyClass: spec.class,
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				if inv.Attempt() <= spec.retries {
					return nil, errTransient
				}
				if spec.permanent {
					return nil, durable.Fail(errPermanent)
				}
				return state, nil
			},
		}
		if spec.unwind {
			sc.UnwindFunc = func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
				return nil
			}
		}
		steps = append(steps, sc)
	}
	return durable.NewDefinition(durable.DefinitionConfig{
		ID:       id,
		Steps:    steps,
		NewInput: func() proto.Message { return &wrapperspb.BytesValue{} },
	})
}

// runPopulation schedules n runs concurrently and waits for all of them,
// returning per-run schedule-to-terminal latencies.
func runPopulation(b *testing.B, pipe *durable.Pipeline, n int, prefix string) []time.Duration {
	b.Helper()
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		lat = make([]time.Duration, 0, n)
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := time.Now()
			run, _, err := pipe.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("%s-%d", prefix, i)), fatInput)
			if err != nil {
				b.Error(err)
				return
			}
			if _, err := run.Wait(context.Background()); err != nil {
				b.Error(err)
				return
			}
			d := time.Since(start)
			mu.Lock()
			lat = append(lat, d)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return lat
}

// report emits the two metric tiers: the exactly-deterministic logical
// write count and near-deterministic disk bytes per run, plus wall-clock
// latency/throughput.
func report(b *testing.B, v *env, runs int, lat []time.Duration, elapsed time.Duration) {
	b.Helper()
	n := float64(runs) * float64(b.N)
	b.ReportMetric(float64(v.store.Stats().TxPageAllocBytes)/n, "diskB/run")
	b.ReportMetric(float64(v.writes.Load())/n, "transitions/run")
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		b.ReportMetric(ms(lat[len(lat)/2]), "p50-ms")
		b.ReportMetric(ms(lat[len(lat)*99/100]), "p99-ms")
	}
	if elapsed > 0 {
		b.ReportMetric(n/elapsed.Seconds(), "runs/sec")
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

type sentinelErr string

func (e sentinelErr) Error() string { return string(e) }

const (
	errTransient sentinelErr = "transient"
	errPermanent sentinelErr = "permanent"
)

// seedPopulation persists nonterminal runs parked one step from completion
// plus a terminal population, writing concurrently so the store's adaptive
// group commit batches the seeding fsyncs. Returns the nonterminal RunIDs.
func seedPopulation(b *testing.B, v *env, nonterminal, terminal int) []durable.RunID {
	b.Helper()
	state := make([]byte, stateSize)
	oc := durable.OutcomeSuccess
	sem := make(chan struct{}, 64)
	var wg sync.WaitGroup
	ids := make([]durable.RunID, nonterminal)

	seed := func(i int, term bool) {
		defer wg.Done()
		defer func() { <-sem }()
		kind := "nt"
		if term {
			kind = "t"
		}
		id := durable.RunID(fmt.Sprintf("seed-%s-%06d", kind, i))
		rec := &durable.RunRecord{
			RunID:      id,
			PipelineID: "recover",
			ResourceID: durable.ResourceID(fmt.Sprintf("seed-%s-%d", kind, i)),
			Input:      fatInputBytes,
			Phase:      durable.PhaseForward,
			Steps:      map[durable.StepID]*durable.StepRecord{},
			CreatedAt:  time.Now(),
		}
		steps := numSteps - 1 // parked before the last step
		if term {
			steps = numSteps
		}
		for s := 0; s < steps; s++ {
			rec.Steps[durable.StepID(fmt.Sprintf("recover-step-%d/v1", s))] = &durable.StepRecord{
				ForwardStatus: durable.OpSucceeded, ForwardAttempts: 1, State: state,
			}
		}
		if _, created, err := v.store.CreateRun(context.Background(), rec); err != nil || !created {
			b.Errorf("seed CreateRun: created=%v err=%v", created, err)
			return
		}
		if term {
			err := v.store.ApplyTransition(context.Background(), id, durable.Transition{
				Cursor:  durable.Cursor{Phase: durable.PhaseDone, UpdatedAt: time.Now()},
				Outcome: &oc,
			})
			if err != nil {
				b.Errorf("seed terminal: %v", err)
			}
		} else {
			ids[i] = id
		}
	}
	for i := 0; i < nonterminal; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go seed(i, false)
	}
	for i := 0; i < terminal; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go seed(i, true)
	}
	wg.Wait()
	return ids
}

var fatInputBytes = mustMarshal()

func mustMarshal() []byte {
	b, err := proto.Marshal(fatInput)
	if err != nil {
		panic(err)
	}
	return b
}
