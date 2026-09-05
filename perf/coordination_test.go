package perf

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
)

// The coordination scenarios use small inputs deliberately: they measure
// the cross-run machinery (conflicts, cancellation, parking, wakes, class
// tokens), not payload costs, so the deterministic byte gates stay
// sensitive to coordination-path writes.

// enteredSignal marks a stale run's arrival exactly once. The step runs
// at-least-once, so a redispatch (e.g. during abnormal shutdown) must not
// close the channel twice.
type enteredSignal struct {
	ch   chan struct{}
	once sync.Once
}

func (s *enteredSignal) signal() { s.once.Do(func() { close(s.ch) }) }

// BenchmarkSupersedeCycle is the reconcile-loop pattern from the flyd
// version-check flows: a stale run holds the slot, newer intent conflicts,
// introspects the blocker's input, cancels it (unwinding its work), and
// reschedules. One cycle = conflict + introspection + cancel + unwind +
// fresh run to completion, per resource, across a concurrent population.
func BenchmarkSupersedeCycle(b *testing.B) {
	cycles := scale(b, 60)

	var (
		mu      sync.Mutex
		entered = map[durable.ResourceID]*enteredSignal{}
	)
	enteredSig := func(res durable.ResourceID) *enteredSignal {
		mu.Lock()
		defer mu.Unlock()
		sig, ok := entered[res]
		if !ok {
			sig = &enteredSignal{ch: make(chan struct{})}
			entered[res] = sig
		}
		return sig
	}

	def := pipelinedef.New(pipelinedef.Config{
		ID: "supersede",
		Steps: []pipelinedef.Step{{
			ID:     "supersede-apply/v1",
			Unwind: true,
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if inv.CancelRequested() {
					return nil, nil // resolve so cancellation can proceed
				}
				in := inv.InputMessage().(*wrapperspb.BytesValue)
				if v := in.GetValue(); len(v) > 0 && v[0] == 1 { // stale generation: hold the slot
					enteredSig(inv.ResourceID()).signal()
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, nil // fresh generation completes immediately
			},
			UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
				return nil
			},
		}},
		NewInput: func() proto.Message { return &wrapperspb.BytesValue{} },
	})
	v := newEnv(b, def)
	v.start(b)
	v1, v2 := wrapperspb.Bytes([]byte{1}), wrapperspb.Bytes([]byte{2})

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for c := 0; c < cycles; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				res := durable.ResourceID(fmt.Sprintf("sup-%d-%d", i, c))
				stale, _, err := v.pipe.Schedule(context.Background(), res, v1)
				if err != nil {
					b.Error(err)
					return
				}
				<-enteredSig(res).ch // stale run holds the slot mid-flight
				// Each resource signals exactly once; drop the entry so the
				// map doesn't accumulate across cycles and iterations
				// (flagged by Copilot review).
				mu.Lock()
				delete(entered, res)
				mu.Unlock()

				_, _, err = v.pipe.Schedule(context.Background(), res, v2)
				var conflict *durable.ScheduleConflictError
				if !errors.As(err, &conflict) {
					b.Errorf("expected conflict, got %v", err)
					return
				}
				blocker, err := v.pipe.Run(context.Background(), conflict.RunID)
				if err != nil {
					b.Error(err)
					return
				}
				if _, err := blocker.InputBytes(context.Background()); err != nil {
					b.Error(err)
					return
				}
				if err := blocker.Cancel(context.Background(), "superseded"); err != nil {
					b.Error(err)
					return
				}
				if res, err := stale.Wait(context.Background()); err != nil || !res.Canceled() {
					b.Errorf("stale Wait = %+v, %v", res, err)
					return
				}
				fresh, created, err := v.pipe.Schedule(context.Background(), res, v2)
				if err != nil || !created {
					b.Errorf("reschedule: created=%v err=%v", created, err)
					return
				}
				if res, err := fresh.Wait(context.Background()); err != nil || !res.Succeeded() {
					b.Errorf("fresh Wait = %+v, %v", res, err)
				}
			}(c)
		}
		wg.Wait()
	}
	elapsed := time.Since(start)
	n := float64(cycles) * float64(b.N)
	b.ReportMetric(float64(v.store.Stats().TxPageAllocBytes)/n, "diskB/cycle")
	b.ReportMetric(float64(v.writes.Load())/n, "transitions/cycle")
	b.ReportMetric(n/elapsed.Seconds(), "cycles/sec")
}

// BenchmarkAwaitFanout is the monitor-style dependency shape: one waiter
// parked on each of N in-flight targets. Releasing every target at once
// measures the wake fan-out — the park-to-completion latency distribution
// of the woken population.
func BenchmarkAwaitFanout(b *testing.B) {
	pairs := scale(b, 60)

	var (
		release      atomic.Pointer[chan struct{}]
		enteredCount atomic.Int64
	)
	target := pipelinedef.New(pipelinedef.Config{
		ID: "fan-target",
		Steps: []pipelinedef.Step{{
			ID: "fan-target-hold/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				enteredCount.Add(1)
				select {
				case <-*release.Load():
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}},
	})
	// The waiter receives the target RunID as its input and parks on it
	// directly: the driver already knows the ID, and a lookup scan here
	// would charge unrelated store-scan costs — growing with the terminal
	// population left behind by earlier iterations — to the wake metrics.
	waiter := pipelinedef.New(pipelinedef.Config{
		ID: "fan-waiter",
		Steps: []pipelinedef.Step{{
			ID: "fan-waiter-wait/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if _, ok := inv.AwaitedRunID(); ok {
					return nil, nil
				}
				in := inv.InputMessage().(*wrapperspb.StringValue)
				return nil, durable.AwaitRun(durable.RunID(in.GetValue()))
			},
		}},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
	})
	// Every target holds a worker slot while blocked, and waiter attempts
	// need workers besides — size the pool for the whole population.
	v := newEnv(b, target, engine.WithConcurrency(pairs*2+16))
	targetPipe := v.pipe
	waiterPipe, err := v.eng.Bind(waiter)
	if err != nil {
		b.Fatal(err)
	}
	v.start(b)

	// Failure handling below never calls b.Fatal while spawned goroutines
	// are live: Fatalf's runtime.Goexit would skip the release/join steps
	// and leave goroutines logging into a completed benchmark. Instead:
	// b.Error, release everything, join, return.
	b.ResetTimer()
	var wakeLat []time.Duration
	for i := 0; i < b.N; i++ {
		ch := make(chan struct{})
		release.Store(&ch)
		enteredCount.Store(0)

		// Targets first, scheduled concurrently — sequential Schedules
		// would keep the store's adaptive group commit disengaged and
		// charge worst-case per-write fsync costs to the gated byte
		// metrics — and all in flight before any waiter parks.
		var (
			failed atomic.Bool
			swg    sync.WaitGroup
		)
		targets := make([]engine.Run, pairs)
		for p := 0; p < pairs; p++ {
			swg.Add(1)
			go func(p int) {
				defer swg.Done()
				run, _, err := targetPipe.Schedule(context.Background(),
					durable.ResourceID(fmt.Sprintf("fan-%d-%d", i, p)), nil)
				if err != nil {
					b.Error(err)
					failed.Store(true)
					return
				}
				targets[p] = run
			}(p)
		}
		swg.Wait()
		if failed.Load() {
			close(ch)
			return
		}
		deadline := time.Now().Add(30 * time.Second)
		for enteredCount.Load() < int64(pairs) {
			if time.Now().After(deadline) {
				b.Errorf("only %d/%d targets entered within 30s", enteredCount.Load(), pairs)
				close(ch)
				return
			}
			time.Sleep(100 * time.Microsecond)
		}

		// Waiters park 1:1 on the targets.
		waiters := make([]engine.Run, pairs)
		for p := 0; p < pairs; p++ {
			swg.Add(1)
			go func(p int) {
				defer swg.Done()
				run, _, err := waiterPipe.Schedule(context.Background(),
					durable.ResourceID(fmt.Sprintf("fan-%d-%d", i, p)),
					wrapperspb.String(string(targets[p].ID())))
				if err != nil {
					b.Error(err)
					failed.Store(true)
					return
				}
				waiters[p] = run
			}(p)
		}
		swg.Wait()
		if failed.Load() {
			close(ch)
			return
		}

		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			done = make([]time.Time, 0, pairs)
		)
		for _, run := range waiters {
			wg.Add(1)
			go func(run engine.Run) {
				defer wg.Done()
				if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
					b.Errorf("waiter Wait = %+v, %v", res, err)
					return
				}
				mu.Lock()
				done = append(done, time.Now())
				mu.Unlock()
			}(run)
		}
		if err := waitAllAwaiting(waiters); err != nil {
			b.Error(err)
			close(ch)
			wg.Wait()
			return
		}

		releasedAt := time.Now()
		close(ch)
		wg.Wait()
		for _, at := range done {
			wakeLat = append(wakeLat, at.Sub(releasedAt))
		}
		for _, t := range targets {
			// All goroutines joined above, so Fatalf is safe here.
			if res, err := t.Wait(context.Background()); err != nil || !res.Succeeded() {
				b.Fatalf("target Wait = %+v, %v", res, err)
			}
		}
	}
	n := float64(pairs*2) * float64(b.N) // targets + waiters
	b.ReportMetric(float64(v.store.Stats().TxPageAllocBytes)/n, "diskB/run")
	b.ReportMetric(float64(v.writes.Load())/n, "transitions/run")
	if len(wakeLat) > 0 {
		sort.Slice(wakeLat, func(i, j int) bool { return wakeLat[i] < wakeLat[j] })
		b.ReportMetric(ms(wakeLat[len(wakeLat)/2]), "wake-p50-ms")
		// The slowest wake, honestly named: at -benchtime 1x there are
		// only `pairs` samples, so a "p99" would be this same single
		// worst sample — too noisy to gate. perfcompare treats
		// wake-max-ms as informative.
		b.ReportMetric(ms(wakeLat[len(wakeLat)-1]), "wake-max-ms")
	}
}

// waitAllAwaiting polls until every run is parked in RunStateAwaiting.
// It returns an error instead of failing the benchmark itself: callers
// have live goroutines holding b and must release and join them first.
func waitAllAwaiting(runs []engine.Run) error {
	deadline := time.Now().Add(30 * time.Second)
	for _, r := range runs {
		for {
			st, err := r.Status(context.Background())
			if err != nil {
				return err
			}
			if st.State == engine.RunStateAwaiting {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("waiter %s never parked (state %v)", r.ID(), st.State)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	return nil
}

// BenchmarkThrottleContention drives the machine-start shape through a
// capacity-4 concurrency class on the reserve-like step: token churn and
// FIFO wake costs under heavy contention. Throttle parks are in-memory, so
// the byte gates double as proof that throttling adds no durable writes.
func BenchmarkThrottleContention(b *testing.B) {
	runs := scale(b, 60)
	var specs [numSteps]stepSpec
	specs[2].class = "gate"
	def := machinePipeline("throttled", specs)
	v := newEnv(b, def, engine.WithConcurrencyClass("gate", 4))
	v.start(b)

	b.ResetTimer()
	start := time.Now()
	var lat []time.Duration
	for i := 0; i < b.N; i++ {
		lat = append(lat, runPopulation(b, v.pipe, runs, fmt.Sprintf("thr-%d", i))...)
	}
	report(b, v, runs, lat, time.Since(start))
}
