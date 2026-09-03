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
)

// The coordination scenarios use small inputs deliberately: they measure
// the cross-run machinery (conflicts, cancellation, parking, wakes, class
// tokens), not payload costs, so the deterministic byte gates stay
// sensitive to coordination-path writes.

// BenchmarkSupersedeCycle is the reconcile-loop pattern from the flyd
// version-check flows: a stale run holds the slot, newer intent conflicts,
// introspects the blocker's input, cancels it (unwinding its work), and
// reschedules. One cycle = conflict + introspection + cancel + unwind +
// fresh run to completion, per resource, across a concurrent population.
func BenchmarkSupersedeCycle(b *testing.B) {
	cycles := scale(b, 60)

	var (
		mu      sync.Mutex
		entered = map[durable.ResourceID]chan struct{}{}
	)
	enteredCh := func(res durable.ResourceID) chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		ch, ok := entered[res]
		if !ok {
			ch = make(chan struct{})
			entered[res] = ch
		}
		return ch
	}

	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "supersede",
		Steps: []durable.StepConfig{{
			ID:     "supersede-apply/v1",
			Unwind: true,
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				if inv.CancelRequested() {
					return nil, nil // resolve so cancellation can proceed
				}
				in := inv.InputMessage().(*wrapperspb.BytesValue)
				if in.GetValue()[0] == 1 { // stale generation: hold the slot
					close(enteredCh(inv.ResourceID()))
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, nil // fresh generation completes immediately
			},
			UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
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
				<-enteredCh(res) // stale run holds the slot mid-flight

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
	target := durable.NewDefinition(durable.DefinitionConfig{
		ID: "fan-target",
		Steps: []durable.StepConfig{{
			ID: "fan-target-hold/v1",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
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
	var targetPipe *durable.Pipeline
	waiter := durable.NewDefinition(durable.DefinitionConfig{
		ID: "fan-waiter",
		Steps: []durable.StepConfig{{
			ID: "fan-waiter-wait/v1",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				if _, ok := inv.AwaitedRunID(); ok {
					return nil, nil
				}
				run, ok, err := targetPipe.ActiveRun(ctx, inv.ResourceID())
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, nil // target already done (never in this scenario)
				}
				return nil, durable.AwaitRun(run.ID())
			},
		}},
	})
	// Every target holds a worker slot while blocked, and waiter attempts
	// need workers besides — size the pool for the whole population.
	v := newEnv(b, target, durable.WithConcurrency(pairs*2+16))
	var err error
	targetPipe = v.pipe
	waiterPipe, err := waiter.Bind(v.engine)
	if err != nil {
		b.Fatal(err)
	}
	v.start(b)

	b.ResetTimer()
	var wakeLat []time.Duration
	for i := 0; i < b.N; i++ {
		ch := make(chan struct{})
		release.Store(&ch)
		enteredCount.Store(0)

		// Targets first, all in flight before any waiter looks for them.
		var targets []durable.Run
		for p := 0; p < pairs; p++ {
			run, _, err := targetPipe.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("fan-%d-%d", i, p)), nil)
			if err != nil {
				b.Fatal(err)
			}
			targets = append(targets, run)
		}
		deadline := time.Now().Add(30 * time.Second)
		for enteredCount.Load() < int64(pairs) {
			if time.Now().After(deadline) {
				b.Fatal("targets never all entered")
			}
			time.Sleep(100 * time.Microsecond)
		}

		// Waiters park 1:1 on the targets.
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			done = make([]time.Time, 0, pairs)
		)
		waiters := make([]durable.Run, pairs)
		for p := 0; p < pairs; p++ {
			run, _, err := waiterPipe.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("fan-%d-%d", i, p)), nil)
			if err != nil {
				b.Fatal(err)
			}
			waiters[p] = run
			wg.Add(1)
			go func(run durable.Run) {
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
		waitAllAwaiting(b, waiters)

		releasedAt := time.Now()
		close(ch)
		wg.Wait()
		for _, at := range done {
			wakeLat = append(wakeLat, at.Sub(releasedAt))
		}
		for _, t := range targets {
			if res, err := t.Wait(context.Background()); err != nil || !res.Succeeded() {
				b.Fatalf("target Wait = %+v, %v", res, err)
			}
		}
	}
	n := float64(pairs*2) * float64(b.N) // targets + waiters
	b.ReportMetric(float64(v.store.Stats().TxPageAllocBytes)/n, "diskB/run")
	b.ReportMetric(float64(v.writes.Load())/n, "transitions/run")
	sort.Slice(wakeLat, func(i, j int) bool { return wakeLat[i] < wakeLat[j] })
	b.ReportMetric(ms(wakeLat[len(wakeLat)/2]), "wake-p50-ms")
	b.ReportMetric(ms(wakeLat[len(wakeLat)*99/100]), "wake-p99-ms")
}

func waitAllAwaiting(b *testing.B, runs []durable.Run) {
	b.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for _, r := range runs {
		for {
			st, err := r.Status(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if st.State == durable.RunStateAwaiting {
				break
			}
			if time.Now().After(deadline) {
				b.Fatalf("waiter %s never parked (state %v)", r.ID(), st.State)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
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
	v := newEnv(b, def, durable.WithConcurrencyClass("gate", 4))
	v.start(b)

	b.ResetTimer()
	start := time.Now()
	var lat []time.Duration
	for i := 0; i < b.N; i++ {
		lat = append(lat, runPopulation(b, v.pipe, runs, fmt.Sprintf("thr-%d", i))...)
	}
	report(b, v, runs, lat, time.Since(start))
}
