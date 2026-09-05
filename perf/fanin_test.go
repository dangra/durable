package perf

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
)

// BenchmarkAwaitFanIn is the release-train shape inverted from
// BenchmarkAwaitFanout: one parent parked with AwaitAll on N children
// that finish one at a time. Every child completion pokes the parent,
// whose gate re-runs and re-parks until the last child is done — so the
// scenario measures what a spurious wake costs, in store reads, as a
// function of the fan-in. reads/child is the metric that keeps the gate
// from going quadratic in N.
func BenchmarkAwaitFanIn(b *testing.B) {
	children := scale(b, 200)

	var (
		gates   sync.Map // ResourceID -> chan struct{}
		entered atomic.Int64
	)
	gate := func(r durable.ResourceID) chan struct{} {
		ch, _ := gates.LoadOrStore(r, make(chan struct{}))
		return ch.(chan struct{})
	}
	child := durable.NewDefinition(durable.DefinitionConfig{
		ID: "fanin-child",
		Steps: []durable.StepConfig{{
			ID: "fanin-child-hold/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				entered.Add(1)
				select {
				case <-gate(inv.ResourceID()):
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}},
	})
	var childPipe *durable.Pipeline
	var iter atomic.Int64
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "fanin-parent",
		Steps: []durable.StepConfig{{
			ID: "fanin-parent-ship/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if w, ok := inv.Awaited(); ok {
					if len(w.Pending()) != 0 || w.Expired {
						return nil, durable.Fail(fmt.Errorf("woke with %d pending, expired=%v", len(w.Pending()), w.Expired))
					}
					return nil, nil
				}
				i := iter.Load()
				ids := make([]durable.RunID, 0, children)
				for c := 0; c < children; c++ {
					run, _, err := childPipe.Schedule(ctx, durable.ResourceID(fmt.Sprintf("fanin-%d-%d", i, c)), nil)
					if err != nil {
						return nil, err
					}
					ids = append(ids, run.ID())
				}
				return nil, durable.AwaitAll(ids)
			},
		}},
	})
	// Every child holds a worker slot while blocked; size the pool for
	// the whole population plus the parent and its wakes.
	v := newEnv(b, parent, durable.WithConcurrency(children+16))
	parentPipe := v.pipe
	cp, err := child.Bind(v.engine)
	if err != nil {
		b.Fatal(err)
	}
	childPipe = cp
	v.start(b)

	b.ResetTimer()
	var lastWake time.Duration
	for i := 0; i < b.N; i++ {
		iter.Store(int64(i))
		entered.Store(0)
		pRun, _, err := parentPipe.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("fanin-parent-%d", i)), nil)
		if err != nil {
			b.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for entered.Load() < int64(children) {
			if time.Now().After(deadline) {
				b.Fatalf("only %d/%d children entered within 30s", entered.Load(), children)
			}
			time.Sleep(100 * time.Microsecond)
		}
		if err := waitAllAwaiting([]durable.Run{pRun}); err != nil {
			b.Fatal(err)
		}

		// One child at a time: each completion is a spurious wake for the
		// parent until the last. The driver waits for the child's terminal
		// commit and then for the store to go quiet — the parent's gate
		// run — before releasing the next, so pokes never coalesce and
		// reads/child is the cost of one gate run, not an amortized one.
		st, err := pRun.Status(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		var releasedLast time.Time
		for c, id := range st.AwaitingRunIDs {
			if !quiesceReads(v.reads) {
				b.Fatalf("store reads never went quiet before child %d: a gate run is stuck", c)
			}
			close(gate(durable.ResourceID(fmt.Sprintf("fanin-%d-%d", i, c))))
			if c == len(st.AwaitingRunIDs)-1 {
				releasedLast = time.Now()
			}
			run, err := childPipe.Run(context.Background(), id)
			if err != nil {
				b.Fatal(err)
			}
			if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
				b.Fatalf("child Wait = %+v, %v", res, err)
			}
		}
		if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
			b.Fatalf("parent Wait = %+v, %v", res, err)
		}
		lastWake = time.Since(releasedLast)
		gates.Clear() // every child of this iteration is terminal
	}
	b.StopTimer()
	n := float64(children+1) * float64(b.N) // children + parent
	b.ReportMetric(float64(v.store.Stats().TxPageAllocBytes)/n, "diskB/run")
	b.ReportMetric(float64(v.writes.Load())/n, "transitions/run")
	b.ReportMetric(float64(v.reads.Load())/(float64(children)*float64(b.N)), "reads/child")
	b.ReportMetric(ms(lastWake), "wake-max-ms")
}

// quiesceReads reports whether the store read counter went unchanged for
// a few milliseconds within a bounded wait — any gate run in flight has
// finished — so a stuck engine fails the scenario instead of hanging or
// skewing it.
func quiesceReads(reads *atomic.Int64) bool {
	const quiet = 5 * time.Millisecond
	deadline := time.Now().Add(10 * time.Second)
	last, since := reads.Load(), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Microsecond)
		if cur := reads.Load(); cur != last {
			last, since = cur, time.Now()
			continue
		}
		if time.Since(since) >= quiet {
			return true
		}
	}
	return false
}
