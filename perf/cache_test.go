package perf

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/bbolt"
)

// BenchmarkColdWorkingSet is the monitor-host shape: a large population
// of runs in flight — each blocked in its step for the life of a
// machine, each carrying the fat config input — polled for Status in a
// skewed pattern (a few hot machines, a long cold tail) from many
// goroutines, the way the API polls machines. The store's blob cache is
// sized to a share of the population's blobs: none, a tenth, and all of
// it. The cost of a poll is the number — bytes allocated and time —
// and the shares show what the cache buys and how it degrades when the
// working set outgrows it. This is the only scenario in which the
// cache overflows, so it is where its eviction policy is measured.
func BenchmarkColdWorkingSet(b *testing.B) {
	runs := scale(b, 2000)
	const polls = 100_000
	for _, share := range []struct {
		name string
		frac float64
	}{{"cache_0", 0}, {"cache_10", 0.1}, {"cache_100", 1}} {
		b.Run(share.name, func(b *testing.B) {
			limit := int(float64(runs*inputSize) * share.frac)
			release := make(chan struct{})
			def := pipelinedef.New(pipelinedef.Config{
				ID: "monitor",
				Steps: []pipelinedef.Step{{
					ID: "monitor/v1",
					Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
						select {
						case <-release:
							return nil, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					},
				}},
				NewInput: func() proto.Message { return fatInput.ProtoReflect().New().Interface() },
			})
			store, err := bbolt.Open(filepath.Join(b.TempDir(), "cold.db"), bbolt.WithBlobCache(limit))
			if err != nil {
				b.Fatal(err)
			}
			// Every run holds a worker for the whole benchmark.
			e := engine.New(store, engine.WithLogger(discardLogger()), engine.WithRecoveryBackoff(0))
			pipe, err := e.Bind(def)
			if err != nil {
				b.Fatal(err)
			}
			if err := e.Start(context.Background()); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				close(release)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = e.Stop(ctx)
				_ = store.Close()
			})
			population := make([]engine.Run, runs)
			for i := range population {
				run, _, err := pipe.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("m-%d", i)), fatInput)
				if err != nil {
					b.Fatal(err)
				}
				population[i] = run
			}
			// Let every run enter its step: the population is in flight.
			time.Sleep(200 * time.Millisecond)

			b.ResetTimer()
			b.ReportAllocs()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				const pollers = 32
				for w := 0; w < pollers; w++ {
					wg.Add(1)
					go func(seed int64) {
						defer wg.Done()
						r := rand.New(rand.NewSource(seed))
						zipf := rand.NewZipf(r, 1.1, 1, uint64(runs-1))
						for p := 0; p < polls/pollers; p++ {
							if _, err := population[zipf.Uint64()].Status(context.Background()); err != nil {
								b.Error(err)
								return
							}
						}
					}(int64(i*pollers + w))
				}
				wg.Wait()
			}
			b.ReportMetric(float64(polls)*float64(b.N)/time.Since(start).Seconds(), "polls/sec")
		})
	}
}
