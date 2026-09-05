package perf

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// BenchmarkBootBurst is the deploy-wave / host-restart case: many
// machine-start-shaped runs scheduled at once, completion latency and
// throughput under the bounded worker pool.
func BenchmarkBootBurst(b *testing.B) {
	runs := scale(b, 150)
	def := machinePipeline("boot", [numSteps]stepSpec{})
	v := newEnv(b, def)
	v.start(b)

	b.ResetTimer()
	start := time.Now()
	var lat []time.Duration
	for i := 0; i < b.N; i++ {
		lat = append(lat, runPopulation(b, v.pipe, runs, fmt.Sprintf("boot-%d", i))...)
	}
	report(b, v, runs, lat, time.Since(start))
}

// BenchmarkRetryStorm is the degraded-host case: a population of runs
// burning bounded retries against a dead dependency while healthy runs
// execute alongside. healthy-p99-ms is the isolation measure: how much the
// storm degrades unrelated work.
func BenchmarkRetryStorm(b *testing.B) {
	const retriesPerRun = 20
	stormRuns := scale(b, 60)
	healthyRuns := scale(b, 20)

	storm := machinePipeline("storm", [numSteps]stepSpec{0: {retries: retriesPerRun}})
	healthy := machinePipeline("healthy", [numSteps]stepSpec{})

	store, err := newStorePair(b, storm, healthy)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	start := time.Now()
	var healthyLat []time.Duration
	for i := 0; i < b.N; i++ {
		done := make(chan []time.Duration, 1)
		go func() {
			done <- runPopulation(b, store.stormPipe, stormRuns, fmt.Sprintf("storm-%d", i))
		}()
		healthyLat = append(healthyLat, runPopulation(b, store.healthyPipe, healthyRuns, fmt.Sprintf("healthy-%d", i))...)
		<-done
	}
	elapsed := time.Since(start)

	total := stormRuns + healthyRuns
	// Per-attempt cost across the storm: the cursor-write efficiency gate.
	attempts := float64(b.N) * float64(stormRuns*(numSteps+retriesPerRun)+healthyRuns*numSteps)
	b.ReportMetric(float64(store.env.store.Stats().TxPageAllocBytes)/attempts, "diskB/attempt")
	report(b, store.env, total, healthyLat, elapsed)
	// report's p50/p99 above are healthy-run latencies: isolation.
}

type dualEnv struct {
	env         *env
	stormPipe   *engine.Pipeline
	healthyPipe *engine.Pipeline
}

func newStorePair(b *testing.B, storm, healthy *pipelinedef.Definition) (*dualEnv, error) {
	v := newEnv(b, storm)
	healthyPipe, err := v.eng.Bind(healthy)
	if err != nil {
		return nil, err
	}
	v.start(b)
	return &dualEnv{env: v, stormPipe: v.pipe, healthyPipe: healthyPipe}, nil
}

// BenchmarkUnwindWave is the mass-failure case: runs succeed through
// unwind-capable steps and permanently fail at the last one, driving full
// reverse unwind for the whole population.
func BenchmarkUnwindWave(b *testing.B) {
	runs := scale(b, 100)
	var specs [numSteps]stepSpec
	for i := 0; i < numSteps-1; i++ {
		specs[i] = stepSpec{unwind: true}
	}
	specs[numSteps-1] = stepSpec{permanent: true}
	def := machinePipeline("unwind-wave", specs)
	v := newEnv(b, def)
	v.start(b)

	b.ResetTimer()
	start := time.Now()
	var lat []time.Duration
	for i := 0; i < b.N; i++ {
		lat = append(lat, runPopulation(b, v.pipe, runs, fmt.Sprintf("wave-%d", i))...)
	}
	elapsed := time.Since(start)
	unwinds := float64(b.N) * float64(runs*(numSteps-1))
	b.ReportMetric(unwinds/elapsed.Seconds(), "unwinds/sec")
	report(b, v, runs, lat, elapsed)
}

// BenchmarkRecovery is the engine-restart case: Start over a store
// populated with nonterminal runs parked one step from completion plus a
// large terminal population, measuring time-to-recover and drain
// throughput (the ListNonterminal scan cost is inside start-ms).
func BenchmarkRecovery(b *testing.B) {
	nonterminal := scale(b, 500)
	terminal := scale(b, 2000)
	def := machinePipeline("recover", [numSteps]stepSpec{})

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		v := newEnv(b, def)
		// seedPopulation writes through the raw v.store, bypassing the
		// engine and thus the StoreOp observer, so v.writes is still
		// zero here: the reported transitions/run window opens at
		// engine start.
		ids := seedPopulation(b, v, nonterminal, terminal)
		before := v.store.Stats()
		b.StartTimer()

		startBegin := time.Now()
		v.start(b)
		startDur := time.Since(startBegin)

		for _, id := range ids {
			run, err := v.pipe.Run(context.Background(), id)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := run.Wait(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
		drain := time.Since(startBegin)

		b.StopTimer()
		stats := v.store.Stats()
		b.ReportMetric(ms(startDur), "start-ms")
		b.ReportMetric(float64(nonterminal)/drain.Seconds(), "runs/sec")
		b.ReportMetric(float64(stats.TxPageAllocBytes-before.TxPageAllocBytes)/float64(nonterminal), "diskB/run")
		b.ReportMetric(float64(v.writes.Load())/float64(nonterminal), "transitions/run")
		b.StartTimer()
	}
}
