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

// BenchmarkFatState is BenchmarkBootBurst with a large committed state
// per step: the shape of a step that records a whole device or config
// description rather than a handle. It pins the store's handling of a
// large value written beside rows that keep changing — every later
// operation row of the same run lands next to the state in key order,
// so a layout that keeps the state in the same leaf node rewrites it on
// each of those resolutions. Compare diskB/run against BootBurst: the
// difference is what the fat states cost beyond their own bytes.
func BenchmarkFatState(b *testing.B) {
	runs := scale(b, 60)
	def := sizedPipeline("fat-state", [numSteps]stepSpec{}, fatStateSize, 0)
	v := newEnv(b, def)
	v.start(b)

	b.ResetTimer()
	start := time.Now()
	var lat []time.Duration
	for i := 0; i < b.N; i++ {
		lat = append(lat, runPopulation(b, v.pipe, runs, fmt.Sprintf("fat-state-%d", i))...)
	}
	report(b, v, runs, lat, time.Since(start))
}

// BenchmarkFatOutput is BenchmarkBootBurst with a large terminal output:
// the shape of a pipeline whose result is a description the size of its
// input. It pins the store's handling of a large value written once at
// terminality beside the other terminal rows — commits land in order,
// so a layout that keeps the output in the terminal row rewrites it
// every time a neighbouring run commits into the same leaf until the
// leaf fills. Compare diskB/run against BootBurst.
func BenchmarkFatOutput(b *testing.B) {
	runs := scale(b, 60)
	def := sizedPipeline("fat-output", [numSteps]stepSpec{}, stateSize, fatOutputSize)
	v := newEnv(b, def)
	v.start(b)

	b.ResetTimer()
	start := time.Now()
	var lat []time.Duration
	for i := 0; i < b.N; i++ {
		lat = append(lat, runPopulation(b, v.pipe, runs, fmt.Sprintf("fat-output-%d", i))...)
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
	drainStore(b, store.env.store)
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
//
// The populations are sized so that every bucket of every store layout
// is well past a single leaf page before the measured window opens: at
// a few hundred runs a layout with many small buckets keeps each in one
// leaf, while a layout with fewer, fuller buckets already pays a branch
// page per write, and the byte gate then compares tree depth rather than
// the layouts. The history is four times the in-flight population, the
// shape of a host that has been up for a while.
func BenchmarkRecovery(b *testing.B) {
	nonterminal := scale(b, 2000)
	terminal := scale(b, 8000)
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
			run, err := v.pipe.GetRun(context.Background(), id)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := run.Wait(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
		drain := time.Since(startBegin)

		b.StopTimer()
		drainStore(b, v.store)
		stats := v.store.Stats()
		b.ReportMetric(ms(startDur), "start-ms")
		b.ReportMetric(float64(nonterminal)/drain.Seconds(), "runs/sec")
		b.ReportMetric(float64(stats.TxPageAllocBytes-before.TxPageAllocBytes)/float64(nonterminal), "diskB/run")
		b.ReportMetric(float64(v.writes.Load())/float64(nonterminal), "transitions/run")
		b.StartTimer()
	}
}
