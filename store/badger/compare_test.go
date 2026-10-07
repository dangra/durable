package badger_test

// The store comparison: the same engine workloads over every in-tree
// store, so the numbers differ only by the store. It lives in this
// module because it is the one that can import all three drivers; the
// root module cannot import a nested one.
//
//	TMPDIR=/path/on/a/real/disk go test -run xxx -bench Compare -benchtime 1x -count 5 ./
//
// Run it with TMPDIR on the disk being measured: on tmpfs every fsync is
// free and the persistent stores look like mem. Every sub-benchmark runs
// one population per iteration (-benchtime 1x) and reports per-run
// metrics, so -count repeats are what a summary takes medians over.

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/badger"
	"github.com/dangra/durable/store/bbolt"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
)

// cmpStore opens one kind of store in dir. persistent stores report
// their disk use after Close.
type cmpStore struct {
	name       string
	persistent bool
	open       func(dir string) (driver.Store, error)
}

var cmpStores = []cmpStore{
	{"mem", false, func(string) (driver.Store, error) { return mem.New(), nil }},
	{"bbolt", true, func(dir string) (driver.Store, error) { return bbolt.Open(filepath.Join(dir, "durable.db")) }},
	{"badger", true, func(dir string) (driver.Store, error) { return badger.Open(filepath.Join(dir, "badger")) }},
	{"badger-nosync", true, func(dir string) (driver.Store, error) {
		return badger.Open(filepath.Join(dir, "badger"), badger.WithSyncWrites(false))
	}},
}

const (
	cmpInputSize = 32 << 10 // the flyd machine config
	cmpStateSize = 1 << 10
	cmpSteps     = 8
)

// Payloads are random bytes: Badger compresses its blocks (Snappy by
// default) and bbolt does not, so zero-filled payloads would measure
// compression rather than storage.
var cmpFatInput = wrapperspb.Bytes(randomBytes(cmpInputSize, 1))

func randomBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

type cmpSentinel string

func (e cmpSentinel) Error() string { return string(e) }

const (
	cmpTransient cmpSentinel = "transient"
	cmpPermanent cmpSentinel = "permanent"
)

// cmpStep is one step's behavior: bounded retries, a permanent failure,
// an unwind.
type cmpStep struct {
	retries   uint64
	permanent bool
	unwind    bool
}

// cmpPipeline builds a pipeline of steps committing stateBytes each (0
// for stateless) with a fat input when input is set.
func cmpPipeline(id durable.PipelineID, specs []cmpStep, input bool, stateBytes int) *pipelinedef.Definition {
	var state proto.Message
	if stateBytes > 0 {
		state = wrapperspb.Bytes(randomBytes(stateBytes, 2))
	}
	var steps []pipelinedef.Step
	for i, spec := range specs {
		spec := spec
		st := pipelinedef.Step{
			ID:       durable.StepID(fmt.Sprintf("%s-%d/v1", id, i)),
			HasState: state != nil,
			Unwind:   spec.unwind,
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if inv.Attempt() <= spec.retries {
					return nil, cmpTransient
				}
				if spec.permanent {
					return nil, durable.Fail(cmpPermanent)
				}
				return state, nil
			},
		}
		if spec.unwind {
			st.UnwindFunc = func(context.Context, durable.Invocation) error { return nil }
		}
		steps = append(steps, st)
	}
	cfg := pipelinedef.Config{ID: id, Steps: steps}
	if input {
		cfg.NewInput = func() proto.Message { return &wrapperspb.BytesValue{} }
	}
	return pipelinedef.New(cfg)
}

func uniform(n int, s cmpStep) []cmpStep {
	out := make([]cmpStep, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// cmpEnv is one store and engine for one sub-benchmark iteration.
type cmpEnv struct {
	kind  cmpStore
	dir   string
	store driver.Store
	eng   *engine.Engine
}

func newCmpEnv(b *testing.B, kind cmpStore, opts ...engine.Option) *cmpEnv {
	b.Helper()
	dir := b.TempDir()
	st, err := kind.open(dir)
	if err != nil {
		b.Fatal(err)
	}
	return &cmpEnv{kind: kind, dir: dir, store: st, eng: newCmpEngine(st, opts...)}
}

func newCmpEngine(st driver.Store, opts ...engine.Option) *engine.Engine {
	return engine.New(st, append([]engine.Option{
		engine.WithRetryPolicy(engine.RetryPolicy{Initial: 500 * time.Microsecond, Max: 2 * time.Millisecond, Multiplier: 2}),
		engine.WithRecoveryBackoff(0),
		engine.WithLogger(slog.New(slog.DiscardHandler)),
	}, opts...)...)
}

func (v *cmpEnv) bind(b *testing.B, def *pipelinedef.Definition) *engine.Pipeline {
	b.Helper()
	p, err := v.eng.Bind(def)
	if err != nil {
		b.Fatal(err)
	}
	return p
}

func (v *cmpEnv) start(b *testing.B) {
	b.Helper()
	if err := v.eng.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
}

// stop stops the engine and closes the store, reporting its disk use
// per run when it has one.
func (v *cmpEnv) stop(b *testing.B, runs int) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := v.eng.Stop(ctx); err != nil {
		b.Fatal(err)
	}
	// Disk is measured twice: while the store is open, which is what a
	// running daemon holds, and after Close, which compacts or drains
	// what each store defers.
	var open int64
	if v.kind.persistent {
		open = diskUse(b, v.dir)
	}
	if err := v.store.Close(); err != nil {
		b.Fatal(err)
	}
	if v.kind.persistent && runs > 0 {
		b.ReportMetric(float64(open)/float64(runs), "disk-open-B/run")
		b.ReportMetric(float64(diskUse(b, v.dir))/float64(runs), "disk-B/run")
	}
}

// diskUse sums the blocks allocated to the files under dir: what the
// store holds on disk, preallocated sparse space excluded.
func diskUse(b *testing.B, dir string) int64 {
	b.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return total
}

// meter measures a window: wall time, allocations, and per-run
// latencies.
type meter struct {
	start time.Time
	ms    runtime.MemStats
	mu    sync.Mutex
	lat   []time.Duration
}

func startMeter() *meter {
	m := &meter{}
	runtime.GC()
	runtime.ReadMemStats(&m.ms)
	m.start = time.Now()
	return m
}

func (m *meter) observe(d time.Duration) {
	m.mu.Lock()
	m.lat = append(m.lat, d)
	m.mu.Unlock()
}

// report emits per-run throughput, allocations, and latency
// percentiles for the window.
func (m *meter) report(b *testing.B, runs int) {
	elapsed := time.Since(m.start)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	n := float64(runs)
	b.ReportMetric(n/elapsed.Seconds(), "runs/sec")
	b.ReportMetric(float64(after.Mallocs-m.ms.Mallocs)/n, "allocs/run")
	b.ReportMetric(float64(after.TotalAlloc-m.ms.TotalAlloc)/n, "B/run")
	if len(m.lat) > 0 {
		sort.Slice(m.lat, func(i, j int) bool { return m.lat[i] < m.lat[j] })
		b.ReportMetric(float64(m.lat[len(m.lat)/2])/1e6, "p50-ms")
		b.ReportMetric(float64(m.lat[len(m.lat)*99/100])/1e6, "p99-ms")
	}
}

// runOne schedules a run and waits for its terminal result.
func runOne(b *testing.B, p *engine.Pipeline, res string, input proto.Message, m *meter) {
	start := time.Now()
	run, _, err := p.Schedule(context.Background(), durable.ResourceID(res), input)
	if err != nil {
		b.Error(err)
		return
	}
	if _, err := run.Wait(context.Background()); err != nil {
		b.Error(err)
		return
	}
	m.observe(time.Since(start))
}

// sequential runs n runs one after another: commit latency, no batching.
func sequential(b *testing.B, p *engine.Pipeline, n int, prefix string, input proto.Message, m *meter) {
	for i := 0; i < n; i++ {
		runOne(b, p, fmt.Sprintf("%s-%d", prefix, i), input, m)
	}
}

// burst runs n runs at once: throughput under concurrent commits.
func burst(b *testing.B, p *engine.Pipeline, n int, prefix string, input proto.Message, m *meter) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runOne(b, p, fmt.Sprintf("%s-%d", prefix, i), input, m)
		}(i)
	}
	wg.Wait()
}

// forEachStore runs body as one sub-benchmark per store.
func forEachStore(b *testing.B, body func(b *testing.B, kind cmpStore)) {
	for _, kind := range cmpStores {
		b.Run(kind.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				body(b, kind)
			}
		})
	}
}

func BenchmarkCompare(b *testing.B) {
	if testing.Short() {
		b.Skip("the store comparison takes about a minute per pass; run it without -short")
	}
	// Say so loudly rather than publish fsync-free numbers.
	if dir := os.TempDir(); isTmpfs(dir) {
		fmt.Fprintf(os.Stderr, "compare: %s is tmpfs; fsync is free there, set TMPDIR to a directory on the disk under test\n", dir)
	}
	// machine_start: the flyd start shape, one run at a time — every
	// transition waits for its own commit.
	b.Run("machine_start_seq", func(b *testing.B) {
		const runs = 50
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			v := newCmpEnv(b, kind)
			p := v.bind(b, cmpPipeline("start", uniform(cmpSteps, cmpStep{}), true, cmpStateSize))
			v.start(b)
			m := startMeter()
			sequential(b, p, runs, "m", cmpFatInput, m)
			m.report(b, runs)
			v.stop(b, runs)
		})
	})
	// machine_stop: a small action, no input, four stateless steps, one
	// at a time — commit latency with almost no bytes.
	b.Run("small_seq", func(b *testing.B) {
		const runs = 100
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			v := newCmpEnv(b, kind)
			p := v.bind(b, cmpPipeline("small", uniform(4, cmpStep{}), false, 0))
			v.start(b)
			m := startMeter()
			sequential(b, p, runs, "s", nil, m)
			m.report(b, runs)
			v.stop(b, runs)
		})
	})
	// boot_burst: a host restart or deploy wave, the machine-start shape
	// scheduled all at once.
	b.Run("machine_start_burst", func(b *testing.B) {
		const runs = 200
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			v := newCmpEnv(b, kind)
			p := v.bind(b, cmpPipeline("start", uniform(cmpSteps, cmpStep{}), true, cmpStateSize))
			v.start(b)
			m := startMeter()
			burst(b, p, runs, "m", cmpFatInput, m)
			m.report(b, runs)
			v.stop(b, runs)
		})
	})
	// small_burst: many small actions at once — commit coalescing with
	// no bytes to move.
	b.Run("small_burst", func(b *testing.B) {
		const runs = 1000
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			v := newCmpEnv(b, kind)
			p := v.bind(b, cmpPipeline("small", uniform(4, cmpStep{}), false, 0))
			v.start(b)
			m := startMeter()
			burst(b, p, runs, "s", nil, m)
			m.report(b, runs)
			v.stop(b, runs)
		})
	})
	// retry_storm: a dead dependency — every run burns twenty retries
	// on its first step, each a cursor rewrite.
	b.Run("retry_storm", func(b *testing.B) {
		const runs = 60
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			v := newCmpEnv(b, kind)
			specs := uniform(cmpSteps, cmpStep{})
			specs[0].retries = 20
			p := v.bind(b, cmpPipeline("storm", specs, true, cmpStateSize))
			v.start(b)
			m := startMeter()
			burst(b, p, runs, "r", cmpFatInput, m)
			m.report(b, runs)
			v.stop(b, runs)
		})
	})
	// unwind_wave: mass failure — every run succeeds through seven
	// unwinding steps, fails at the last, and unwinds all seven.
	b.Run("unwind_wave", func(b *testing.B) {
		const runs = 100
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			v := newCmpEnv(b, kind)
			specs := uniform(cmpSteps, cmpStep{unwind: true})
			specs[cmpSteps-1] = cmpStep{permanent: true}
			p := v.bind(b, cmpPipeline("wave", specs, true, cmpStateSize))
			v.start(b)
			m := startMeter()
			burst(b, p, runs, "w", cmpFatInput, m)
			m.report(b, runs)
			v.stop(b, runs)
		})
	})
	// await_fanin: release-train shape — parents each schedule children
	// and park on all of them.
	b.Run("await_fanin", func(b *testing.B) {
		const parents, children = 20, 25
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			v := newCmpEnv(b, kind)
			child := v.bind(b, cmpPipeline("child", uniform(2, cmpStep{}), false, 0))
			parent := v.bind(b, pipelinedef.New(pipelinedef.Config{
				ID: "parent",
				Steps: []pipelinedef.Step{{
					ID: "fanout/v1",
					Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
						if _, ok := inv.Awaited(); ok {
							return nil, nil
						}
						ids := make([]durable.RunID, children)
						for c := range ids {
							run, _, err := child.Schedule(ctx, durable.ResourceID(fmt.Sprintf("%s-c%d", inv.ResourceID(), c)), nil)
							if err != nil {
								return nil, err
							}
							ids[c] = run.ID()
						}
						return nil, durable.AwaitAll(ids)
					},
				}},
			}))
			v.start(b)
			m := startMeter()
			burst(b, parent, parents, "p", nil, m)
			total := parents * (children + 1)
			m.lat = m.lat[:0] // parent latency is reported below; per-run metrics cover every run
			m.report(b, total)
			v.stop(b, total)
		})
	})
	// status_poll: the monitor host — runs in flight, blocked in their
	// step, polled for Status in a skewed pattern from many goroutines.
	// The cost is the store's head read.
	b.Run("status_poll", func(b *testing.B) {
		const runs, polls, pollers = 2000, 100_000, 32
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			release := make(chan struct{})
			v := newCmpEnv(b, kind)
			p := v.bind(b, pipelinedef.New(pipelinedef.Config{
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
				NewInput: func() proto.Message { return &wrapperspb.BytesValue{} },
			}))
			v.start(b)
			population := make([]engine.Run, runs)
			for i := range population {
				run, _, err := p.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("m-%d", i)), cmpFatInput)
				if err != nil {
					b.Fatal(err)
				}
				population[i] = run
			}
			for _, run := range population {
				for {
					st, err := run.Status(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if st.Attempt >= 1 {
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
			var ms0 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&ms0)
			start := time.Now()
			var wg sync.WaitGroup
			for w := 0; w < pollers; w++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					zipf := rand.NewZipf(rand.New(rand.NewSource(seed)), 1.1, 1, uint64(runs-1))
					for i := 0; i < polls/pollers; i++ {
						if _, err := population[zipf.Uint64()].Status(context.Background()); err != nil {
							b.Error(err)
							return
						}
					}
				}(int64(w))
			}
			wg.Wait()
			elapsed := time.Since(start)
			var ms1 runtime.MemStats
			runtime.ReadMemStats(&ms1)
			b.ReportMetric(float64(polls)/elapsed.Seconds(), "polls/sec")
			b.ReportMetric(float64(ms1.Mallocs-ms0.Mallocs)/polls, "allocs/poll")
			b.ReportMetric(float64(ms1.TotalAlloc-ms0.TotalAlloc)/polls, "B/poll")
			close(release)
			v.stop(b, runs)
		})
	})
	// recovery: a host restart — a store holding runs parked one step
	// from completion and a terminal history four times larger is
	// reopened, the engine started, and every parked run finished.
	b.Run("recovery", func(b *testing.B) {
		const nonterminal, terminal = 2000, 8000
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			if !kind.persistent {
				b.Skip("nothing to recover: mem does not survive the process")
			}
			dir := b.TempDir()
			st, err := kind.open(dir)
			if err != nil {
				b.Fatal(err)
			}
			ids := seedCmp(b, st, nonterminal, terminal)
			if err := st.Close(); err != nil {
				b.Fatal(err)
			}
			m := startMeter()
			openStart := time.Now()
			st, err = kind.open(dir)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(time.Since(openStart))/1e6, "open-ms")
			v := &cmpEnv{kind: kind, dir: dir, store: st, eng: newCmpEngine(st)}
			p := v.bind(b, cmpPipeline("recover", uniform(cmpSteps, cmpStep{}), true, cmpStateSize))
			startBegin := time.Now()
			v.start(b)
			b.ReportMetric(float64(time.Since(startBegin))/1e6, "start-ms")
			for _, id := range ids {
				run, err := p.GetRun(context.Background(), id)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := run.Wait(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(time.Since(m.start))/1e6, "recover-ms")
			v.stop(b, nonterminal+terminal)
		})
	})
	// reap: retention — a terminal history deleted in the engine's
	// batch size, oldest first.
	b.Run("reap", func(b *testing.B) {
		const terminal, batch = 10000, 1000
		forEachStore(b, func(b *testing.B, kind cmpStore) {
			dir := b.TempDir()
			st, err := kind.open(dir)
			if err != nil {
				b.Fatal(err)
			}
			seedCmp(b, st, 0, terminal)
			start := time.Now()
			total := 0
			for {
				n, err := st.ReapTerminal(context.Background(), time.Now().Add(time.Hour), batch)
				if err != nil {
					b.Fatal(err)
				}
				total += n
				if n < batch {
					break
				}
			}
			if total != terminal {
				b.Fatalf("reaped %d of %d", total, terminal)
			}
			b.ReportMetric(float64(terminal)/time.Since(start).Seconds(), "reaped/sec")
			if kind.persistent {
				b.ReportMetric(float64(diskUse(b, dir)), "disk-open-B-after")
			}
			if err := st.Close(); err != nil {
				b.Fatal(err)
			}
			if kind.persistent {
				b.ReportMetric(float64(diskUse(b, dir)), "disk-B-after")
			}
		})
	})
}

// seedCmp writes runs straight to the store: nonterminal ones parked
// before their last step with seven committed states, and terminal ones
// complete. Writes go 64 at a time so the stores batch their commits.
// It returns the nonterminal ids.
func seedCmp(b testing.TB, st driver.Store, nonterminal, terminal int) []durable.RunID {
	b.Helper()
	state := randomBytes(cmpStateSize, 3)
	input, _ := proto.Marshal(cmpFatInput)
	oc := durable.OutcomeSuccess
	ids := make([]durable.RunID, nonterminal)
	var failed atomic.Bool
	sem := make(chan struct{}, 64)
	var wg sync.WaitGroup
	seed := func(i int, term bool) {
		defer wg.Done()
		defer func() { <-sem }()
		kind := "nt"
		if term {
			kind = "t"
		}
		id := durable.RunID(fmt.Sprintf("seed-%s-%06d", kind, i))
		now := time.Now()
		rec := &driver.RunRecord{
			RunID: id, PipelineID: "recover", ResourceID: durable.ResourceID(id),
			Input: input, Phase: durable.PhaseForward, Steps: map[durable.StepID]*driver.StepRecord{},
			CreatedAt: now, UpdatedAt: now,
		}
		steps := cmpSteps - 1
		if term {
			steps = cmpSteps
		}
		for s := 0; s < steps; s++ {
			rec.Steps[durable.StepID(fmt.Sprintf("recover-%d/v1", s))] = &driver.StepRecord{Forward: driver.OperationRecord{
				Status: driver.OpSucceeded, Attempts: 1, State: state, Order: uint32(s + 1)}}
		}
		if _, created, err := st.CreateRun(context.Background(), rec, nil); err != nil || !created {
			b.Errorf("seed CreateRun %s: created=%v err=%v", id, created, err)
			failed.Store(true)
			return
		}
		if term {
			if err := st.ApplyTransition(context.Background(), id, driver.Transition{
				Cursor: driver.Cursor{Phase: durable.PhaseDone, UpdatedAt: now}, Outcome: &oc, Output: []byte{1},
			}); err != nil {
				b.Errorf("seed terminal %s: %v", id, err)
				failed.Store(true)
			}
			return
		}
		ids[i] = id
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
	if failed.Load() {
		b.FailNow()
	}
	return ids
}

func isTmpfs(dir string) bool {
	var s syscall.Statfs_t
	if err := syscall.Statfs(dir, &s); err != nil {
		return false
	}
	return s.Type == 0x01021994 // TMPFS_MAGIC
}
