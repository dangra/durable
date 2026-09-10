// Run classes: a run-scoped counting bound with an eligibility-ordered
// line, an admission cap, and tokens that survive restarts.
package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// classGates records the order handlers first entered and holds each
// Run on a per-resource gate until released.
type classGates struct {
	mu      sync.Mutex
	gates   map[durable.ResourceID]chan struct{}
	entered []durable.ResourceID
	seen    map[durable.ResourceID]bool
}

func newClassGates() *classGates {
	return &classGates{gates: map[durable.ResourceID]chan struct{}{}, seen: map[durable.ResourceID]bool{}}
}

func (g *classGates) gate(r durable.ResourceID) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	ch, ok := g.gates[r]
	if !ok {
		ch = make(chan struct{})
		g.gates[r] = ch
	}
	return ch
}

func (g *classGates) enter(r durable.ResourceID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.seen[r] {
		g.seen[r] = true
		g.entered = append(g.entered, r)
	}
}

func (g *classGates) order() []durable.ResourceID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]durable.ResourceID(nil), g.entered...)
}

func (g *classGates) release(r durable.ResourceID) { close(g.gate(r)) }

// waitForEntered blocks until n distinct Runs have entered a handler.
func (g *classGates) waitForEntered(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(g.order()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("entered = %v, want %d", g.order(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// gatedClassDef is a one-step pipeline in run class "m" whose handler
// records its entry and blocks on its resource's gate.
func gatedClassDef(id durable.PipelineID, g *classGates) *pipelinedef.Definition {
	return pipelinedef.New(pipelinedef.Config{
		ID:       id,
		RunClass: "m",
		Steps: []pipelinedef.Step{
			stateless("hold/v1", func(ctx context.Context, inv durable.Invocation) error {
				g.enter(inv.ResourceID())
				select {
				case <-g.gate(inv.ResourceID()):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
	})
}

func startClassEngine(t *testing.T, store driver.Store, rc engine.RunClass, defs ...*pipelinedef.Definition) (*engine.Engine, []*engine.Pipeline) {
	t.Helper()
	e := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0), engine.WithRunClass("m", rc))
	var pipes []*engine.Pipeline
	for _, d := range defs {
		p, err := e.Bind(d)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		pipes = append(pipes, p)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	return e, pipes
}

func schedule(t *testing.T, p *engine.Pipeline, r durable.ResourceID, opts ...durable.ScheduleOption) engine.Run {
	t.Helper()
	run, created, err := p.Schedule(context.Background(), r, str("in"), opts...)
	if err != nil || !created {
		t.Fatalf("Schedule(%s) = created=%v err=%v", r, created, err)
	}
	return run
}

func sameOrder(a, b []durable.ResourceID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRunClassStartsInCreationOrder: capacity 2 with five Runs starts
// exactly two, each terminal Run starts the next in creation order, and
// the ones in line report Queued with the class.
func TestRunClassStartsInCreationOrder(t *testing.T) {
	g := newClassGates()
	e, pipes := startClassEngine(t, mem.New(), engine.RunClass{Capacity: 2}, gatedClassDef("p", g))
	p := pipes[0]

	resources := []durable.ResourceID{"r1", "r2", "r3", "r4", "r5"}
	runs := map[durable.ResourceID]engine.Run{}
	for _, r := range resources {
		runs[r] = schedule(t, p, r)
	}
	g.waitForEntered(t, 2)
	st := waitForState(t, runs["r3"], engine.RunStateQueued)
	if st.QueuedClass != "m" || !st.StartedAt.IsZero() {
		t.Fatalf("queued status = %+v", st)
	}
	if got := e.Stats(); got.QueuedRuns != 3 || got.RunClasses["m"].InUse != 2 || got.RunClasses["m"].Queued != 3 {
		t.Fatalf("Stats = %+v", got.RunClasses["m"])
	}
	time.Sleep(20 * time.Millisecond) // nothing else starts on its own
	// The two granted together enter their handlers in whatever order
	// the scheduler runs them; the class orders grants, not goroutines.
	if got := g.order(); len(got) != 2 || (got[0] != "r1" && got[1] != "r1") || (got[0] != "r2" && got[1] != "r2") {
		t.Fatalf("started %v, want r1 and r2", got)
	}

	for i, r := range resources {
		g.release(r)
		if res, err := runs[r].Wait(context.Background()); err != nil || !res.Succeeded() {
			t.Fatalf("Wait(%s) = %+v, %v", r, res, err)
		}
		if i+2 < len(resources) {
			g.waitForEntered(t, i+3)
		}
	}
	if got := g.order(); !sameOrder(got[2:], resources[2:]) {
		t.Fatalf("start order %v, want %v after the first two", got, resources[2:])
	}
	if st, _ := runs["r5"].Status(context.Background()); st.StartedAt.IsZero() {
		t.Fatal("terminal Status lost StartedAt")
	}
	if got := e.Stats().RunClasses["m"]; got.InUse != 0 || got.Waiting != 0 || got.Queued != 0 {
		t.Fatalf("Stats after all terminal = %+v", got)
	}
}

// TestRunClassDelayedRunJoinsAtItsStartTime: a delayed Run created
// first but due later starts after the Runs eligible before its start
// time.
func TestRunClassDelayedRunJoinsAtItsStartTime(t *testing.T) {
	g := newClassGates()
	_, pipes := startClassEngine(t, mem.New(), engine.RunClass{Capacity: 1}, gatedClassDef("p", g))
	p := pipes[0]

	a := schedule(t, p, "a")
	g.waitForEntered(t, 1)
	d := schedule(t, p, "d", durable.StartAfter(30*time.Millisecond))
	b := schedule(t, p, "b")
	c := schedule(t, p, "c")
	waitForState(t, d, engine.RunStateScheduled)
	time.Sleep(60 * time.Millisecond) // d is due and in line, behind b and c
	waitForState(t, d, engine.RunStateQueued)

	want := []durable.ResourceID{"a", "b", "c", "d"}
	for i, run := range []engine.Run{a, b, c, d} {
		g.release(want[i])
		if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
			t.Fatalf("Wait(%s) = %+v, %v", want[i], res, err)
		}
		if i+1 < len(want) {
			g.waitForEntered(t, i+2)
		}
	}
	if got := g.order(); !sameOrder(got, want) {
		t.Fatalf("start order %v, want %v", got, want)
	}
}

// TestRunClassCancelQueuedRunNeedsNoToken: a cancel on a queued Run goes
// terminal without a token and without moving the line.
func TestRunClassCancelQueuedRunNeedsNoToken(t *testing.T) {
	g := newClassGates()
	e, pipes := startClassEngine(t, mem.New(), engine.RunClass{Capacity: 1}, gatedClassDef("p", g))
	p := pipes[0]

	a := schedule(t, p, "a")
	g.waitForEntered(t, 1)
	b := schedule(t, p, "b")
	c := schedule(t, p, "c")
	waitForState(t, b, engine.RunStateQueued)
	waitForState(t, c, engine.RunStateQueued)

	if err := b.Cancel(context.Background(), "changed my mind"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := b.Wait(context.Background())
	if err != nil || res.Succeeded() || res.Failure == nil || res.Failure.Kind != durable.FailureKindCanceled {
		t.Fatalf("Wait(b) = %+v, %v", res, err)
	}
	if st, _ := b.Status(context.Background()); !st.StartedAt.IsZero() {
		t.Fatal("a Run canceled in line reports a start")
	}
	if got := e.Stats().RunClasses["m"]; got.InUse != 1 || got.Waiting != 1 || got.Queued != 1 {
		t.Fatalf("Stats after cancel = %+v", got)
	}
	if got := g.order(); len(got) != 1 {
		t.Fatalf("a cancel started something: %v", got)
	}

	g.release("a")
	if res, err := a.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait(a) = %+v, %v", res, err)
	}
	g.waitForEntered(t, 2)
	g.release("c")
	if res, err := c.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait(c) = %+v, %v", res, err)
	}
}

// TestRunClassMaxQueuedRefusesSchedule: the cap refuses the next
// Schedule with RunClassFullError, a duplicate hit still returns the
// existing Run, and a terminal unstarted Run frees a place.
func TestRunClassMaxQueuedRefusesSchedule(t *testing.T) {
	g := newClassGates()
	_, pipes := startClassEngine(t, mem.New(), engine.RunClass{Capacity: 1, MaxQueued: 2}, gatedClassDef("p", g))
	p := pipes[0]

	a := schedule(t, p, "a")
	g.waitForEntered(t, 1) // started: not queued
	b := schedule(t, p, "b")
	schedule(t, p, "c")

	_, created, err := p.Schedule(context.Background(), "d", str("in"))
	var full *engine.RunClassFullError
	if !errors.As(err, &full) || created {
		t.Fatalf("Schedule past the cap = created=%v err=%v, want RunClassFullError", created, err)
	}
	if full.Class != "m" || full.MaxQueued != 2 {
		t.Fatalf("RunClassFullError = %+v", full)
	}
	if _, ok, err := p.GetActiveRun(context.Background(), "d"); ok || err != nil {
		t.Fatalf("refused Schedule created a Run: ok=%v err=%v", ok, err)
	}

	dup, created, err := p.Schedule(context.Background(), "b", str("in"))
	if err != nil || created || dup.ID() != b.ID() {
		t.Fatalf("duplicate Schedule at the cap = %s created=%v err=%v", dup.ID(), created, err)
	}

	if err := b.Cancel(context.Background(), "make room"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := b.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(b): %v", err)
	}
	schedule(t, p, "d")

	g.release("a")
	if res, err := a.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait(a) = %+v, %v", res, err)
	}
}

// TestRunClassRestartReholdsAndKeepsOrder: after a restart the started
// Runs hold by right, the queued ones keep their order, and a lowered
// capacity admits nothing until use falls below it.
func TestRunClassRestartReholdsAndKeepsOrder(t *testing.T) {
	store := mem.New()
	g := newClassGates()

	e1 := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0), engine.WithRunClass("m", engine.RunClass{Capacity: 2}))
	p1, err := e1.Bind(gatedClassDef("p", g))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	resources := []durable.ResourceID{"a", "b", "c", "d", "e"}
	runs := map[durable.ResourceID]durable.RunID{}
	for _, r := range resources {
		runs[r] = schedule(t, p1, r).ID()
	}
	g.waitForEntered(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := e1.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	cancel()

	// The next deployment lowers the capacity to 1.
	e2, pipes := startClassEngine(t, store, engine.RunClass{Capacity: 1}, gatedClassDef("p", g))
	p2 := pipes[0]
	handle := func(r durable.ResourceID) engine.Run {
		run, err := p2.GetRun(context.Background(), runs[r])
		if err != nil {
			t.Fatalf("GetRun(%s): %v", r, err)
		}
		return run
	}
	waitForState(t, handle("c"), engine.RunStateQueued)
	if got := e2.Stats().RunClasses["m"]; got.Capacity != 1 || got.InUse != 2 || got.Queued != 3 {
		t.Fatalf("Stats after restart = %+v, want two holders over a capacity of 1", got)
	}

	g.release("a")
	if res, err := handle("a").Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait(a) = %+v, %v", res, err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := e2.Stats().RunClasses["m"]; got.InUse != 1 || got.Waiting != 3 {
		t.Fatalf("Stats with use at capacity = %+v, want no new grant", got)
	}
	if got := g.order(); len(got) != 2 {
		t.Fatalf("a Run started over capacity: %v", got)
	}

	for _, r := range resources[1:] {
		g.release(r)
		if res, err := handle(r).Wait(context.Background()); err != nil || !res.Succeeded() {
			t.Fatalf("Wait(%s) = %+v, %v", r, res, err)
		}
	}
	// a and b were granted together under the first engine; the line
	// order is what the restart must keep.
	if got := g.order(); !sameOrder(got[2:], resources[2:]) {
		t.Fatalf("start order across the restart %v, want %v after the first two", got, resources[2:])
	}
}

// TestRunClassTokenHeldAcrossRetry: a Run keeps its token through a
// retry wait, so the next in line stays queued until it ends.
func TestRunClassTokenHeldAcrossRetry(t *testing.T) {
	g := newClassGates()
	var attempts sync.Map
	def := pipelinedef.New(pipelinedef.Config{
		ID:       "p",
		RunClass: "m",
		Steps: []pipelinedef.Step{
			stateless("flaky/v1", func(ctx context.Context, inv durable.Invocation) error {
				n, _ := attempts.LoadOrStore(inv.ResourceID(), new(int))
				*n.(*int)++
				if *n.(*int) == 1 {
					return errors.New("first attempt fails")
				}
				g.enter(inv.ResourceID())
				select {
				case <-g.gate(inv.ResourceID()):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
	})
	e, pipes := startClassEngine(t, mem.New(), engine.RunClass{Capacity: 1}, def)
	p := pipes[0]

	a := schedule(t, p, "a")
	b := schedule(t, p, "b")
	g.waitForEntered(t, 1) // a, on its second attempt
	if st := waitForState(t, a, engine.RunStateRunning); st.Attempt != 2 {
		t.Fatalf("a Attempt = %d, want 2", st.Attempt)
	}
	waitForState(t, b, engine.RunStateQueued)
	if got := e.Stats().RunClasses["m"]; got.InUse != 1 || got.Waiting != 1 {
		t.Fatalf("Stats during a's retry = %+v", got)
	}
	g.release("a")
	if res, err := a.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait(a) = %+v, %v", res, err)
	}
	g.waitForEntered(t, 2)
	g.release("b")
	if res, err := b.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait(b) = %+v, %v", res, err)
	}
}

// TestRunClassNameCollisionIsAStartError: one name cannot size both a
// run class and a concurrency class.
func TestRunClassNameCollisionIsAStartError(t *testing.T) {
	e := engine.New(mem.New(), engine.WithRunClass("x", engine.RunClass{Capacity: 1}), engine.WithConcurrencyClass("x", 1))
	if err := e.Start(context.Background()); err == nil {
		t.Fatal("Start accepted a name used as both class kinds")
	}
}
