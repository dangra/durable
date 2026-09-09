package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/pipelinedef"
	"google.golang.org/protobuf/proto"
)

func threeSteps(id durable.PipelineID) *pipelinedef.Definition {
	noop := func(ctx context.Context, inv durable.Invocation) error { return nil }
	return pipelinedef.New(pipelinedef.Config{
		ID: id,
		Steps: []pipelinedef.Step{
			stateless("a/v1", noop), stateless("b/v1", noop), stateless("c/v1", noop),
		},
	})
}

func countOps(log *eventLog) map[string]int {
	counts := map[string]int{}
	log.locked(func() {
		for _, op := range log.storeOps {
			counts[op.Op]++
		}
	})
	return counts
}

// The reconcile loop reads the full record once per dispatch and carries
// it: the iterations that follow each resolution read nothing. The only
// heads are Wait's own.
func TestReconcileReadsTheFullRecordOncePerDispatch(t *testing.T) {
	log := &eventLog{}
	_, pipes := startObservedEngine(t, log, []*pipelinedef.Definition{threeSteps("p")})
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	counts := countOps(log)
	if counts["GetRun"] != 1 {
		t.Fatalf("GetRun ops = %d; want exactly one, the dispatch's (%v)", counts["GetRun"], counts)
	}
	// Wait reads a head after registering and one more after the wake.
	if counts["GetRunHead"] > 2 {
		t.Fatalf("GetRunHead ops = %d; want Wait's at most, the loop reads none (%v)", counts["GetRunHead"], counts)
	}
}

// A cancel marks the Run dirty; the doomed operation's retry ends the
// pass, and the redispatch reads the record fresh. Nothing in between
// reads a head.
func TestCanceledPassReadsNothing(t *testing.T) {
	log := &eventLog{}
	blocked := make(chan struct{})
	noUnwind := func(ctx context.Context, inv durable.Invocation) error { return nil }
	def := pipelinedef.New(pipelinedef.Config{
		ID: "p",
		Steps: []pipelinedef.Step{
			{ID: "a/v1", Unwind: true, Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil }, UnwindFunc: noUnwind},
			{ID: "b/v1", Unwind: true, Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if inv.CancelRequested() {
					return nil, nil
				}
				close(blocked)
				<-ctx.Done()
				return nil, ctx.Err()
			}, UnwindFunc: noUnwind},
			stateless("c/v1", noUnwind),
		},
	})
	_, pipes := startObservedEngine(t, log, []*pipelinedef.Definition{def})
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-blocked
	before := countOps(log)
	if err := run.Cancel(context.Background(), "op"); err != nil {
		t.Fatal(err)
	}
	// Terminality observed through the observer, so the test itself
	// reads nothing before counting.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var done bool
		log.locked(func() { done = len(log.terminal) == 1 })
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run never terminated")
		}
		time.Sleep(5 * time.Millisecond)
	}
	counts := countOps(log)
	if heads := counts["GetRunHead"] - before["GetRunHead"]; heads != 0 {
		t.Fatalf("GetRunHead ops after the cancel = %d; the loop reads no head (%v)", heads, counts)
	}
	if counts["GetRun"] < 2 {
		t.Fatalf("GetRun ops = %d; want one per dispatch, and the retry redispatched (%v)", counts["GetRun"], counts)
	}
	if res, err := run.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
}

// A cancel that lands between two resolutions of one pass marks the Run
// dirty; the next iteration re-reads the record and sees it, on that one
// dispatch: the pass that started forward ends canceled and unwound.
func TestCancelMidPassRereadsTheRecord(t *testing.T) {
	log := &eventLog{}
	blocked, release := make(chan struct{}), make(chan struct{})
	noUnwind := func(ctx context.Context, inv durable.Invocation) error { return nil }
	def := pipelinedef.New(pipelinedef.Config{
		ID: "p",
		Steps: []pipelinedef.Step{
			{ID: "a/v1", Unwind: true, Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil }, UnwindFunc: noUnwind},
			{ID: "b/v1", Unwind: true, Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				// Resolves cleanly whatever the context says: the pass
				// continues rather than retries.
				close(blocked)
				<-release
				return nil, nil
			}, UnwindFunc: noUnwind},
			stateless("c/v1", noUnwind),
		},
	})
	_, pipes := startObservedEngine(t, log, []*pipelinedef.Definition{def})
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-blocked
	if err := run.Cancel(context.Background(), "op"); err != nil {
		t.Fatal(err)
	}
	close(release)
	if res, err := run.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
	// c never ran: the cancel was seen before it was selected, on the
	// record the pass carried.
	var cRan bool
	log.locked(func() {
		for _, a := range log.attempts {
			if a.StepID == "c/v1" {
				cRan = true
			}
		}
	})
	if cRan {
		t.Fatal("step c ran: the mid-pass cancel was not seen on the carried record")
	}
	// The dispatch's read, the dirty re-read, and at most one more for
	// the cancel's re-poke after the worker exited.
	if counts := countOps(log); counts["GetRun"] < 2 || counts["GetRun"] > 3 {
		t.Fatalf("GetRun ops = %d; want the dispatch, the dirty re-read, and at most the re-poke (%v)", counts["GetRun"], counts)
	}
}
