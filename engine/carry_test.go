package engine_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
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
// it: the iterations that follow each resolution read the head, for the
// cancel request, never the record again.
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
	// Three resolutions and the completion each start an iteration that
	// refreshes the head.
	if counts["GetRunHead"] < 3 {
		t.Fatalf("GetRunHead ops = %d; want at least one per iteration after the first (%v)", counts["GetRunHead"], counts)
	}
}

// lyingHeads is a store whose heads never match the carried record.
type lyingHeads struct{ driver.Store }

func (s lyingHeads) GetRunHead(ctx context.Context, id durable.RunID) (*driver.RunRecord, error) {
	h, err := s.Store.GetRunHead(ctx, id)
	if err == nil {
		h.UpdatedAt = h.UpdatedAt.Add(time.Hour)
	}
	return h, err
}

// A head that disagrees with the carried record is a contract bug: the
// loop logs it and re-reads the full record rather than trust memory,
// and the Run still completes.
func TestCarriedRecordDisagreementFallsBackToFullRead(t *testing.T) {
	log := &eventLog{}
	buf := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	e := engine.New(lyingHeads{mem.New()}, fastRetry, engine.WithRecoveryBackoff(0),
		engine.WithObserver(log.observer()), engine.WithLogger(logger))
	pipe, err := e.Bind(threeSteps("p"))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	run, _, err := pipe.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if counts := countOps(log); counts["GetRun"] < 3 {
		t.Fatalf("GetRun ops = %d; want a full re-read per disagreement (%v)", counts["GetRun"], counts)
	}
	if !strings.Contains(buf.String(), "carried run record disagrees") {
		t.Fatalf("no disagreement logged:\n%s", buf.String())
	}
}
