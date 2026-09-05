package durable_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
)

// lockedBuffer serializes writes from concurrent engine goroutines.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func startLoggingEngine(t *testing.T, def *durable.Definition) (*lockedBuffer, *durable.Pipeline) {
	t.Helper()
	buf := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	e := durable.NewEngine(durabletest.NewMemStore(),
		fastRetry, durable.WithRecoveryBackoff(0), durable.WithLogger(logger))
	pipe, err := def.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	return buf, pipe
}

// mustLog waits for every wanted line to appear in buf, then reports the
// ones that never did. Waiting is essential, not tolerance: the engine
// logs a run's completion after committing the terminal outcome that
// Run.Wait observes, so a caller returning from Wait can read the buffer
// before the "run complete" line lands.
func mustLog(t *testing.T, buf *lockedBuffer, wants ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var logs string
	for {
		logs = buf.String()
		missing := false
		for _, w := range wants {
			if !strings.Contains(logs, w) {
				missing = true
				break
			}
		}
		if !missing || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	for _, w := range wants {
		if !strings.Contains(logs, w) {
			t.Errorf("logs missing %q\nlogs:\n%s", w, logs)
		}
	}
}

// TestLoggingLifecycle drives a run through retry, success, permanent
// forward failure, and a permanently failing unwind, asserting the engine
// emits each lifecycle event at its documented level with the canonical
// keys, and that Invocation.Logger pre-attaches the same keys.
func TestLoggingLifecycle(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "logging",
		Steps: []durable.StepConfig{
			{
				ID:     "flaky/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					if inv.Attempt() == 1 {
						return nil, errors.New("transient boom")
					}
					inv.Logger().Info("hello from handler")
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					return durable.Fail(errors.New("cleanup broken"))
				},
			},
			stateless("explode/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("permanent boom"))
			}),
		},
	})
	buf, pipe := startLoggingEngine(t, def)

	run, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Outcome != durable.OutcomeFailure {
		t.Fatalf("outcome = %v, want failure", res.Outcome)
	}

	id := string(run.ID())
	mustLog(t, buf,
		`level=DEBUG msg="durable: run scheduled" pipeline=logging resource=res-1 run=`+id,
		`level=DEBUG msg="durable: operation failed; will retry" pipeline=logging resource=res-1 run=`+id+` step=flaky/v1 phase=forward attempt=1 error="transient boom" next_attempt_at=`,
		`level=DEBUG msg="durable: operation succeeded" pipeline=logging resource=res-1 run=`+id+` step=flaky/v1 phase=forward attempt=2`,
		`level=INFO msg="durable: run failed; unwinding" pipeline=logging resource=res-1 run=`+id+` step=explode/v1 attempt=1 error="permanent boom"`,
		`level=WARN msg="durable: unwind step failed permanently" pipeline=logging resource=res-1 run=`+id+` step=flaky/v1 attempt=1 error="cleanup broken"`,
		`level=INFO msg="durable: run complete" pipeline=logging resource=res-1 run=`+id+` outcome=failure elapsed=`,
		// Invocation.Logger pre-attaches the canonical keys.
		`level=INFO msg="hello from handler" pipeline=logging resource=res-1 run=`+id+` step=flaky/v1 phase=forward attempt=2`,
	)
}

// TestLoggingCancel asserts the cancellation request and its acceptance
// are logged, including the cause.
func TestLoggingCancel(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "logging-cancel",
		Steps: []durable.StepConfig{
			stateless("never-runs/v1", func(ctx context.Context, inv durable.Invocation) error {
				return nil
			}),
		},
	})
	buf, pipe := startLoggingEngine(t, def)

	run, _, err := pipe.Schedule(context.Background(), "res-1", nil, durable.StartAfter(time.Hour))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := run.Cancel(context.Background(), "operator request"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Outcome != durable.OutcomeFailure {
		t.Fatalf("outcome = %v, want failure", res.Outcome)
	}

	id := string(run.ID())
	mustLog(t, buf,
		`level=DEBUG msg="durable: run scheduled" pipeline=logging-cancel resource=res-1 run=`+id+` start_at=`,
		`level=DEBUG msg="durable: cancel requested" run=`+id+` cause="operator request"`,
		`level=INFO msg="durable: cancellation accepted; unwinding" pipeline=logging-cancel resource=res-1 run=`+id+` cause="operator request"`,
		`level=INFO msg="durable: run complete" pipeline=logging-cancel resource=res-1 run=`+id+` outcome=failure`,
	)
}
