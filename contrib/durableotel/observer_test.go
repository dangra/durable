package durableotel_test

import (
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/dangra/durable"
	"github.com/dangra/durable/contrib/durableotel"
	"github.com/dangra/durable/durabletest"
)

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func counterValue(t *testing.T, m metricdata.Metrics, want ...attribute.KeyValue) int64 {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s: not an int64 counter: %T", m.Name, m.Data)
	}
	wantSet := attribute.NewSet(want...)
	var total int64
	for _, dp := range sum.DataPoints {
		if len(want) == 0 || dp.Attributes.Equals(&wantSet) {
			total += dp.Value
		}
	}
	return total
}

func histogramCount(t *testing.T, m metricdata.Metrics, want ...attribute.KeyValue) uint64 {
	t.Helper()
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s: not a float64 histogram: %T", m.Name, m.Data)
	}
	wantSet := attribute.NewSet(want...)
	var count uint64
	for _, dp := range h.DataPoints {
		if len(want) == 0 || dp.Attributes.Equals(&wantSet) {
			count += dp.Count
		}
	}
	return count
}

// TestNewObserverSyntheticEvents drives every callback with a synthetic
// event and asserts each instrument records under the documented name
// and attributes.
func TestNewObserverSyntheticEvents(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(t.Context())
	obs, err := durableotel.NewObserver(durableotel.WithMeterProvider(mp))
	if err != nil {
		t.Fatalf("NewObserver: %v", err)
	}

	obs.RunScheduled(durable.RunEvent{PipelineID: "p"})
	obs.AttemptDone(durable.AttemptEvent{
		PipelineID: "p", StepID: "s/v1", Phase: durable.PhaseForward,
		Attempt: 1, Duration: 250 * time.Millisecond, Result: durable.AttemptRetrying,
		Err: errors.New("transient"), RetryIn: time.Second,
	})
	obs.RunUnwinding(durable.RunFailureEvent{
		PipelineID: "p", Kind: durable.FailureKindUser, Reason: "invalid-input"})
	obs.RunTerminal(durable.RunTerminalEvent{
		PipelineID: "p", Outcome: durable.OutcomeFailure,
		Kind: durable.FailureKindUser, Reason: "invalid-input", Duration: 3 * time.Second})
	obs.RunTerminal(durable.RunTerminalEvent{
		PipelineID: "p", Outcome: durable.OutcomeSuccess, Duration: time.Second})
	obs.RunInvalid(durable.RunFailureEvent{PipelineID: "p", Reason: "step-retired"})
	obs.WaiterWoken(durable.WakeEvent{PipelineID: "p", Duration: time.Minute})
	obs.ClassWait(durable.ClassWaitEvent{PipelineID: "p", Class: "db", Duration: time.Second})
	obs.RunsReaped(7)
	obs.StoreOp(durable.StoreOpEvent{Op: "ApplyTransition", Write: true, Duration: time.Millisecond})
	obs.StoreOp(durable.StoreOpEvent{Op: "GetRun", Duration: time.Millisecond, Err: errors.New("io")})

	got := collect(t, reader)
	pipeline := durableotel.AttrPipeline.String("p")

	if v := counterValue(t, got["durable.runs.scheduled"], pipeline); v != 1 {
		t.Errorf("runs.scheduled = %d, want 1", v)
	}
	attemptAttrs := []attribute.KeyValue{pipeline,
		durableotel.AttrStep.String("s/v1"),
		durableotel.AttrPhase.String("forward"),
		durableotel.AttrResult.String("retrying")}
	if v := counterValue(t, got["durable.attempts"], attemptAttrs...); v != 1 {
		t.Errorf("attempts = %d, want 1", v)
	}
	if c := histogramCount(t, got["durable.attempt.duration"], attemptAttrs...); c != 1 {
		t.Errorf("attempt.duration count = %d, want 1", c)
	}
	if v := counterValue(t, got["durable.runs.unwinding"], pipeline,
		durableotel.AttrFailureKind.String("user"),
		durableotel.AttrReason.String("invalid-input")); v != 1 {
		t.Errorf("runs.unwinding = %d, want 1", v)
	}
	if v := counterValue(t, got["durable.runs.terminal"], pipeline,
		durableotel.AttrOutcome.String("failure"),
		durableotel.AttrFailureKind.String("user"),
		durableotel.AttrReason.String("invalid-input")); v != 1 {
		t.Errorf("runs.terminal{failure} = %d, want 1", v)
	}
	// Success events carry no failure attribution.
	if v := counterValue(t, got["durable.runs.terminal"], pipeline,
		durableotel.AttrOutcome.String("success")); v != 1 {
		t.Errorf("runs.terminal{success} = %d, want 1", v)
	}
	if c := histogramCount(t, got["durable.run.duration"]); c != 2 {
		t.Errorf("run.duration count = %d, want 2", c)
	}
	if v := counterValue(t, got["durable.runs.invalidated"], pipeline,
		durableotel.AttrReason.String("step-retired")); v != 1 {
		t.Errorf("runs.invalidated = %d, want 1", v)
	}
	if c := histogramCount(t, got["durable.await.duration"], pipeline); c != 1 {
		t.Errorf("await.duration count = %d, want 1", c)
	}
	if c := histogramCount(t, got["durable.class.wait.duration"],
		durableotel.AttrClass.String("db")); c != 1 {
		t.Errorf("class.wait.duration count = %d, want 1", c)
	}
	if v := counterValue(t, got["durable.runs.reaped"]); v != 7 {
		t.Errorf("runs.reaped = %d, want 7", v)
	}
	if c := histogramCount(t, got["durable.store.op.duration"],
		durableotel.AttrStoreOp.String("ApplyTransition"),
		durableotel.AttrStoreWrite.Bool(true),
		durableotel.AttrError.Bool(false)); c != 1 {
		t.Errorf("store.op.duration{write} count = %d, want 1", c)
	}
	if c := histogramCount(t, got["durable.store.op.duration"],
		durableotel.AttrStoreOp.String("GetRun"),
		durableotel.AttrStoreWrite.Bool(false),
		durableotel.AttrError.Bool(true)); c != 1 {
		t.Errorf("store.op.duration{read,error} count = %d, want 1", c)
	}
}

// TestEndToEndMetrics runs the saga on a real engine with the bridge
// installed and asserts the aggregate story: four attempts, one failed
// terminal Run, one unwinding, store traffic observed.
func TestEndToEndMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(t.Context())
	obs, err := durableotel.NewObserver(durableotel.WithMeterProvider(mp))
	if err != nil {
		t.Fatalf("NewObserver: %v", err)
	}

	runSaga(t, nil, durable.WithObserver(obs))

	got := collect(t, reader)
	pipeline := durableotel.AttrPipeline.String("saga")
	if v := counterValue(t, got["durable.runs.scheduled"], pipeline); v != 1 {
		t.Errorf("runs.scheduled = %d, want 1", v)
	}
	if v := counterValue(t, got["durable.attempts"]); v != 4 {
		t.Errorf("attempts = %d, want 4", v)
	}
	if v := counterValue(t, got["durable.runs.terminal"], pipeline,
		durableotel.AttrOutcome.String("failure"),
		durableotel.AttrFailureKind.String("user"),
		durableotel.AttrReason.String("invalid-input")); v != 1 {
		t.Errorf("runs.terminal = %d, want 1", v)
	}
	if v := counterValue(t, got["durable.runs.unwinding"], pipeline,
		durableotel.AttrFailureKind.String("user"),
		durableotel.AttrReason.String("invalid-input")); v != 1 {
		t.Errorf("runs.unwinding = %d, want 1", v)
	}
	if c := histogramCount(t, got["durable.store.op.duration"]); c == 0 {
		t.Error("store.op.duration recorded nothing; StoreOp should wrap the store")
	}
}

// TestRegisterStats registers the occupancy gauges against a live
// engine and asserts a collection observes every documented instrument.
func TestRegisterStats(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(t.Context())

	engine := durable.NewEngine(durabletest.NewMemStore(), fastRetry, quietLogger())
	if err := engine.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop(t.Context())

	reg, err := durableotel.RegisterStats(engine, durableotel.WithMeterProvider(mp))
	if err != nil {
		t.Fatalf("RegisterStats: %v", err)
	}

	got := collect(t, reader)
	for _, name := range []string{
		"durable.engine.runs.active", "durable.engine.runs.awaiting",
		"durable.engine.runs.throttled", "durable.engine.runs.delayed",
		"durable.engine.runs.invalid",
	} {
		m, ok := got[name]
		if !ok {
			t.Fatalf("gauge %s not collected", name)
		}
		g, ok := m.Data.(metricdata.Gauge[int64])
		if !ok || len(g.DataPoints) != 1 || g.DataPoints[0].Value != 0 {
			t.Fatalf("gauge %s = %+v, want one zero point on an idle engine", name, m.Data)
		}
	}

	if err := reg.Unregister(); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if after := collect(t, reader); len(after) != 0 {
		t.Fatalf("gauges still collected after Unregister: %v", after)
	}
}
