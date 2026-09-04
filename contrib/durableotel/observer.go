package durableotel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/dangra/durable"
)

// NewObserver returns a durable.Observer that translates engine
// lifecycle events into OpenTelemetry metrics, for installation via
// durable.WithObserver. Instruments, all under the durable.* namespace
// with durations in seconds:
//
//   - durable.runs.scheduled      counter    {pipeline}
//   - durable.attempts            counter    {pipeline, step, phase, result}
//   - durable.attempt.duration    histogram  {pipeline, step, phase, result}
//   - durable.runs.unwinding      counter    {pipeline, failure_kind, reason}
//   - durable.runs.terminal       counter    {pipeline, outcome; failure_kind, reason on failure}
//   - durable.run.duration        histogram  {pipeline, outcome}
//   - durable.runs.invalidated    counter    {pipeline, reason}
//   - durable.await.duration      histogram  {pipeline}
//   - durable.class.wait.duration histogram  {class}
//   - durable.runs.reaped         counter    {}
//   - durable.store.op.duration   histogram  {store.op, store.write, error}
//
// Every attribute is bounded-cardinality: reasons are the
// low-cardinality slugs FailureReasoner is documented to carry, and Run
// and resource identities never label metrics. Counters built from
// engine events are operational signal, not accounting — see the
// durable.Observer contract. For point-in-time occupancy gauges, see
// RegisterStats.
func NewObserver(opts ...Option) (durable.Observer, error) {
	cfg := newConfig(opts)
	meter := cfg.meterProvider.Meter(ScopeName)

	var errs []error
	counter := func(name, desc, unit string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return c
	}
	histogram := func(name, desc string) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"))
		errs = append(errs, err)
		return h
	}

	var (
		scheduled       = counter("durable.runs.scheduled", "Runs accepted by Schedule.", "{run}")
		attempts        = counter("durable.attempts", "Operation attempts, by resolution.", "{attempt}")
		attemptDuration = histogram("durable.attempt.duration", "Handler execution time of one attempt.")
		unwinding       = counter("durable.runs.unwinding", "Runs whose RootFailure was established.", "{run}")
		terminal        = counter("durable.runs.terminal", "Runs committing a terminal outcome.", "{run}")
		runDuration     = histogram("durable.run.duration", "Run duration, acceptance to terminal.")
		invalidated     = counter("durable.runs.invalidated", "Runs marked invalid for the current deployment.", "{run}")
		awaitDuration   = histogram("durable.await.duration", "Time parked on AwaitRun, first park to resolution.")
		classWait       = histogram("durable.class.wait.duration", "Time throttled on a concurrency class before a token.")
		reaped          = counter("durable.runs.reaped", "Terminal Runs deleted by retention sweeps.", "{run}")
		storeOpDuration = histogram("durable.store.op.duration", "Store call latency.")
	)
	if err := errors.Join(errs...); err != nil {
		return durable.Observer{}, err
	}

	// Observer callbacks run synchronously on engine goroutines; recording
	// against pre-built or cheaply-built attribute sets keeps them fast.
	ctx := context.Background()
	return durable.Observer{
		RunScheduled: func(ev durable.RunEvent) {
			scheduled.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				AttrPipeline.String(string(ev.PipelineID)))))
		},
		AttemptDone: func(ev durable.AttemptEvent) {
			set := attribute.NewSet(
				AttrPipeline.String(string(ev.PipelineID)),
				AttrStep.String(string(ev.StepID)),
				AttrPhase.String(ev.Phase.String()),
				AttrResult.String(ev.Result.String()),
			)
			attempts.Add(ctx, 1, metric.WithAttributeSet(set))
			attemptDuration.Record(ctx, ev.Duration.Seconds(), metric.WithAttributeSet(set))
		},
		RunUnwinding: func(ev durable.RunFailureEvent) {
			unwinding.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				AttrPipeline.String(string(ev.PipelineID)),
				AttrFailureKind.String(ev.Kind.String()),
				AttrReason.String(ev.Reason),
			)))
		},
		RunTerminal: func(ev durable.RunTerminalEvent) {
			attrs := []attribute.KeyValue{
				AttrPipeline.String(string(ev.PipelineID)),
				AttrOutcome.String(ev.Outcome.String()),
			}
			if ev.Outcome == durable.OutcomeFailure {
				attrs = append(attrs,
					AttrFailureKind.String(ev.Kind.String()),
					AttrReason.String(ev.Reason))
			}
			terminal.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attrs...)))
			runDuration.Record(ctx, ev.Duration.Seconds(), metric.WithAttributeSet(attribute.NewSet(
				AttrPipeline.String(string(ev.PipelineID)),
				AttrOutcome.String(ev.Outcome.String()),
			)))
		},
		RunInvalid: func(ev durable.RunFailureEvent) {
			invalidated.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				AttrPipeline.String(string(ev.PipelineID)),
				AttrReason.String(ev.Reason),
			)))
		},
		WaiterWoken: func(ev durable.WakeEvent) {
			awaitDuration.Record(ctx, ev.Duration.Seconds(), metric.WithAttributeSet(attribute.NewSet(
				AttrPipeline.String(string(ev.PipelineID)))))
		},
		ClassWait: func(ev durable.ClassWaitEvent) {
			classWait.Record(ctx, ev.Duration.Seconds(), metric.WithAttributeSet(attribute.NewSet(
				AttrClass.String(ev.Class))))
		},
		RunsReaped: func(count int) {
			reaped.Add(ctx, int64(count))
		},
		StoreOp: func(ev durable.StoreOpEvent) {
			storeOpDuration.Record(ctx, ev.Duration.Seconds(), metric.WithAttributeSet(attribute.NewSet(
				AttrStoreOp.String(ev.Op),
				AttrStoreWrite.Bool(ev.Write),
				AttrError.Bool(ev.Err != nil),
			)))
		},
	}, nil
}

// RegisterStats registers observable gauges that publish Engine.Stats
// occupancy on every metric collection:
//
//   - durable.engine.runs.active     durable.engine.runs.awaiting
//   - durable.engine.runs.throttled  durable.engine.runs.delayed
//   - durable.engine.runs.invalid
//   - durable.engine.class.capacity  {class}
//   - durable.engine.class.in_use    {class}
//   - durable.engine.class.waiting   {class}
//
// The returned Registration unregisters the callback; unregister before
// discarding the engine.
func RegisterStats(engine *durable.Engine, opts ...Option) (metric.Registration, error) {
	cfg := newConfig(opts)
	meter := cfg.meterProvider.Meter(ScopeName)

	var errs []error
	gauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return g
	}
	var (
		active    = gauge("durable.engine.runs.active", "Runs with a live worker (executing or in delayed dispatch).", "{run}")
		awaiting  = gauge("durable.engine.runs.awaiting", "Runs parked via AwaitRun.", "{run}")
		throttled = gauge("durable.engine.runs.throttled", "Runs parked on concurrency classes.", "{run}")
		delayed   = gauge("durable.engine.runs.delayed", "Runs waiting out a retry backoff or delayed start.", "{run}")
		invalid   = gauge("durable.engine.runs.invalid", "Runs marked invalid for the current deployment.", "{run}")
		capacity  = gauge("durable.engine.class.capacity", "Configured token capacity of the concurrency class.", "{token}")
		inUse     = gauge("durable.engine.class.in_use", "Concurrency class tokens currently held.", "{token}")
		waiting   = gauge("durable.engine.class.waiting", "Runs waiting for a token of the concurrency class.", "{run}")
	)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := engine.Stats()
		o.ObserveInt64(active, int64(st.ActiveRuns))
		o.ObserveInt64(awaiting, int64(st.AwaitingRuns))
		o.ObserveInt64(throttled, int64(st.ThrottledRuns))
		o.ObserveInt64(delayed, int64(st.DelayedRuns))
		o.ObserveInt64(invalid, int64(st.InvalidRuns))
		for name, cs := range st.Classes {
			set := metric.WithAttributeSet(attribute.NewSet(AttrClass.String(name)))
			o.ObserveInt64(capacity, int64(cs.Capacity), set)
			o.ObserveInt64(inUse, int64(cs.InUse), set)
			o.ObserveInt64(waiting, int64(cs.Waiting), set)
		}
		return nil
	}, active, awaiting, throttled, delayed, invalid, capacity, inUse, waiting)
}
