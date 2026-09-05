package durable

import (
	"maps"
	"time"
)

// ScheduleOptions is what ScheduleOption functions fill and the engine
// reads when it accepts a Run. Handlers and wiring code use the option
// constructors below; the struct is exported so the engine, in its own
// package, can read the result.
type ScheduleOptions struct {
	// StartAt, when set, delays the Run's first operation until then.
	StartAt time.Time
	// StartAfter, when positive and StartAt is zero, delays the Run's
	// first operation until this long after acceptance, measured by the
	// engine clock.
	StartAfter time.Duration
	// Annotations accumulate across options; later keys win.
	Annotations map[string]string
}

// ScheduleOption configures acceptance of a Run. Options never contribute
// to duplicate-scheduling identity: Input equivalence alone decides whether
// an active Run matches. They live in this package because handlers
// schedule child Runs: a step that fans out needs nothing beyond the
// handler contract to do so.
type ScheduleOption func(*ScheduleOptions)

// WithAnnotations attaches caller-supplied propagation metadata to the
// Run at acceptance: a W3C traceparent, tenant tags, request ids. The
// map is copied; repeated options merge with later keys winning.
// Annotations are immutable for the life of the Run, readable by
// handlers through Invocation.Annotations and by callers through
// Run.Annotations, and are never part of duplicate-scheduling identity —
// on a dedup hit the active Run keeps its own annotations. Keys and
// values must be valid UTF-8 (the durable representation stores them as
// protobuf strings); Schedule rejects violations.
func WithAnnotations(annotations map[string]string) ScheduleOption {
	return func(o *ScheduleOptions) {
		if o.Annotations == nil {
			o.Annotations = make(map[string]string, len(annotations))
		}
		maps.Copy(o.Annotations, annotations)
	}
}

// StartAt delays the Run's first operation until t. The Run is created and
// occupies its resource slot immediately; the delay survives restart. If
// multiple start options are given, the last wins.
func StartAt(t time.Time) ScheduleOption {
	return func(o *ScheduleOptions) {
		o.StartAt, o.StartAfter = t, 0
	}
}

// StartAfter delays the Run's first operation until d after acceptance,
// measured by the engine clock. Sugar for StartAt(now.Add(d)).
func StartAfter(d time.Duration) ScheduleOption {
	return func(o *ScheduleOptions) {
		o.StartAt, o.StartAfter = time.Time{}, d
	}
}
