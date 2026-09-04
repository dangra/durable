package storagepb

import (
	"strings"
	"testing"
	"time"

	"github.com/dangra/durable"
)

// FuzzRoundTrip exercises the storage converters both ways: every value
// built from fuzz primitives must survive a marshal/unmarshal round trip
// unchanged, and every unmarshal fed the arbitrary blob must not panic
// (it may error, or even decode, since random bytes can form a valid
// encoding) — a store file written by a crashed or newer process must
// never take the engine down.
func FuzzRoundTrip(f *testing.F) {
	f.Add(byte(1), byte(0), "step/v1", "boom", uint64(3), int64(1_700_000_000), []byte{1, 2, 3})
	f.Add(byte(2), byte(3), "", "", uint64(0), int64(0), []byte{})
	f.Fuzz(func(t *testing.T, a, b byte, s1, s2 string, n uint64, sec int64, blob []byte) {
		// The engine guarantees valid UTF-8 in every string reaching the
		// storage layer (identifier validation + free-text sanitizing);
		// mirror that contract here. Corrupt DECODING still sees raw
		// bytes below.
		s1 = strings.ToValidUTF8(s1, "�")
		s2 = strings.ToValidUTF8(s2, "�")
		when := time.Time{}
		if sec != 0 {
			when = time.Unix(sec%4_000_000_000, int64(n%1_000_000_000))
		}
		phase := durable.Phase(a%3 + 1)
		sameTime := func(x, y time.Time) bool { return x.Equal(y) }

		// Cursor.
		cur := durable.Cursor{
			Phase: phase, StepID: durable.StepID(s1), Attempts: n,
			AwaitingRunID: durable.RunID(s2),
			NextAttemptAt: when, LastError: s2, LastReason: s1, LastErrorAt: when,
			UpdatedAt: when,
		}
		cb, err := MarshalCursor(cur)
		if err != nil {
			t.Fatalf("MarshalCursor: %v", err)
		}
		got, err := UnmarshalCursor(cb)
		if err != nil {
			t.Fatalf("UnmarshalCursor: %v", err)
		}
		if got.Phase != cur.Phase || got.StepID != cur.StepID || got.Attempts != cur.Attempts ||
			got.AwaitingRunID != cur.AwaitingRunID || got.LastError != cur.LastError ||
			got.LastReason != cur.LastReason ||
			!sameTime(got.NextAttemptAt, cur.NextAttemptAt) ||
			!sameTime(got.LastErrorAt, cur.LastErrorAt) || !sameTime(got.UpdatedAt, cur.UpdatedAt) {
			t.Fatalf("cursor round trip: %+v != %+v", got, cur)
		}

		// StepRecord.
		sr := &durable.StepRecord{
			ForwardStatus: durable.OpStatus(a % 4), ForwardAttempts: n,
			State:        append([]byte(nil), blob...),
			UnwindStatus: durable.OpStatus(b % 4), UnwindAttempts: n / 2,
		}
		sb, err := MarshalStepRecord(sr)
		if err != nil {
			t.Fatalf("MarshalStepRecord: %v", err)
		}
		gsr, err := UnmarshalStepRecord(sb)
		if err != nil {
			t.Fatalf("UnmarshalStepRecord: %v", err)
		}
		if gsr.ForwardStatus != sr.ForwardStatus || gsr.ForwardAttempts != sr.ForwardAttempts ||
			gsr.UnwindStatus != sr.UnwindStatus || gsr.UnwindAttempts != sr.UnwindAttempts ||
			string(gsr.State) != string(sr.State) {
			t.Fatalf("step record round trip: %+v != %+v", gsr, sr)
		}

		// Failures.
		var root *durable.RootFailure
		if a%2 == 0 {
			root = &durable.RootFailure{FailureRecord: durable.FailureRecord{
				StepID: durable.StepID(s1), Phase: phase, Attempt: n,
				Message: s2, At: when, Kind: durable.FailureKind(b % 3), Reason: s1,
			}}
		}
		unwind := []durable.UnwindFailure{{FailureRecord: durable.FailureRecord{
			StepID: durable.StepID(s2), Phase: phase, Attempt: n / 3, Message: s1, At: when,
		}}}
		fb, err := MarshalFailures(root, unwind)
		if err != nil {
			t.Fatalf("MarshalFailures: %v", err)
		}
		groot, gunwind, err := UnmarshalFailures(fb)
		if err != nil {
			t.Fatalf("UnmarshalFailures: %v", err)
		}
		if (root == nil) != (groot == nil) || len(gunwind) != len(unwind) {
			t.Fatalf("failures round trip shape: root %v/%v unwind %d/%d", root, groot, len(unwind), len(gunwind))
		}
		if root != nil && (groot.StepID != root.StepID || groot.Kind != root.Kind ||
			groot.Message != root.Message || !sameTime(groot.At, root.At)) {
			t.Fatalf("root failure round trip: %+v != %+v", groot, root)
		}

		// Terminal.
		oc := durable.Outcome(a%2 + 1)
		tb, err := MarshalTerminal(oc, blob)
		if err != nil {
			t.Fatalf("MarshalTerminal: %v", err)
		}
		goc, gout, err := UnmarshalTerminal(tb)
		if err != nil {
			t.Fatalf("UnmarshalTerminal: %v", err)
		}
		if goc != oc || string(gout) != string(blob) {
			t.Fatalf("terminal round trip: %v/%q != %v/%q", goc, gout, oc, blob)
		}

		// Cancel.
		xb, err := MarshalCancel(&durable.CancelRequest{Cause: s1, At: when})
		if err != nil {
			t.Fatalf("MarshalCancel: %v", err)
		}
		gc, err := UnmarshalCancel(xb)
		if err != nil {
			t.Fatalf("UnmarshalCancel: %v", err)
		}
		if gc.Cause != s1 || !sameTime(gc.At, when) {
			t.Fatalf("cancel round trip: %+v", gc)
		}

		// RunMeta.
		rec := &durable.RunRecord{
			RunID: durable.RunID(s1), PipelineID: durable.PipelineID(s2),
			ResourceID: durable.ResourceID(s1), Group: s2,
			Input: append([]byte(nil), blob...), CreatedAt: when,
		}
		mb, err := MarshalRunMeta(rec)
		if err != nil {
			t.Fatalf("MarshalRunMeta: %v", err)
		}
		grec := &durable.RunRecord{}
		if err := UnmarshalRunMetaInto(mb, grec); err != nil {
			t.Fatalf("UnmarshalRunMetaInto: %v", err)
		}
		if grec.RunID != rec.RunID || grec.PipelineID != rec.PipelineID ||
			grec.ResourceID != rec.ResourceID || grec.Group != rec.Group ||
			string(grec.Input) != string(rec.Input) || !sameTime(grec.CreatedAt, rec.CreatedAt) {
			t.Fatalf("run meta round trip: %+v != %+v", grec, rec)
		}

		// Arbitrary input: decoding may succeed or error; it must not
		// panic.
		_, _ = UnmarshalCursor(blob)
		_, _ = UnmarshalStepRecord(blob)
		_, _, _ = UnmarshalFailures(blob)
		_, _, _ = UnmarshalTerminal(blob)
		_, _ = UnmarshalCancel(blob)
		_ = UnmarshalRunMetaInto(blob, &durable.RunRecord{})
	})
}
