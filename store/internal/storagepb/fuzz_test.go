package storagepb

import (
	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/driver"
	"slices"
	"strings"
	"testing"
	"time"
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
		phase := kernel.Phase(a%3 + 1)
		sameTime := func(x, y time.Time) bool { return x.Equal(y) }

		// Cursor.
		cur := driver.Cursor{
			Phase: phase, StepID: kernel.StepID(s1), Attempts: n,
			NextAttemptAt: when, LastError: s2, LastReason: s1, LastErrorAt: when,
			UpdatedAt: when,
		}
		if s2 != "" {
			cur.Awaiting = &kernel.Await{
				Mode: kernel.AwaitMode(b%2 + 1), Targets: []kernel.RunID{kernel.RunID(s2), kernel.RunID(s1)}, Deadline: when}
			cur.Awaited = &kernel.Wake{
				Targets: []kernel.RunID{kernel.RunID(s1)}, Done: []kernel.RunID{kernel.RunID(s1)}, Expired: a%2 == 1}
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
			got.LastError != cur.LastError || got.LastReason != cur.LastReason ||
			!sameTime(got.NextAttemptAt, cur.NextAttemptAt) ||
			!sameTime(got.LastErrorAt, cur.LastErrorAt) || !sameTime(got.UpdatedAt, cur.UpdatedAt) {
			t.Fatalf("cursor round trip: %+v != %+v", got, cur)
		}
		if (got.Awaiting == nil) != (cur.Awaiting == nil) || (got.Awaited == nil) != (cur.Awaited == nil) {
			t.Fatalf("cursor await round trip: %+v != %+v", got, cur)
		}
		if cur.Awaiting != nil {
			if got.Awaiting.Mode != cur.Awaiting.Mode || !slices.Equal(got.Awaiting.Targets, cur.Awaiting.Targets) ||
				!sameTime(got.Awaiting.Deadline, cur.Awaiting.Deadline) {
				t.Fatalf("awaiting round trip: %+v != %+v", got.Awaiting, cur.Awaiting)
			}
			if !slices.Equal(got.Awaited.Targets, cur.Awaited.Targets) || !slices.Equal(got.Awaited.Done, cur.Awaited.Done) ||
				got.Awaited.Expired != cur.Awaited.Expired {
				t.Fatalf("awaited round trip: %+v != %+v", got.Awaited, cur.Awaited)
			}
		}

		// OperationRecord, with a failure when the status says so.
		op := driver.OperationRecord{
			Status: driver.OpStatus(a % 4), Attempts: n,
			State: append([]byte(nil), blob...), Order: uint32(b),
		}
		if op.Status == driver.OpFailed {
			op.Failure = &kernel.Failure{
				StepID: kernel.StepID(s2), Phase: phase, Attempt: n / 3, Message: s1, At: when,
				Kind: kernel.FailureKind(b % 3), Reason: s2,
			}
		}
		ob, err := MarshalOperationRecord(&op)
		if err != nil {
			t.Fatalf("MarshalOperationRecord: %v", err)
		}
		gop, err := UnmarshalOperationRecord(ob)
		if err != nil {
			t.Fatalf("UnmarshalOperationRecord: %v", err)
		}
		if gop.Status != op.Status || gop.Attempts != op.Attempts || gop.Order != op.Order ||
			string(gop.State) != string(op.State) || (gop.Failure == nil) != (op.Failure == nil) {
			t.Fatalf("operation record round trip: %+v != %+v", gop, op)
		}
		if op.Failure != nil && (gop.Failure.StepID != op.Failure.StepID || gop.Failure.Kind != op.Failure.Kind ||
			gop.Failure.Message != op.Failure.Message || !sameTime(gop.Failure.At, op.Failure.At)) {
			t.Fatalf("operation failure round trip: %+v != %+v", gop.Failure, op.Failure)
		}

		// Root Failure.
		var root *kernel.Failure
		if a%2 == 0 {
			root = &kernel.Failure{
				StepID: kernel.StepID(s1), Phase: phase, Attempt: n,
				Message: s2, At: when, Kind: kernel.FailureKind(b % 3), Reason: s1,
			}
		}
		var groot *kernel.Failure
		if root != nil {
			fb, err := MarshalFailureRecord(*root)
			if err != nil {
				t.Fatalf("MarshalFailureRecord: %v", err)
			}
			f, err := UnmarshalFailureRecord(fb)
			if err != nil {
				t.Fatalf("UnmarshalFailureRecord: %v", err)
			}
			groot = &f
		}
		if root != nil && (groot.StepID != root.StepID || groot.Kind != root.Kind ||
			groot.Message != root.Message || !sameTime(groot.At, root.At)) {
			t.Fatalf("run failure round trip: %+v != %+v", groot, root)
		}

		// Terminal.
		oc := kernel.Outcome(a%2 + 1)
		term := &driver.RunRecord{
			RunID: kernel.RunID(s1), PipelineID: kernel.PipelineID(s2), ResourceID: kernel.ResourceID(s1),
			Phase: kernel.Phase(a % 4), Outcome: &oc, Output: append([]byte(nil), blob...),
			CreatedAt: when, UpdatedAt: when.Add(time.Second),
		}
		if a%2 == 0 {
			term.Annotations = map[string]string{s1: s2}
		}
		tb, err := MarshalTerminal(term, false)
		if err != nil {
			t.Fatalf("MarshalTerminal: %v", err)
		}
		gterm := &driver.RunRecord{}
		if _, err := UnmarshalTerminalInto(tb, gterm); err != nil {
			t.Fatalf("UnmarshalTerminalInto: %v", err)
		}
		if gterm.RunID != term.RunID || gterm.PipelineID != term.PipelineID || gterm.ResourceID != term.ResourceID ||
			gterm.Outcome == nil || *gterm.Outcome != oc || string(gterm.Output) != string(blob) ||
			gterm.Phase != term.Phase || !sameTime(gterm.CreatedAt, term.CreatedAt) || !sameTime(gterm.UpdatedAt, term.UpdatedAt) ||
			len(gterm.Annotations) != len(term.Annotations) {
			t.Fatalf("terminal round trip: %+v != %+v", gterm, term)
		}

		// Cancel.
		xb, err := MarshalCancel(&driver.CancelRequest{Cause: s1, At: when})
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
		rec := &driver.RunRecord{
			RunID: kernel.RunID(s1), PipelineID: kernel.PipelineID(s2),
			ResourceID: kernel.ResourceID(s1),
			CreatedAt:  when,
		}
		if a%2 == 0 {
			rec.Annotations = map[string]string{s1: s2}
		}
		mb, err := MarshalRunMeta(rec)
		if err != nil {
			t.Fatalf("MarshalRunMeta: %v", err)
		}
		grec := &driver.RunRecord{}
		if err := UnmarshalRunMetaInto(mb, grec); err != nil {
			t.Fatalf("UnmarshalRunMetaInto: %v", err)
		}
		if grec.RunID != rec.RunID || grec.PipelineID != rec.PipelineID ||
			grec.ResourceID != rec.ResourceID ||
			!sameTime(grec.CreatedAt, rec.CreatedAt) ||
			len(grec.Annotations) != len(rec.Annotations) {
			t.Fatalf("run meta round trip: %+v != %+v", grec, rec)
		}
		for k, v := range rec.Annotations {
			if grec.Annotations[k] != v {
				t.Fatalf("annotation %q round trip: %q != %q", k, grec.Annotations[k], v)
			}
		}

		// Arbitrary input: decoding may succeed or error; it must not
		// panic.
		_, _ = UnmarshalCursor(blob)
		_, _ = UnmarshalOperationRecord(blob)
		_, _ = UnmarshalFailureRecord(blob)
		_, _ = UnmarshalTerminalInto(blob, &driver.RunRecord{})
		_, _ = UnmarshalCancel(blob)
		_ = UnmarshalRunMetaInto(blob, &driver.RunRecord{})
	})
}
