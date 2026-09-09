package storagepb

import (
	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/driver"
	"reflect"
	"testing"
	"time"
)

// Times constructed via time.Unix in UTC survive the Timestamp conversion
// bit-for-bit, so reflect.DeepEqual is exact.
func at(sec int64) time.Time { return time.Unix(sec, 123456789).UTC() }

func TestRunMetaRoundTrip(t *testing.T) {
	rec := &driver.RunRecord{
		RunID:      "run-1",
		PipelineID: "provision-machine",
		ResourceID: "machine-1",
		// The input is not part of meta; it must not leak into the row.
		Input:     []byte{0x0a, 0x03, 'o', 'r', 'd'},
		CreatedAt: at(1),
	}
	b, err := MarshalRunMeta(rec)
	if err != nil {
		t.Fatalf("MarshalRunMeta: %v", err)
	}
	got := &driver.RunRecord{}
	if err := UnmarshalRunMetaInto(b, got); err != nil {
		t.Fatalf("UnmarshalRunMetaInto: %v", err)
	}
	want := *rec
	want.Input = nil
	if !reflect.DeepEqual(&want, got) {
		t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, &want)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	cases := []driver.Cursor{
		{
			Phase:         kernel.PhaseForward,
			StepID:        "reserve/v1",
			Attempts:      7,
			NextAttemptAt: at(100),
			LastError:     "device busy",
			LastReason:    "device-busy",
			LastErrorAt:   at(90),
			UpdatedAt:     at(101),
		},
		{
			Phase:    kernel.PhaseForward,
			StepID:   "ship/v1",
			Attempts: 2,
			Awaiting: &kernel.Await{
				Mode:     kernel.AwaitModeAny,
				Targets:  []kernel.RunID{"01ARZ3NDEKTSV4RRFFQ69G5FAV", "01ARZ3NDEKTSV4RRFFQ69G5FAW"},
				Deadline: at(500),
			},
			UpdatedAt: at(150),
		},
		{
			Phase:    kernel.PhaseUnwind,
			StepID:   "ship/v1",
			Attempts: 3,
			Awaited: &kernel.Wake{
				Targets: []kernel.RunID{"01ARZ3NDEKTSV4RRFFQ69G5FAV", "01ARZ3NDEKTSV4RRFFQ69G5FAW"},
				Done:    []kernel.RunID{"01ARZ3NDEKTSV4RRFFQ69G5FAW"},
				Expired: true,
			},
			UpdatedAt: at(160),
		},
		{
			// A single-target park with no deadline: the common case.
			Phase:     kernel.PhaseForward,
			StepID:    "ship/v1",
			Attempts:  1,
			Awaiting:  &kernel.Await{Mode: kernel.AwaitModeAll, Targets: []kernel.RunID{"01ARZ3NDEKTSV4RRFFQ69G5FAV"}},
			UpdatedAt: at(170),
		},
		{Phase: kernel.PhaseDone, UpdatedAt: at(200)}, // idle, zero times preserved
	}
	for _, c := range cases {
		b, err := MarshalCursor(c)
		if err != nil {
			t.Fatalf("MarshalCursor: %v", err)
		}
		got, err := UnmarshalCursor(b)
		if err != nil {
			t.Fatalf("UnmarshalCursor: %v", err)
		}
		if !reflect.DeepEqual(c, got) {
			t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, c)
		}
	}
}

func TestOperationRecordRoundTrip(t *testing.T) {
	for _, op := range []driver.OperationRecord{
		{Status: driver.OpSucceeded, Attempts: 3, State: []byte{1, 2, 3}, Order: 7},
		{Status: driver.OpUnresolved, Attempts: 2},
		{Status: driver.OpFailed, Attempts: 4, Order: 9, Failure: &kernel.Failure{
			StepID:  "reserve/v1",
			Phase:   kernel.PhaseUnwind,
			Attempt: 4,
			Message: "release rejected",
			At:      at(200),
			Kind:    kernel.FailureKindSystem,
			Reason:  "release-rejected",
		}},
	} {
		b, err := MarshalOperationRecord(&op)
		if err != nil {
			t.Fatalf("MarshalOperationRecord: %v", err)
		}
		got, err := UnmarshalOperationRecord(b)
		if err != nil {
			t.Fatalf("UnmarshalOperationRecord: %v", err)
		}
		if !reflect.DeepEqual(op, got) {
			t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, op)
		}
	}
}

func TestFailureRecordRoundTrip(t *testing.T) {
	root := kernel.Failure{
		StepID:  "create/v1",
		Phase:   kernel.PhaseForward,
		Attempt: 5,
		Message: "no capacity",
		At:      at(100),
		Kind:    kernel.FailureKindUser,
		Reason:  "insufficient-capacity",
	}
	b, err := MarshalFailureRecord(root)
	if err != nil {
		t.Fatalf("MarshalFailureRecord: %v", err)
	}
	got, err := UnmarshalFailureRecord(b)
	if err != nil {
		t.Fatalf("UnmarshalFailureRecord: %v", err)
	}
	if !reflect.DeepEqual(root, got) {
		t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, root)
	}
	// Cancellation roots have no StepID.
	b, err = MarshalFailureRecord(kernel.Failure{Message: "canceled", At: at(1), Kind: kernel.FailureKindCanceled})
	if err != nil {
		t.Fatalf("MarshalFailureRecord: %v", err)
	}
	got, err = UnmarshalFailureRecord(b)
	if err != nil || got.StepID != "" || got.Kind != kernel.FailureKindCanceled {
		t.Fatalf("cancel root round trip = %+v %v", got, err)
	}
}

func TestTerminalRoundTrip(t *testing.T) {
	oc := kernel.OutcomeSuccess
	rec := &driver.RunRecord{
		RunID: "run-1", PipelineID: "p", ResourceID: "r",
		Annotations: map[string]string{"tenant": "t1"},
		// The terminal stage carries neither the input nor any operation
		// but a failed unwind; they must not leak into the encoding.
		Input: []byte{1, 2, 3},
		Steps: map[kernel.StepID]*driver.StepRecord{
			"a": {Forward: driver.OperationRecord{Status: driver.OpSucceeded, State: []byte{4}, Order: 1}},
			"b": {
				Forward: driver.OperationRecord{Status: driver.OpSucceeded, State: []byte{5}, Order: 2},
				Unwind:  driver.OperationRecord{Status: driver.OpFailed, Attempts: 3, Order: 4, Failure: &kernel.Failure{StepID: "b", Phase: kernel.PhaseUnwind, Attempt: 3, Message: "stuck", At: at(150), Reason: "r"}},
			},
		},
		Phase: kernel.PhaseDone, Outcome: &oc, Output: []byte{9, 9},
		Failure:   &kernel.Failure{StepID: "b", Phase: kernel.PhaseForward, Attempt: 1, Message: "boom", At: at(120), Kind: kernel.FailureKindUser, Reason: "why"},
		Cancel:    &driver.CancelRequest{Cause: "late", At: at(130)},
		CreatedAt: at(100), UpdatedAt: at(200),
	}
	b, err := MarshalTerminal(rec)
	if err != nil {
		t.Fatalf("MarshalTerminal: %v", err)
	}
	got := &driver.RunRecord{}
	if err := UnmarshalTerminalInto(b, got); err != nil {
		t.Fatalf("UnmarshalTerminalInto: %v", err)
	}
	want := &driver.RunRecord{
		RunID: "run-1", PipelineID: "p", ResourceID: "r",
		Annotations: map[string]string{"tenant": "t1"},
		Steps: map[kernel.StepID]*driver.StepRecord{
			"b": {Unwind: driver.OperationRecord{Status: driver.OpFailed, Attempts: 3, Order: 4, Failure: &kernel.Failure{StepID: "b", Phase: kernel.PhaseUnwind, Attempt: 3, Message: "stuck", At: at(150), Reason: "r"}}},
		},
		Phase: kernel.PhaseDone, Outcome: &oc, Output: []byte{9, 9},
		Failure:   &kernel.Failure{StepID: "b", Phase: kernel.PhaseForward, Attempt: 1, Message: "boom", At: at(120), Kind: kernel.FailureKindUser, Reason: "why"},
		Cancel:    &driver.CancelRequest{Cause: "late", At: at(130)},
		CreatedAt: at(100), UpdatedAt: at(200),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
	if _, err := MarshalTerminal(&driver.RunRecord{RunID: "run-2"}); err == nil {
		t.Fatal("MarshalTerminal of a nonterminal record must fail")
	}
}

func TestCancelRoundTrip(t *testing.T) {
	c := &driver.CancelRequest{Cause: "operator", At: at(500)}
	b, err := MarshalCancel(c)
	if err != nil {
		t.Fatalf("MarshalCancel: %v", err)
	}
	got, err := UnmarshalCancel(b)
	if err != nil || !reflect.DeepEqual(c, got) {
		t.Fatalf("round trip = %+v %v, want %+v", got, err, c)
	}
}
