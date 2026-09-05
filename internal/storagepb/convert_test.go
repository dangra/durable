package storagepb

import (
	"github.com/dangra/durable/storedriver"
	"reflect"
	"testing"
	"time"

	"github.com/dangra/durable"
)

// Times constructed via time.Unix in UTC survive the Timestamp conversion
// bit-for-bit, so reflect.DeepEqual is exact.
func at(sec int64) time.Time { return time.Unix(sec, 123456789).UTC() }

func TestRunMetaRoundTrip(t *testing.T) {
	rec := &storedriver.RunRecord{
		RunID:      "run-1",
		PipelineID: "provision-machine",
		ResourceID: "machine-1",
		Group:      "group/machine-lifecycle",
		Input:      []byte{0x0a, 0x03, 'o', 'r', 'd'},
		CreatedAt:  at(1),
	}
	b, err := MarshalRunMeta(rec)
	if err != nil {
		t.Fatalf("MarshalRunMeta: %v", err)
	}
	got := &storedriver.RunRecord{}
	if err := UnmarshalRunMetaInto(b, got); err != nil {
		t.Fatalf("UnmarshalRunMetaInto: %v", err)
	}
	if !reflect.DeepEqual(rec, got) {
		t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, rec)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	cases := []storedriver.Cursor{
		{
			Phase:         durable.PhaseForward,
			StepID:        "reserve/v1",
			Attempts:      7,
			NextAttemptAt: at(100),
			LastError:     "device busy",
			LastReason:    "device-busy",
			LastErrorAt:   at(90),
			UpdatedAt:     at(101),
		},
		{Phase: durable.PhaseDone, UpdatedAt: at(200)}, // idle, zero times preserved
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

func TestStepRecordRoundTrip(t *testing.T) {
	sr := &storedriver.StepRecord{
		ForwardStatus:   storedriver.OpSucceeded,
		ForwardAttempts: 3,
		State:           []byte{1, 2, 3},
		UnwindStatus:    storedriver.OpUnresolved,
		UnwindAttempts:  2,
	}
	b, err := MarshalStepRecord(sr)
	if err != nil {
		t.Fatalf("MarshalStepRecord: %v", err)
	}
	got, err := UnmarshalStepRecord(b)
	if err != nil {
		t.Fatalf("UnmarshalStepRecord: %v", err)
	}
	if !reflect.DeepEqual(sr, got) {
		t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, sr)
	}
}

func TestFailuresRoundTrip(t *testing.T) {
	root := &durable.RootFailure{FailureRecord: durable.FailureRecord{
		StepID:  "create/v1",
		Phase:   durable.PhaseForward,
		Attempt: 5,
		Message: "no capacity",
		At:      at(100),
		Kind:    durable.FailureKindUser,
		Reason:  "insufficient-capacity",
	}}
	unwind := []durable.UnwindFailure{{FailureRecord: durable.FailureRecord{
		StepID:  "reserve/v1",
		Phase:   durable.PhaseUnwind,
		Attempt: 1,
		Message: "release rejected",
		At:      at(200),
		Kind:    durable.FailureKindSystem,
		Reason:  "release-rejected",
	}}}
	b, err := MarshalFailures(root, unwind)
	if err != nil {
		t.Fatalf("MarshalFailures: %v", err)
	}
	gotRoot, gotUnwind, err := UnmarshalFailures(b)
	if err != nil {
		t.Fatalf("UnmarshalFailures: %v", err)
	}
	if !reflect.DeepEqual(root, gotRoot) || !reflect.DeepEqual(unwind, gotUnwind) {
		t.Fatalf("round trip mismatch:\n got: %+v %+v\nwant: %+v %+v", gotRoot, gotUnwind, root, unwind)
	}
	// Cancellation roots have no StepID and no unwind failures yet.
	b, err = MarshalFailures(&durable.RootFailure{FailureRecord: durable.FailureRecord{
		Message: "canceled", At: at(1), Kind: durable.FailureKindCanceled,
	}}, nil)
	if err != nil {
		t.Fatalf("MarshalFailures: %v", err)
	}
	gotRoot, gotUnwind, err = UnmarshalFailures(b)
	if err != nil || gotRoot == nil || gotRoot.Kind != durable.FailureKindCanceled || gotUnwind != nil {
		t.Fatalf("cancel root round trip = %+v %+v %v", gotRoot, gotUnwind, err)
	}
}

func TestTerminalRoundTrip(t *testing.T) {
	b, err := MarshalTerminal(durable.OutcomeSuccess, []byte{9, 9})
	if err != nil {
		t.Fatalf("MarshalTerminal: %v", err)
	}
	oc, out, err := UnmarshalTerminal(b)
	if err != nil || oc != durable.OutcomeSuccess || len(out) != 2 {
		t.Fatalf("round trip = %v %v %v", oc, out, err)
	}
}

func TestCancelRoundTrip(t *testing.T) {
	c := &storedriver.CancelRequest{Cause: "operator", At: at(500)}
	b, err := MarshalCancel(c)
	if err != nil {
		t.Fatalf("MarshalCancel: %v", err)
	}
	got, err := UnmarshalCancel(b)
	if err != nil || !reflect.DeepEqual(c, got) {
		t.Fatalf("round trip = %+v %v, want %+v", got, err, c)
	}
}
