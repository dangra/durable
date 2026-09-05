package storagepb

import (
	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/driver"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// Times constructed via time.Unix in UTC survive the Timestamp conversion
// bit-for-bit, so reflect.DeepEqual is exact.
func at(sec int64) time.Time { return time.Unix(sec, 123456789).UTC() }

func TestRunMetaRoundTrip(t *testing.T) {
	rec := &driver.RunRecord{
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
	got := &driver.RunRecord{}
	if err := UnmarshalRunMetaInto(b, got); err != nil {
		t.Fatalf("UnmarshalRunMetaInto: %v", err)
	}
	if !reflect.DeepEqual(rec, got) {
		t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, rec)
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

// A cursor written by durable <= v0.3 carries its park in the singular
// awaiting_run_id field; it decodes as an ALL park of one target.
func TestCursorDecodesLegacyPark(t *testing.T) {
	b, err := proto.Marshal(&Cursor{Phase: Phase_PHASE_FORWARD, StepId: "ship/v1", Attempts: 1, AwaitingRunId: "01ARZ3NDEKTSV4RRFFQ69G5FAV"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalCursor(b)
	if err != nil {
		t.Fatalf("UnmarshalCursor: %v", err)
	}
	want := &kernel.Await{Mode: kernel.AwaitModeAll, Targets: []kernel.RunID{"01ARZ3NDEKTSV4RRFFQ69G5FAV"}}
	if !reflect.DeepEqual(got.Awaiting, want) {
		t.Fatalf("Awaiting = %+v, want %+v", got.Awaiting, want)
	}
	// Re-encoding writes only the new field.
	b2, err := MarshalCursor(got)
	if err != nil {
		t.Fatalf("MarshalCursor: %v", err)
	}
	pb := &Cursor{}
	if err := proto.Unmarshal(b2, pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if pb.GetAwaitingRunId() != "" || pb.GetAwaiting() == nil {
		t.Fatalf("re-encoded cursor = %+v; want park in the awaiting message only", pb)
	}
}

func TestStepRecordRoundTrip(t *testing.T) {
	sr := &driver.StepRecord{
		ForwardStatus:   driver.OpSucceeded,
		ForwardAttempts: 3,
		State:           []byte{1, 2, 3},
		UnwindStatus:    driver.OpUnresolved,
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
	root := &kernel.RootFailure{FailureRecord: kernel.FailureRecord{
		StepID:  "create/v1",
		Phase:   kernel.PhaseForward,
		Attempt: 5,
		Message: "no capacity",
		At:      at(100),
		Kind:    kernel.FailureKindUser,
		Reason:  "insufficient-capacity",
	}}
	unwind := []kernel.UnwindFailure{{FailureRecord: kernel.FailureRecord{
		StepID:  "reserve/v1",
		Phase:   kernel.PhaseUnwind,
		Attempt: 1,
		Message: "release rejected",
		At:      at(200),
		Kind:    kernel.FailureKindSystem,
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
	b, err = MarshalFailures(&kernel.RootFailure{FailureRecord: kernel.FailureRecord{
		Message: "canceled", At: at(1), Kind: kernel.FailureKindCanceled,
	}}, nil)
	if err != nil {
		t.Fatalf("MarshalFailures: %v", err)
	}
	gotRoot, gotUnwind, err = UnmarshalFailures(b)
	if err != nil || gotRoot == nil || gotRoot.Kind != kernel.FailureKindCanceled || gotUnwind != nil {
		t.Fatalf("cancel root round trip = %+v %+v %v", gotRoot, gotUnwind, err)
	}
}

func TestTerminalRoundTrip(t *testing.T) {
	b, err := MarshalTerminal(kernel.OutcomeSuccess, []byte{9, 9})
	if err != nil {
		t.Fatalf("MarshalTerminal: %v", err)
	}
	oc, out, err := UnmarshalTerminal(b)
	if err != nil || oc != kernel.OutcomeSuccess || len(out) != 2 {
		t.Fatalf("round trip = %v %v %v", oc, out, err)
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
