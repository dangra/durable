package storagepb

import (
	"reflect"
	"testing"
	"time"

	"github.com/dangra/durable"
)

func TestRunRecordRoundTrip(t *testing.T) {
	// Times constructed via time.Unix in UTC survive the Timestamp
	// conversion bit-for-bit, so reflect.DeepEqual is exact.
	at := func(sec int64) time.Time { return time.Unix(sec, 123456789).UTC() }

	oc := durable.OutcomeFailure
	rec := &durable.RunRecord{
		RunID:      "run-1",
		PipelineID: "provision-machine",
		ResourceID: "machine-1",
		Input:      []byte{0x0a, 0x03, 'o', 'r', 'd'},
		Phase:      durable.PhaseUnwind,
		Steps: map[durable.StepID]*durable.StepRecord{
			"validate/v1": {ForwardStatus: durable.OpSucceeded, ForwardAttempts: 1},
			"reserve/v1": {
				ForwardStatus:   durable.OpSucceeded,
				ForwardAttempts: 3,
				State:           []byte{1, 2, 3},
				UnwindStatus:    durable.OpUnresolved,
				UnwindAttempts:  2,
			},
			"create/v1": {ForwardStatus: durable.OpFailed, ForwardAttempts: 5},
		},
		RootFailure: &durable.RootFailure{FailureRecord: durable.FailureRecord{
			StepID:  "create/v1",
			Phase:   durable.PhaseForward,
			Attempt: 5,
			Message: "no capacity",
			At:      at(100),
			Kind:    durable.FailureKindUser,
			Reason:  "insufficient-capacity",
		}},
		UnwindFailures: []durable.UnwindFailure{{FailureRecord: durable.FailureRecord{
			StepID:  "reserve/v1",
			Phase:   durable.PhaseUnwind,
			Attempt: 1,
			Message: "release rejected",
			At:      at(200),
			Kind:    durable.FailureKindSystem,
			Reason:  "release-rejected",
		}}},
		Output:        []byte{9, 9},
		Outcome:       &oc,
		NextAttemptAt: at(300),
		LastError:     "still busy",
		LastReason:    "device-busy",
		LastErrorAt:   at(400),
		Cancel:        &durable.CancelRequest{Cause: "operator", At: at(500)},
		CreatedAt:     at(1),
		UpdatedAt:     at(600),
	}

	b, err := MarshalRunRecord(rec)
	if err != nil {
		t.Fatalf("MarshalRunRecord: %v", err)
	}
	got, err := UnmarshalRunRecord(b)
	if err != nil {
		t.Fatalf("UnmarshalRunRecord: %v", err)
	}
	if !reflect.DeepEqual(rec, got) {
		t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, rec)
	}
}

func TestRunRecordRoundTripZeroValues(t *testing.T) {
	rec := &durable.RunRecord{
		RunID:      "run-min",
		PipelineID: "p",
		ResourceID: "r",
		Phase:      durable.PhaseForward,
	}
	b, err := MarshalRunRecord(rec)
	if err != nil {
		t.Fatalf("MarshalRunRecord: %v", err)
	}
	got, err := UnmarshalRunRecord(b)
	if err != nil {
		t.Fatalf("UnmarshalRunRecord: %v", err)
	}
	if !reflect.DeepEqual(rec, got) {
		t.Fatalf("round trip mismatch:\n got: %+v\nwant: %+v", got, rec)
	}
	if got.Terminal() {
		t.Fatal("absent outcome decoded as terminal")
	}
	if !got.NextAttemptAt.IsZero() || !got.LastErrorAt.IsZero() || got.Cancel != nil {
		t.Fatalf("zero values not preserved: %+v", got)
	}
}
