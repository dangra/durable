// Package storagepb holds the internal storage encoding of durable runs:
// the generated durable.storage.v1 component types and the conversion
// layer between them and the public Go structs. The wire format is private
// to this module's store implementations.
//
// A run is stored as components with distinct write cadences: RunMeta
// (once), StepRecord rows (once per resolution), Failures/Terminal/
// CancelRequest (rare), and the small Cursor (every attempt).
package storagepb

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dangra/durable"
)

func marshal(what string, m proto.Message) ([]byte, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("storagepb: encoding %s: %w", what, err)
	}
	return b, nil
}

func unmarshal(what string, b []byte, m proto.Message) error {
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("storagepb: decoding %s: %w", what, err)
	}
	return nil
}

// MarshalRunMeta encodes the write-once identity fields of rec.
func MarshalRunMeta(rec *durable.RunRecord) ([]byte, error) {
	return marshal("run meta", &RunMeta{
		RunId:       string(rec.RunID),
		PipelineId:  string(rec.PipelineID),
		ResourceId:  string(rec.ResourceID),
		SlotGroup:   rec.Group,
		Input:       rec.Input,
		CreatedAt:   ts(rec.CreatedAt),
		Annotations: rec.Annotations,
	})
}

// UnmarshalRunMetaInto decodes identity fields into rec.
func UnmarshalRunMetaInto(b []byte, rec *durable.RunRecord) error {
	pb := &RunMeta{}
	if err := unmarshal("run meta", b, pb); err != nil {
		return err
	}
	rec.RunID = durable.RunID(pb.GetRunId())
	rec.PipelineID = durable.PipelineID(pb.GetPipelineId())
	rec.ResourceID = durable.ResourceID(pb.GetResourceId())
	rec.Group = pb.GetSlotGroup()
	rec.Input = pb.GetInput()
	rec.CreatedAt = fromTS(pb.GetCreatedAt())
	if len(pb.GetAnnotations()) > 0 {
		rec.Annotations = pb.GetAnnotations()
	}
	return nil
}

func MarshalCursor(c durable.Cursor) ([]byte, error) {
	return marshal("cursor", &Cursor{
		Phase:         phaseToProto(c.Phase),
		StepId:        string(c.StepID),
		Attempts:      c.Attempts,
		NextAttemptAt: ts(c.NextAttemptAt),
		LastError:     c.LastError,
		LastReason:    c.LastReason,
		LastErrorAt:   ts(c.LastErrorAt),
		UpdatedAt:     ts(c.UpdatedAt),
		AwaitingRunId: string(c.AwaitingRunID),
	})
}

func UnmarshalCursor(b []byte) (durable.Cursor, error) {
	pb := &Cursor{}
	if err := unmarshal("cursor", b, pb); err != nil {
		return durable.Cursor{}, err
	}
	return durable.Cursor{
		Phase:         phaseFromProto(pb.GetPhase()),
		StepID:        durable.StepID(pb.GetStepId()),
		Attempts:      pb.GetAttempts(),
		NextAttemptAt: fromTS(pb.GetNextAttemptAt()),
		LastError:     pb.GetLastError(),
		LastReason:    pb.GetLastReason(),
		LastErrorAt:   fromTS(pb.GetLastErrorAt()),
		UpdatedAt:     fromTS(pb.GetUpdatedAt()),
		AwaitingRunID: durable.RunID(pb.GetAwaitingRunId()),
	}, nil
}

func MarshalStepRecord(sr *durable.StepRecord) ([]byte, error) {
	return marshal("step record", &StepRecord{
		ForwardStatus:   opStatusToProto(sr.ForwardStatus),
		ForwardAttempts: sr.ForwardAttempts,
		State:           sr.State,
		UnwindStatus:    opStatusToProto(sr.UnwindStatus),
		UnwindAttempts:  sr.UnwindAttempts,
	})
}

func UnmarshalStepRecord(b []byte) (*durable.StepRecord, error) {
	pb := &StepRecord{}
	if err := unmarshal("step record", b, pb); err != nil {
		return nil, err
	}
	return &durable.StepRecord{
		ForwardStatus:   opStatusFromProto(pb.GetForwardStatus()),
		ForwardAttempts: pb.GetForwardAttempts(),
		State:           pb.GetState(),
		UnwindStatus:    opStatusFromProto(pb.GetUnwindStatus()),
		UnwindAttempts:  pb.GetUnwindAttempts(),
	}, nil
}

func MarshalFailures(root *durable.RootFailure, unwind []durable.UnwindFailure) ([]byte, error) {
	pb := &Failures{}
	if root != nil {
		f := root.FailureRecord
		pb.Root = failureRecordToProto(&f)
	}
	for _, uf := range unwind {
		f := uf.FailureRecord
		pb.Unwind = append(pb.Unwind, failureRecordToProto(&f))
	}
	return marshal("failures", pb)
}

func UnmarshalFailures(b []byte) (*durable.RootFailure, []durable.UnwindFailure, error) {
	pb := &Failures{}
	if err := unmarshal("failures", b, pb); err != nil {
		return nil, nil, err
	}
	var root *durable.RootFailure
	if pb.GetRoot() != nil {
		root = &durable.RootFailure{FailureRecord: failureRecordFromProto(pb.GetRoot())}
	}
	var unwind []durable.UnwindFailure
	for _, f := range pb.GetUnwind() {
		unwind = append(unwind, durable.UnwindFailure{FailureRecord: failureRecordFromProto(f)})
	}
	return root, unwind, nil
}

func MarshalTerminal(outcome durable.Outcome, output []byte) ([]byte, error) {
	return marshal("terminal", &Terminal{Outcome: outcomeToProto(outcome), Output: output})
}

func UnmarshalTerminal(b []byte) (durable.Outcome, []byte, error) {
	pb := &Terminal{}
	if err := unmarshal("terminal", b, pb); err != nil {
		return 0, nil, err
	}
	return outcomeFromProto(pb.GetOutcome()), pb.GetOutput(), nil
}

func MarshalCancel(c *durable.CancelRequest) ([]byte, error) {
	return marshal("cancel request", &CancelRequest{Cause: c.Cause, At: ts(c.At)})
}

func UnmarshalCancel(b []byte) (*durable.CancelRequest, error) {
	pb := &CancelRequest{}
	if err := unmarshal("cancel request", b, pb); err != nil {
		return nil, err
	}
	return &durable.CancelRequest{Cause: pb.GetCause(), At: fromTS(pb.GetAt())}, nil
}

func failureRecordToProto(f *durable.FailureRecord) *FailureRecord {
	return &FailureRecord{
		StepId:  string(f.StepID),
		Phase:   phaseToProto(f.Phase),
		Attempt: f.Attempt,
		Message: f.Message,
		At:      ts(f.At),
		Kind:    kindToProto(f.Kind),
		Reason:  f.Reason,
	}
}

func failureRecordFromProto(f *FailureRecord) durable.FailureRecord {
	return durable.FailureRecord{
		StepID:  durable.StepID(f.GetStepId()),
		Phase:   phaseFromProto(f.GetPhase()),
		Attempt: f.GetAttempt(),
		Message: f.GetMessage(),
		At:      fromTS(f.GetAt()),
		Kind:    kindFromProto(f.GetKind()),
		Reason:  f.GetReason(),
	}
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func fromTS(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

func phaseToProto(p durable.Phase) Phase {
	switch p {
	case durable.PhaseForward:
		return Phase_PHASE_FORWARD
	case durable.PhaseUnwind:
		return Phase_PHASE_UNWIND
	case durable.PhaseDone:
		return Phase_PHASE_DONE
	default:
		return Phase_PHASE_UNSPECIFIED
	}
}

func phaseFromProto(p Phase) durable.Phase {
	switch p {
	case Phase_PHASE_FORWARD:
		return durable.PhaseForward
	case Phase_PHASE_UNWIND:
		return durable.PhaseUnwind
	case Phase_PHASE_DONE:
		return durable.PhaseDone
	default:
		return 0
	}
}

func outcomeToProto(o durable.Outcome) Outcome {
	switch o {
	case durable.OutcomeSuccess:
		return Outcome_OUTCOME_SUCCESS
	case durable.OutcomeFailure:
		return Outcome_OUTCOME_FAILURE
	default:
		return Outcome_OUTCOME_UNSPECIFIED
	}
}

func outcomeFromProto(o Outcome) durable.Outcome {
	switch o {
	case Outcome_OUTCOME_SUCCESS:
		return durable.OutcomeSuccess
	case Outcome_OUTCOME_FAILURE:
		return durable.OutcomeFailure
	default:
		return 0
	}
}

func opStatusToProto(s durable.OpStatus) OpStatus {
	switch s {
	case durable.OpUnresolved:
		return OpStatus_OP_STATUS_UNRESOLVED
	case durable.OpSucceeded:
		return OpStatus_OP_STATUS_SUCCEEDED
	case durable.OpFailed:
		return OpStatus_OP_STATUS_FAILED
	default:
		return OpStatus_OP_STATUS_UNSPECIFIED
	}
}

func opStatusFromProto(s OpStatus) durable.OpStatus {
	switch s {
	case OpStatus_OP_STATUS_UNRESOLVED:
		return durable.OpUnresolved
	case OpStatus_OP_STATUS_SUCCEEDED:
		return durable.OpSucceeded
	case OpStatus_OP_STATUS_FAILED:
		return durable.OpFailed
	default:
		return durable.OpNone
	}
}

func kindToProto(k durable.FailureKind) FailureKind {
	switch k {
	case durable.FailureKindUser:
		return FailureKind_FAILURE_KIND_USER
	case durable.FailureKindCanceled:
		return FailureKind_FAILURE_KIND_CANCELED
	default:
		return FailureKind_FAILURE_KIND_SYSTEM
	}
}

func kindFromProto(k FailureKind) durable.FailureKind {
	switch k {
	case FailureKind_FAILURE_KIND_USER:
		return durable.FailureKindUser
	case FailureKind_FAILURE_KIND_CANCELED:
		return durable.FailureKindCanceled
	default:
		return durable.FailureKindSystem
	}
}
