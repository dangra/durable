// Package storagepb holds the internal storage encoding of durable run
// records: the generated durable.storage.v1 types and the conversion layer
// between them and the public Go structs. The wire format is private to
// this module's store implementations.
package storagepb

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dangra/durable"
)

// MarshalRunRecord encodes a record for storage.
func MarshalRunRecord(rec *durable.RunRecord) ([]byte, error) {
	b, err := proto.Marshal(toProto(rec))
	if err != nil {
		return nil, fmt.Errorf("storagepb: encoding run record: %w", err)
	}
	return b, nil
}

// UnmarshalRunRecord decodes a stored record.
func UnmarshalRunRecord(b []byte) (*durable.RunRecord, error) {
	pb := &RunRecord{}
	if err := proto.Unmarshal(b, pb); err != nil {
		return nil, fmt.Errorf("storagepb: decoding run record: %w", err)
	}
	return fromProto(pb), nil
}

func toProto(rec *durable.RunRecord) *RunRecord {
	pb := &RunRecord{
		RunId:         string(rec.RunID),
		PipelineId:    string(rec.PipelineID),
		ResourceId:    string(rec.ResourceID),
		Input:         rec.Input,
		Phase:         phaseToProto(rec.Phase),
		Steps:         make(map[string]*StepRecord, len(rec.Steps)),
		RootFailure:   failureToProto(rec.RootFailure),
		Output:        rec.Output,
		NextAttemptAt: ts(rec.NextAttemptAt),
		LastError:     rec.LastError,
		LastReason:    rec.LastReason,
		LastErrorAt:   ts(rec.LastErrorAt),
		CreatedAt:     ts(rec.CreatedAt),
		UpdatedAt:     ts(rec.UpdatedAt),
		SlotGroup:     rec.Group,
	}
	for id, sr := range rec.Steps {
		pb.Steps[string(id)] = &StepRecord{
			ForwardStatus:   opStatusToProto(sr.ForwardStatus),
			ForwardAttempts: sr.ForwardAttempts,
			State:           sr.State,
			UnwindStatus:    opStatusToProto(sr.UnwindStatus),
			UnwindAttempts:  sr.UnwindAttempts,
		}
	}
	for _, uf := range rec.UnwindFailures {
		f := uf.FailureRecord
		pb.UnwindFailures = append(pb.UnwindFailures, failureRecordToProto(&f))
	}
	if rec.Outcome != nil {
		pb.Outcome = outcomeToProto(*rec.Outcome).Enum()
	}
	if rec.Cancel != nil {
		pb.Cancel = &CancelRequest{Cause: rec.Cancel.Cause, At: ts(rec.Cancel.At)}
	}
	return pb
}

func fromProto(pb *RunRecord) *durable.RunRecord {
	rec := &durable.RunRecord{
		RunID:         durable.RunID(pb.GetRunId()),
		PipelineID:    durable.PipelineID(pb.GetPipelineId()),
		ResourceID:    durable.ResourceID(pb.GetResourceId()),
		Input:         pb.GetInput(),
		Phase:         phaseFromProto(pb.GetPhase()),
		Output:        pb.GetOutput(),
		NextAttemptAt: fromTS(pb.GetNextAttemptAt()),
		LastError:     pb.GetLastError(),
		LastReason:    pb.GetLastReason(),
		LastErrorAt:   fromTS(pb.GetLastErrorAt()),
		CreatedAt:     fromTS(pb.GetCreatedAt()),
		UpdatedAt:     fromTS(pb.GetUpdatedAt()),
		Group:         pb.GetSlotGroup(),
	}
	if len(pb.GetSteps()) > 0 {
		rec.Steps = make(map[durable.StepID]*durable.StepRecord, len(pb.GetSteps()))
		for id, sr := range pb.GetSteps() {
			rec.Steps[durable.StepID(id)] = &durable.StepRecord{
				ForwardStatus:   opStatusFromProto(sr.GetForwardStatus()),
				ForwardAttempts: sr.GetForwardAttempts(),
				State:           sr.GetState(),
				UnwindStatus:    opStatusFromProto(sr.GetUnwindStatus()),
				UnwindAttempts:  sr.GetUnwindAttempts(),
			}
		}
	}
	if rf := pb.GetRootFailure(); rf != nil {
		rec.RootFailure = &durable.RootFailure{FailureRecord: failureRecordFromProto(rf)}
	}
	for _, f := range pb.GetUnwindFailures() {
		rec.UnwindFailures = append(rec.UnwindFailures, durable.UnwindFailure{FailureRecord: failureRecordFromProto(f)})
	}
	if pb.Outcome != nil {
		oc := outcomeFromProto(pb.GetOutcome())
		rec.Outcome = &oc
	}
	if c := pb.GetCancel(); c != nil {
		rec.Cancel = &durable.CancelRequest{Cause: c.GetCause(), At: fromTS(c.GetAt())}
	}
	return rec
}

func failureToProto(rf *durable.RootFailure) *FailureRecord {
	if rf == nil {
		return nil
	}
	f := rf.FailureRecord
	return failureRecordToProto(&f)
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
