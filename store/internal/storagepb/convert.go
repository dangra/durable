// Package storagepb holds the internal storage encoding of durable runs:
// the generated durable.storage.v1 component types and the conversion
// layer between them and the public Go structs. The wire format is private
// to this module's store implementations.
//
// A run is stored as components with distinct write cadences: RunMeta and
// the raw input (once), one OperationRecord row per step operation (once,
// at its resolution, carrying its own failure), the root Failure and
// CancelRequest (once each), and the small Cursor (every attempt). The
// terminality commit replaces all of them with one Terminal record that
// carries the identity, failure, and cancel request forward.
package storagepb

import (
	"fmt"
	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/driver"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// MarshalRunMeta encodes the write-once identity fields of rec. The input
// is not among them; stores keep it as a raw value of its own.
func MarshalRunMeta(rec *driver.RunRecord) ([]byte, error) {
	return marshal("run meta", &RunMeta{
		RunId:       string(rec.RunID),
		PipelineId:  string(rec.PipelineID),
		ResourceId:  string(rec.ResourceID),
		CreatedAt:   ts(rec.CreatedAt),
		Annotations: rec.Annotations,
	})
}

// UnmarshalRunMetaInto decodes identity fields into rec, leaving Input
// untouched.
func UnmarshalRunMetaInto(b []byte, rec *driver.RunRecord) error {
	pb := &RunMeta{}
	if err := unmarshal("run meta", b, pb); err != nil {
		return err
	}
	rec.RunID = kernel.RunID(pb.GetRunId())
	rec.PipelineID = kernel.PipelineID(pb.GetPipelineId())
	rec.ResourceID = kernel.ResourceID(pb.GetResourceId())
	rec.CreatedAt = fromTS(pb.GetCreatedAt())
	if len(pb.GetAnnotations()) > 0 {
		rec.Annotations = pb.GetAnnotations()
	}
	return nil
}

func MarshalCursor(c driver.Cursor) ([]byte, error) {
	return marshal("cursor", &Cursor{
		Phase:         phaseToProto(c.Phase),
		StepId:        string(c.StepID),
		Attempts:      c.Attempts,
		NextAttemptAt: ts(c.NextAttemptAt),
		LastError:     c.LastError,
		LastReason:    c.LastReason,
		LastErrorAt:   ts(c.LastErrorAt),
		UpdatedAt:     ts(c.UpdatedAt),
		Awaiting:      awaitToProto(c.Awaiting),
		Awaited:       wakeToProto(c.Awaited),
	})
}

func UnmarshalCursor(b []byte) (driver.Cursor, error) {
	pb := &Cursor{}
	if err := unmarshal("cursor", b, pb); err != nil {
		return driver.Cursor{}, err
	}
	return driver.Cursor{
		Phase:         phaseFromProto(pb.GetPhase()),
		StepID:        kernel.StepID(pb.GetStepId()),
		Attempts:      pb.GetAttempts(),
		NextAttemptAt: fromTS(pb.GetNextAttemptAt()),
		LastError:     pb.GetLastError(),
		LastReason:    pb.GetLastReason(),
		LastErrorAt:   fromTS(pb.GetLastErrorAt()),
		UpdatedAt:     fromTS(pb.GetUpdatedAt()),
		Awaiting:      awaitFromProto(pb.GetAwaiting()),
		Awaited:       wakeFromProto(pb.GetAwaited()),
	}, nil
}

func awaitToProto(a *kernel.Await) *Await {
	if a == nil {
		return nil
	}
	return &Await{
		Mode:     awaitModeToProto(a.Mode),
		RunIds:   runIDsToProto(a.Targets),
		Deadline: ts(a.Deadline),
	}
}

func awaitFromProto(pb *Await) *kernel.Await {
	if pb == nil {
		return nil
	}
	return &kernel.Await{
		Mode:     awaitModeFromProto(pb.GetMode()),
		Targets:  runIDsFromProto(pb.GetRunIds()),
		Deadline: fromTS(pb.GetDeadline()),
	}
}

func wakeToProto(w *kernel.Wake) *Wake {
	if w == nil {
		return nil
	}
	return &Wake{
		Targets: runIDsToProto(w.Targets),
		Done:    runIDsToProto(w.Done),
		Expired: w.Expired,
	}
}

func wakeFromProto(pb *Wake) *kernel.Wake {
	if pb == nil {
		return nil
	}
	return &kernel.Wake{
		Targets: runIDsFromProto(pb.GetTargets()),
		Done:    runIDsFromProto(pb.GetDone()),
		Expired: pb.GetExpired(),
	}
}

func runIDsToProto(ids []kernel.RunID) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func runIDsFromProto(ids []string) []kernel.RunID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]kernel.RunID, len(ids))
	for i, id := range ids {
		out[i] = kernel.RunID(id)
	}
	return out
}

func awaitModeToProto(m kernel.AwaitMode) AwaitMode {
	switch m {
	case kernel.AwaitModeAll:
		return AwaitMode_AWAIT_MODE_ALL
	case kernel.AwaitModeAny:
		return AwaitMode_AWAIT_MODE_ANY
	default:
		return AwaitMode_AWAIT_MODE_UNSPECIFIED
	}
}

func awaitModeFromProto(m AwaitMode) kernel.AwaitMode {
	switch m {
	case AwaitMode_AWAIT_MODE_ANY:
		return kernel.AwaitModeAny
	default:
		return kernel.AwaitModeAll
	}
}

// MarshalOperationRecord encodes one operation's facts; the row key names
// the step and phase. A store keeping a large state beside the row
// passes the record without it.
func MarshalOperationRecord(op *driver.OperationRecord) ([]byte, error) {
	return marshal("operation record", operationRecordToProto(op))
}

func UnmarshalOperationRecord(b []byte) (driver.OperationRecord, error) {
	pb := &OperationRecord{}
	if err := unmarshal("operation record", b, pb); err != nil {
		return driver.OperationRecord{}, err
	}
	return operationRecordFromProto(pb), nil
}

func operationRecordToProto(op *driver.OperationRecord) *OperationRecord {
	pb := &OperationRecord{
		Status:   opStatusToProto(op.Status),
		Attempts: op.Attempts,
		State:    op.State,
		Order:    op.Order,
	}
	if op.Failure != nil {
		pb.Failure = failureRecordToProto(op.Failure)
	}
	return pb
}

func operationRecordFromProto(pb *OperationRecord) driver.OperationRecord {
	op := driver.OperationRecord{
		Status:   opStatusFromProto(pb.GetStatus()),
		Attempts: pb.GetAttempts(),
		State:    pb.GetState(),
		Order:    pb.GetOrder(),
	}
	if pb.GetFailure() != nil {
		f := failureRecordFromProto(pb.GetFailure())
		op.Failure = &f
	}
	return op
}

// MarshalFailureRecord encodes one failure record on its own: the run's
// write-once run failure row.
func MarshalFailureRecord(f kernel.Failure) ([]byte, error) {
	return marshal("failure record", failureRecordToProto(&f))
}

func UnmarshalFailureRecord(b []byte) (kernel.Failure, error) {
	pb := &FailureRecord{}
	if err := unmarshal("failure record", b, pb); err != nil {
		return kernel.Failure{}, err
	}
	return failureRecordFromProto(pb), nil
}

// MarshalTerminal encodes the terminal stage of rec: identity, outcome,
// output, phase, the commit time taken from rec.UpdatedAt, and the
// failed unwind operations found in rec.Steps. outputBeside marks an
// output the store keeps beside the row; rec.Output is then left out. rec must carry an
// Outcome; the caller compacts it (driver.RunRecord.CompactTerminal)
// first, so that what is encoded is what a read returns.
func MarshalTerminal(rec *driver.RunRecord, outputBeside bool) ([]byte, error) {
	if rec.Outcome == nil {
		return nil, fmt.Errorf("storagepb: encoding terminal: record has no outcome")
	}
	pb := &Terminal{
		Outcome:      outcomeToProto(*rec.Outcome),
		OutputBeside: outputBeside,
		RunId:        string(rec.RunID),
		PipelineId:   string(rec.PipelineID),
		ResourceId:   string(rec.ResourceID),
		CreatedAt:    ts(rec.CreatedAt),
		Annotations:  rec.Annotations,
		Phase:        phaseToProto(rec.Phase),
		CommittedAt:  ts(rec.UpdatedAt),
	}
	if !outputBeside {
		pb.Output = rec.Output
	}
	for id, sr := range rec.Steps {
		if sr.Unwind.Status != driver.OpFailed {
			continue
		}
		if pb.FailedUnwinds == nil {
			pb.FailedUnwinds = make(map[string]*OperationRecord)
		}
		pb.FailedUnwinds[string(id)] = operationRecordToProto(&sr.Unwind)
	}
	if rec.Failure != nil {
		pb.Failure = failureRecordToProto(rec.Failure)
	}
	if rec.Cancel != nil {
		pb.Cancel = &CancelRequest{Cause: rec.Cancel.Cause, At: ts(rec.Cancel.At)}
	}
	return marshal("terminal", pb)
}

// UnmarshalTerminalInto decodes a terminal record into rec: identity,
// outcome, output, phase, the commit time as UpdatedAt, Failure, Cancel,
// and the failed unwind operations as Steps entries, reporting whether
// the store keeps the output beside the row. It leaves the fields the
// terminal stage does not carry untouched.
func UnmarshalTerminalInto(b []byte, rec *driver.RunRecord) (outputBeside bool, err error) {
	pb := &Terminal{}
	if err := unmarshal("terminal", b, pb); err != nil {
		return false, err
	}
	rec.RunID = kernel.RunID(pb.GetRunId())
	rec.PipelineID = kernel.PipelineID(pb.GetPipelineId())
	rec.ResourceID = kernel.ResourceID(pb.GetResourceId())
	rec.CreatedAt = fromTS(pb.GetCreatedAt())
	if len(pb.GetAnnotations()) > 0 {
		rec.Annotations = pb.GetAnnotations()
	}
	rec.Phase = phaseFromProto(pb.GetPhase())
	rec.UpdatedAt = fromTS(pb.GetCommittedAt())
	oc := outcomeFromProto(pb.GetOutcome())
	rec.Outcome = &oc
	rec.Output = pb.GetOutput()
	for id, op := range pb.GetFailedUnwinds() {
		rec.Step(kernel.StepID(id)).Unwind = operationRecordFromProto(op)
	}
	if pb.GetFailure() != nil {
		f := failureRecordFromProto(pb.GetFailure())
		rec.Failure = &f
	}
	if c := pb.GetCancel(); c != nil {
		rec.Cancel = &driver.CancelRequest{Cause: c.GetCause(), At: fromTS(c.GetAt())}
	}
	return pb.GetOutputBeside(), nil
}

func MarshalCancel(c *driver.CancelRequest) ([]byte, error) {
	return marshal("cancel request", &CancelRequest{Cause: c.Cause, At: ts(c.At)})
}

func UnmarshalCancel(b []byte) (*driver.CancelRequest, error) {
	pb := &CancelRequest{}
	if err := unmarshal("cancel request", b, pb); err != nil {
		return nil, err
	}
	return &driver.CancelRequest{Cause: pb.GetCause(), At: fromTS(pb.GetAt())}, nil
}

func failureRecordToProto(f *kernel.Failure) *FailureRecord {
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

func failureRecordFromProto(f *FailureRecord) kernel.Failure {
	return kernel.Failure{
		StepID:  kernel.StepID(f.GetStepId()),
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

func phaseToProto(p kernel.Phase) Phase {
	switch p {
	case kernel.PhaseForward:
		return Phase_PHASE_FORWARD
	case kernel.PhaseUnwind:
		return Phase_PHASE_UNWIND
	case kernel.PhaseDone:
		return Phase_PHASE_DONE
	default:
		return Phase_PHASE_UNSPECIFIED
	}
}

func phaseFromProto(p Phase) kernel.Phase {
	switch p {
	case Phase_PHASE_FORWARD:
		return kernel.PhaseForward
	case Phase_PHASE_UNWIND:
		return kernel.PhaseUnwind
	case Phase_PHASE_DONE:
		return kernel.PhaseDone
	default:
		return 0
	}
}

func outcomeToProto(o kernel.Outcome) Outcome {
	switch o {
	case kernel.OutcomeSuccess:
		return Outcome_OUTCOME_SUCCESS
	case kernel.OutcomeFailure:
		return Outcome_OUTCOME_FAILURE
	default:
		return Outcome_OUTCOME_UNSPECIFIED
	}
}

func outcomeFromProto(o Outcome) kernel.Outcome {
	switch o {
	case Outcome_OUTCOME_SUCCESS:
		return kernel.OutcomeSuccess
	case Outcome_OUTCOME_FAILURE:
		return kernel.OutcomeFailure
	default:
		return 0
	}
}

func opStatusToProto(s driver.OpStatus) OpStatus {
	switch s {
	case driver.OpUnresolved:
		return OpStatus_OP_STATUS_UNRESOLVED
	case driver.OpSucceeded:
		return OpStatus_OP_STATUS_SUCCEEDED
	case driver.OpFailed:
		return OpStatus_OP_STATUS_FAILED
	default:
		return OpStatus_OP_STATUS_UNSPECIFIED
	}
}

func opStatusFromProto(s OpStatus) driver.OpStatus {
	switch s {
	case OpStatus_OP_STATUS_UNRESOLVED:
		return driver.OpUnresolved
	case OpStatus_OP_STATUS_SUCCEEDED:
		return driver.OpSucceeded
	case OpStatus_OP_STATUS_FAILED:
		return driver.OpFailed
	default:
		return driver.OpNone
	}
}

func kindToProto(k kernel.FailureKind) FailureKind {
	switch k {
	case kernel.FailureKindUser:
		return FailureKind_FAILURE_KIND_USER
	case kernel.FailureKindCanceled:
		return FailureKind_FAILURE_KIND_CANCELED
	default:
		return FailureKind_FAILURE_KIND_SYSTEM
	}
}

func kindFromProto(k FailureKind) kernel.FailureKind {
	switch k {
	case FailureKind_FAILURE_KIND_USER:
		return kernel.FailureKindUser
	case FailureKind_FAILURE_KIND_CANCELED:
		return kernel.FailureKindCanceled
	default:
		return kernel.FailureKindSystem
	}
}
