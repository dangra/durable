package bboltstore_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dangra/durable/storedriver"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/durabletest"
)

// canonRecord is a RunRecord flattened for comparison: times as
// nanoseconds (a proto round trip drops monotonic clocks and locations),
// empty byte slices normalized to nil, and the Steps map as a sorted
// slice.
type canonRecord struct {
	RunID, PipelineID, ResourceID, Group string
	Annotations                          map[string]string
	Input                                []byte
	Phase                                durable.Phase
	Steps                                []canonStep
	Root                                 *durable.RootFailure
	UnwindFailures                       []durable.UnwindFailure
	Output                               []byte
	Outcome                              *durable.Outcome
	NextAttemptAt, LastErrorAt           int64
	Awaiting                             *canonAwait
	Awaited                              *storedriver.Wake
	LastError, LastReason                string
	Cancel                               *canonCancel
	CreatedAt, UpdatedAt                 int64
}

type canonStep struct {
	ID     string
	Record storedriver.StepRecord
}

type canonCancel struct {
	Cause string
	At    int64
}

type canonAwait struct {
	Mode     storedriver.AwaitMode
	Targets  []durable.RunID
	Deadline int64
}

func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func normBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func canonFailure(f durable.FailureRecord) durable.FailureRecord {
	f.At = time.Unix(0, nanos(f.At)).UTC()
	return f
}

func canonicalize(rec *storedriver.RunRecord) *canonRecord {
	if rec == nil {
		return nil
	}
	c := &canonRecord{
		RunID: string(rec.RunID), PipelineID: string(rec.PipelineID),
		ResourceID: string(rec.ResourceID), Group: rec.SlotGroup(),
		Input: normBytes(rec.Input), Phase: rec.Phase,
		Annotations:   rec.Annotations,
		Output:        normBytes(rec.Output),
		NextAttemptAt: nanos(rec.NextAttemptAt), LastErrorAt: nanos(rec.LastErrorAt),
		Awaited:   rec.Awaited.Clone(),
		LastError: rec.LastError, LastReason: rec.LastReason,
		CreatedAt: nanos(rec.CreatedAt), UpdatedAt: nanos(rec.UpdatedAt),
	}
	if a := rec.Awaiting; a != nil {
		c.Awaiting = &canonAwait{Mode: a.Mode, Targets: append([]durable.RunID(nil), a.Targets...), Deadline: nanos(a.Deadline)}
	}
	if rec.Outcome != nil {
		oc := *rec.Outcome
		c.Outcome = &oc
	}
	if rec.RootFailure != nil {
		c.Root = &durable.RootFailure{FailureRecord: canonFailure(rec.RootFailure.FailureRecord)}
	}
	for _, uf := range rec.UnwindFailures {
		c.UnwindFailures = append(c.UnwindFailures, durable.UnwindFailure{FailureRecord: canonFailure(uf.FailureRecord)})
	}
	if rec.Cancel != nil {
		c.Cancel = &canonCancel{Cause: rec.Cancel.Cause, At: nanos(rec.Cancel.At)}
	}
	for id, sr := range rec.Steps {
		s := *sr
		s.State = normBytes(s.State)
		c.Steps = append(c.Steps, canonStep{ID: string(id), Record: s})
	}
	sort.Slice(c.Steps, func(i, j int) bool { return c.Steps[i].ID < c.Steps[j].ID })
	return c
}

// FuzzStoreContract drives identical operation sequences against the
// bbolt store and durabletest.MemStore — the executable specification of
// the Store contract — and fails on any observable divergence: results,
// errors, record contents, list membership, or ordering.
func FuzzStoreContract(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 3, 0, 0, 9, 1, 8, 4, 0, 2, 0, 5, 0, 3, 0})
	f.Add([]byte{0, 5, 0, 13, 1, 21, 1, 42, 2, 5, 3, 5, 4, 13, 5, 200})
	f.Fuzz(func(t *testing.T, data []byte) {
		bs, err := bboltstore.Open(filepath.Join(t.TempDir(), "fuzz.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		ms := durabletest.NewMemStore()
		ctx := context.Background()

		runIDs := []durable.RunID{
			"01RUN000000000000000000001", "01RUN000000000000000000002",
			"01RUN000000000000000000003", "01RUN000000000000000000004",
		}
		pipelines := []durable.PipelineID{"p1", "p2"}
		resources := []durable.ResourceID{"r1", "r2"}
		groups := []string{"", "group/g1"}
		steps := []durable.StepID{"s1/v1", "s2/v1"}
		outcomes := []durable.Outcome{durable.OutcomeSuccess, durable.OutcomeFailure}

		base := time.Unix(1_700_000_000, 0).UTC()
		tick := 0
		next := func() time.Time { tick++; return base.Add(time.Duration(tick) * time.Millisecond) }

		r := bytes.NewReader(data)
		byteOr0 := func() byte {
			b, err := r.ReadByte()
			if err != nil {
				return 0
			}
			return b
		}

		mustEqual := func(what string, a, b any) {
			t.Helper()
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("%s diverged:\nbbolt: %+v\nmodel: %+v", what, a, b)
			}
		}
		compareRun := func(id durable.RunID) {
			t.Helper()
			br, berr := bs.GetRun(ctx, id)
			mr, merr := ms.GetRun(ctx, id)
			mustEqual(fmt.Sprintf("GetRun(%s) error", id), berr, merr)
			mustEqual(fmt.Sprintf("GetRun(%s)", id), canonicalize(br), canonicalize(mr))
		}

		for op := byteOr0(); r.Len() > 0; op = byteOr0() {
			arg := byteOr0()
			id := runIDs[int(arg)%len(runIDs)]
			switch op % 6 {
			case 0: // CreateRun
				now := next()
				rec := &storedriver.RunRecord{
					RunID:      id,
					PipelineID: pipelines[int(arg/4)%len(pipelines)],
					ResourceID: resources[int(arg/8)%len(resources)],
					Group:      groups[int(arg/16)%len(groups)],
					Input:      normBytes([]byte{arg}),
					Phase:      durable.PhaseForward,
					Steps:      map[durable.StepID]*storedriver.StepRecord{},
					CreatedAt:  now,
					UpdatedAt:  now,
				}
				if arg%3 == 0 {
					rec.NextAttemptAt = now.Add(time.Hour)
				}
				if arg%2 == 0 {
					rec.Annotations = map[string]string{"traceparent": fmt.Sprintf("00-%02x", arg), "tenant": "t1"}
				}
				// RunID freshness is a documented CreateRun precondition:
				// reusing any existing id — terminal, or live under a
				// different slot — is undefined. The one in-contract
				// re-create is the dedup probe: a live id occupying the
				// same (group, resource) slot.
				if prev, err := ms.GetRun(ctx, id); err == nil {
					if prev.Terminal() || prev.SlotGroup() != rec.SlotGroup() || prev.ResourceID != rec.ResourceID {
						continue
					}
				}
				bex, bcreated, berr := bs.CreateRun(ctx, rec)
				mex, mcreated, merr := ms.CreateRun(ctx, rec)
				mustEqual("CreateRun error", berr, merr)
				mustEqual("CreateRun created", bcreated, mcreated)
				mustEqual("CreateRun existing", canonicalize(bex), canonicalize(mex))
			case 1: // ApplyTransition
				now := next()
				phase := durable.PhaseForward
				if arg%2 == 1 {
					phase = durable.PhaseUnwind
				}
				tr := storedriver.Transition{Cursor: storedriver.Cursor{
					Phase:     phase,
					UpdatedAt: now,
				}}
				if arg%3 == 0 {
					tr.Cursor.StepID = steps[int(arg/3)%len(steps)]
					tr.Cursor.Attempts = uint64(arg%5) + 1
					tr.Cursor.LastError = "boom"
					tr.Cursor.LastReason = "reason"
					tr.Cursor.LastErrorAt = now
					tr.Cursor.NextAttemptAt = now.Add(time.Minute)
				}
				if arg%4 == 0 {
					tr.Steps = []storedriver.StepWrite{{
						StepID: steps[int(arg/2)%len(steps)],
						Record: storedriver.StepRecord{
							ForwardStatus:   storedriver.OpStatus(arg % 4),
							ForwardAttempts: uint64(arg % 7),
							State:           normBytes([]byte{arg, arg}),
							UnwindStatus:    storedriver.OpStatus((arg / 4) % 4),
							UnwindAttempts:  uint64(arg % 3),
						},
					}}
				}
				if arg%5 == 0 {
					tr.RootFailure = &durable.RootFailure{FailureRecord: durable.FailureRecord{
						StepID: steps[0], Phase: durable.PhaseForward,
						Attempt: 1, Message: "root", At: now,
						Kind: durable.FailureKindUser, Reason: "why",
					}}
				}
				if arg%6 == 0 {
					tr.UnwindFailure = &durable.UnwindFailure{FailureRecord: durable.FailureRecord{
						StepID: steps[1], Phase: durable.PhaseUnwind,
						Attempt: 2, Message: "uw", At: now,
					}}
				}
				if arg%7 == 0 {
					oc := outcomes[int(arg)%len(outcomes)]
					tr.Outcome = &oc
					tr.Output = normBytes([]byte{1, 2, arg})
				}
				// Uphold the engine's delta contract: every previously
				// unresolved operation must be covered by the cursor or
				// an explicit step write (engine.apply flushes them the
				// same way). Raw sequences that abandon an in-flight
				// reservation are outside the Store contract.
				if prev, err := ms.GetRun(ctx, id); err == nil {
					for sid, sr := range prev.Steps {
						if sr.ForwardStatus != storedriver.OpUnresolved && sr.UnwindStatus != storedriver.OpUnresolved {
							continue
						}
						if sid == tr.Cursor.StepID {
							continue
						}
						covered := false
						for _, sw := range tr.Steps {
							if sw.StepID == sid {
								covered = true
								break
							}
						}
						if !covered {
							tr.Steps = append(tr.Steps, storedriver.StepWrite{StepID: sid, Record: *sr})
						}
					}
				}
				berr := bs.ApplyTransition(ctx, id, tr)
				merr := ms.ApplyTransition(ctx, id, tr)
				mustEqual("ApplyTransition error", berr, merr)
				compareRun(id)
			case 2: // RequestCancel
				req := storedriver.CancelRequest{Cause: "cause", At: next()}
				bacc, berr := bs.RequestCancel(ctx, id, req)
				macc, merr := ms.RequestCancel(ctx, id, req)
				mustEqual("RequestCancel error", berr, merr)
				mustEqual("RequestCancel accepted", bacc, macc)
			case 3: // GetRun
				compareRun(id)
			case 4: // ListRuns: membership and CreatedAt order
				p := pipelines[int(arg)%len(pipelines)]
				res := resources[int(arg/2)%len(resources)]
				brs, berr := bs.ListRuns(ctx, p, res)
				mrs, merr := ms.ListRuns(ctx, p, res)
				mustEqual("ListRuns error", berr, merr)
				var bc, mc []*canonRecord
				for _, rr := range brs {
					bc = append(bc, canonicalize(rr))
				}
				for _, rr := range mrs {
					mc = append(mc, canonicalize(rr))
				}
				mustEqual("ListRuns", bc, mc)
			case 5: // ReapTerminal with a limit covering everything
				before := base.Add(time.Duration(int(arg)) * 10 * time.Millisecond)
				bn, berr := bs.ReapTerminal(ctx, before, 1000)
				mn, merr := ms.ReapTerminal(ctx, before, 1000)
				mustEqual("ReapTerminal error", berr, merr)
				mustEqual("ReapTerminal count", bn, mn)
			}
		}

		// Final sweep: every run and the nonterminal set must agree.
		for _, id := range runIDs {
			compareRun(id)
		}
		bn, berr := bs.ListNonterminal(ctx)
		mn, merr := ms.ListNonterminal(ctx)
		mustEqual("ListNonterminal error", berr, merr)
		key := func(rs []*storedriver.RunRecord) map[string]*canonRecord {
			out := map[string]*canonRecord{}
			for _, rr := range rs {
				out[string(rr.RunID)] = canonicalize(rr)
			}
			return out
		}
		mustEqual("ListNonterminal", key(bn), key(mn))
	})
}
