package kernel

import (
	"slices"
	"testing"
	"time"
)

func TestWakePending(t *testing.T) {
	ids := []RunID{"a", "b", "c"}
	cases := []struct {
		name string
		w    Wake
		want []RunID
	}{
		{"none done", Wake{Targets: ids}, ids},
		{"some done", Wake{Targets: ids, Done: []RunID{"b"}}, []RunID{"a", "c"}},
		{"all done", Wake{Targets: ids, Done: ids}, nil},
		{"done not in targets", Wake{Targets: ids, Done: []RunID{"z"}}, ids},
		{"empty", Wake{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.w.Pending(); !slices.Equal(got, tc.want) {
				t.Fatalf("Pending() = %v, want %v", got, tc.want)
			}
		})
	}
	// Pending never aliases Targets.
	w := Wake{Targets: []RunID{"a"}}
	w.Pending()[0] = "mutated"
	if w.Targets[0] != "a" {
		t.Fatal("Pending aliased Targets")
	}
}

func TestClonesAreDeep(t *testing.T) {
	var nilAwait *Await
	var nilWake *Wake
	if nilAwait.Clone() != nil || nilWake.Clone() != nil {
		t.Fatal("Clone of nil must be nil")
	}

	a := &Await{Mode: AwaitModeAny, Targets: []RunID{"a", "b"}, Deadline: time.Unix(1, 0)}
	ac := a.Clone()
	ac.Targets[0] = "z"
	if a.Targets[0] != "a" || ac.Mode != AwaitModeAny || !ac.Deadline.Equal(a.Deadline) {
		t.Fatalf("Await.Clone shares or drops state: %+v vs %+v", a, ac)
	}

	w := &Wake{Targets: []RunID{"a", "b"}, Done: []RunID{"a"}, Expired: true}
	wc := w.Clone()
	wc.Targets[0], wc.Done[0] = "z", "z"
	if w.Targets[0] != "a" || w.Done[0] != "a" || !wc.Expired {
		t.Fatalf("Wake.Clone shares or drops state: %+v vs %+v", w, wc)
	}
}

func TestStrings(t *testing.T) {
	checks := []struct{ got, want string }{
		{PhaseForward.String(), "forward"},
		{PhaseUnwind.String(), "unwind"},
		{PhaseDone.String(), "done"},
		{Phase(0).String(), "unknown"},
		{AwaitModeAll.String(), "all"},
		{AwaitModeAny.String(), "any"},
		{AwaitMode(0).String(), "unknown"},
		{OutcomeSuccess.String(), "success"},
		{OutcomeFailure.String(), "failure"},
		{Outcome(0).String(), "unknown"},
		{FailureKindSystem.String(), "system"},
		{FailureKindUser.String(), "user"},
		{FailureKindCanceled.String(), "canceled"},
		{FailureKind(99).String(), "system"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
	if StepID("s").ID() != "s" {
		t.Error("StepID.ID must return itself")
	}
}
