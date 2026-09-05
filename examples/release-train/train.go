// The release-train pipeline: a parent run that ships each service by
// scheduling a child deploy and parking on it via AwaitRun.
package main

import (
	"context"
	"errors"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/examples/release-train/releasepb"
)

// shipper wires the train's steps to whichever deploy pipeline the
// current build binds; scheduling and canceling child runs goes through
// it so the train handlers stay build-agnostic.
type shipper struct {
	w        *world
	schedule func(ctx context.Context, service, image string) (durable.RunID, error)
	cancel   func(ctx context.Context, id durable.RunID, cause string) error
}

func (s *shipper) plan(ctx context.Context, inv releasepb.PlanReleaseInvocation) error {
	s.w.logf("[train] planning release %s: web, api", inv.Input().GetImageTag())
	return nil
}

// shipInvocation is the slice of the generated invocation types the
// ship step logic needs; ShipWebInvocation and ShipApiInvocation both
// satisfy it.
type shipInvocation interface {
	Input() *releasepb.ReleaseTrainInput
	AwaitedRunID() (durable.RunID, bool)
	CancelRequested() bool
}

func (s *shipper) ship(ctx context.Context, inv shipInvocation, service string, store *durable.RunID) error {
	if inv.CancelRequested() {
		s.w.logf("[train] release frozen — canceling %s deploy", service)
		s.w.mu.Lock()
		id := *store
		s.w.mu.Unlock()
		if id != "" {
			// Best-effort: the child may have finished (or been reaped)
			// between the freeze and this wake; that is not a failure.
			err := s.cancel(ctx, id, "release frozen")
			if err != nil && !errors.Is(err, engine.ErrRunTerminal) && !errors.Is(err, engine.ErrRunNotFound) {
				return err
			}
		}
		return nil // resolve; the engine applies the cancel and unwinds
	}
	if _, woken := inv.AwaitedRunID(); woken {
		s.w.logf("[train] %s shipped", service)
		return nil
	}
	id, err := s.schedule(ctx, service, inv.Input().GetImageTag())
	if err != nil {
		return err
	}
	s.w.mu.Lock()
	*store = id
	s.w.mu.Unlock()
	s.w.logf("[train] %s deploy scheduled; parking until it lands", service)
	return durable.AwaitRun(id)
}

func (s *shipper) shipWeb(ctx context.Context, inv releasepb.ShipWebInvocation) error {
	return s.ship(ctx, inv, "web", &s.w.webDeployID)
}

func (s *shipper) shipApi(ctx context.Context, inv releasepb.ShipApiInvocation) error {
	return s.ship(ctx, inv, "api", &s.w.apiDeployID)
}

// announce wraps the release once every service shipped. In the demo it
// never runs: the train is frozen during ship-api, and a canceled run
// selects no new forward work — the wrap-up simply never happens.
func (s *shipper) announce(ctx context.Context, inv releasepb.AnnounceInvocation) (*releasepb.Announce, error) {
	url := "https://releases.example.com/" + inv.Input().GetImageTag()
	s.w.mu.Lock()
	s.w.announced = true
	s.w.mu.Unlock()
	s.w.logf("[train] release %s announced: %s", inv.Input().GetImageTag(), url)
	return &releasepb.Announce{ChangelogUrl: url}, nil
}
