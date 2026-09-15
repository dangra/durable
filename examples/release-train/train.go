// The release-train pipeline: a parent run that ships each service by
// scheduling a child deploy and parking on it via AwaitRun.
package main

import (
	"context"

	"github.com/dangra/durable"
	"github.com/dangra/durable/examples/release-train/releasepb"
)

// shipper implements releasepb.ReleaseTrainHandlers and wires the
// train's steps to whichever deploy pipeline the current build binds;
// scheduling child runs goes through it so the handlers stay
// build-agnostic.
type shipper struct {
	w        *world
	schedule func(ctx context.Context, service, image string) (durable.RunID, error)
}

func (s *shipper) PlanRelease(ctx context.Context, inv releasepb.ReleaseTrainInvocation) error {
	s.w.logf("[train] planning release %s: web, api", inv.Input().GetImageTag())
	return nil
}

// ship schedules the service's deploy as a child run and parks until
// it lands, with CancelCascade: if the train is canceled while parked,
// the park resolves as canceled without this handler running again and
// the engine cancels the child with it.
func (s *shipper) ship(ctx context.Context, inv releasepb.ReleaseTrainInvocation, service string, store *durable.RunID) error {
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
	return durable.AwaitRun(id, durable.WithCancelCascade())
}

func (s *shipper) ShipWeb(ctx context.Context, inv releasepb.ReleaseTrainInvocation) error {
	return s.ship(ctx, inv, "web", &s.w.webDeployID)
}

func (s *shipper) ShipApi(ctx context.Context, inv releasepb.ReleaseTrainInvocation) error {
	return s.ship(ctx, inv, "api", &s.w.apiDeployID)
}

// announce wraps the release once every service shipped. In the demo it
// never runs: the train is frozen during ship-api, and a canceled run
// selects no new forward work — the wrap-up simply never happens.
func (s *shipper) Announce(ctx context.Context, inv releasepb.ReleaseTrainInvocation) (*releasepb.ReleaseTrain_Announce, error) {
	url := "https://releases.example.com/" + inv.Input().GetImageTag()
	s.w.mu.Lock()
	s.w.announced = true
	s.w.mu.Unlock()
	s.w.logf("[train] release %s announced: %s", inv.Input().GetImageTag(), url)
	return &releasepb.ReleaseTrain_Announce{ChangelogUrl: url}, nil
}
