package engine

import (
	"context"

	"github.com/dangra/durable"
)

// GetRun returns a handle to any Run the engine's store holds, whatever
// pipeline it belongs to; durable.ErrRunNotFound if none exists. It is
// the lookup for callers holding only a RunID — an API client polling an
// operation id, say. The handle is the untyped one: Status reports the
// PipelineID, so a caller that needs the typed Output routes there
// (provision.GetRun(ctx, run.ID())).
func (e *Engine) GetRun(ctx context.Context, id durable.RunID) (Run, error) {
	if _, err := e.store.GetRun(ctx, id); err != nil {
		return Run{}, err
	}
	return Run{id: id, engine: e}, nil
}
