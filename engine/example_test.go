package engine_test

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
)

// ExampleWithMiddleware installs a logging middleware around every
// operation, in the style of net/http middleware.
func ExampleWithMiddleware() {
	logging := func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
			state, err := next(ctx, inv)
			fmt.Printf("%s %s attempt=%d\n", inv.Phase(), inv.StepID(), inv.Attempt())
			return state, err
		}
	}

	eng := engine.New(durabletest.NewMemStore(), engine.WithMiddleware(logging))
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "greeter",
		Steps: []engine.StepConfig{{
			ID: "hello/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		}},
	})
	pipeline, err := def.Bind(eng)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	run, _, err := pipeline.Schedule(ctx, "resource-1", nil)
	if err != nil {
		panic(err)
	}
	result, err := run.Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("succeeded:", result.Succeeded())
	// Output:
	// forward hello/v1 attempt=1
	// succeeded: true
}
