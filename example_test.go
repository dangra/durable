package durable_test

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
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

	engine := durable.NewEngine(durabletest.NewMemStore(), durable.WithMiddleware(logging))
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "greeter",
		Steps: []durable.StepConfig{{
			ID: "hello/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		}},
	})
	pipeline, err := def.Bind(engine)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		panic(err)
	}
	defer engine.Stop(ctx)

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
