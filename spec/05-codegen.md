# Protobuf and code generation

Part of the [`durable` specification](README.md).

## Protobuf declarations

Conceptually:

```proto
syntax = "proto3";

package durable.v1;

import "google/protobuf/descriptor.proto";

message StepOptions {
  string id = 1;
  bool unwind = 2;
  bool retired = 3;
  string concurrency_class = 4;
}

message PipelineOptions {
  string id = 1;
  string input = 2;
  string output = 3;
  string concurrency_class = 6;
  repeated string mutexes = 7;
  string failure_output = 8;
  string run_class = 9;
  reserved 5;
  reserved "exclusion_group";
}

extend google.protobuf.MessageOptions {
  StepOptions step = <globally-allocated-extension-number>;
  PipelineOptions pipeline = <globally-allocated-extension-number>;
}
```

Published protobuf extensions MUST use globally allocated extension numbers.

---

## Code generation

`protoc-gen-durable` generates, per pipeline:

- typed Step references,
- one Invocation alias, `XxxInvocation = durable.TypedInvocation[*XxxInput]`
  (`durable.NoInput` for an Input-less pipeline), with a
  `NewXxxInvocation(core)` constructor for engine-free handler tests,
- one handler interface, `XxxHandlers`: a method per Step named after
  the Step, `Unwind<Step>` for each Step that unwinds, and `ReduceOutput` /
  `ReduceFailure` when the pipeline declares outputs,
- the Pipeline constructor, `NewXxx(h XxxHandlers, opts ...Option)`, and
  once per package the `Option` alias of `pipelinedef.Option` and the
  `WithMiddleware(mw...)` option, the pipeline's own middleware chain
  ([04-engine](04-engine.md#middleware)); wiring code never imports
  pipelinedef,
- `ReduceXxxOutput(h, view)` and `ReduceXxxFailure(h, view)`, the folds the
  engine reduces through and reducer tests call,
- runtime methods on the Pipeline marker type,
- the bound Pipeline handle,
- the typed Run and Result,
- runtime adapters.

Generated code imports three packages and is the only code that does:
`durable` for the handler contract and the `TypedInvocation` its alias
names, `pipelinedef` for the `Definition` its constructor builds and the
step references it exports, and `engine` for `Bind`, `Pipeline`, `Run`,
`Result`, and `Status` beneath its typed handles.

---

## Generation-time validation

A pipeline is a top-level message carrying the pipeline option; its
Steps are the messages nested directly in it, each carrying the step
option, and their declaration order is the topology. Nothing lists the
Steps twice, a Step belongs to exactly one pipeline by construction, and
two pipelines in one package may name their Steps alike. A Step's Go
type is protoc-gen-go's nested name, `Pipeline_Step`; its handler method
is the short name.

Generation MUST reject:

- missing PipelineID,
- missing StepID,
- duplicate StepID,
- a step message outside a pipeline message, or nested deeper than
  directly in one,
- a message nested in a pipeline that is not a step,
- a pipeline message that is not top-level,
- empty Pipelines,
- invalid Input/Output references,
- malformed capability declarations.

Generated APIs MUST make these compile-time errors where possible:

- a missing Step method, a Step added to the pipeline included,
- a wrong method signature,
- a missing `Unwind<Step>`,
- an invalid `ReduceOutput` or `ReduceFailure` signature,
- passing a stateless `StepRef` to `State`.

Generation MUST also reject a pipeline whose Step and reducer method
names collide.

The structural checks (missing or duplicate identifiers, empty
Pipelines, a step without a forward adapter, an unwind declaration
without its adapter) are enforced again by `engine.Bind` at runtime, so
a hand-rolled `pipelinedef.Definition` gets the same validation as a
generated one — as an error, never a panic.

---

## Buf

Use Buf for:

```text
buf lint
buf breaking
buf generate
```

Buf is build/CI tooling only.

Runtime does not depend on Buf.
