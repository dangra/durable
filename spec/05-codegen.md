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
  repeated string steps = 4;
  string exclusion_group = 5;
  string concurrency_class = 6;
}

extend google.protobuf.MessageOptions {
  StepOptions step = <globally-allocated-extension-number>;
  PipelineOptions pipeline = <globally-allocated-extension-number>;
}
```

Published protobuf extensions MUST use globally allocated extension numbers.

---

## Code generation

`protoc-gen-durable` generates:

- typed Step handler interfaces,
- handler func adapters (`http.HandlerFunc` style),
- typed concrete Invocation types,
- typed Step references,
- generic concrete `State` methods,
- Pipeline constructors,
- Reducer function types,
- runtime methods on Pipeline marker types,
- bound Pipeline handles,
- typed Runs,
- typed Results,
- runtime adapters.

Generated code imports three packages and is the only code that does:
`durable` for the handler contract its typed Invocations wrap,
`pipelinedef` for the `Definition` its constructor builds and the step
references it exports, and `engine` for `Bind`, `Pipeline`, `Run`,
`Result`, and `Status` beneath its typed handles.

---

## Generation-time validation

Generation MUST reject:

- missing PipelineID,
- missing StepID,
- duplicate StepID,
- nonexistent Step references,
- non-Step messages in Pipeline topology,
- empty Pipelines,
- invalid Input/Output references,
- Step reuse across active Pipelines,
- malformed capability declarations.

Generated APIs MUST make these compile-time errors where possible:

- missing handler,
- wrong handler signature,
- missing required `Unwind`,
- invalid Reducer signature,
- passing a stateless `StepRef` to `State`.

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
