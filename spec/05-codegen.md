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
