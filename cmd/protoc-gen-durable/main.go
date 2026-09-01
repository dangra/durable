// protoc-gen-durable generates typed durable pipeline APIs from protobuf
// declarations annotated with durable.v1.pipeline and durable.v1.step
// options. It runs alongside protoc-gen-go and augments its output.
package main

import (
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/dangra/durable/cmd/protoc-gen-durable/internal/gen"
)

func main() {
	protogen.Options{}.Run(func(p *protogen.Plugin) error {
		p.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		return gen.Generate(p)
	})
}
