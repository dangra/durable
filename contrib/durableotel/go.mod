module github.com/dangra/durable/contrib/durableotel

go 1.27.0

// In-tree development and CI build against the working-tree core; the
// directive is ignored when this module is consumed via go get, where
// the required release version below applies. Releases are lockstep:
// this module is tagged at the same version and commit as the core, and
// scripts/release.sh writes the require line (see docs/releasing.md).
replace github.com/dangra/durable => ../..

require (
	github.com/dangra/durable v0.5.0
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/metric v1.46.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/sdk/metric v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
