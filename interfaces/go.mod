module github.com/the-protobuf-project/runtime-go/interfaces

go 1.26.4

require (
	github.com/the-protobuf-project/runtime-go/database v0.0.0
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20260722084318-b90e81eeadb7 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260807164820-c8921c73eeea // indirect
)

// database is versioned alongside this module in runtime-go and has no
// published tag of its own, so it resolves from the tree rather than the proxy.
replace github.com/the-protobuf-project/runtime-go/database => ../database

replace github.com/the-protobuf-project/runtime-go/cache => ../cache

replace github.com/the-protobuf-project/runtime-go/observability => ../observability

replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry

replace github.com/the-protobuf-project/runtime-go/ulid => ../ulid
