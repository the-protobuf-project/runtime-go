module github.com/the-protobuf-project/runtime-go/events/stream

go 1.26.4

require (
	github.com/the-protobuf-project/runtime-go/events v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/streams v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20261021141431-6d4588022f2b
)

require (
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-20261021171349-70cd275896ec // indirect
)

// events, streams and the modules they share are versioned alongside this one
// in runtime-go. Drop these once a release carries the contracts used here.
replace github.com/the-protobuf-project/runtime-go/events => ../

replace github.com/the-protobuf-project/runtime-go/streams => ../../streams

replace github.com/the-protobuf-project/runtime-go/telemetry => ../../telemetry

replace github.com/the-protobuf-project/runtime-go/ulid => ../../ulid

replace github.com/the-protobuf-project/runtime-go/observability => ../../observability
