module github.com/the-protobuf-project/runtime-go/events

go 1.26.4

require (
	github.com/lib/pq v1.12.3
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20261021141431-6d4588022f2b
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-20261021171349-70cd275896ec
)

require github.com/oklog/ulid/v2 v2.1.2 // indirect

// telemetry is versioned alongside this module in runtime-go. Drop this once
// a release carries the contract used here.
replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry

replace github.com/the-protobuf-project/runtime-go/ulid => ../ulid
