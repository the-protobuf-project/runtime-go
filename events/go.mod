module github.com/the-protobuf-project/runtime-go/events

go 1.26.4

require (
	github.com/lib/pq v1.12.3
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20261118052056-98385b147b4d
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-20261118052056-98385b147b4d
)

require github.com/oklog/ulid/v2 v2.1.2 // indirect

// telemetry is versioned alongside this module in runtime-go. Drop this once
// a release carries the contract used here.
replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry

replace github.com/the-protobuf-project/runtime-go/ulid => ../ulid
