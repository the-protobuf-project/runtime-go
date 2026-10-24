module github.com/the-protobuf-project/runtime-go/events

go 1.26.4

require github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20261021141431-6d4588022f2b

// telemetry is versioned alongside this module in runtime-go. Drop this once
// a release carries the contract used here.
replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry
