module github.com/the-protobuf-project/runtime-go/redis

go 1.26.4

require (
	github.com/redis/go-redis/v9 v9.18.0
	github.com/the-protobuf-project/runtime-go/cache v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/database v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/streams v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20260722084318-b90e81eeadb7
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-00010101000000-000000000000
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

// These are versioned alongside this module in runtime-go and have no tagged
// release carrying the current contracts yet. Drop them once published.
replace (
	github.com/the-protobuf-project/runtime-go/cache => ../cache
	github.com/the-protobuf-project/runtime-go/database => ../database
	github.com/the-protobuf-project/runtime-go/streams => ../streams
	github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry
	github.com/the-protobuf-project/runtime-go/ulid => ../ulid
)
