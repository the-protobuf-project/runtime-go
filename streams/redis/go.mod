// The Redis provider is its own module.
//
// A program that reaches for NATS should not pull in the Redis client to get
// there, and the streams contract itself depends on neither. Keeping each
// provider in its own module is what makes that true.
module github.com/the-protobuf-project/runtime-go/streams/redis

go 1.26.4

require (
	github.com/redis/go-redis/v9 v9.18.0
	github.com/the-protobuf-project/runtime-go/streams v0.0.0
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20260722084318-b90e81eeadb7
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-00010101000000-000000000000 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/the-protobuf-project/runtime-go/streams => ../

replace github.com/the-protobuf-project/runtime-go/telemetry => ../../telemetry

replace github.com/the-protobuf-project/runtime-go/observability => ../../observability

replace github.com/the-protobuf-project/runtime-go/ulid => ../../ulid
