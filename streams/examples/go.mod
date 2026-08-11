// The examples are their own module.
//
// Three reasons here. They import the contract and the providers by their
// published paths rather than by relative ones, so they exercise what an
// outside consumer actually writes. They are commands rather than library
// code, so `go build ./...` on any of those modules never depends on whether a
// demo still compiles. And a provider module requires the contract module — so
// the contract cannot import a provider back without a cycle, which is exactly
// where a demo that needs both has to live.
module github.com/the-protobuf-project/runtime-go/streams/examples

go 1.26.4

require (
	github.com/nats-io/nats.go v1.51.0
	github.com/redis/go-redis/v9 v9.18.0
	github.com/the-protobuf-project/runtime-go/streams v0.0.0
	github.com/the-protobuf-project/runtime-go/streams/nats v0.0.0
	github.com/the-protobuf-project/runtime-go/streams/redis v0.0.0
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20260722084318-b90e81eeadb7
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-00010101000000-000000000000 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/the-protobuf-project/runtime-go/streams => ../

replace github.com/the-protobuf-project/runtime-go/streams/nats => ../nats

replace github.com/the-protobuf-project/runtime-go/streams/redis => ../redis

replace github.com/the-protobuf-project/runtime-go/telemetry => ../../telemetry

replace github.com/the-protobuf-project/runtime-go/observability => ../../observability

replace github.com/the-protobuf-project/runtime-go/ulid => ../../ulid
