// The examples are their own module.
//
// Two reasons. They import the streams module by its published path rather
// than by a relative one, so they exercise what an outside consumer actually
// writes — an example that reached into the package next door would compile
// under conditions no user has. And they are commands rather than library
// code: keeping them out of the module means `go build ./...` on the library
// never depends on whether a demo still compiles.
module github.com/the-protobuf-project/runtime-go/streams/examples

go 1.26.4

require (
	github.com/nats-io/nats.go v1.52.0
	github.com/redis/go-redis/v9 v9.18.0
	github.com/the-protobuf-project/runtime-go/streams v0.0.0
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20260722084318-b90e81eeadb7
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	github.com/the-protobuf-project/runtime-go/ulid v0.0.0-00010101000000-000000000000 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/the-protobuf-project/runtime-go/streams => ../

replace github.com/the-protobuf-project/runtime-go/observability => ../../observability

replace github.com/the-protobuf-project/runtime-go/telemetry => ../../telemetry

replace github.com/the-protobuf-project/runtime-go/ulid => ../../ulid
