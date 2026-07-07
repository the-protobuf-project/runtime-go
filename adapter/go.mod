module github.com/the-protobuf-project/runtime-go/adapter

go 1.26.0

require (
	github.com/the-protobuf-project/runtime-go/store v0.0.0
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)

replace github.com/the-protobuf-project/runtime-go/store => ../store
