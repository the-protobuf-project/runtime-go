module github.com/the-protobuf-project/runtime-go/fabric

go 1.26.0

require (
	github.com/the-protobuf-project/runtime-go/store v0.0.0
	google.golang.org/protobuf v1.36.11
)

replace github.com/the-protobuf-project/runtime-go/store => ../store
