package rabbitmq

import "github.com/the-protobuf-project/runtime-go/streams"

// ConnectionName is the connection_name a store from [Connect] or [Use] gives
// the broker, so a test can find its own connection in the management API.
func ConnectionName(s streams.Streams) string { return s.(*store).name }
