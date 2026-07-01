// Package shared holds the process-wide singletons the grpc server initializes
// once and tears down on exit — currently the Pulse observability client
// ([Pulse]), constructed in an init with the service identity, log level, and
// tracing enabled. Import the package to get the client wired up; call [Close]
// during shutdown to flush and release it.
//
// # Example
//
//	import _ "github.com/the-protobuf-project/runtime-go/grpc/shared" // init Pulse
//
//	func main() {
//	    defer shared.Close() // flush and release Pulse on shutdown
//	    // ... start the server
//	}
package shared
