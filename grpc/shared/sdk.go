package shared

// The SDK accessor lives apart from telemetry.go: the SDK's package and the
// runtime-go contract's are both named telemetry, so each file imports one.

import "github.com/the-protobuf-project/telemetry/telemetry-go"

// Telemetry returns the underlying SDK client.
//
// grpc logs through the SDK's own logger — its call sites use the formatted
// helpers (Debugf, Errorf) that the backend-agnostic runtime-go Logger
// contract does not carry — so this exposes the client directly rather than
// fronting it. New code should prefer [Log].
func Telemetry() *telemetry.Telemetry {
	return obs().Otel()
}
