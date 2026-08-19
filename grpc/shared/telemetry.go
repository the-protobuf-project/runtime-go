// Package shared holds the telemetry the grpc package logs and measures
// through.
//
// It is a thin binding: [github.com/the-protobuf-project/runtime-go/observability]
// does the work of standing up the backend, and this only names the service so
// grpc records are distinguishable from a sibling module's.
package shared

import (
	"github.com/the-protobuf-project/runtime-go/observability"
	telemetrysdk "github.com/the-protobuf-project/telemetry/telemetry-go"
)

// obs is this module's telemetry client, built on first use.
//
// Deferring the build matters here: a package-level init cannot report a
// failure to anyone, and an unreachable collector must not stop a binary from
// starting just because it imported the grpc package.
var obs = observability.Lazy("runtime-go-grpc", "1.0.0")

// Telemetry returns the underlying SDK client.
//
// grpc logs through the SDK's own logger — its call sites use the formatted
// helpers (Debugf, Errorf) that the backend-agnostic [observability.Logger]
// contract does not carry — so this exposes the client directly rather than
// fronting it. New code should prefer [Log].
func Telemetry() *telemetrysdk.Telemetry {
	return obs().Otel()
}

// Log returns the backend-agnostic logger, tagged with the component.
//
// Prefer this over [Telemetry] for anything new: it keeps the caller off the
// SDK type, so the same code works against a test logger or a different
// backend.
func Log() observability.Logger {
	return obs().Log().With(observability.Fields{"component": "grpc"})
}

// Meter returns this module's meter.
func Meter() observability.Meter {
	return obs().Meter()
}

// Close releases the telemetry client. The main application should call it on
// shutdown.
func Close() error {
	return obs().Close()
}
