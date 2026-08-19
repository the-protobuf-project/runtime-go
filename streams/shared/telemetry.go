package shared

import (
	"github.com/the-protobuf-project/runtime-go/observability"
)

// Obs is this module's telemetry client.
//
// It is built on first use rather than in an init function: a package-level
// init cannot report a failure to anyone, and an unreachable collector must not
// stop a binary from starting.
var Obs = observability.Lazy("runtime-go-streams", "1.0.0")

// Log returns this module's logger, tagged with the component so its records
// stand out in a mixed stream.
func Log() observability.Logger {
	return Obs().Log().With(observability.Fields{"component": "streams"})
}

// Meter returns this module's meter.
func Meter() observability.Meter {
	return Obs().Meter()
}
