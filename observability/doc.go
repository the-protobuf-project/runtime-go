// Package observability wires runtime-go modules to the telemetry-go SDK.
//
// It is the one place that knows about a concrete telemetry backend. Every
// other runtime-go module holds only the [telemetry] contracts — Logger and
// Meter — so a consumer that wants a different backend, or none, never links
// this package or the OTel SDK behind it.
//
// # Why this is a separate module
//
// The dependency has to flow one way. telemetry-go already imports
// runtime-go/telemetry and implements [telemetry.Meter]; if the telemetry
// module imported it back, the two would cycle and every piece of generated
// code binding to the contract would drag the whole SDK in — the exact thing
// the contract exists to prevent. This module sits above both:
//
//	observability ──> telemetry-go ──> telemetry
//	      └─────────────────────────────>┘
//
// # Use
//
// Each module's shared package sets one up for itself:
//
//	var Obs = observability.Must("runtime-go-cache", "1.0.0")
//
// and then hands its Logger and Meter to whatever needs them:
//
//	c, _ := cache.Redis(cache.RedisConfig{Client: rdb, Logger: shared.Obs.Log()})
//	c = cache.WithLogging(c, shared.Obs.Log())
//	c = cache.WithTelemetry(c, shared.Obs.Meter())
//
// An application hands its own SDK instance to every module with [Adopt], so
// the process runs one pipeline under one service name:
//
//	sdk, _ := telemetry.New().WithService("freebusy", "1.0.0").WithTracing().Build()
//	observability.Adopt(sdk)
package observability
