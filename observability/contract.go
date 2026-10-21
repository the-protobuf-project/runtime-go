package observability

// The runtime-go/telemetry side of Client. observability.go imports the SDK,
// whose package is also named telemetry, so the contract lives here.

import "github.com/the-protobuf-project/runtime-go/telemetry"

// Log returns the [telemetry.Logger] for this client. It is safe on a zero
// Client, returning [telemetry.NoopLogger].
func (c *Client) Log() telemetry.Logger {
	if c == nil || c.otel == nil || c.otel.Logger == nil {
		return telemetry.NoopLogger
	}
	c.logOnce.Do(func() { c.log = logger{bind: c.bindLogger} })
	return c.log
}

// Meter returns the [telemetry.Meter] for this client.
//
// The SDK implements the contract itself, so this is a pass-through rather
// than an adapter. It is safe on a zero Client, returning
// [telemetry.NoopMeter].
func (c *Client) Meter() telemetry.Meter {
	if c == nil || c.otel == nil {
		return telemetry.NoopMeter
	}
	return c.otel.Meter()
}
