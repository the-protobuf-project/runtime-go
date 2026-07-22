// Package telemetry defines the backend-agnostic Meter contract that generated
// and hand-written code instruments against. It has no dependency on any
// specific telemetry SDK — OTel, Prometheus, or otherwise — so core runtime-go
// packages and generated ORM code can hold a Meter without pulling in a
// concrete backend. A concrete implementation (e.g. the opentelementry-go
// Telemetry plugin) is wired in by the caller; when none is wired in,
// [NoopMeter] keeps every call a safe, zero-cost no-op.
//
// Instruments follow a create-once, record-many pattern: call
// [Meter.Counter]/[Meter.UpDownCounter]/[Meter.Gauge]/[Meter.Histogram] once
// (e.g. in a constructor) and hold the returned handle, rather than
// re-resolving it on every measurement.
//
// # Example
//
//	m := telemetry.NoopMeter // swap for a real provider's Meter when one is wired in
//	created := m.Counter("books_created_total", telemetry.WithUnit("1"))
//	created.Add(ctx, 1, telemetry.Labels{"genre": "FICTION"})
package telemetry
