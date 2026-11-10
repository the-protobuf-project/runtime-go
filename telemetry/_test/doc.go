// Package telemetrytest records what code reports through the telemetry
// contracts, for its tests to check: one [Recorder] is a [telemetry.Meter], a
// [telemetry.Tracer] and a [telemetry.Logger], and keeps every measurement,
// span and log record in memory.
//
//	rec := telemetrytest.New()
//	db := store.Instrument(db, store.WithSignals(rec.Signals()))
//	...
//	if s := rec.Span("database.get"); s == nil || s.Failed() { ... }
//
// Its spans carry trace context through Inject and Extract as a
// "traceparent" header holding the span's id, so a test can follow a trace
// from a publisher to a receiver.
package telemetry_test
