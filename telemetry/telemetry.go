package telemetry

import "context"

// Meter creates instruments. Implementations create each named instrument once
// and cache it, so calling Counter (or UpDownCounter/Gauge/Histogram) twice
// with the same name on the same Meter returns equivalent instruments rather
// than allocating a second one.
type Meter interface {
	// Counter returns a monotonically increasing instrument (e.g. requests
	// served, rows created).
	Counter(name string, opts ...InstrumentOption) Counter

	// UpDownCounter returns an instrument that can increase or decrease (e.g.
	// in-flight requests, open connections).
	UpDownCounter(name string, opts ...InstrumentOption) UpDownCounter

	// Gauge returns an instrument that reports the current value of something
	// that isn't naturally a sum (e.g. queue depth, cache size).
	Gauge(name string, opts ...InstrumentOption) Gauge

	// Histogram returns an instrument that records a distribution of values
	// (e.g. request latency, payload size).
	Histogram(name string, opts ...InstrumentOption) Histogram
}

// Counter is a monotonically increasing instrument. delta should be >= 0;
// implementations may choose how to handle a negative delta (e.g. drop and
// log), since Counter's contract — unlike UpDownCounter's — assumes it never
// happens in correct callers.
type Counter interface {
	Add(ctx context.Context, delta float64, labels Labels)
}

// UpDownCounter is an instrument whose value can increase or decrease.
type UpDownCounter interface {
	Add(ctx context.Context, delta float64, labels Labels)
}

// Gauge reports the current value of something at the moment Set is called.
type Gauge interface {
	Set(ctx context.Context, value float64, labels Labels)
}

// Histogram records individual observations of a distribution.
type Histogram interface {
	Record(ctx context.Context, value float64, labels Labels)
}

// Labels are the dimensions recorded alongside a measurement, e.g.
// {"table": "books", "op": "create", "status": "ok"}. Keep cardinality low —
// see telemetry.v1's field-inference rules for which proto fields are safe to
// use as labels versus span-attribute-only.
type Labels map[string]string
