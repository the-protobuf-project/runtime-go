// Package telemetry defines the backend-agnostic contracts generated and
// hand-written code instruments against: [Meter] for metrics, [Logger] for
// logs and [Tracer] for spans. It has no dependency on any specific telemetry
// SDK — OTel, Prometheus, or otherwise — so core runtime-go packages and
// generated ORM code can hold them without pulling in a concrete backend. A
// concrete implementation (runtime-go/observability over the telemetry-go SDK)
// is wired in by the caller; when none is wired in, [NoopMeter], [NoopLogger]
// and [NoopTracer] keep every call a safe, zero-cost no-op.
//
// # Operations
//
// [Signals] holds the three handles, and [Signals.Observer] gives one runtime
// component its instruments, built once. Every unit of work is an
// [Operation]: a span named component.operation, and a share of the same
// metrics across every component, labeled component, backend, operation and
// outcome:
//
//	runtime.operation.count       {operation}  completed operations
//	runtime.operation.duration    s            operation duration
//	runtime.operation.active      {operation}  operations in flight
//	runtime.records.read          {record}     records returned or delivered
//	runtime.records.written       {record}     records the backend acknowledged
//	runtime.ingestion.queue.depth {record}     admitted records not yet written
//
// It is the Go half of runtime-rs's tpp-telemetry: the same names, the same
// [Outcome] labels, the same no-op default.
//
//	obs := telemetry.Signals{Meter: m, Tracer: tr, Logger: log}.Observer("streams")
//	ctx, op := obs.Start(ctx, "nats", "send", telemetry.SpanKindProducer)
//	err := send(ctx)
//	op.FinishErr(err)
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
