package telemetry

import "context"

// NoopMeter is a Meter whose instruments discard every measurement. It's the
// safe default when no telemetry backend has been wired in: generated code and
// libraries can hold a Meter unconditionally (no nil checks) and it costs
// nothing until a real provider is plugged in.
var NoopMeter Meter = noopMeter{}

type noopMeter struct{}

func (noopMeter) Counter(string, ...InstrumentOption) Counter             { return noopInstrument{} }
func (noopMeter) UpDownCounter(string, ...InstrumentOption) UpDownCounter { return noopInstrument{} }
func (noopMeter) Gauge(string, ...InstrumentOption) Gauge                 { return noopInstrument{} }
func (noopMeter) Histogram(string, ...InstrumentOption) Histogram         { return noopInstrument{} }

// noopInstrument satisfies Counter, UpDownCounter, Gauge, and Histogram at
// once — every method is a no-op, so one zero-size type covers all four.
type noopInstrument struct{}

func (noopInstrument) Add(context.Context, float64, Labels)    {}
func (noopInstrument) Set(context.Context, float64, Labels)    {}
func (noopInstrument) Record(context.Context, float64, Labels) {}
