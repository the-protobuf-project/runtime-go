package telemetry

import (
	"context"
	"runtime/pprof"
)

// Profiler attributes the samples a continuous profiler takes (CPU,
// allocations, contention) to the operation that was running: every
// [Observer] operation labels its goroutine component, backend and operation
// while it runs, so a profile can be read per operation, not per process.
type Profiler interface {
	// Label returns ctx carrying labels and applies them to the calling
	// goroutine until restore is called. Call restore once, on the same
	// goroutine; a goroutine started in between inherits the labels.
	Label(ctx context.Context, labels Labels) (labeled context.Context, restore func())
}

// NoopProfiler labels nothing. It is what a nil [Signals.Profiler] means.
var NoopProfiler Profiler = noopProfiler{}

type noopProfiler struct{}

func (noopProfiler) Label(ctx context.Context, _ Labels) (context.Context, func()) {
	return ctx, func() {}
}

// PprofProfiler labels with runtime/pprof, which is what Pyroscope's Go agent
// (the telemetry SDK's profiler), go tool pprof and every other pprof-based
// profiler read. Labeling is cheap, but not free: wire it only when a profiler
// is collecting.
var PprofProfiler Profiler = pprofProfiler{}

type pprofProfiler struct{}

func (pprofProfiler) Label(ctx context.Context, labels Labels) (context.Context, func()) {
	pairs := make([]string, 0, len(labels)*2)
	for k, v := range labels {
		pairs = append(pairs, k, v)
	}
	labeled := pprof.WithLabels(ctx, pprof.Labels(pairs...))
	pprof.SetGoroutineLabels(labeled)
	return labeled, func() { pprof.SetGoroutineLabels(ctx) }
}
