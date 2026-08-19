package observability

import (
	"log/slog"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// The backend-agnostic contract, re-exported so callers import this package
// and nothing else.
//
// These are type aliases, not wrappers: observability.Meter and
// telemetry.Meter are the same type, so a value satisfying one satisfies the
// other. The aliased module is consumed from the proxy at a pinned version and
// is not developed here; the SDK implements its Meter from internal state this
// package cannot reach, which is why that module still exists at all. Treat it
// as a private detail of the wiring.
type (
	Logger           = telemetry.Logger
	Meter            = telemetry.Meter
	Counter          = telemetry.Counter
	UpDownCounter    = telemetry.UpDownCounter
	Gauge            = telemetry.Gauge
	Histogram        = telemetry.Histogram
	Labels           = telemetry.Labels
	Fields           = telemetry.Fields
	Level            = telemetry.Level
	InstrumentConfig = telemetry.InstrumentConfig
	InstrumentOption = telemetry.InstrumentOption
)

// Log severities, matching log/slog's values.
const (
	LevelDebug = telemetry.LevelDebug
	LevelInfo  = telemetry.LevelInfo
	LevelWarn  = telemetry.LevelWarn
	LevelError = telemetry.LevelError
)

// Inert implementations, used whenever no backend is wired in.
var (
	NoopLogger = telemetry.NoopLogger
	NoopMeter  = telemetry.NoopMeter
)

// WithDescription sets an instrument's human-readable description.
func WithDescription(description string) InstrumentOption {
	return telemetry.WithDescription(description)
}

// WithUnit sets an instrument's UCUM-style unit ("1", "ms", "By").
func WithUnit(unit string) InstrumentOption { return telemetry.WithUnit(unit) }

// WithBuckets sets explicit histogram bucket boundaries.
func WithBuckets(buckets ...float64) InstrumentOption {
	return telemetry.WithBuckets(buckets...)
}

// NewInstrumentConfig folds opts into an InstrumentConfig. Meter
// implementations call this rather than repeating the fold.
func NewInstrumentConfig(opts ...InstrumentOption) InstrumentConfig {
	return telemetry.NewInstrumentConfig(opts...)
}

// NewSlogLogger adapts a [log/slog.Logger] to [Logger], for callers who want
// structured logging without standing up a backend.
func NewSlogLogger(l *slog.Logger) Logger { return telemetry.NewSlogLogger(l) }
