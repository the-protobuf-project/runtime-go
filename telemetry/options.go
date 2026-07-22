package telemetry

// InstrumentConfig holds the optional metadata an instrument is created with.
// Meter implementations read it inside Counter/UpDownCounter/Gauge/Histogram;
// callers never construct it directly — they pass InstrumentOptions instead.
type InstrumentConfig struct {
	Description string
	Unit        string
	Buckets     []float64
}

// InstrumentOption configures an InstrumentConfig at instrument-creation time.
type InstrumentOption func(*InstrumentConfig)

// WithDescription sets the human-readable description an instrument is
// registered with (e.g. shown in a metrics explorer).
func WithDescription(description string) InstrumentOption {
	return func(c *InstrumentConfig) { c.Description = description }
}

// WithUnit sets the instrument's unit using UCUM-style short strings (e.g.
// "1" for a count, "ms" for milliseconds, "By" for bytes) — the convention
// OTel instruments use.
func WithUnit(unit string) InstrumentOption {
	return func(c *InstrumentConfig) { c.Unit = unit }
}

// WithBuckets sets explicit histogram bucket boundaries. It only applies to
// Histogram instruments; implementations that don't support explicit buckets
// (e.g. an exponential-histogram backend) may ignore it.
func WithBuckets(buckets ...float64) InstrumentOption {
	return func(c *InstrumentConfig) { c.Buckets = buckets }
}

// NewInstrumentConfig applies opts to a zero-value InstrumentConfig and
// returns it. Meter implementations call this inside
// Counter/UpDownCounter/Gauge/Histogram instead of re-implementing the same
// fold themselves.
func NewInstrumentConfig(opts ...InstrumentOption) InstrumentConfig {
	var c InstrumentConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}
