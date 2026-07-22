package telemetry_test

import (
	"context"
	"testing"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// fakeMeter is a minimal Meter that records every call, used to check the
// interfaces compose the way generated code expects: create once per name,
// record many times, labels flow through untouched. Its existence also
// doubles as a compile-time check that an external type can implement Meter.
type fakeMeter struct {
	instruments map[string]*fakeInstrument
}

var _ telemetry.Meter = (*fakeMeter)(nil)

func newFakeMeter() *fakeMeter {
	return &fakeMeter{instruments: make(map[string]*fakeInstrument)}
}

func (m *fakeMeter) instrument(name string, opts ...telemetry.InstrumentOption) *fakeInstrument {
	if existing, ok := m.instruments[name]; ok {
		return existing
	}
	cfg := telemetry.NewInstrumentConfig(opts...)
	inst := &fakeInstrument{name: name, config: cfg}
	m.instruments[name] = inst
	return inst
}

func (m *fakeMeter) Counter(name string, opts ...telemetry.InstrumentOption) telemetry.Counter {
	return m.instrument(name, opts...)
}

func (m *fakeMeter) UpDownCounter(name string, opts ...telemetry.InstrumentOption) telemetry.UpDownCounter {
	return m.instrument(name, opts...)
}

func (m *fakeMeter) Gauge(name string, opts ...telemetry.InstrumentOption) telemetry.Gauge {
	return m.instrument(name, opts...)
}

func (m *fakeMeter) Histogram(name string, opts ...telemetry.InstrumentOption) telemetry.Histogram {
	return m.instrument(name, opts...)
}

// fakeInstrument records every measurement made against it, satisfying
// Counter, UpDownCounter, Gauge, and Histogram at once.
type fakeInstrument struct {
	name    string
	config  telemetry.InstrumentConfig
	adds    []float64
	sets    []float64
	records []float64
	labels  []telemetry.Labels
}

func (i *fakeInstrument) Add(_ context.Context, delta float64, labels telemetry.Labels) {
	i.adds = append(i.adds, delta)
	i.labels = append(i.labels, labels)
}

func (i *fakeInstrument) Set(_ context.Context, value float64, labels telemetry.Labels) {
	i.sets = append(i.sets, value)
	i.labels = append(i.labels, labels)
}

func (i *fakeInstrument) Record(_ context.Context, value float64, labels telemetry.Labels) {
	i.records = append(i.records, value)
	i.labels = append(i.labels, labels)
}

func TestMeter_CreateOnceRecordMany(t *testing.T) {
	ctx := context.Background()
	m := newFakeMeter()

	created := m.Counter("books_created_total", telemetry.WithUnit("1"))
	created.Add(ctx, 1, telemetry.Labels{"genre": "FICTION"})
	created.Add(ctx, 1, telemetry.Labels{"genre": "NONFICTION"})

	// Resolving the same name again must return the same instrument, not a
	// fresh one — this is the "create-once, record-many" contract generated
	// code relies on when it holds a handle as a struct field.
	again := m.Counter("books_created_total")
	again.Add(ctx, 1, telemetry.Labels{"genre": "FICTION"})

	inst := m.instruments["books_created_total"]
	if len(inst.adds) != 3 {
		t.Fatalf("adds = %v, want 3 recorded measurements", inst.adds)
	}
	if inst.config.Unit != "1" {
		t.Fatalf("unit = %q, want %q (config set on first creation only)", inst.config.Unit, "1")
	}
	if got := inst.labels[0]["genre"]; got != "FICTION" {
		t.Fatalf("first label = %q, want FICTION", got)
	}
}

func TestMeter_InstrumentKindsAreIndependent(t *testing.T) {
	ctx := context.Background()
	m := newFakeMeter()

	m.UpDownCounter("inflight_requests").Add(ctx, 1, nil)
	m.UpDownCounter("inflight_requests").Add(ctx, -1, nil)
	m.Gauge("queue_depth").Set(ctx, 42, nil)
	m.Histogram("request_latency_ms", telemetry.WithBuckets(5, 10, 25, 50)).Record(ctx, 12.5, nil)

	if got := m.instruments["inflight_requests"].adds; len(got) != 2 || got[0] != 1 || got[1] != -1 {
		t.Fatalf("inflight_requests adds = %v, want [1 -1]", got)
	}
	if got := m.instruments["queue_depth"].sets; len(got) != 1 || got[0] != 42 {
		t.Fatalf("queue_depth sets = %v, want [42]", got)
	}
	hist := m.instruments["request_latency_ms"]
	if len(hist.records) != 1 || hist.records[0] != 12.5 {
		t.Fatalf("request_latency_ms records = %v, want [12.5]", hist.records)
	}
	if len(hist.config.Buckets) != 4 {
		t.Fatalf("buckets = %v, want 4 explicit boundaries", hist.config.Buckets)
	}
}

func TestNoopMeter_SafeWithoutProvider(t *testing.T) {
	ctx := context.Background()
	m := telemetry.NoopMeter

	// None of these should panic — NoopMeter is the fallback that keeps
	// unconfigured code (no provider wired in) safe to call unconditionally.
	m.Counter("c", telemetry.WithDescription("d"), telemetry.WithUnit("1")).Add(ctx, 1, telemetry.Labels{"k": "v"})
	m.UpDownCounter("udc").Add(ctx, -1, nil)
	m.Gauge("g").Set(ctx, 3.14, nil)
	m.Histogram("h", telemetry.WithBuckets(1, 2, 3)).Record(ctx, 2.5, nil)
}

func TestNewInstrumentConfig(t *testing.T) {
	cfg := telemetry.NewInstrumentConfig(
		telemetry.WithDescription("rows created"),
		telemetry.WithUnit("1"),
		telemetry.WithBuckets(1, 2, 4, 8),
	)
	if cfg.Description != "rows created" {
		t.Fatalf("Description = %q, want %q", cfg.Description, "rows created")
	}
	if cfg.Unit != "1" {
		t.Fatalf("Unit = %q, want %q", cfg.Unit, "1")
	}
	if len(cfg.Buckets) != 4 {
		t.Fatalf("Buckets = %v, want 4 entries", cfg.Buckets)
	}

	empty := telemetry.NewInstrumentConfig()
	if empty.Description != "" || empty.Unit != "" || empty.Buckets != nil {
		t.Fatalf("zero-opt config should be the zero value, got %+v", empty)
	}
}
