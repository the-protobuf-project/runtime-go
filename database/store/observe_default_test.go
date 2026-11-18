package store_test

import (
	"testing"

	"github.com/the-protobuf-project/runtime-go/database/store"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// A database selected after the application set the process's signals is
// observed without being handed any, and reports every signal.
func TestADefaultInstrumentedDatabaseReportsThroughTheDefault(t *testing.T) {
	rec := recorder.New()
	telemetry.SetDefault(rec.Signals())
	t.Cleanup(func() { telemetry.SetDefault(telemetry.Signals{}) })

	db := store.InstrumentDefault(store.Build(&stub{}, "stub", "shop", nil))
	if _, err := db.Get(t.Context(), userRes, "u-1"); err != nil {
		t.Fatal(err)
	}
	if rec.Span("database.get") == nil {
		t.Error("no database.get span")
	}
	if rec.Sum("runtime.operation.count") != 1 {
		t.Errorf("runtime.operation.count = %v, want 1", rec.Sum("runtime.operation.count"))
	}
	if len(rec.Profiles()) != 1 {
		t.Errorf("profiled sections = %d, want 1", len(rec.Profiles()))
	}
}

// Without default signals nothing is reported and nothing is wrapped.
func TestWithoutDefaultSignalsADatabaseIsLeftAsItIs(t *testing.T) {
	telemetry.SetDefault(telemetry.Signals{})
	raw := store.Build(&stub{}, "stub", "shop", nil)
	if got := store.InstrumentDefault(raw); got != raw {
		t.Error("a database was wrapped with no signals to report through")
	}
}

// Instrumenting an instrumented database replaces its signals: the caller's
// own win, and an operation is reported once, not once per layer.
func TestInstrumentingAgainReplacesRatherThanStacks(t *testing.T) {
	first, second := recorder.New(), recorder.New()
	db := store.Instrument(store.Build(&stub{}, "stub", "shop", nil), store.WithSignals(first.Signals()))
	db = store.Instrument(db, store.WithSignals(second.Signals()))
	if _, err := db.Get(t.Context(), userRes, "u-1"); err != nil {
		t.Fatal(err)
	}
	if n := len(first.Spans()); n != 0 {
		t.Errorf("the replaced signals still saw %d spans", n)
	}
	if n := len(second.Spans()); n != 1 {
		t.Errorf("the caller's signals saw %d spans, want 1", n)
	}
}
