package adapter_test

import (
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/the-protobuf-project/runtime-go/database/store"
	"github.com/the-protobuf-project/runtime-go/interfaces/adapter"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// A driver served directly is observed through the process's default signals,
// and one that is already observed is reported once, not once per wrapper.
func TestAServedDriverIsObservedOnce(t *testing.T) {
	rec := recorder.New()
	telemetry.SetDefault(rec.Signals())
	t.Cleanup(func() { telemetry.SetDefault(telemetry.Signals{}) })

	svc := adapter.New(&fakeDriver{getMsg: wrapperspb.String("x")}, registry())
	if _, err := svc.Get(t.Context(), "Book", "books/x"); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Named("database.get")); n != 1 {
		t.Fatalf("database.get spans = %d, want 1", n)
	}

	observed := store.InstrumentDefault(store.Build(&fakeDriver{getMsg: wrapperspb.String("y")}, "fake", "", nil)).Driver
	svc = adapter.New(observed, registry())
	if _, err := svc.Get(t.Context(), "Book", "books/y"); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Named("database.get")); n != 2 {
		t.Errorf("database.get spans = %d after an already-observed driver, want 2 (one each)", n)
	}
}
