package core

import (
	"context"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// A database built after the application set the process's signals reports
// every strategy through them, labeled with the real backend, and instrumenting
// it again with signals of the caller's own replaces them.
func TestABuiltDatabaseIsObservedThroughTheDefault(t *testing.T) {
	def := recorder.New()
	telemetry.SetDefault(def.Signals())
	t.Cleanup(func() { telemetry.SetDefault(telemetry.Signals{}) })

	db, driver := build(t, Spec{Prefix: "t", DefaultTTL: time.Minute})
	ctx := context.Background()
	if _, err := db.Document.Create(ctx, record{Name: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Volatile.Set(ctx, "k", record{Name: "v"}); err != nil {
		t.Fatal(err)
	}
	var got record
	if err := db.Aside(func(context.Context, string) (any, error) { return record{Name: "l"}, nil }).GetOrLoad(ctx, "x", &got); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cache.create", "cache.set", "cache.get_or_load"} {
		s := def.Span(name)
		if s == nil {
			t.Fatalf("no %s span: %v", name, def.Spans())
		}
		if got := s.Attr("backend"); got != driver.Name() {
			t.Errorf("%s backend = %v, want %q", name, got, driver.Name())
		}
	}

	mine := recorder.New()
	db = cache.Instrument(db, mine.Signals())
	before := len(def.Spans())
	if _, err := db.Document.Create(ctx, record{Name: "b"}); err != nil {
		t.Fatal(err)
	}
	if mine.Span("cache.create") == nil {
		t.Error("the caller's signals saw no create")
	}
	if n := len(def.Spans()) - before; n != 0 {
		t.Errorf("the replaced default still saw %d spans", n)
	}
}
