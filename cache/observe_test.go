package cache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/telemetrytest"
)

// doc is a Document whose Get answers with err.
type doc struct {
	err error
}

func (d doc) Create(context.Context, any, ...cache.Option) (string, error) { return "k", nil }
func (d doc) Get(context.Context, string, any) error                       { return d.err }
func (d doc) Update(context.Context, string, any, ...cache.Option) error   { return nil }
func (d doc) Delete(context.Context, string) error                         { return nil }
func (d doc) Keys(context.Context) ([]string, error)                       { return nil, nil }
func (d doc) List(context.Context, any) error                              { return nil }
func (d doc) TTL(context.Context, string) (time.Duration, error)           { return 0, nil }
func (d doc) ByIndex(context.Context, string, string, any) error           { return nil }
func (d doc) IDsByIndex(context.Context, string, string) ([]string, error) {
	return []string{"a", "b"}, nil
}
func (d doc) DeleteByIndex(context.Context, string, string) (int, error) { return 2, nil }

type plainDoc struct{ doc }

// plainDoc hides the index methods behind the Document interface alone.
func (p plainDoc) document() cache.Document {
	return struct{ cache.Document }{p.doc}
}

func TestWithoutSignalsTheCacheIsUnwrapped(t *testing.T) {
	d := cache.Document(doc{})
	if cache.WithObservability(d, telemetry.Signals{}) != d {
		t.Fatal("WithObservability without signals should return the cache itself")
	}
}

func TestAMissIsNotAFailure(t *testing.T) {
	rec := telemetrytest.New()
	c := cache.WithObservability(doc{err: fmt.Errorf("%w: k", cache.ErrNotFound)}, rec.Signals())
	_ = c.Get(t.Context(), "k", nil)

	s := rec.Span("cache.get")
	if s == nil || s.Failed() || s.Kind != telemetry.SpanKindClient {
		t.Fatalf("span %+v", s)
	}
	if got := rec.Outcomes()["get"]; got != "not_found" {
		t.Errorf("outcome %q", got)
	}
}

func TestAFailureFailsTheSpan(t *testing.T) {
	rec := telemetrytest.New()
	boom := errors.New("connection reset")
	c := cache.WithObservability(doc{err: boom}, rec.Signals())
	_ = c.Get(t.Context(), "k", nil)
	if s := rec.Span("cache.get"); s == nil || !errors.Is(s.Err(), boom) {
		t.Fatalf("span %+v", s)
	}
	if got := rec.Outcomes()["get"]; got != "error" {
		t.Errorf("outcome %q", got)
	}
}

func TestAnIndexedCacheStaysIndexed(t *testing.T) {
	rec := telemetrytest.New()
	c := cache.WithObservability(doc{}, rec.Signals())
	ix, ok := c.(cache.Indexed)
	if !ok {
		t.Fatal("instrumenting lost Indexed")
	}
	if _, err := ix.IDsByIndex(t.Context(), "email", "a@b"); err != nil {
		t.Fatal(err)
	}
	if rec.Span("cache.ids_by_index") == nil || rec.Sum("runtime.records.read") != 2 {
		t.Errorf("ids_by_index not reported: %v", rec.Spans())
	}

	plain := cache.WithObservability(plainDoc{}.document(), rec.Signals())
	if _, ok := plain.(cache.Indexed); ok {
		t.Error("instrumenting invented Indexed")
	}
}

func TestKeysNeverReachTheSignals(t *testing.T) {
	rec := telemetrytest.New()
	c := cache.WithObservability(doc{}, rec.Signals())
	_ = c.Get(t.Context(), "secret-key-42", nil)
	for _, m := range rec.Measurements("runtime.operation.count") {
		for _, v := range m.Labels {
			if v == "secret-key-42" {
				t.Fatalf("a key became a label: %v", m.Labels)
			}
		}
	}
	for _, l := range rec.Logs() {
		for _, v := range l.Fields {
			if v == "secret-key-42" {
				t.Fatalf("a key was logged: %v", l.Fields)
			}
		}
	}
}
