package filterx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/the-protobuf-project/runtime-go/database/filterx"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// The observer over runtime signals spans each query, failing the span when
// the query fails, and logs rejected inputs at debug.
func TestSignalsObserverSpansAndLogs(t *testing.T) {
	rec := recorder.New()
	o := filterx.SignalsObserver(rec.Signals())

	boom := errors.New("boom")
	if err := o.Span(context.Background(), "filterx.list", func(context.Context) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Span returned %v", err)
	}
	s := rec.Span("filterx.list")
	if s == nil || !errors.Is(s.Err(), boom) || s.Kind != telemetry.SpanKindClient {
		t.Fatalf("span %+v", s)
	}
	o.Debug("filterx: rejected filter", map[string]any{"field": "x"})
	logs := rec.Logs()
	if len(logs) != 1 || logs[0].Level != telemetry.LevelDebug || logs[0].Fields["field"] != "x" {
		t.Errorf("logs %+v", logs)
	}
}

// Without signals the observer is the no-op.
func TestSignalsObserverWithoutSignalsIsTheNoOp(t *testing.T) {
	if _, ok := filterx.SignalsObserver(telemetry.Signals{}).(filterx.NopObserver); !ok {
		t.Error("no signals did not give the NopObserver")
	}
}
