package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/store"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// txStub runs a transaction by handing fn a DB over itself.
type txStub struct {
	stub
}

func (s *txStub) Run(ctx context.Context, fn func(tx *store.DB) error) error {
	return fn(store.Build(s, "stub", "", nil))
}

func traced(d store.Driver) (*store.DB, *recorder.Recorder) {
	rec := recorder.New()
	return store.Instrument(store.Build(d, "stub", "shop", nil), store.WithSignals(rec.Signals())), rec
}

func TestEveryOperationIsAClientSpan(t *testing.T) {
	db, rec := traced(&stub{})
	if _, err := db.Get(t.Context(), userRes, "u-1"); err != nil {
		t.Fatal(err)
	}
	s := rec.Span("database.get")
	if s == nil || s.Kind != telemetry.SpanKindClient || !s.Ended() {
		t.Fatalf("span %+v", s)
	}
	if s.Attr("store.resource") != "User" || s.Attr("db.system") != "stub" || s.Attr("db.namespace") != "shop" {
		t.Errorf("attributes resource %v system %v namespace %v", s.Attr("store.resource"), s.Attr("db.system"), s.Attr("db.namespace"))
	}
	if read := rec.Sum("runtime.records.read"); read != 1 {
		t.Errorf("records read %v", read)
	}
}

func TestAMissingRecordDoesNotFailTheSpan(t *testing.T) {
	db, rec := traced(&stub{err: fmt.Errorf("%w: u-1", store.ErrNotFound)})
	_, _ = db.Get(t.Context(), userRes, "u-1")
	if s := rec.Span("database.get"); s == nil || s.Failed() {
		t.Errorf("span %+v", s)
	}
	if got := rec.Outcomes()["get"]; got != "not_found" {
		t.Errorf("outcome %q", got)
	}
	logs := rec.Logs()
	if len(logs) != 1 || logs[0].Level != telemetry.LevelInfo || logs[0].Msg != "runtime.operation.completed" {
		t.Errorf("logs %+v", logs)
	}
}

func TestAFailureFailsTheSpanAndLogsAnError(t *testing.T) {
	boom := errors.New("connection reset")
	db, rec := traced(&stub{err: boom})
	_, _ = db.Get(t.Context(), userRes, "u-1")
	s := rec.Span("database.get")
	if s == nil || !errors.Is(s.Err(), boom) {
		t.Fatalf("span %+v", s)
	}
	if logs := rec.Logs(); len(logs) != 1 || logs[0].Level != telemetry.LevelError || logs[0].SpanID != s.ID {
		t.Errorf("logs %+v", logs)
	}
}

func TestOperationsInATransactionAreReported(t *testing.T) {
	db, rec := traced(&txStub{})
	err := db.Tx.Run(t.Context(), func(tx *store.DB) error {
		_, err := tx.Get(t.Context(), userRes, "u-1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, get := rec.Span("database.transaction"), rec.Span("database.get")
	if tx == nil || get == nil {
		t.Fatalf("spans %v", rec.Spans())
	}
	if !get.Ended() || !tx.Ended() {
		t.Error("an operation was left open")
	}
}

func TestTheOtherHalvesAreInstrumented(t *testing.T) {
	db, rec := traced(&stub{})
	_ = db.Schema.EnsureSchema(t.Context(), userRes)
	_, _ = db.Graph.Neighbors(t.Context(), store.Ref{Resource: "User", Key: "u-1"}, store.TraverseOptions{})
	_, _ = db.Series.Range(t.Context(), userRes, store.RangeOptions{})

	for _, name := range []string{"database.ensure_schema", "database.neighbors", "database.range"} {
		if rec.Span(name) == nil {
			t.Errorf("no %s span", name)
		}
	}
	// The stub has none of these, so each is the backend's refusal: unsupported,
	// an error a dashboard should show.
	if got := rec.Outcomes()["range"]; got != "unsupported" {
		t.Errorf("range outcome %q", got)
	}
}

func TestABatchCountsWhatItWrote(t *testing.T) {
	db, rec := traced(&batcherStub{})
	b := db.Driver.(store.Batcher)
	if _, err := b.CreateMany(t.Context(), userRes, make([]proto.Message, 3)); err != nil {
		t.Fatal(err)
	}
	if written := rec.Sum("runtime.records.written"); written != 3 {
		t.Errorf("records written %v", written)
	}
}
