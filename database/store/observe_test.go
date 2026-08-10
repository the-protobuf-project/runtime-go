package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/the-protobuf-project/runtime-go/database/store"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// recorder captures what the instrumentation emitted, so these assert the
// signals a dashboard would actually read rather than that the code ran.
type recorder struct {
	mu       sync.Mutex
	counters map[string][]telemetry.Labels
}

func newRecorder() *recorder {
	return &recorder{counters: map[string][]telemetry.Labels{}}
}

func (r *recorder) Counter(name string, _ ...telemetry.InstrumentOption) telemetry.Counter {
	return &recCounter{r: r, name: name}
}
func (r *recorder) UpDownCounter(string, ...telemetry.InstrumentOption) telemetry.UpDownCounter {
	return telemetry.NoopMeter.UpDownCounter("")
}
func (r *recorder) Gauge(string, ...telemetry.InstrumentOption) telemetry.Gauge {
	return telemetry.NoopMeter.Gauge("")
}
func (r *recorder) Histogram(name string, _ ...telemetry.InstrumentOption) telemetry.Histogram {
	return &recHistogram{r: r, name: name}
}

type recCounter struct {
	r    *recorder
	name string
}

func (c *recCounter) Add(_ context.Context, _ float64, labels telemetry.Labels) {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.r.counters[c.name] = append(c.r.counters[c.name], labels)
}

type recHistogram struct {
	r    *recorder
	name string
}

func (h *recHistogram) Record(_ context.Context, _ float64, labels telemetry.Labels) {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	h.r.counters[h.name] = append(h.r.counters[h.name], labels)
}

func (r *recorder) labelsFor(metric string) []telemetry.Labels {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]telemetry.Labels(nil), r.counters[metric]...)
}

// stub is a driver whose outcomes a test chooses.
type stub struct {
	store.Driver
	err   error
	items int
	gets  atomic.Int64
}

// A real message on success: a driver returning (nil, nil) is malformed, and a
// stub that did so would be testing the wrong thing.
func (s *stub) Get(context.Context, *store.Resource, string) (proto.Message, error) {
	s.gets.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	return timestamppb.Now(), nil
}

func (s *stub) List(context.Context, *store.Resource, store.ListOptions) (store.ListResult, error) {
	items := make([]proto.Message, s.items)
	return store.ListResult{Items: items, Total: int64(s.items)}, s.err
}

var userRes = &store.Resource{Name: "User", Table: "users", PKColumn: "id"}

func instrumented(d store.Driver, r *recorder) *store.DB {
	return store.Instrument(store.Build(d, "stub", "", nil), store.WithMeter(r))
}

// A missing record is a normal answer, not a failure — counting it as one makes
// an error rate useless.
func TestNotFoundIsNotAnError(t *testing.T) {
	r := newRecorder()
	db := instrumented(&stub{err: fmt.Errorf("%w: x", store.ErrNotFound)}, r)

	_, _ = db.Get(context.Background(), userRes, "x")

	labels := r.labelsFor("database_operations_total")
	if len(labels) != 1 {
		t.Fatalf("recorded %d operations, want 1", len(labels))
	}
	if got := labels[0]["outcome"]; got != "not_found" {
		t.Errorf("outcome = %q, want not_found", got)
	}
	if got := labels[0]["resource"]; got != "User" {
		t.Errorf("resource = %q, want User", got)
	}
	if got := labels[0]["backend"]; got != "stub" {
		t.Errorf("backend = %q, want stub", got)
	}
}

func TestOutcomesAreDistinguished(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{nil, "ok"},
		{fmt.Errorf("%w: x", store.ErrNotFound), "not_found"},
		{fmt.Errorf("%w: x", store.ErrAlreadyExists), "conflict"},
		{fmt.Errorf("%w: x", store.ErrUnimplemented), "unimplemented"},
		{errors.New("connection reset"), "error"},
	} {
		r := newRecorder()
		db := instrumented(&stub{err: tc.err}, r)
		_, _ = db.Get(context.Background(), userRes, "x")

		labels := r.labelsFor("database_operations_total")
		if len(labels) != 1 || labels[0]["outcome"] != tc.want {
			t.Errorf("err %v -> outcome %v, want %q", tc.err, labels, tc.want)
		}
	}
}

// A page taking a second because it returned ten thousand rows and one taking a
// second because the query is bad look identical without this.
func TestRowsReadIsCounted(t *testing.T) {
	r := newRecorder()
	db := instrumented(&stub{items: 250}, r)

	if _, err := db.List(context.Background(), userRes, store.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := len(r.labelsFor("database_rows_read_total")); got != 1 {
		t.Fatalf("rows counter recorded %d times, want 1", got)
	}
}

// The record's key must never become a label: it is unbounded, and a label per
// key is how a metrics pipeline falls over at the scale this was added for.
func TestKeysAreNotLabels(t *testing.T) {
	r := newRecorder()
	db := instrumented(&stub{}, r)

	for i := range 100 {
		_, _ = db.Get(context.Background(), userRes, fmt.Sprintf("user-%d", i))
	}
	seen := map[string]bool{}
	for _, l := range r.labelsFor("database_operations_total") {
		seen[fmt.Sprint(l)] = true
	}
	if len(seen) != 1 {
		t.Errorf("100 distinct keys produced %d label sets; keys leaked into labels", len(seen))
	}
}

// batcherStub has a bulk read; plainStub does not. Instrumenting must not change
// which is which.
type batcherStub struct{ stub }

func (b *batcherStub) CreateMany(context.Context, *store.Resource, []proto.Message) ([]store.WriteResult, error) {
	return nil, nil
}

func (b *batcherStub) GetMany(_ context.Context, _ *store.Resource, keys []string) ([]proto.Message, error) {
	b.gets.Add(1)
	out := make([]proto.Message, len(keys))
	for i := range out {
		out[i] = timestamppb.Now()
	}
	return out, nil
}

func TestInstrumentPreservesCapabilities(t *testing.T) {
	r := newRecorder()

	// A driver with a bulk read keeps it.
	withBulk := instrumented(&batcherStub{}, r)
	if _, ok := withBulk.Driver.(store.Batcher); !ok {
		t.Error("instrumenting lost the Batcher capability")
	}

	// One without must not gain it — a capability that exists only because
	// something was instrumented would fail at the call instead of at
	// construction.
	withoutBulk := instrumented(&stub{}, r)
	if _, ok := withoutBulk.Driver.(store.Batcher); ok {
		t.Error("instrumenting invented a Batcher capability")
	}

	// And the DB's own capability fields come across untouched.
	if withBulk.Tx == nil || withBulk.Schema == nil || withBulk.Graph == nil || withBulk.Series == nil {
		t.Error("a capability field was lost")
	}
}

// With neither a meter nor a logger there is nothing to record, so there should
// be no wrapper to pay for.
func TestInstrumentWithNothingIsAPassthrough(t *testing.T) {
	db := store.Build(&stub{}, "stub", "", nil)
	if store.Instrument(db) != db {
		t.Error("Instrument with no options still wrapped the driver")
	}
}

// The N+1 this used to have: 500 refs cost 500 round trips even where the driver
// could read them in one.
func TestResolveBatchesThroughBatcher(t *testing.T) {
	reg := store.NewRegistry(*userRes)
	refs := make([]store.Ref, 500)
	for i := range refs {
		refs[i] = store.Ref{Resource: "User", Key: fmt.Sprintf("u-%d", i)}
	}

	b := &batcherStub{}
	if _, err := store.Resolve[proto.Message](context.Background(), b, reg, refs); err != nil {
		t.Fatal(err)
	}
	if got := b.gets.Load(); got > 2 {
		t.Errorf("Resolve over 500 refs cost %d round trips, want about 1", got)
	}

	// A driver with no bulk read still works, one at a time.
	p := &stub{}
	if _, err := store.Resolve[proto.Message](context.Background(), p, reg, refs); err != nil {
		t.Fatal(err)
	}
	if got := p.gets.Load(); got != 500 {
		t.Errorf("without a Batcher, Resolve made %d reads, want 500", got)
	}
}
