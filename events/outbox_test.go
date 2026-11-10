package events

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// memOutbox is an outbox table in memory, with one lock per (shard, shards)
// as the advisory lock is.
type memOutbox struct {
	mu        sync.Mutex
	rows      []OutboxRow
	published map[string]time.Time
	held      map[string]bool
	// commitFails makes the next Commit fail, as a crash between publishing
	// and marking does.
	commitFails bool
	pruned      []time.Time
}

func newMemOutbox() *memOutbox {
	return &memOutbox{published: map[string]time.Time{}, held: map[string]bool{}}
}

func (o *memOutbox) add(id, aggregate, typ string, at time.Time, payload string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rows = append(o.rows, OutboxRow{ID: id, AggregateID: aggregate, EventType: typ, Payload: []byte(payload), CreateTime: at})
}

func (o *memOutbox) Claim(_ context.Context, shard, shards, limit int, upTo time.Time) (OutboxClaim, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := fmt.Sprint(shard, "/", shards)
	if o.held[key] {
		return nil, false, nil
	}
	o.held[key] = true
	var rows []OutboxRow
	for i := range o.rows {
		r := &o.rows[i]
		if _, done := o.published[r.ID]; done || r.CreateTime.After(upTo) || ShardOf(r.AggregateID, shards) != shard {
			continue
		}
		rows = append(rows, *r)
	}
	slices.SortStableFunc(rows, func(a, b OutboxRow) int {
		if c := a.CreateTime.Compare(b.CreateTime); c != 0 {
			return c
		}
		return compare(a.ID, b.ID)
	})
	return &memClaim{o: o, key: key, rows: rows[:min(limit, len(rows))]}, true, nil
}

func compare(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (o *memOutbox) Prune(_ context.Context, before time.Time, _ int) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pruned = append(o.pruned, before)
	return nil
}

func (o *memOutbox) isPublished(id string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.published[id]
	return ok
}

type memClaim struct {
	o    *memOutbox
	key  string
	rows []OutboxRow
}

func (c *memClaim) Rows() []OutboxRow { return c.rows }

func (c *memClaim) Commit(_ context.Context, ids []string) error {
	c.o.mu.Lock()
	defer c.o.mu.Unlock()
	delete(c.o.held, c.key)
	if c.o.commitFails {
		c.o.commitFails = false
		return errors.New("connection lost")
	}
	for _, id := range ids {
		c.o.published[id] = time.Now()
	}
	return nil
}

func (c *memClaim) Release() error {
	c.o.mu.Lock()
	defer c.o.mu.Unlock()
	delete(c.o.held, c.key)
	return nil
}

// memSink records what it publishes, in order per key, and fails as scripted.
type memSink struct {
	mu   sync.Mutex
	got  []Event
	fail map[string]error // by event id
}

func (s *memSink) Deliver(_ context.Context, batch []Event) ([]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sent := make([]bool, len(batch))
	stopped := map[string]bool{}
	var errs []error
	for i := range batch {
		e := &batch[i]
		if stopped[e.PartitionKey] {
			continue
		}
		if err := s.fail[e.ID]; err != nil {
			stopped[e.PartitionKey] = true
			errs = append(errs, err)
			continue
		}
		s.got = append(s.got, *e)
		sent[i] = true
	}
	return sent, errors.Join(errs...)
}

func (s *memSink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.got))
	for i := range s.got {
		out[i] = s.got[i].ID
	}
	return out
}

// outboxFixture is the dispatcher fixture with an outbox and a sink.
func outboxFixture(t *testing.T, spec OutboxSpec) (*fixture, *memOutbox, *memSink) {
	t.Helper()
	table, sink := newMemOutbox(), &memSink{fail: map[string]error{}}
	if spec.Name == "" {
		spec.Name = "shop.orders"
	}
	f := newFixture(t, Options{})
	opts := f.d.opts
	opts.Outboxes = []*Outbox{NewOutbox(table, spec)}
	opts.Sink = sink
	f.d = NewDispatcher(f.store, f.d.feeds, opts)
	return f, table, sink
}

// One outbox row is one event: its id is the CloudEvent's, the webhook's
// webhook-id and delivery name, and the message the sink publishes, and the
// bytes POSTed are the bytes published.
func TestOutboxIsOneIdentityEndToEnd(t *testing.T) {
	f, table, sink := outboxFixture(t, OutboxSpec{})
	secret := f.hook(t, "webhooks/a", nil)
	f.round(t)
	at := f.clock.now()
	const id = "01HZY000000000000000000001"
	table.add(id, "01HZYAGG00000000000000000A", "shop.order.created", at, `{"name": "orders/1", "total": 7}`)
	f.clock.add(time.Second)
	f.round(t)

	if got := sink.ids(); !slices.Equal(got, []string{id}) {
		t.Fatalf("published %v, want [%s]", got, id)
	}
	e := sink.got[0]
	if e.Subject != "orders/1" || e.PartitionKey != "01HZYAGG00000000000000000A" || e.Type != "shop.order.created" {
		t.Fatalf("event %+v", e)
	}
	if f.recv.count() != 1 {
		t.Fatalf("%d POSTs, want 1", f.recv.count())
	}
	req, body := f.recv.got[0], f.recv.bodies[0]
	if req.Header.Get(HeaderID) != id || body != string(e.Body) ||
		!Verify(secret, id, req.Header.Get(HeaderTimestamp), []byte(body), req.Header.Get(HeaderSignature)) {
		t.Fatalf("the webhook got id %q and body %s; the sink %s", req.Header.Get(HeaderID), body, e.Body)
	}
	var ev map[string]any
	if err := json.Unmarshal(e.Body, &ev); err != nil || ev["id"] != id || ev["subject"] != "orders/1" {
		t.Fatalf("CloudEvent %s, %v", e.Body, err)
	}
	if ds := f.store.all(); len(ds) != 1 || ds[0].Name != DeliveryName("webhooks/a", id) {
		t.Fatalf("deliveries %+v", ds)
	}
	if !table.isPublished(id) {
		t.Fatal("the row was not marked published")
	}
}

// A row the sink does not take stays outstanding, and so do the later rows of
// its aggregate; another aggregate's rows go on.
func TestOutboxStopsOnlyTheFailedAggregate(t *testing.T) {
	f, table, sink := outboxFixture(t, OutboxSpec{})
	f.round(t)
	at := f.clock.now()
	table.add("01A", "a", "t", at, `{}`)
	table.add("01B", "a", "t", at.Add(time.Millisecond), `{}`)
	table.add("01C", "b", "t", at.Add(2*time.Millisecond), `{}`)
	sink.fail["01A"] = errors.New("broker down")
	f.clock.add(time.Second)
	if err := f.d.Round(f.ctx); err == nil {
		t.Fatal("a failed publish was not reported")
	}
	if table.isPublished("01A") || table.isPublished("01B") || !table.isPublished("01C") {
		t.Fatalf("published: 01A %v 01B %v 01C %v; want only 01C",
			table.isPublished("01A"), table.isPublished("01B"), table.isPublished("01C"))
	}
	// The shard backs off, then sends a's rows in order once the broker is back.
	delete(sink.fail, "01A")
	f.round(t)
	if table.isPublished("01A") {
		t.Fatal("a backing-off shard was drained")
	}
	f.clock.add(OutboxBackoff(1))
	f.round(t)
	if got := sink.ids(); !slices.Equal(got, []string{"01C", "01A", "01B"}) {
		t.Fatalf("published %v, want 01C then a's in order", got)
	}
}

// A crash between publishing and marking republishes under the same id; the
// webhook delivery collides on its name and is not POSTed twice.
func TestOutboxRepublishesUnderTheSameID(t *testing.T) {
	f, table, sink := outboxFixture(t, OutboxSpec{})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	table.add("01A", "a", "t", f.clock.now(), `{}`)
	table.commitFails = true
	f.clock.add(time.Second)
	if err := f.d.Round(f.ctx); err == nil {
		t.Fatal("a failed commit was not reported")
	}
	f.clock.add(OutboxBackoff(1))
	f.round(t)
	if got := sink.ids(); !slices.Equal(got, []string{"01A", "01A"}) || !table.isPublished("01A") {
		t.Fatalf("published %v, want 01A twice and then marked", got)
	}
	if f.recv.count() != 1 || len(f.store.all()) != 1 {
		t.Fatalf("%d POSTs, %d deliveries; want one of each", f.recv.count(), len(f.store.all()))
	}
}

// An undeliverable event stalls its aggregate and is counted as failed, not
// dropped.
func TestOutboxStallsAnUndeliverableEvent(t *testing.T) {
	f, table, sink := outboxFixture(t, OutboxSpec{})
	f.round(t)
	table.add("01A", "a", "t", f.clock.now(), `{}`)
	sink.fail["01A"] = fmt.Errorf("%w: subject not declared", ErrUndeliverable)
	f.clock.add(time.Second)
	err := f.d.Round(f.ctx)
	if !errors.Is(err, ErrUndeliverable) || table.isPublished("01A") {
		t.Fatalf("Round = %v, published %v; want ErrUndeliverable and the row outstanding", err, table.isPublished("01A"))
	}
}

// A row written after the round's read of the webhooks, less HookSettle, is
// left to the next round, as a feed's changes are.
func TestOutboxHonorsHookSettle(t *testing.T) {
	f, table, sink := outboxFixture(t, OutboxSpec{})
	f.round(t)
	table.add("01A", "a", "t", f.clock.now(), `{}`)
	f.round(t)
	if len(sink.ids()) != 0 {
		t.Fatal("a row newer than the round's cut-off was drained")
	}
	f.clock.add(time.Second)
	f.round(t)
	if len(sink.ids()) != 1 {
		t.Fatal("the row was not drained the round after")
	}
}

// A shard another server holds is skipped this round.
func TestOutboxSkipsAContendedShard(t *testing.T) {
	f, table, sink := outboxFixture(t, OutboxSpec{})
	f.round(t)
	table.add("01A", "a", "t", f.clock.now(), `{}`)
	table.held["0/1"] = true
	f.clock.add(time.Second)
	f.round(t)
	if len(sink.ids()) != 0 {
		t.Fatal("a held shard was drained")
	}
}

// Shards split the table by aggregate, and every row is drained once.
func TestOutboxShards(t *testing.T) {
	f, table, sink := outboxFixture(t, OutboxSpec{Shards: 4})
	f.round(t)
	at := f.clock.now()
	for i := range 40 {
		table.add(fmt.Sprintf("01%02d", i), fmt.Sprintf("agg-%d", i%7), "t", at.Add(time.Duration(i)*time.Millisecond), `{}`)
	}
	f.clock.add(time.Second)
	f.round(t)
	got := sink.ids()
	if len(got) != 40 {
		t.Fatalf("published %d rows, want 40", len(got))
	}
	// Within an aggregate, the order written.
	last := map[string]string{}
	for _, e := range sink.got {
		if prev, ok := last[e.PartitionKey]; ok && prev > e.ID {
			t.Fatalf("aggregate %s published %s after %s", e.PartitionKey, e.ID, prev)
		}
		last[e.PartitionKey] = e.ID
	}
}

// A feed's changes reach the sink too, and the cursor does not pass one the
// sink did not take.
func TestFeedChangesReachTheSink(t *testing.T) {
	f, _, sink := outboxFixture(t, OutboxSpec{})
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	id := EventID("orders/1", "e1")
	sink.fail[id] = errors.New("broker down")
	f.clock.add(time.Second)
	if err := f.d.Round(f.ctx); err == nil {
		t.Fatal("a failed publish was not reported")
	}
	if cur, _ := f.store.GetCursor(f.ctx, "orders"); cur.Position.After(f.clock.now().Add(-2 * time.Second)) {
		t.Fatalf("the cursor moved to %v past an unpublished change", cur.Position)
	}
	delete(sink.fail, id)
	f.round(t)
	if got := sink.ids(); !slices.Equal(got, []string{id}) || sink.got[0].PartitionKey != "orders/1" {
		t.Fatalf("published %v", got)
	}
	// Remembered once the sink took it: the next reread does not send it again.
	f.round(t)
	if len(sink.ids()) != 1 {
		t.Fatalf("a reread published again: %v", sink.ids())
	}
}

// Published rows past retention are pruned each round.
func TestOutboxPrunes(t *testing.T) {
	f, table, _ := outboxFixture(t, OutboxSpec{Retention: time.Hour})
	f.round(t)
	if len(table.pruned) != 1 || !table.pruned[0].Equal(f.clock.now().Add(-time.Hour)) {
		t.Fatalf("pruned %v", table.pruned)
	}
}

func TestDefaultSubject(t *testing.T) {
	for payload, want := range map[string]string{
		`{"name": "orders/1"}`: "orders/1",
		`{"name": 7}`:          "agg",
		`{}`:                   "agg",
		`[1]`:                  "agg",
	} {
		if got := DefaultSubject(OutboxRow{AggregateID: "agg", Payload: []byte(payload)}); got != want {
			t.Errorf("DefaultSubject(%s) = %q, want %q", payload, got, want)
		}
	}
}

func TestOutboxBackoff(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	for i, w := range want {
		if got := OutboxBackoff(i + 1); got != w {
			t.Errorf("OutboxBackoff(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := OutboxBackoff(30); got != time.Minute {
		t.Errorf("OutboxBackoff(30) = %v, want the minute cap", got)
	}
}

// The contract's lock key and shard function, against values computed
// independently (Python's hashlib).
func TestOutboxContractVectors(t *testing.T) {
	if got := LockKey("shop.orders_outbox", 0, 1); got != -4484193236283674297 {
		t.Errorf("LockKey(shop.orders_outbox, 0, 1) = %d", got)
	}
	if got := LockKey("shop.orders_outbox", 3, 4); got != 1578950922978399321 {
		t.Errorf("LockKey(shop.orders_outbox, 3, 4) = %d", got)
	}
	for agg, want := range map[string]int{"01HZY0000000000000000000A1": 0, "orders/1": 1, "42": 0} {
		if got := ShardOf(agg, 4); got != want {
			t.Errorf("ShardOf(%q, 4) = %d, want %d", agg, got, want)
		}
	}
	if ShardOf("anything", 1) != 0 {
		t.Error("one shard is shard 0")
	}
}

func TestQuoteTable(t *testing.T) {
	for in, want := range map[string]string{"orders_outbox": `"orders_outbox"`, "shop.orders_outbox": `"shop"."orders_outbox"`} {
		if got, err := quoteTable(in); err != nil || got != want {
			t.Errorf("quoteTable(%q) = %q, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "a.b.c", `x"; drop table t; --`, "1x", "shop.orders outbox"} {
		if _, err := quoteTable(bad); err == nil {
			t.Errorf("quoteTable(%q) accepted", bad)
		}
	}
}

// execRecorder is an Execer that records the statement it is given.
type execRecorder struct {
	query string
	args  []any
}

func (e *execRecorder) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	return driver.RowsAffected(1), nil
}

func TestAppendOutbox(t *testing.T) {
	rec := &execRecorder{}
	id, err := AppendOutbox(context.Background(), rec, "shop.orders_outbox", OutboxEntry{
		AggregateID: "01AGG", Type: "shop.order.created", Payload: []byte(`{"name":"orders/1"}`),
	})
	if err != nil || len(id) != 26 {
		t.Fatalf("AppendOutbox = %q, %v", id, err)
	}
	want := `INSERT INTO "shop"."orders_outbox" (id, aggregate_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4::jsonb, clock_timestamp())`
	if rec.query != want || rec.args[0] != id || rec.args[1] != "01AGG" || rec.args[3] != `{"name":"orders/1"}` {
		t.Fatalf("executed %q %v", rec.query, rec.args)
	}
	for _, e := range []OutboxEntry{
		{AggregateID: "a", Payload: []byte(`{}`)},                  // no type
		{Type: "t", Payload: []byte(`{}`)},                         // no aggregate
		{AggregateID: "a", Type: "t", Payload: []byte(`not json`)}, // not JSON
	} {
		if _, err := AppendOutbox(context.Background(), rec, "t_outbox", e); err == nil {
			t.Errorf("AppendOutbox(%+v) accepted", e)
		}
	}
	if _, err := AppendOutbox(context.Background(), rec, "bad name", OutboxEntry{AggregateID: "a", Type: "t", Payload: []byte(`{}`)}); err == nil {
		t.Error("AppendOutbox accepted an unquotable table")
	}
}
