//go:build live

package events

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// The outbox against a real PostgreSQL (docker/compose.postgres.yaml), over
// the table protoc-gen-store generates for an outbox-enabled resource.

// outboxDDL is protoc-gen-store's companion table, as its golden output has it.
const outboxDDL = `
CREATE SCHEMA IF NOT EXISTS "shop";
DROP TABLE IF EXISTS "shop"."orders_outbox";
CREATE TABLE "shop"."orders_outbox" (
    "id"  CHAR(26)  NOT NULL  PRIMARY KEY,
    "aggregate_id"  CHAR(26)  NOT NULL,
    "event_type"  VARCHAR(255)  NOT NULL,
    "payload"  JSONB  NOT NULL,
    "created_at"  TIMESTAMPTZ  NOT NULL  DEFAULT now(),
    "published_at"  TIMESTAMPTZ
);
CREATE INDEX "idx_orders_outbox_unpublished" ON "shop"."orders_outbox" ("published_at", "created_at");
CREATE INDEX "idx_orders_outbox_aggregate" ON "shop"."orders_outbox" ("aggregate_id");`

const liveTable = "shop.orders_outbox"

// liveDB connects to the test database and recreates the outbox table, or
// skips when nothing answers.
func liveDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("EVENTS_TEST_DSN")
	if dsn == "" {
		dsn = "host=127.0.0.1 port=15432 user=events password=events dbname=events sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("no PostgreSQL at %s: %v", dsn, err)
	}
	if _, err := db.ExecContext(t.Context(), outboxDDL); err != nil {
		t.Fatal(err)
	}
	return db
}

// aggregate is the n-th test aggregate's id, a CHAR(26) as the owner's is.
func aggregate(n int) string { return fmt.Sprintf("01AGG%021d", n) }

// appendCommitted writes count events of one aggregate, each in a transaction
// of its own. It reports failures with t.Error, so it may run off the test's
// goroutine.
func appendCommitted(t *testing.T, db *sql.DB, agg string, count int) []string {
	t.Helper()
	ids := make([]string, 0, count)
	for i := range count {
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Error(err)
			return ids
		}
		id, err := AppendOutbox(t.Context(), tx, liveTable, OutboxEntry{
			AggregateID: agg, Type: "shop.order.updated",
			Payload: fmt.Appendf(nil, `{"name": "orders/%s", "seq": %d}`, agg, i),
		})
		if err != nil {
			t.Error(err)
			return ids
		}
		if err := tx.Commit(); err != nil {
			t.Error(err)
			return ids
		}
		ids = append(ids, id)
	}
	return ids
}

// liveDispatcher drains the table into sink.
func liveDispatcher(t *testing.T, db *sql.DB, sink Sink, shards int) *Dispatcher {
	t.Helper()
	c, err := PostgresOutbox(db, liveTable)
	if err != nil {
		t.Fatal(err)
	}
	// The clock runs a minute ahead: created_at is the database's clock, and
	// Docker Desktop's VM may drift from the host's.
	return NewDispatcher(newMemStore(), nil, Options{
		Source: "//shop.example.com", HookSettle: time.Millisecond,
		Now:      func() time.Time { return time.Now().Add(time.Minute) },
		Outboxes: []*Outbox{NewOutbox(c, OutboxSpec{Name: "shop.orders", Shards: shards})},
		Sink:     sink,
	})
}

func outstanding(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM "shop"."orders_outbox" WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A committed write leaves one event; a rolled-back one leaves none.
func TestLiveAppendFollowsTheTransaction(t *testing.T) {
	db := liveDB(t)
	appendCommitted(t, db, aggregate(1), 1)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendOutbox(t.Context(), tx, liveTable, OutboxEntry{AggregateID: aggregate(2), Type: "t", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := outstanding(t, db); n != 1 {
		t.Fatalf("%d rows outstanding, want the committed one", n)
	}

	sink := &memSink{fail: map[string]error{}}
	d := liveDispatcher(t, db, sink, 1)
	if err := d.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(sink.got) != 1 || sink.got[0].Subject != "orders/"+aggregate(1) || outstanding(t, db) != 0 {
		t.Fatalf("published %v, %d outstanding", sink.ids(), outstanding(t, db))
	}
}

// Two servers on four shards publish every row exactly once, each aggregate
// in the order it was written.
func TestLiveTwoServersPublishEveryRowOnceInOrder(t *testing.T) {
	db := liveDB(t)
	const aggregates, each = 50, 40
	written := map[string][]string{}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for a := range aggregates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := appendCommitted(t, db, aggregate(a), each)
			mu.Lock()
			written[aggregate(a)] = ids
			mu.Unlock()
		}()
	}
	wg.Wait()

	sink := &memSink{fail: map[string]error{}}
	one, two := liveDispatcher(t, db, sink, 4), liveDispatcher(t, db, sink, 4)
	for round := 0; outstanding(t, db) > 0; round++ {
		if round > 50 {
			t.Fatalf("%d rows still outstanding after 50 rounds", outstanding(t, db))
		}
		var rounds sync.WaitGroup
		for _, d := range []*Dispatcher{one, two} {
			rounds.Add(1)
			go func() {
				defer rounds.Done()
				if err := d.Round(t.Context()); err != nil {
					t.Error(err)
				}
			}()
		}
		rounds.Wait()
	}
	got := map[string][]string{}
	seen := map[string]bool{}
	for _, e := range sink.got {
		if seen[e.ID] {
			t.Fatalf("%s published twice", e.ID)
		}
		seen[e.ID] = true
		got[e.PartitionKey] = append(got[e.PartitionKey], e.ID)
	}
	if len(seen) != aggregates*each {
		t.Fatalf("published %d events, want %d", len(seen), aggregates*each)
	}
	for agg, ids := range written {
		for i := range ids {
			if got[agg][i] != ids[i] {
				t.Fatalf("aggregate %s published %v, written %v", agg, got[agg], ids)
			}
		}
	}
}

// crashSink publishes, then kills the claim's connection before it commits,
// as a process dying between the two would.
type crashSink struct {
	memSink
	db    *sql.DB
	crash bool
}

func (s *crashSink) Deliver(ctx context.Context, batch []Event) ([]bool, error) {
	sent, err := s.memSink.Deliver(ctx, batch)
	if s.crash {
		s.crash = false
		if _, kerr := s.db.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE datname = current_database() AND state = 'idle in transaction' AND pid <> pg_backend_pid()`); kerr != nil {
			return sent, errors.Join(err, kerr)
		}
	}
	return sent, err
}

// A crash between publishing and marking republishes the same ids.
func TestLiveCrashBeforeCommitRepublishesTheSameIDs(t *testing.T) {
	db := liveDB(t)
	ids := appendCommitted(t, db, aggregate(7), 3)
	sink := &crashSink{memSink: memSink{fail: map[string]error{}}, db: db, crash: true}
	d := liveDispatcher(t, db, sink, 1)
	if err := d.Round(t.Context()); err == nil {
		t.Fatal("a claim whose connection died committed")
	}
	if outstanding(t, db) != 3 {
		t.Fatalf("%d outstanding after the crash, want 3", outstanding(t, db))
	}
	d.opts.Now = func() time.Time { return time.Now().Add(3 * time.Minute) } // past the backoff
	if err := d.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := append(append([]string{}, ids...), ids...)
	if got := sink.ids(); fmt.Sprint(got) != fmt.Sprint(want) || outstanding(t, db) != 0 {
		t.Fatalf("published %v, want %v; %d outstanding", got, want, outstanding(t, db))
	}
}

// The claim reads the unpublished index, and pruning deletes only published
// rows past retention.
func TestLiveClaimPlanAndPrune(t *testing.T) {
	db := liveDB(t)
	appendCommitted(t, db, aggregate(1), 3)
	if _, err := db.ExecContext(t.Context(), `ANALYZE "shop"."orders_outbox"`); err != nil {
		t.Fatal(err)
	}
	o := &pgOutbox{db: db, table: liveTable, quoted: `"shop"."orders_outbox"`}
	q, args := o.claimQuery(0, 1, 500, time.Now())
	rows, err := db.QueryContext(t.Context(), "EXPLAIN "+q, args...)
	if err != nil {
		t.Fatal(err)
	}
	var plan string
	for rows.Next() {
		var line string
		_ = rows.Scan(&line)
		plan += line + "\n"
	}
	_ = rows.Close()
	t.Logf("claim plan:\n%s", plan)

	if _, err := db.ExecContext(t.Context(), `UPDATE "shop"."orders_outbox" SET published_at = now() - interval '2 hours'
		WHERE id = (SELECT min(id) FROM "shop"."orders_outbox")`); err != nil {
		t.Fatal(err)
	}
	if err := o.Prune(t.Context(), time.Now().Add(-time.Hour), 10); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM "shop"."orders_outbox"`).Scan(&left); err != nil || left != 2 {
		t.Fatalf("%d rows left, %v; want the two unpublished", left, err)
	}
}
