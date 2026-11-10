package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestFetchFollowsBackendPagination(t *testing.T) {
	calls := 0
	f := NewFeed(FeedSpec[*order]{ID: "paged", List: func(_ context.Context, q Query) ([]*order, string, error) {
		calls++
		if !q.ShowDeleted || len(q.ReadMask) != 0 || len(q.Names) != 2 {
			t.Fatalf("fetch query: %+v", q)
		}
		if q.PageToken == "" {
			return []*order{{Name: "orders/a", Etag: "a"}}, "next", nil
		}
		return []*order{{Name: "orders/b", Etag: "b"}}, "", nil
	}, Data: func(o *order) ([]byte, error) { return json.Marshal(o.Name) }})
	got, err := f.fetch(context.Background(), []string{"orders/a", "orders/b"})
	if err != nil || len(got) != 2 || calls != 2 {
		t.Fatalf("fetch: %v, %v, calls=%d", got, err, calls)
	}
	for name, row := range got {
		data, err := row.data()
		if err != nil || string(data) != `"`+name+`"` {
			t.Fatalf("data for %s: %s, %v", name, data, err)
		}
	}
}

func TestFetchRejectsIncompletePageRead(t *testing.T) {
	want := errors.New("backend unavailable")
	f := NewFeed(FeedSpec[*order]{ID: "paged", List: func(_ context.Context, q Query) ([]*order, string, error) {
		if q.PageToken == "" {
			return []*order{{Name: "orders/a"}}, "next", nil
		}
		return nil, "", want
	}})
	got, err := f.fetch(context.Background(), []string{"orders/a", "orders/b"})
	if !errors.Is(err, want) || got != nil {
		t.Fatalf("fetch returned partial success: %v, %v", got, err)
	}
}

func TestFetchRejectsRepeatedPageToken(t *testing.T) {
	f := NewFeed(FeedSpec[*order]{ID: "paged", List: func(context.Context, Query) ([]*order, string, error) { return nil, "same", nil }})
	if _, err := f.fetch(context.Background(), []string{"orders/a"}); err == nil {
		t.Fatal("repeated token accepted")
	}
}

func TestRoundRequestsSoftDeletedChanges(t *testing.T) {
	f := newFixture(t, Options{})
	f.hook(t, "webhooks/a", nil)
	f.d.feeds = []Feed{NewFeed(FeedSpec[*order]{ID: "orders", List: f.table.list,
		Change: func(o *order) Change { return Change{Time: o.Update, Type: Lifecycle(o.Create, o.Update, o.Delete)} },
		Data:   func(o *order) ([]byte, error) { return json.Marshal(o) }})}
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/deleted", Etag: "deleted", Update: f.clock.now(), Delete: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)
	if f.recv.count() != 1 {
		t.Fatal("soft-deleted change was skipped")
	}
}

func TestPausedWebhookRecordsNewChanges(t *testing.T) {
	f := newFixture(t, Options{})
	f.hook(t, "webhooks/a", func(w *Webhook) { w.Paused = true })
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/a", Etag: "a", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)
	if len(f.store.all()) != 1 || f.recv.count() != 0 {
		t.Fatal("paused webhook did not retain pending delivery")
	}
	w := f.store.webhooks["webhooks/a"]
	w.Paused = false
	f.store.webhooks[w.Name] = w
	f.round(t)
	if f.recv.count() != 1 {
		t.Fatal("resume did not deliver queued change")
	}
}
