package cdn_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cloudflare/cloudflare-go/v7"
	"github.com/cloudflare/cloudflare-go/v7/option"

	"github.com/the-protobuf-project/runtime-go/cache/cdn"
)

const zone = "zone1"

// purges records every purge body the zone received.
type purges struct {
	mu     sync.Mutex
	bodies []map[string]any
	status int
}

func newZone(t *testing.T) (*purges, *cloudflare.Client) {
	t.Helper()
	p := &purges{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost || r.URL.Path != "/zones/"+zone+"/purge_cache" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":7003,"message":"no route"}],"messages":[],"result":null}`))
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		p.bodies = append(p.bodies, body)
		if p.status != http.StatusOK {
			w.WriteHeader(p.status)
			_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":1134,"message":"Unable to purge, rate limit reached."}],"messages":[],"result":null}`))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"messages":[],"result":{"id":"p1"}}`))
	}))
	t.Cleanup(srv.Close)
	return p, cloudflare.NewClient(option.WithBaseURL(srv.URL+"/"), option.WithAPIToken("test"), option.WithMaxRetries(0))
}

func TestEachFormSendsItsField(t *testing.T) {
	got, cf := newZone(t)
	edge := cdn.NewPurger(cf, zone)
	ctx := t.Context()

	steps := []struct {
		field string
		run   func() error
	}{
		{"files", func() error { return edge.URLs(ctx, "https://example.com/a.css") }},
		{"tags", func() error { return edge.Tags(ctx, "book-42") }},
		{"hosts", func() error { return edge.Hosts(ctx, "www.example.com") }},
		{"prefixes", func() error { return edge.Prefixes(ctx, "www.example.com/blog/") }},
		{"purge_everything", func() error { return edge.Everything(ctx) }},
	}
	for i, s := range steps {
		if err := s.run(); err != nil {
			t.Fatalf("%s: %v", s.field, err)
		}
		body := got.bodies[i]
		if _, ok := body[s.field]; !ok || len(body) != 1 {
			t.Fatalf("%s: sent %v", s.field, body)
		}
	}
}

func TestLongListsGoInPieces(t *testing.T) {
	got, cf := newZone(t)
	tags := make([]string, 250)
	for i := range tags {
		tags[i] = "t" + strconv.Itoa(i)
	}
	if err := cdn.NewPurger(cf, zone).Tags(t.Context(), tags...); err != nil {
		t.Fatal(err)
	}
	if len(got.bodies) != 3 {
		t.Fatalf("sent %d requests, want 3", len(got.bodies))
	}
	var sent int
	for _, b := range got.bodies {
		sent += len(b["tags"].([]any))
	}
	if sent != 250 {
		t.Fatalf("sent %d tags", sent)
	}
}

func TestNothingToPurgeSendsNothing(t *testing.T) {
	got, cf := newZone(t)
	if err := cdn.NewPurger(cf, zone).URLs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(got.bodies) != 0 {
		t.Fatalf("sent %v", got.bodies)
	}
}

func TestRefusalsReachTheCaller(t *testing.T) {
	got, cf := newZone(t)
	got.status = http.StatusTooManyRequests
	err := cdn.NewPurger(cf, zone).Tags(t.Context(), "a")
	if err == nil || !strings.Contains(err.Error(), "rate limit") {
		t.Fatalf("got %v", err)
	}
	if err := cdn.NewPurger(nil, zone).Everything(t.Context()); err == nil {
		t.Fatal("a purger without a client purged")
	}
}
