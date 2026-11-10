package nats

// What can be checked without a broker, plus the publish path's bookkeeping,
// which needs a server that leaves an ack unanswered and so runs one
// in-process.

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/the-protobuf-project/runtime-go/streams"
)

func TestHeadersMapBothWaysWithoutTheServersOwn(t *testing.T) {
	headers := streams.Headers{"lang": "go", "traceparent": "00-ab"}
	h := toHeader(headers)
	if h.Get("lang") != "go" {
		t.Errorf("lang = %q, want go", h.Get("lang"))
	}
	h.Set("Nats-Msg-Id", "m-1")
	h.Set("Nats-TTL", "5")
	h.Add("traceparent", "second")
	if got := fromHeader(h); !maps.Equal(got, headers) {
		t.Errorf("fromHeader = %v, want %v (first value, no Nats- keys)", got, headers)
	}
	if got := fromHeader(nil); got != nil {
		t.Errorf("fromHeader(nil) = %v, want nil", got)
	}
	if got := fromHeader(gonats.Header{"Nats-Msg-Id": {"x"}}); got != nil {
		t.Errorf("only server headers = %v, want nil", got)
	}
}

func TestTheNatsPrefixIsReserved(t *testing.T) {
	for _, bad := range []streams.Stream{
		{Headers: streams.Headers{"Nats-Msg-Id": "x"}},
		{Headers: streams.Headers{"bad key": "x"}},
		{Headers: streams.Headers{"lang": "two\nlines"}},
	} {
		if err := checkStreamHeaders(bad); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("checkStreamHeaders(%v) = %v, want ErrInvalidHeader", bad.Headers, err)
		}
	}
	s := streams.Stream{Headers: streams.Headers{"lang": "go"}}
	if err := checkStreamHeaders(s); err != nil {
		t.Errorf("checkStreamHeaders(lang) = %v", err)
	}
	if _, err := outgoing(s, streams.NewOptions(streams.Header("Nats-TTL", "1"))); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("outgoing(Nats-TTL) = %v, want ErrInvalidHeader", err)
	}
	sent, err := outgoing(s, streams.NewOptions(streams.Header("lang", "rust")))
	if err != nil || sent.Get("lang") != "rust" {
		t.Errorf("outgoing = %v, %v; want the publish's lang to win", sent, err)
	}
}

func TestRetainIsRefusedByName(t *testing.T) {
	if err := refuseRetain(streams.Options{}, "x"); err != nil {
		t.Errorf("refuseRetain without Retain = %v", err)
	}
	err := refuseRetain(streams.NewOptions(streams.Retain()), "why")
	if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "retain") || !strings.Contains(err.Error(), "why") {
		t.Errorf("refuseRetain = %v, want ErrUnsupported naming retain and the reason", err)
	}
}

func TestConfigOptions(t *testing.T) {
	cfg := newConfig(WithQueueGroup("workers"), WithAckWait(2*time.Second), WithMaxDeliver(3), WithInactiveThreshold(4*time.Second))
	if cfg.queue != "workers" || cfg.ackWait != 2*time.Second || cfg.maxDeliver != 3 || cfg.inactiveThreshold != 4*time.Second {
		t.Errorf("newConfig = %+v", cfg)
	}
}

func TestJetStreamSettingsDefaultToTheServer(t *testing.T) {
	cfg := newConfig()
	if cfg.ackWait != 0 || cfg.maxDeliver != 0 {
		t.Errorf("ackWait %s, maxDeliver %d; want both left to the server", cfg.ackWait, cfg.maxDeliver)
	}
	if cfg.inactiveThreshold != 30*time.Second {
		t.Errorf("inactiveThreshold = %s, want 30s", cfg.inactiveThreshold)
	}
}

func TestSafeNameMatchesRust(t *testing.T) {
	if got := safeName("01J:orders.us/eu\\x y\tz\n*>"); got != "01J:orders-us-eu-x-y-z---" {
		t.Errorf("safeName = %q", got)
	}
	if got := safeName("plain-id_1"); got != "plain-id_1" {
		t.Errorf("safeName = %q", got)
	}
}

func TestMetadataRoundTrips(t *testing.T) {
	p := &jsStreams{}
	in := streams.Stream{ID: "ignored", Name: "orders", Description: "placed and shipped", Subjects: []string{"orders.*"}, UserID: "ada"}
	cfg := p.configFor("S-1", in)
	if cfg.Name != "S-1" || cfg.Metadata[metaName] != "orders" || cfg.Metadata[metaUser] != "ada" || cfg.Metadata[metaActive] != "true" {
		t.Errorf("configFor = %+v", cfg)
	}
	back := toStream(&cfg)
	want := p.streamFrom("S-1", in)
	if back.ID != want.ID || back.Name != want.Name || back.Description != want.Description ||
		back.UserID != want.UserID || !back.Active || !slices.Equal(back.Subjects, want.Subjects) || back.Headers != nil {
		t.Errorf("toStream = %+v, want %+v", back, want)
	}
}

func TestStreamHeadersLiveInOneMetadataKey(t *testing.T) {
	p := &jsStreams{}
	plain := p.configFor("S", streams.Stream{Name: "orders"})
	if _, ok := plain.Metadata[metaHeaders]; ok {
		t.Error("a stream without headers wrote the headers key")
	}
	if !plain.AllowMsgTTL {
		t.Error("configFor left AllowMsgTTL unset")
	}
	if h := toStream(&plain).Headers; h != nil {
		t.Errorf("headers = %v, want none", h)
	}

	in := streams.Stream{Name: "orders", Subjects: []string{"orders.*"}}.WithHeader("site", "plant-3").WithHeader("lang", "go")
	cfg := p.configFor("S", in)
	if got := cfg.Metadata[metaHeaders]; got != `{"lang":"go","site":"plant-3"}` {
		t.Errorf("metadata = %s", got)
	}
	if got := toStream(&cfg).Headers; !maps.Equal(got, in.Headers) {
		t.Errorf("headers = %v, want %v", got, in.Headers)
	}

	cfg.Metadata[metaHeaders] = "not json"
	if got := toStream(&cfg).Headers; got != nil {
		t.Errorf("garbled headers = %v, want none", got)
	}
	if _, err := storedHeaders(&cfg); err == nil {
		t.Error("storedHeaders accepted garbage")
	}
}

func TestExpiryIsWholeSecondsRoundedUp(t *testing.T) {
	for in, want := range map[time.Duration]int64{
		5 * time.Second:         5,
		1500 * time.Millisecond: 2,
		time.Second:             1,
	} {
		if got, err := ttlSeconds(in); err != nil || got != want {
			t.Errorf("ttlSeconds(%s) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := ttlSeconds(999 * time.Millisecond); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("ttlSeconds(999ms) = %v, want ErrUnsupported", err)
	}
}

func TestAForeignStreamReadsInactiveAndUnnamed(t *testing.T) {
	s := toStream(&jetstream.StreamConfig{Name: "FOREIGN", Subjects: []string{"x"}})
	if s.ID != "FOREIGN" || s.Name != "" || s.Description != "" || s.Active {
		t.Errorf("toStream = %+v", s)
	}
}

func TestBudgetTakesAndGivesBack(t *testing.T) {
	b := newBudget(3)
	if !b.tryTake(2) || b.tryTake(2) {
		t.Fatal("tryTake did not honor the count")
	}
	done := make(chan error, 1)
	go func() { done <- b.take(t.Context(), nil, 2) }()
	select {
	case <-done:
		t.Fatal("take did not wait for permits")
	case <-time.After(50 * time.Millisecond):
	}
	b.give(1)
	if err := <-done; err != nil {
		t.Fatalf("take: %v", err)
	}
	if b.available() != 0 {
		t.Errorf("available = %d, want 0", b.available())
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := b.take(ctx, nil, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("take past ctx = %v", err)
	}
	closed := make(chan struct{})
	close(closed)
	if err := b.take(t.Context(), closed, 1); !errors.Is(err, errClosed) {
		t.Errorf("take on a closed provider = %v", err)
	}
}

func TestACreditGivesBackSpareAndFinishedAtRelease(t *testing.T) {
	b := newBudget(maxPending)
	c := &credit{b: b}
	if err := c.take(t.Context(), 4); err != nil {
		t.Fatal(err)
	}
	if b.available() != maxPending-4 {
		t.Fatalf("available = %d", b.available())
	}
	c.spend()
	c.spend()
	c.finish()
	c.finish()
	c.release()
	if b.available() != maxPending {
		t.Errorf("available after release = %d, want %d", b.available(), maxPending)
	}
	if c.tryTake(10_000); c.spare != chunk {
		t.Errorf("tryTake took %d, want at most a chunk of %d", c.spare, chunk)
	}
	c.release()
}

// silent returns a JetStream provider and a manager whose stream has been
// deleted behind its back, with a core subscriber on the subject that never
// replies: a publish there is sent, and its ack never comes.
func silent(t *testing.T) (*jsStreams, *jsManager, string) {
	t.Helper()

	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)

	nc, err := gonats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	s, err := UseJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	p := s.(*jsStreams)
	const subject = "silent.a"
	stream, err := p.Create(t.Context(), streams.Stream{Name: "silent", Subjects: []string{subject}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := p.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatal(err)
	}
	if perr := p.Delete(t.Context(), stream.ID); perr != nil {
		t.Fatal(perr)
	}
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return p, m.(*jsManager), subject
}

// waitAvailable waits up to within for the budget to be whole again.
func waitAvailable(t *testing.T, b *budget, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for b.available() != maxPending {
		if time.Now().After(deadline) {
			t.Fatalf("available = %d after %s, want %d", b.available(), within, maxPending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestAPublishNobodyAcksGivesUpAtTheAckDeadline(t *testing.T) {
	p, m, subject := silent(t)

	start := time.Now()
	_, err := m.Publish(t.Context(), subject, "x")
	if !errors.Is(err, jetstream.ErrAsyncPublishTimeout) {
		t.Fatalf("Publish = %v, want the ack timeout", err)
	}
	if waited := time.Since(start); waited < ackTimeout {
		t.Errorf("gave up after %s, before the %s deadline", waited, ackTimeout)
	}
	if p.budget.available() != maxPending {
		t.Errorf("available = %d, want every permit back", p.budget.available())
	}
}

func TestAnAbandonedWaitKeepsItsPermitUntilTheAckCannotCome(t *testing.T) {
	p, m, subject := silent(t)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := m.Publish(ctx, subject, "x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish = %v, want the caller's deadline", err)
	}
	if got := p.budget.available(); got != maxPending-1 {
		t.Errorf("available = %d right after cancel, want %d: the open wait keeps its permit", got, maxPending-1)
	}
	waitAvailable(t, p.budget, ackTimeout+time.Second)
}

func TestClosingTheProviderEndsWaitsAndRefusesPublishes(t *testing.T) {
	p, m, subject := silent(t)

	done := make(chan error, 1)
	go func() {
		_, err := m.Publish(t.Context(), subject, "x")
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, errClosed) {
			t.Errorf("waiting Publish = %v, want errClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a waiting publish outlived Close")
	}
	if _, err := m.Publish(t.Context(), subject, "x"); !errors.Is(err, errClosed) {
		t.Errorf("Publish after Close = %v, want errClosed", err)
	}
	if _, err := m.PublishBatch(t.Context(), subject, []any{"x"}); !errors.Is(err, errClosed) {
		t.Errorf("PublishBatch after Close = %v, want errClosed", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("a second Close = %v", err)
	}
	waitAvailable(t, p.budget, ackTimeout+time.Second)
}
