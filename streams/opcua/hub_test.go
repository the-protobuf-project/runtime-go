package opcua_test

// The cases runtime-rs's tests/opcua.rs adds, against gopcua's in-process
// server: refusals of what a Write cannot carry, sequential batches, the one
// shared subscription, namespace URIs, and reconnects.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsopcua "github.com/the-protobuf-project/runtime-go/streams/opcua"
)

// monitored is how many subscriptions and monitored items srv holds.
func monitored(srv *server.Server) (subs, items int) {
	srv.SubscriptionService.Mu.Lock()
	subs = len(srv.SubscriptionService.Subs)
	srv.SubscriptionService.Mu.Unlock()
	srv.MonitoredItemService.Mu.Lock()
	items = len(srv.MonitoredItemService.Items)
	srv.MonitoredItemService.Mu.Unlock()
	return subs, items
}

// eventually fails the test unless check holds within ten seconds.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// client dials endpoint with a short reconnect interval, for the restart
// tests.
func client(t *testing.T, endpoint string) *gopcua.Client {
	t.Helper()
	c, err := gopcua.NewClient(endpoint,
		gopcua.SecurityMode(ua.MessageSecurityModeNone),
		gopcua.AuthAnonymous(),
		gopcua.ReconnectInterval(200*time.Millisecond),
		gopcua.RequestTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// use binds a stream over subjects through a store on c.
func use(t *testing.T, c *gopcua.Client, subjects ...string) (streams.Streams, streams.Manager) {
	t.Helper()
	s := streamsopcua.Use(c, streamsopcua.WithPublishingInterval(20*time.Millisecond))
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	stream, err := s.Create(t.Context(), streams.Stream{Name: "line1", Subjects: subjects})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return s, m
}

// value decodes msg into an int32.
func value(t *testing.T, msg streams.Message) int32 {
	t.Helper()
	var got int32
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return got
}

// A subject that is not a node id fails at declaration, not at first use, and
// one the stream does not declare is refused by name.
func TestDeclarationsAreNodeIDs(t *testing.T) {
	s, m := connect(t)
	ctx := t.Context()
	for _, subject := range []string{"orders.placed", "", "ns=x;s=a", "nsu=;s=a", "nsu=urn:a", "nsu=urn:a;ns=2;s=a"} {
		_, err := s.Create(ctx, streams.Stream{Subjects: []string{subject}})
		if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "node id") {
			t.Errorf("Create(%q) = %v, want ErrUnsupported naming a node id", subject, err)
		}
	}
	if _, err := s.Create(ctx, streams.Stream{Subjects: []string{"nsu=urn:plant%3bline;i=1001", "i=2258", "ns=2;g=72962b91-fa75-4ae6-8d28-b404dc7daf63"}}); err != nil {
		t.Errorf("Create with every form: %v", err)
	}
	if _, err := m.Subscribe(ctx, "ns=9;s=Nowhere"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe = %v, want ErrUnknownSubject", err)
	}
}

// A Write has nowhere to put headers, nothing that could expire, and no retain
// flag: each is refused by name rather than dropped.
func TestHeadersExpiryAndRetainAreRefused(t *testing.T) {
	s, m := connect(t)
	ctx := t.Context()

	stream := streams.Stream{Name: "h", Subjects: []string{temperature}}.WithHeader("lang", "go")
	if _, err := s.Create(ctx, stream); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "headers") {
		t.Errorf("Create with headers = %v", err)
	}
	created, _ := s.Create(ctx, streams.Stream{Name: "h", Subjects: []string{temperature}})
	if _, err := s.Update(ctx, created.ID, stream); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "headers") {
		t.Errorf("Update with headers = %v", err)
	}

	b, _ := streams.AsBatch(m)
	for _, tc := range []struct {
		opt  streams.Option
		says string
	}{
		{streams.Header("lang", "go"), "headers"},
		{streams.Expires(5 * time.Second), "expire"},
		{streams.Retain(), "retain"},
	} {
		if _, err := m.Publish(ctx, temperature, 1.0, tc.opt); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), tc.says) {
			t.Errorf("Publish (%s) = %v", tc.says, err)
		}
		if _, err := b.PublishBatch(ctx, temperature, []any{1.0}, tc.opt); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), tc.says) {
			t.Errorf("PublishBatch (%s) = %v", tc.says, err)
		}
	}
	if _, err := m.Publish(ctx, temperature, 1.0, streams.Header("k", "line\nbreak")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish with an invalid header = %v, want ErrInvalidHeader", err)
	}
	// A zero expiry is none, and a plain write still goes through.
	if _, err := m.Publish(ctx, temperature, 1.0, streams.Expires(0)); err != nil {
		t.Errorf("Publish: %v", err)
	}
}

// A batch is a Write per value, in order, and one bad value fails alone.
func TestBatchWritesInOrder(t *testing.T) {
	_, m := connect(t)
	ctx := t.Context()
	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	msgs, err := m.Subscribe(sub, count, streams.Prefetch(100))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	settleServer()

	b, _ := streams.AsBatch(m)
	values := make([]any, 50)
	for i := range values {
		values[i] = int32(i + 1)
	}
	ids, err := b.PublishBatch(ctx, count, values)
	if err != nil || len(ids) != 50 {
		t.Fatalf("PublishBatch = %d ids, %v", len(ids), err)
	}
	// gopcua's test server holds one change per item between publishing
	// cycles, so what is certain here is the order and the last value; the
	// live suite, against a server that queues, sees every one.
	for last := int32(0); last != 50; {
		got := value(t, next(t, msgs))
		if got <= last {
			t.Fatalf("got %d after %d: out of order", got, last)
		}
		last = got
	}

	// Go's int has no OPC UA type, so entry 1 fails, alone.
	ids, err = b.PublishBatch(ctx, count, []any{int32(1), 2, int32(3)})
	if err == nil || len(ids) != 3 || !strings.Contains(err.Error(), "1 of 3") || !strings.Contains(err.Error(), "entry 1") {
		t.Fatalf("PublishBatch = %d ids, %v; want entry 1 of 3 to fail", len(ids), err)
	}
	if _, err := b.PublishBatch(ctx, count, nil, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("PublishBatch with an id = %v", err)
	}
}

// Every subscribe adds an item to one subscription, and the last to go takes
// the subscription with it.
//
// That a subscribe after the last makes a new subscription is in the live
// suite: gopcua's test server hands the new one the id the deleted one had,
// and the deleted one's goroutine deletes by that id on its way out.
func TestDroppingASubscriptionDeletesTheMonitoredItem(t *testing.T) {
	srv := startServer(t, freePort(t), false)
	c := client(t, srv.URLs()[0])
	s, m := use(t, c, temperature, count)
	ctx := t.Context()

	a, cancelA := context.WithCancel(ctx)
	if _, err := m.Subscribe(a, temperature); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, cancelB := context.WithCancel(ctx)
	if _, err := m.Subscribe(b, count); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	msgs, err := m.Subscribe(ctx, count)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if n := streamsopcua.MonitoredItems(s); n != 3 {
		t.Fatalf("MonitoredItems = %d, want 3", n)
	}
	if subs, items := monitored(srv); subs != 1 || items != 3 {
		t.Fatalf("server holds %d subscriptions, %d items; want one holding three", subs, items)
	}

	cancelA()
	eventually(t, "the first item to go", func() bool {
		subs, items := monitored(srv)
		return subs == 1 && items == 2 && streamsopcua.MonitoredItems(s) == 2
	})
	cancelB()
	eventually(t, "the second item to go", func() bool {
		subs, items := monitored(srv)
		return subs == 1 && items == 1 && streamsopcua.MonitoredItems(s) == 1
	})
	settleServer()
	if _, err := m.Publish(ctx, count, int32(5)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := value(t, next(t, msgs)); got != 5 {
		t.Fatalf("got %d, want 5", got)
	}

	// Close ends the channels and deletes the subscription; the session the
	// caller gave stays.
	if err := s.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	eventually(t, "the channel to close", func() bool {
		select {
		case _, ok := <-msgs:
			return !ok
		default:
			return false
		}
	})
	eventually(t, "close to delete the subscription", func() bool {
		subs, items := monitored(srv)
		return subs == 0 && items == 0
	})
	if _, err := c.NamespaceArray(ctx); err != nil {
		t.Fatalf("the caller's session: %v", err)
	}
	if _, err := m.Subscribe(ctx, count); err == nil {
		t.Fatal("Subscribe after Close succeeded")
	}
}

// A subscribe whose context ends while the server is being asked leaves no
// item and no subscription behind.
func TestACanceledSubscribeLeavesNothingBehind(t *testing.T) {
	srv := startServer(t, freePort(t), false)
	s, m := use(t, client(t, srv.URLs()[0]), count)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.Subscribe(ctx, count); err == nil {
		t.Fatal("a canceled Subscribe succeeded")
	}
	eventually(t, "the canceled item to go", func() bool {
		subs, items := monitored(srv)
		return subs == 0 && items == 0 && streamsopcua.MonitoredItems(s) == 0
	})
}

// A stalled reader loses notifications past its buffer rather than stalling
// the session every other subscription shares.
func TestAStalledReaderDoesNotStallTheOthers(t *testing.T) {
	_, m := connect(t, streamsopcua.WithBuffer(1))
	ctx := t.Context()
	stalled := subscribe(t, m, temperature)
	live := subscribe(t, m, count)

	for i := range 5 {
		if _, err := m.Publish(ctx, temperature, float64(i)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if _, err := m.Publish(ctx, count, int32(9)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := value(t, next(t, live)); got != 9 {
		t.Fatalf("got %d, want 9", got)
	}
	_ = stalled
}

// A namespace URI is resolved when the stream is bound, so one the server does
// not have fails there, and a subject naming it is written to and monitored.
func TestNamespaceURIsResolveAtBind(t *testing.T) {
	s, _ := connect(t)
	ctx := t.Context()
	subject := "nsu=" + namespaceURI + ";s=count"
	stream, err := s.Create(ctx, streams.Stream{Subjects: []string{subject, "nsu=urn:nobody;s=x"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, berr := s.Bind(ctx, stream.ID); berr == nil || !strings.Contains(berr.Error(), "urn:nobody") {
		t.Fatalf("Bind = %v, want an error naming urn:nobody", berr)
	}
	stream.Subjects = []string{subject}
	if _, uerr := s.Update(ctx, stream.ID, stream); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}
	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	msgs := subscribe(t, m, subject)
	if _, err := m.Publish(ctx, subject, int32(9)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := next(t, msgs)
	if msg.Subject != subject || value(t, msg) != 9 {
		t.Fatalf("got %d on %q, want 9 on %q", value(t, msg), msg.Subject, subject)
	}
}

// proxy stands between the client and the server in the restart tests.
//
// gopcua's test server does not close its connections when it is closed, so
// a client would never notice it had gone. The proxy cuts every connection,
// which is what a real server going away does, and sends new ones to
// wherever the server came back.
type proxy struct {
	l      net.Listener
	mu     sync.Mutex
	target string
	conns  []net.Conn
}

// newProxy forwards a new port to srv until the test ends.
func newProxy(t *testing.T, srv *server.Server) *proxy {
	t.Helper()
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	p := &proxy{l: l, target: hostPort(t, srv)}
	t.Cleanup(func() {
		_ = l.Close()
		p.cut("")
	})
	go func() {
		var d net.Dialer
		for {
			in, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			out, err := d.DialContext(context.Background(), "tcp", p.target)
			if err != nil {
				p.mu.Unlock()
				_ = in.Close()
				continue
			}
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	return p
}

// URL is the endpoint a client dials to reach the server through p.
func (p *proxy) URL() string { return "opc.tcp://" + p.l.Addr().String() }

// cut closes every connection and sends new ones to target.
func (p *proxy) cut(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
	if target != "" {
		p.target = target
	}
}

// hostPort is the address srv listens on.
func hostPort(t *testing.T, srv *server.Server) string {
	t.Helper()
	u, err := url.Parse(srv.URLs()[0])
	if err != nil {
		t.Fatalf("the server's URL: %v", err)
	}
	return u.Host
}

// restart replaces srv with a new server, renumbered or not, and cuts every
// connection p carried to it: the server went away with every session and
// subscription it had.
func restart(t *testing.T, srv *server.Server, p *proxy, renumbered bool) *server.Server {
	t.Helper()
	again := startServer(t, freePort(t), renumbered)
	p.cut(hostPort(t, again))
	_ = srv.Close()
	return again
}

// publishUntil writes v until a write succeeds, as one fails while the client
// reconnects.
func publishUntil(t *testing.T, m streams.Manager, subject string, v any) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := m.Publish(t.Context(), subject, v)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a write after the restart: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// receiveUntil reads until want arrives, writing v again between reads, since
// a write made before the item was recreated reaches nobody.
func receiveUntil(t *testing.T, m streams.Manager, subject string, msgs <-chan streams.Message, want int32) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("the subscription ended")
			}
			if value(t, msg) == want {
				return
			}
		case <-time.After(500 * time.Millisecond):
			_, _ = m.Publish(t.Context(), subject, want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d never arrived", want)
		}
	}
}

// The server goes away with every session and subscription it had; the client
// reconnects and recreates them, and the channel carries on.
func TestSubscriptionsSurviveAServerRestart(t *testing.T) {
	srv := startServer(t, freePort(t), false)
	p := newProxy(t, srv)
	s, m := use(t, client(t, p.URL()), count)
	ctx, cancel := context.WithCancel(t.Context())
	msgs, err := m.Subscribe(ctx, count)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	settleServer()
	if _, err := m.Publish(t.Context(), count, int32(1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := value(t, next(t, msgs)); got != 1 {
		t.Fatalf("got %d, want 1", got)
	}

	srv = restart(t, srv, p, false)
	publishUntil(t, m, count, int32(3))
	receiveUntil(t, m, count, msgs, 3)

	// The item recreated on the new server is still the one a cancel deletes.
	cancel()
	eventually(t, "the recreated subscription to go", func() bool {
		subs, items := monitored(srv)
		return subs == 0 && items == 0 && streamsopcua.MonitoredItems(s) == 0
	})
}

// A server that comes back with another namespace ahead of the test one is
// written to, and monitored on, the node the URI names, at its new index.
func TestNamespaceURIsFollowARenumberedServer(t *testing.T) {
	srv := startServer(t, freePort(t), false)
	subject := "nsu=" + namespaceURI + ";s=count"
	p := newProxy(t, srv)
	_, m := use(t, client(t, p.URL()), subject)
	msgs := subscribe(t, m, subject)
	if _, err := m.Publish(t.Context(), subject, int32(1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := value(t, next(t, msgs)); got != 1 {
		t.Fatalf("got %d, want 1", got)
	}

	restart(t, srv, p, true)
	publishUntil(t, m, subject, int32(7))
	receiveUntil(t, m, subject, msgs, 9)
}

// A store on a client the caller also subscribes through leaves the caller's
// subscription alone, adding to and deleting only its own.
func TestASharedClientKeepsItsOtherSubscriptions(t *testing.T) {
	srv := startServer(t, freePort(t), false)
	c := client(t, srv.URLs()[0])
	ctx := t.Context()
	notifs := make(chan *gopcua.PublishNotificationData, 16)
	go func() {
		for range notifs {
		}
	}()
	mine, err := c.Subscribe(ctx, &gopcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifs)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = mine.Cancel(context.Background()) })

	_, m := use(t, c, count, temperature)
	sub, cancel := context.WithCancel(ctx)
	if _, err := m.Subscribe(sub, count); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := m.Subscribe(sub, temperature); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if subs, items := monitored(srv); subs != 2 || items != 2 {
		t.Fatalf("server holds %d subscriptions, %d items; want the caller's empty one and ours with two", subs, items)
	}
	cancel()
	eventually(t, "the store's subscription to go", func() bool {
		subs, items := monitored(srv)
		return subs == 1 && items == 0
	})
	ids := c.SubscriptionIDs()
	if len(ids) != 1 || ids[0] != mine.SubscriptionID {
		t.Fatalf("client holds %v, want only the caller's %d", ids, mine.SubscriptionID)
	}
}
