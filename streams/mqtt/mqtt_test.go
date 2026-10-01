package mqtt_test

// These run against an MQTT broker started in-process by mochi-mqtt, so they
// need no broker installed and no container.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user/created"

// testBroker starts a broker on a free port and returns its address.
//
// The port comes from mochi rather than being reserved here. AddListener calls
// the listener's Init, which binds the socket synchronously, and TCP.Address
// then reports the port the kernel chose -- so by the time this returns the
// socket is bound and the backlog is accepting, whether or not Serve's accept
// loop has been scheduled yet.
//
// The two things that replaced were both races. Reserving a port with our own
// listener and closing it before handing mochi the address left a window for
// anything else on the machine to take it. Dialing afterwards to check the
// broker was up was worse: it left a connection that sent no CONNECT packet,
// so mochi was still inside EstablishConnection for it when the cleanup below
// called Close, and those two race on the server's own fields inside the
// library (server.go:401 against server.go:1499 in v2.7.9). That surfaced as
// an intermittent "race detected during execution of test" in CI, on
// whichever test happened to finish fastest.
func testBroker(t *testing.T) string {
	t.Helper()

	srv := mochi.New(&mochi.Options{InlineClient: true})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("AddHook: %v", err)
	}
	ln := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	if err := srv.AddListener(ln); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	go func() { _ = srv.Serve() }()

	// Whatever mochi registers for itself at startup, recorded before any
	// test client connects, so the cleanup below knows what "drained" means.
	baseline := srv.Clients.Len()

	t.Cleanup(func() {
		// Wait for the client registry to drain before closing.
		//
		// Server.Close walks the clients on each listener to disconnect them,
		// and mochi v2.7.9 has a lock bug on that path: Clients.GetByListener
		// takes the read lock and then calls Clients.Len, which takes it a
		// second time (clients.go:94). sync.RWMutex forbids a recursive
		// RLock, and against a concurrent write lock from Clients.Add or
		// Delete -- a client attaching or detaching -- the race detector
		// fires. That is what "race detected during execution of test" was in
		// CI, and it is upstream: v2.7.9 is the newest release, so there is no
		// version to move to.
		//
		// Closing with the registry already empty keeps Close off the racing
		// branch, since there is then nothing to iterate and no concurrent
		// Add or Delete to collide with. Cleanups run last-in-first-out, so
		// testStreams has already closed its client by the time this runs and
		// the wait is normally over immediately.
		//
		// This narrows the window rather than fixing the bug. The fix belongs
		// upstream, in GetByListener.
		// The baseline is not zero: InlineClient registers a client of its
		// own that never disconnects, so waiting for an empty registry would
		// just burn the whole timeout on every broker.
		// Bounded deliberately tightly. Some tests end with a session still
		// attached on purpose -- the resume case does -- so this must not
		// wait for a registry that is never going to drain. A short wait
		// covers the case that matters, a client still tearing down, and
		// gives up rather than adding seconds per broker.
		deadline := time.Now().Add(250 * time.Millisecond)
		for srv.Clients.Len() > baseline && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		_ = srv.Close()
	})

	return ln.Address()
}

func testStreams(t *testing.T, opts ...streamsmqtt.Option) streams.Streams {
	t.Helper()

	s, err := streamsmqtt.Connect(t.Context(), testBroker(t), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := s.(streams.Closer); ok {
			_ = c.Close()
		}
	})
	return s
}

func declare(t *testing.T, s streams.Streams, subjects ...string) (streams.Stream, streams.Manager) {
	t.Helper()

	stream, err := s.Create(t.Context(), streams.Stream{Name: "test", Subjects: subjects})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return stream, m
}

func recvDelivery(t *testing.T, ch <-chan streams.Delivery, within time.Duration) streams.Delivery {
	t.Helper()

	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatal("delivery channel closed before a message arrived")
		}
		return d
	case <-time.After(within):
		t.Fatalf("no delivery within %s", within)
		return streams.Delivery{}
	}
}

func TestLifecycle(t *testing.T) {
	s := testStreams(t)
	ctx := t.Context()

	created, err := s.Create(ctx, streams.Stream{
		Name: "users", Description: "before", Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == "" {
		t.Fatal("Create did not assign an id")
	}

	if _, uerr := s.Update(ctx, created.ID, streams.Stream{
		Name: "users", Description: "after", Subjects: []string{subject},
	}); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}
	if after, _ := s.Get(ctx, created.ID); after.Description != "after" {
		t.Errorf("Update did not take: %q", after.Description)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("List returned %d streams, want 1", len(list))
	}

	if derr := s.Delete(ctx, created.ID); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}
	if _, gerr := s.Get(ctx, created.ID); !errors.Is(gerr, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", gerr)
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := event{User: "ada", Action: "created"}
	id, err := m.Publish(ctx, subject, want)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-ch:
		if msg.ID != id {
			t.Errorf("delivered id %q, want the published %q", msg.ID, id)
		}
		var got event
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got != want {
			t.Errorf("decoded %+v, want %+v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}
}

// MQTT is durable but has no log to seek, which is exactly why the contract
// keeps the two capabilities apart.
func TestMQTTIsDurableButNotPositioned(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := streams.AsDurable(m); err != nil {
		t.Errorf("AsDurable on MQTT: %v", err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned on MQTT = %v, want ErrUnsupported", err)
	}
}

func TestConsumeDeliversAndAcknowledges(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, err := streams.AsDurable(m)
	if err != nil {
		t.Fatalf("AsDurable: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	want := event{User: "grace", Action: "created"}
	if _, perr := m.Publish(ctx, subject, want); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	got := recvDelivery(t, ch, 10*time.Second)
	var decoded event
	if derr := got.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded != want {
		t.Errorf("decoded %+v, want %+v", decoded, want)
	}
	// MQTT flags a repeat but does not count them, and the contract asks for
	// zero rather than a number that would be invented.
	if got.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0 where the provider cannot count", got.Attempt)
	}
	if aerr := got.Ack(ctx); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
}

// The session is the name: a message published while the consumer was away is
// waiting when it comes back.
func TestASessionKeepsMessagesWhileTheConsumerIsAway(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	// Attach once so the broker creates the session and its subscription.
	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(first, subject, event{User: "first"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	got := recvDelivery(t, ch, 10*time.Second)
	if aerr := got.Ack(first); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
	// The acknowledgement is a packet on the wire, and disconnecting in the
	// same breath can close the connection before it is written.
	time.Sleep(300 * time.Millisecond)
	cancelFirst()
	time.Sleep(500 * time.Millisecond) // let the disconnect land

	// Published with nobody attached. A clean session would drop this.
	if _, perr := m.Publish(t.Context(), subject, event{User: "second"}); perr != nil {
		t.Fatalf("Publish while detached: %v", perr)
	}

	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()

	again, err := d.Consume(second, subject, "billing")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}
	resumed := recvDelivery(t, again, 10*time.Second)

	var decoded event
	if derr := resumed.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "second" {
		t.Errorf("resumed at %+v, want the message published while detached", decoded)
	}
}

// A shared subscription is how MQTT splits work, and it is what Group means
// here — unlike the other providers, where the consumer name is the group.
func TestGroupSharesTheSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, err := d.Consume(ctx, subject, "worker-a", streams.Group("billing"))
	if err != nil {
		t.Fatalf("Consume (first): %v", err)
	}
	second, err := d.Consume(ctx, subject, "worker-b", streams.Group("billing"))
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}

	const total = 6
	for i := range total {
		if _, perr := m.Publish(ctx, subject, event{User: "u", Action: fmt.Sprint(i)}); perr != nil {
			t.Fatalf("Publish %d: %v", i, perr)
		}
	}

	seen := 0
	deadline := time.After(15 * time.Second)
	for seen < total {
		select {
		case msg := <-first:
			seen++
			_ = msg.Ack(ctx)
		case msg := <-second:
			seen++
			_ = msg.Ack(ctx)
		case <-deadline:
			t.Fatalf("saw %d of %d messages across the group", seen, total)
		}
	}
}

// A stream may declare a filter; a message still has to land somewhere specific.
func TestWildcardSubjects(t *testing.T) {
	_, m := declare(t, testStreams(t), "user/+")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, "user/+")
	if err != nil {
		t.Fatalf("Subscribe to the filter: %v", err)
	}
	if _, perr := m.Publish(ctx, "user/created", event{User: "ada"}); perr != nil {
		t.Fatalf("Publish to a subject the filter covers: %v", perr)
	}

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}

	if _, perr := m.Publish(ctx, "user/+", event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish to a wildcard = %v, want ErrUnknownSubject", perr)
	}
}

func TestPublishRejectsAnUndeclaredSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := m.Publish(t.Context(), "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish = %v, want ErrUnknownSubject", err)
	}
}

func TestPublishRejectsATTL(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	_, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}

func TestConnectRejectsAnEmptyAddress(t *testing.T) {
	if _, err := streamsmqtt.Connect(t.Context(), ""); err == nil {
		t.Error("Connect accepted an empty address")
	}
}
