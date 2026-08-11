package nats_test

// These run against a NATS server started in-process, so they need nothing
// installed and no container. Each test gets its own server and its own store
// directory, which is what keeps them independent.

import (
	"context"
	"errors"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	gonats "github.com/nats-io/nats.go"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsnats "github.com/the-protobuf-project/runtime-go/streams/nats"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user.created"

// testConn starts a server with JetStream enabled and returns a connection to
// it. Both halves of the package are exercised against the same server.
func testConn(t *testing.T) *gonats.Conn {
	t.Helper()

	opts := natstest.DefaultTestOptions
	opts.Port = -1 // any free port, so tests may run in parallel
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("the test server did not come up")
	}

	nc, err := gonats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// declare creates a stream and binds a manager to it.
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

func jetStream(t *testing.T) streams.Streams {
	t.Helper()

	s, err := streamsnats.UseJetStream(testConn(t))
	if err != nil {
		t.Fatalf("ConnectJetStream: %v", err)
	}
	return s
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

// ---------------------------------------------------------------- core NATS

func TestPlainLifecycle(t *testing.T) {
	s := streamsnats.Use(testConn(t))
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

	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Description != "after" {
		t.Errorf("Get returned stale metadata: %q", got.Description)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("List returned %d streams, want 1", len(list))
	}

	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, created.ID); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestPlainPublishAndSubscribe(t *testing.T) {
	s := streamsnats.Use(testConn(t))
	_, m := declare(t, s, subject)

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
		if err := msg.Decode(&got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != want {
			t.Errorf("decoded %+v, want %+v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was delivered")
	}
}

// A stream may declare a wildcard; a message still has to land somewhere
// specific.
func TestPlainWildcardSubjects(t *testing.T) {
	s := streamsnats.Use(testConn(t))
	_, m := declare(t, s, "orders.*")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, "orders.*")
	if err != nil {
		t.Fatalf("Subscribe to the wildcard: %v", err)
	}
	if _, err := m.Publish(ctx, "orders.placed", event{User: "ada"}); err != nil {
		t.Fatalf("Publish to a subject the wildcard covers: %v", err)
	}

	select {
	case msg := <-ch:
		if msg.Subject != "orders.placed" {
			t.Errorf("Subject = %q, want the concrete subject it arrived on", msg.Subject)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was delivered")
	}

	if _, err := m.Publish(ctx, "orders.*", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish to a wildcard = %v, want ErrUnknownSubject", err)
	}
}

func TestPlainRejectsAnUndeclaredSubject(t *testing.T) {
	s := streamsnats.Use(testConn(t))
	_, m := declare(t, s, subject)

	if _, err := m.Publish(t.Context(), "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish = %v, want ErrUnknownSubject", err)
	}
}

func TestPlainRejectsATTL(t *testing.T) {
	s := streamsnats.Use(testConn(t))
	_, m := declare(t, s, subject)

	_, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

// Core NATS keeps no log, so it must refuse the durable half by name rather
// than pretend.
func TestPlainIsNotDurable(t *testing.T) {
	s := streamsnats.Use(testConn(t))
	_, m := declare(t, s, subject)

	if _, err := streams.AsDurable(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsDurable on core NATS = %v, want ErrUnsupported", err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned on core NATS = %v, want ErrUnsupported", err)
	}
}

// A queue group makes several subscribers share a subject instead of each
// receiving everything.
func TestPlainQueueGroupSharesTheSubject(t *testing.T) {
	s := streamsnats.Use(testConn(t), streamsnats.WithQueueGroup("workers"))
	_, m := declare(t, s, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe (first): %v", err)
	}
	second, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe (second): %v", err)
	}

	const total = 6
	for range total {
		if _, err := m.Publish(ctx, subject, event{User: "u"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Shared, so the two together see each message once: 6 in total rather than
	// 6 each. Counting to more than total would mean the group was ignored.
	seen := 0
	deadline := time.After(5 * time.Second)
	for seen < total {
		select {
		case <-first:
			seen++
		case <-second:
			seen++
		case <-deadline:
			t.Fatalf("saw %d of %d messages across the group", seen, total)
		}
	}

	select {
	case <-first:
		t.Error("a message was delivered twice; the queue group was not honored")
	case <-second:
		t.Error("a message was delivered twice; the queue group was not honored")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestJetStreamRejectsAQueueGroup(t *testing.T) {
	_, err := streamsnats.UseJetStream(testConn(t), streamsnats.WithQueueGroup("workers"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("ConnectJetStream with a queue group = %v, want ErrUnsupported", err)
	}
}

// ---------------------------------------------------------------- JetStream

func TestJetStreamLifecycle(t *testing.T) {
	s := jetStream(t)
	ctx := t.Context()

	created, err := s.Create(ctx, streams.Stream{
		Name: "users", Description: "before", Subjects: []string{subject}, UserID: "u-1",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Name and UserID have nowhere to live in a JetStream config, so they ride
	// in the stream's metadata. If that breaks, they come back empty.
	if got.Name != "users" || got.UserID != "u-1" {
		t.Errorf("Get lost metadata: name=%q user=%q", got.Name, got.UserID)
	}
	if !got.Active {
		t.Error("Get returned an inactive stream")
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

	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, created.ID); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestJetStreamGetOnAMissingStreamIsNotFound(t *testing.T) {
	if _, err := jetStream(t).Get(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}

func TestJetStreamBindOnAMissingStreamIsNotFound(t *testing.T) {
	if _, err := jetStream(t).Bind(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Bind = %v, want ErrNotFound", err)
	}
}

func TestJetStreamIsDurableAndPositioned(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)

	if _, err := streams.AsDurable(m); err != nil {
		t.Errorf("AsDurable on JetStream: %v", err)
	}
	if _, err := streams.AsPositioned(m); err != nil {
		t.Errorf("AsPositioned on JetStream: %v", err)
	}
}

func TestJetStreamPublishAndSubscribe(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := event{User: "ada", Action: "created"}
	if _, err := m.Publish(ctx, subject, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-ch:
		var got event
		if err := msg.Decode(&got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != want {
			t.Errorf("decoded %+v, want %+v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}
}

func TestConsumeDeliversAndAcknowledges(t *testing.T) {
	s := jetStream(t)
	_, m := declare(t, s, subject)

	d, err := streams.AsDurable(m)
	if err != nil {
		t.Fatalf("AsDurable: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	want := event{User: "grace", Action: "created"}
	if _, err := m.Publish(ctx, subject, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := recvDelivery(t, ch, 10*time.Second)
	if got.Attempt != 1 {
		t.Errorf("Attempt = %d on a first delivery, want 1", got.Attempt)
	}

	var decoded event
	if err := got.Decode(&decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != want {
		t.Errorf("decoded %+v, want %+v", decoded, want)
	}
	if err := got.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// Nak returns the message, and the attempt count is what a consumer uses to
// notice it is in a loop it cannot escape.
func TestNakRedeliversWithAHigherAttempt(t *testing.T) {
	s := jetStream(t)
	_, m := declare(t, s, subject)

	d, _ := streams.AsDurable(m)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, err := m.Publish(ctx, subject, event{User: "ada"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	first := recvDelivery(t, ch, 10*time.Second)
	if first.Attempt != 1 {
		t.Errorf("first Attempt = %d, want 1", first.Attempt)
	}
	if err := first.Nak(ctx); err != nil {
		t.Fatalf("Nak: %v", err)
	}

	second := recvDelivery(t, ch, 10*time.Second)
	if second.Attempt < 2 {
		t.Errorf("Attempt after a Nak = %d, want at least 2", second.Attempt)
	}
	if second.ID != first.ID {
		t.Errorf("redelivered a different message: %q then %q", first.ID, second.ID)
	}
	if err := second.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// A named consumer's position is server-side, so it survives the process.
func TestAConsumerResumesWhereTheNameLeftOff(t *testing.T) {
	s := jetStream(t)
	_, m := declare(t, s, subject)
	d, _ := streams.AsDurable(m)

	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "workers")
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
	cancelFirst()

	// Published while nobody was consuming.
	if _, perr := m.Publish(t.Context(), subject, event{User: "second"}); perr != nil {
		t.Fatalf("Publish while detached: %v", perr)
	}

	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()

	again, err := d.Consume(second, subject, "workers")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}
	resumed := recvDelivery(t, again, 10*time.Second)

	var decoded event
	if err := resumed.Decode(&decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.User != "second" {
		t.Errorf("resumed at %+v, want the message published while detached", decoded)
	}
}

func TestConsumeFromEarliestReplaysHistory(t *testing.T) {
	s := jetStream(t)
	_, m := declare(t, s, subject)

	p, err := streams.AsPositioned(m)
	if err != nil {
		t.Fatalf("AsPositioned: %v", err)
	}
	if _, perr := m.Publish(t.Context(), subject, event{User: "before"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := p.ConsumeFrom(ctx, subject, "history", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	got := recvDelivery(t, ch, 10*time.Second)

	var decoded event
	if err := got.Decode(&decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.User != "before" {
		t.Errorf("replayed %+v, want the message published before consuming", decoded)
	}
}

func TestConsumeFromNewSkipsHistory(t *testing.T) {
	s := jetStream(t)
	_, m := declare(t, s, subject)

	p, _ := streams.AsPositioned(m)
	if _, perr := m.Publish(t.Context(), subject, event{User: "before"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := p.ConsumeFrom(ctx, subject, "tail", streams.FromNew)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	select {
	case d := <-ch:
		t.Fatalf("FromNew replayed history: %+v", d.Message)
	case <-time.After(2 * time.Second):
	}
}

// The message id is sent as the JetStream message id, so a republish under the
// same id is collapsed by the server rather than appended twice.
func TestPublishingTheSameIDTwiceDeliversOnce(t *testing.T) {
	s := jetStream(t)
	_, m := declare(t, s, subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	for range 2 {
		if _, err := m.Publish(ctx, subject, event{User: "ada"}, streams.ID("fixed")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	got := recvDelivery(t, ch, 10*time.Second)
	if err := got.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	select {
	case dup := <-ch:
		t.Errorf("the duplicate was delivered too: %+v", dup.Message)
	case <-time.After(2 * time.Second):
	}
}

func TestJetStreamRejectsATTL(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)

	_, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRejectsAGroupOption(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)
	d, _ := streams.AsDurable(m)

	_, err := d.Consume(t.Context(), subject, "workers", streams.Group("other"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRejectsAnUndeclaredSubject(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), "typo", "workers"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Consume on an undeclared subject = %v, want ErrUnknownSubject", err)
	}
}

// The typed view is the same wrapper over either provider.
func TestTypedViewOverJetStream(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := streams.For[event](m)
	ch, err := events.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := events.Publish(ctx, subject, event{User: "ada", Action: "created"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-ch:
		if got.User != "ada" {
			t.Errorf("decoded %+v, want the published event", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}
}
