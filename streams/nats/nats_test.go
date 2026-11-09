package nats_test

// These run against a NATS server started in-process, so they need nothing
// installed and no container. Each test gets its own server and its own store
// directory, which is what keeps them independent.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// ---------------------------------------------------------------- headers, expiry, delayed nak

// headed is a declaration over subjects with default headers lang and site.
func headed(subjects ...string) streams.Stream {
	return streams.Stream{Name: "headed", Subjects: subjects}.
		WithHeader("lang", "go").
		WithHeader("site", "plant-3")
}

func recvMessage(t *testing.T, ch <-chan streams.Message, within time.Duration) streams.Message {
	t.Helper()

	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("message channel closed before a message arrived")
		}
		return m
	case <-time.After(within):
		t.Fatalf("no message within %s", within)
		return streams.Message{}
	}
}

// The stream's headers arrive, a publish's own win, and both are NATS headers
// another client reads, outside the payload. A message without any is a plain
// PUB.
func TestPlainHeadersTravelAsNatsHeaders(t *testing.T) {
	nc := testConn(t)
	s := streamsnats.Use(nc)
	ctx := t.Context()

	stream, err := s.Create(ctx, headed(subject))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got, _ := s.Get(ctx, stream.ID); got.Headers.Get("lang") != "go" {
		t.Errorf("Get lost the headers: %v", got.Headers)
	}
	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	raw, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	id, err := m.Publish(ctx, subject, event{User: "ada"}, streams.Header("site", "plant-4"), streams.Header("trace", "00-ab"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := recvMessage(t, ch, 5*time.Second)
	if got.ID != id || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-4" || got.Headers.Get("trace") != "00-ab" {
		t.Errorf("delivered %q with %v", got.ID, got.Headers)
	}
	native, err := raw.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	if native.Header.Get("site") != "plant-4" {
		t.Errorf("NATS headers = %v", native.Header)
	}

	bare, _ := s.Create(ctx, streams.Stream{Name: "bare", Subjects: []string{subject}})
	bm, _ := s.Bind(ctx, bare.ID)
	if _, perr := bm.Publish(ctx, subject, event{User: "go"}); perr != nil {
		t.Fatalf("Publish (bare): %v", perr)
	}
	if got := recvMessage(t, ch, 5*time.Second); got.Headers != nil {
		t.Errorf("a header-less publish arrived with %v", got.Headers)
	}
	if native, _ := raw.NextMsg(5 * time.Second); native == nil || native.Header != nil {
		t.Error("a header-less publish was not a plain PUB")
	}

	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, subject, []any{event{User: "a"}, event{User: "b"}})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for _, id := range ids {
		if got := recvMessage(t, ch, 5*time.Second); got.ID != id || got.Headers.Get("lang") != "go" {
			t.Errorf("batch entry %q arrived as %q with %v", id, got.ID, got.Headers)
		}
	}
}

func TestPlainRefusesBadHeadersExpiryAndRetain(t *testing.T) {
	s := streamsnats.Use(testConn(t))
	ctx := t.Context()

	if _, err := s.Create(ctx, headed(subject).WithHeader("Nats-Msg-Id", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create with Nats-Msg-Id = %v, want ErrInvalidHeader", err)
	}
	stream, m := declare(t, s, subject)
	if _, err := s.Update(ctx, stream.ID, headed(subject).WithHeader("Nats-TTL", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Update with Nats-TTL = %v, want ErrInvalidHeader", err)
	}
	for _, opt := range []streams.Option{streams.Header("Nats-Expected-Stream", "x"), streams.Header("a b", "1")} {
		if _, err := m.Publish(ctx, subject, event{}, opt); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Publish = %v, want ErrInvalidHeader", err)
		}
	}
	b, _ := streams.AsBatch(m)
	if _, err := b.PublishBatch(ctx, subject, []any{event{}}, streams.Header("Nats-Msg-Id", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("PublishBatch = %v, want ErrInvalidHeader", err)
	}
	if _, err := m.Publish(ctx, subject, event{}, streams.Expires(5*time.Second)); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "expire") {
		t.Errorf("Publish with Expires = %v, want ErrUnsupported naming the expiry", err)
	}
	if _, err := m.Publish(ctx, subject, event{}, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "retain") {
		t.Errorf("Publish with Retain = %v, want ErrUnsupported naming retain", err)
	}
	if _, err := m.Publish(ctx, subject, event{}, streams.Expires(0)); err != nil {
		t.Errorf("Publish with a zero expiry = %v, want it treated as none", err)
	}
}

// The stream's headers live in its metadata, so another provider binding it
// stamps the same ones; they arrive on deliveries, redeliveries included, and
// the stored message carries them as NATS headers.
func TestJetStreamHeadersAreStoredAndRedelivered(t *testing.T) {
	nc := testConn(t)
	s, _ := streamsnats.UseJetStream(nc)
	ctx := t.Context()

	stream, err := s.Create(ctx, headed(subject))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	other, _ := streamsnats.UseJetStream(nc)
	if got, _ := other.Get(ctx, stream.ID); got.Headers.Get("site") != "plant-3" {
		t.Errorf("another provider read %v", got.Headers)
	}
	m, _ := other.Bind(ctx, stream.ID)
	d, _ := streams.AsDurable(m)
	ch, err := d.Consume(ctx, subject, "headed")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	id, err := m.Publish(ctx, subject, event{User: "ada"}, streams.Header("site", "plant-4"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := recvDelivery(t, ch, 10*time.Second)
	if first.Headers.Get("lang") != "go" || first.Headers.Get("site") != "plant-4" {
		t.Errorf("headers = %v", first.Headers)
	}
	if _, ok := first.Headers["Nats-Msg-Id"]; ok {
		t.Errorf("the server's header leaked: %v", first.Headers)
	}
	if perr := first.Nak(ctx); perr != nil {
		t.Fatalf("Nak: %v", perr)
	}
	second := recvDelivery(t, ch, 10*time.Second)
	if second.ID != id || second.Attempt != 2 || second.Headers.Get("site") != "plant-4" {
		t.Errorf("redelivery %q attempt %d headers %v", second.ID, second.Attempt, second.Headers)
	}
	_ = second.Ack(ctx)

	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, subject, []any{event{User: "a"}, event{User: "b"}}, streams.Header("site", "plant-4"))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for _, id := range ids {
		got := recvDelivery(t, ch, 10*time.Second)
		if got.ID != id || got.Headers.Get("site") != "plant-4" {
			t.Errorf("batch entry %q arrived as %q with %v", id, got.ID, got.Headers)
		}
		_ = got.Ack(ctx)
	}

	js, _ := jetstream.New(nc)
	handle, _ := js.Stream(ctx, stream.ID)
	raw, err := handle.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("GetMsg: %v", err)
	}
	if raw.Header.Get("lang") != "go" || raw.Header.Get("Nats-Msg-Id") != id {
		t.Errorf("stored headers = %v", raw.Header)
	}

	// An update replaces the set; one without headers leaves none behind.
	updated, err := s.Update(ctx, stream.ID, streams.Stream{Name: "headed", Subjects: []string{subject}})
	if err != nil || updated.Headers != nil {
		t.Errorf("Update = %v, %v", updated.Headers, err)
	}
	if got, _ := s.Get(ctx, stream.ID); got.Headers != nil {
		t.Errorf("Get after the update = %v", got.Headers)
	}
	if _, err := s.Update(ctx, stream.ID, headed(subject).WithHeader("Nats-Expected-Stream", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Update with a reserved key = %v", err)
	}
}

func TestJetStreamRefusesBadHeadersAndRetain(t *testing.T) {
	s := jetStream(t)
	ctx := t.Context()

	if _, err := s.Create(ctx, headed(subject).WithHeader("Nats-TTL", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create with Nats-TTL = %v", err)
	}
	if _, err := s.Create(ctx, headed(subject).WithHeader("", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create with an empty key = %v", err)
	}
	_, m := declare(t, s, subject)
	reserved := streams.Header("Nats-Msg-Id", "x")
	if _, err := m.Publish(ctx, subject, event{}, reserved); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish = %v", err)
	}
	b, _ := streams.AsBatch(m)
	if _, err := b.PublishBatch(ctx, subject, []any{event{}}, reserved); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("PublishBatch = %v", err)
	}
	if _, err := m.Publish(ctx, subject, event{}, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "retain") {
		t.Errorf("Publish with Retain = %v", err)
	}
	if _, err := m.Publish(ctx, subject, event{}, streams.Expires(500*time.Millisecond)); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "whole seconds") {
		t.Errorf("Publish with a sub-second expiry = %v", err)
	}
}

// Nats-TTL: the server removes the message once its expiry passes, while one
// without an expiry stays.
func TestJetStreamExpiryRemovesTheMessage(t *testing.T) {
	nc := testConn(t)
	s, _ := streamsnats.UseJetStream(nc)
	stream, m := declare(t, s, subject)
	ctx := t.Context()

	gone, err := m.Publish(ctx, subject, event{User: "late"}, streams.Expires(time.Second))
	if err != nil {
		t.Fatalf("Publish with Expires: %v", err)
	}
	kept, _ := m.Publish(ctx, subject, event{User: "kept"})

	js, _ := jetstream.New(nc)
	handle, _ := js.Stream(ctx, stream.ID)
	raw, err := handle.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("GetMsg: %v", err)
	}
	if raw.Header.Get("Nats-TTL") != "1" {
		t.Errorf("Nats-TTL = %q, want 1", raw.Header.Get("Nats-TTL"))
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		info, _ := handle.Info(ctx)
		if info.State.Msgs == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the message did not expire")
		}
		time.Sleep(200 * time.Millisecond)
	}
	p, _ := streams.AsPositioned(m)
	ch, _ := p.ConsumeFrom(ctx, subject, "late", streams.FromEarliest)
	d := recvDelivery(t, ch, 10*time.Second)
	if d.ID == gone || d.ID != kept {
		t.Errorf("delivered %q, want only %q", d.ID, kept)
	}
}

// A stream another tool created without allow_msg_ttl refuses an expiry by
// name until it is updated through this provider.
func TestJetStreamExpiryNeedsAllowMsgTTL(t *testing.T) {
	nc := testConn(t)
	s, _ := streamsnats.UseJetStream(nc)
	ctx := t.Context()

	js, _ := jetstream.New(nc)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "FOREIGN", Subjects: []string{subject}}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	m, _ := s.Bind(ctx, "FOREIGN")
	if _, err := m.Publish(ctx, subject, event{}, streams.Expires(5*time.Second)); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "allow_msg_ttl") {
		t.Errorf("Publish with Expires = %v, want ErrUnsupported naming allow_msg_ttl", err)
	}
	if _, err := m.Publish(ctx, subject, event{}); err != nil {
		t.Errorf("Publish without an expiry: %v", err)
	}
	if _, err := s.Update(ctx, "FOREIGN", streams.Stream{Name: "foreign", Subjects: []string{subject}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	m, _ = s.Bind(ctx, "FOREIGN")
	if _, err := m.Publish(ctx, subject, event{}, streams.Expires(5*time.Second)); err != nil {
		t.Errorf("Publish with Expires after the update: %v", err)
	}
}

// A delayed nak: the server holds the redelivery back for the delay.
func TestJetStreamNakAfterHoldsTheRedeliveryBack(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)
	d, _ := streams.AsDurable(m)
	ctx := t.Context()

	ch, _ := d.Consume(ctx, subject, "patient")
	id, _ := m.Publish(ctx, subject, event{User: "ada"})
	first := recvDelivery(t, ch, 10*time.Second)
	if !first.CanDelayRedelivery() || first.MaxRedeliveryDelay() != 0 {
		t.Errorf("CanDelayRedelivery %v, MaxRedeliveryDelay %s", first.CanDelayRedelivery(), first.MaxRedeliveryDelay())
	}
	naked := time.Now()
	if err := first.NakAfter(ctx, 2*time.Second); err != nil {
		t.Fatalf("NakAfter: %v", err)
	}
	select {
	case early := <-ch:
		t.Fatalf("redelivered after %s, before the delay: %+v", time.Since(naked), early.Message)
	case <-time.After(1500 * time.Millisecond):
	}
	second := recvDelivery(t, ch, 10*time.Second)
	if time.Since(naked) < 2*time.Second || second.ID != id || second.Attempt != 2 {
		t.Errorf("redelivered %q attempt %d after %s", second.ID, second.Attempt, time.Since(naked))
	}
	_ = second.Ack(ctx)
}

// A pipelined batch several times its in-flight window, and larger than the
// provider's in-flight limit, finishes and is stored in order.
func TestJetStreamALargeBatchLandsInOrder(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)
	ctx := t.Context()

	values := make([]any, 6000)
	for i := range values {
		values[i] = event{User: strconv.Itoa(i)}
	}
	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, subject, values)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(ids) != len(values) {
		t.Fatalf("got %d ids, want %d", len(ids), len(values))
	}
	p, _ := streams.AsPositioned(m)
	ch, _ := p.ConsumeFrom(ctx, subject, "in-order", streams.FromEarliest)
	for i, id := range ids {
		d := recvDelivery(t, ch, 10*time.Second)
		if d.ID != id {
			t.Fatalf("entry %d arrived as %q, want %q", i, d.ID, id)
		}
		_ = d.Ack(ctx)
	}
}

// Several batches at once, together far beyond the provider's in-flight limit,
// all finish: a batch waiting for room settles its own waits first.
func TestJetStreamConcurrentBatchesDoNotDeadlock(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)
	b, _ := streams.AsBatch(m)

	values := make([]any, 1000)
	for i := range values {
		values[i] = event{User: "u"}
	}
	errs := make(chan error, 16)
	for range 16 {
		go func() {
			_, err := b.PublishBatch(t.Context(), subject, values)
			errs <- err
		}()
	}
	for range 16 {
		select {
		case err := <-errs:
			if err != nil {
				t.Errorf("PublishBatch: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the batches did not finish")
		}
	}
}

// The consumer settings land on the consumers the server holds.
func TestJetStreamConsumerSettings(t *testing.T) {
	nc := testConn(t)
	ctx := t.Context()
	js, _ := jetstream.New(nc)

	for _, tc := range []struct {
		name              string
		opts              []streamsnats.Option
		ackWait, inactive time.Duration
		maxDeliver        int
	}{
		{"defaults", nil, 30 * time.Second, 30 * time.Second, -1},
		{"set", []streamsnats.Option{
			streamsnats.WithAckWait(time.Second), streamsnats.WithMaxDeliver(3), streamsnats.WithInactiveThreshold(2 * time.Second),
		}, time.Second, 2 * time.Second, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := streamsnats.UseJetStream(nc, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			subj := "settings." + tc.name
			stream, err := s.Create(ctx, streams.Stream{Name: tc.name, Subjects: []string{subj}})
			if err != nil {
				t.Fatal(err)
			}
			m, _ := s.Bind(ctx, stream.ID)
			handle, _ := js.Stream(ctx, stream.ID)

			subCtx, cancel := context.WithCancel(ctx)
			if _, perr := m.Subscribe(subCtx, subj); perr != nil {
				t.Fatal(perr)
			}
			lister := handle.ListConsumers(ctx)
			var found int
			for info := range lister.Info() {
				found++
				if info.Config.InactiveThreshold != tc.inactive || info.Config.Durable != "" {
					t.Errorf("ordered consumer: inactive %s durable %q", info.Config.InactiveThreshold, info.Config.Durable)
				}
			}
			if found != 1 {
				t.Errorf("found %d consumers, want the one ordered consumer", found)
			}
			cancel()

			d, _ := streams.AsDurable(m)
			if _, perr := d.Consume(ctx, subj, "c"); perr != nil {
				t.Fatal(perr)
			}
			c, err := handle.Consumer(ctx, "c")
			if err != nil {
				t.Fatal(err)
			}
			cfg := c.CachedInfo().Config
			if cfg.AckWait != tc.ackWait || cfg.MaxDeliver != tc.maxDeliver {
				t.Errorf("durable: ack wait %s, max deliver %d", cfg.AckWait, cfg.MaxDeliver)
			}
		})
	}
}

// UseJetStream asks the server, so one without JetStream fails at startup.
func TestUseJetStreamRefusesAServerWithoutJetStream(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := gonats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	if _, err := streamsnats.UseJetStream(nc); err == nil {
		t.Error("UseJetStream succeeded against a server without JetStream")
	}
}
