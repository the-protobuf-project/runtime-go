package kafka_test

// These run against a Kafka cluster started in-process by kfake, so they need
// no broker installed and no container.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamskafka "github.com/the-protobuf-project/runtime-go/streams/kafka"
	"github.com/twmb/franz-go/pkg/kfake"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user.created"

// testStreams starts a cluster and returns a provider pointed at it.
func testStreams(t testing.TB, opts ...streamskafka.Option) streams.Streams {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	s, err := streamskafka.Connect(t.Context(), cluster.ListenAddrs(), opts...)
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

// declare creates a stream and binds a manager to it.
func declare(t testing.TB, s streams.Streams, subjects ...string) (streams.Stream, streams.Manager) {
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

func recvDelivery(t testing.TB, ch <-chan streams.Delivery, within time.Duration) streams.Delivery {
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
		Name: "users", Description: "before", Subjects: []string{subject}, UserID: "u-1",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == "" {
		t.Fatal("Create did not assign an id")
	}

	// Kafka has nowhere on a topic to hang a description or a user id, so these
	// ride in a compacted metadata topic. If that breaks they come back empty.
	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "users" || got.UserID != "u-1" || got.Description != "before" {
		t.Errorf("Get lost metadata: %+v", got)
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

func TestGetOnAMissingStreamIsNotFound(t *testing.T) {
	if _, err := testStreams(t).Get(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}

func TestBindOnAMissingStreamIsNotFound(t *testing.T) {
	if _, err := testStreams(t).Bind(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Bind = %v, want ErrNotFound", err)
	}
}

func TestKafkaIsDurableAndPositioned(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := streams.AsDurable(m); err != nil {
		t.Errorf("AsDurable on Kafka: %v", err)
	}
	if _, err := streams.AsPositioned(m); err != nil {
		t.Errorf("AsPositioned on Kafka: %v", err)
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
	// The consumer starts at the end of the log, and joining is not instant.
	time.Sleep(500 * time.Millisecond)

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
	case <-time.After(15 * time.Second):
		t.Fatal("nothing was delivered")
	}
}

func TestConsumeDeliversAndAcknowledges(t *testing.T) {
	s := testStreams(t)
	_, m := declare(t, s, subject)

	p, err := streams.AsPositioned(m)
	if err != nil {
		t.Fatalf("AsPositioned: %v", err)
	}
	if _, perr := m.Publish(t.Context(), subject, event{User: "grace"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// FromEarliest so the publish above is in range; a fresh group with no
	// committed offset starting at the end would skip it.
	ch, err := p.ConsumeFrom(ctx, subject, "billing", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}

	got := recvDelivery(t, ch, 20*time.Second)
	var decoded event
	if derr := got.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "grace" {
		t.Errorf("decoded %+v, want the published event", decoded)
	}
	// Kafka cannot count redeliveries, and the contract asks for zero there
	// rather than a number that would be made up.
	if got.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0 where the provider cannot count", got.Attempt)
	}
	if aerr := got.Ack(ctx); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
}

// A committed offset is the position that survives the process.
func TestAConsumerResumesWhereTheNameLeftOff(t *testing.T) {
	s := testStreams(t)
	_, m := declare(t, s, subject)
	p, _ := streams.AsPositioned(m)

	for _, who := range []string{"first", "second"} {
		if _, err := m.Publish(t.Context(), subject, event{User: who}); err != nil {
			t.Fatalf("Publish %s: %v", who, err)
		}
	}

	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := p.ConsumeFrom(first, subject, "billing", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	got := recvDelivery(t, ch, 20*time.Second)
	if aerr := got.Ack(first); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
	cancelFirst()

	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()

	// The same name, and this time FromNew: the position is the committed
	// offset, not the reset policy, so it picks up at the second message.
	again, err := p.ConsumeFrom(second, subject, "billing", streams.FromNew)
	if err != nil {
		t.Fatalf("ConsumeFrom (second): %v", err)
	}
	resumed := recvDelivery(t, again, 20*time.Second)

	var decoded event
	if derr := resumed.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "second" {
		t.Errorf("resumed at %+v, want the message after the acknowledged one", decoded)
	}
}

// PartitionKey is the reason this provider exists in the contract: it is the
// only one where the option does anything.
func TestPartitionKeyIsCarried(t *testing.T) {
	s := testStreams(t, streamskafka.WithPartitions(4))
	_, m := declare(t, s, subject)
	p, _ := streams.AsPositioned(m)

	// Two messages sharing a key must land on one partition, and so keep their
	// order relative to each other.
	for _, action := range []string{"one", "two"} {
		if _, err := m.Publish(t.Context(), subject,
			event{User: "ada", Action: action}, streams.PartitionKey("ada")); err != nil {
			t.Fatalf("Publish %s: %v", action, err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := p.ConsumeFrom(ctx, subject, "ordering", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}

	for _, want := range []string{"one", "two"} {
		got := recvDelivery(t, ch, 20*time.Second)
		var decoded event
		if derr := got.Decode(&decoded); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if decoded.Action != want {
			t.Fatalf("received %q, want %q — messages sharing a key lost their order", decoded.Action, want)
		}
		if aerr := got.Ack(ctx); aerr != nil {
			t.Fatalf("Ack: %v", aerr)
		}
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

func TestConsumeRejectsAGroupOption(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	_, err := d.Consume(t.Context(), subject, "billing", streams.Group("other"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}

func TestConnectRejectsNoSeeds(t *testing.T) {
	if _, err := streamskafka.Connect(t.Context(), nil); err == nil {
		t.Error("Connect accepted an empty broker list")
	}
}

// recvMessage returns the next message within the deadline.
func recvMessage(t testing.TB, ch <-chan streams.Message, within time.Duration) streams.Message {
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

// A tail is pinned at the end before Subscribe returns, so a publish made
// right after is delivered without waiting for anything.
func TestATailPublishedToRightAfterSubscribeIsNotRaced(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := m.Publish(ctx, subject, event{User: "ada"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recvMessage(t, ch, 15*time.Second); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
}

// Stream defaults arrive, a publish's own win, on a tail, a batch and a
// durable consumer; a publish with none carries none.
func TestHeadersTravelAsRecordHeaders(t *testing.T) {
	s := testStreams(t)
	stream, err := s.Create(t.Context(), streams.Stream{Name: "headers", Subjects: []string{subject}}.
		WithHeaders(streams.Headers{"lang": "go", "site": "plant-3"}))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	tail, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := m.Publish(ctx, subject, event{Action: "one"},
		streams.Header("site", "plant-4"), streams.Header("traceparent", "00-abc-def-01")); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	got := recvMessage(t, tail, 15*time.Second)
	want := streams.Headers{"lang": "go", "site": "plant-4", "traceparent": "00-abc-def-01"}
	if !maps.Equal(got.Headers, want) {
		t.Errorf("headers %v, want %v", got.Headers, want)
	}

	b, _ := streams.AsBatch(m)
	if _, berr := b.PublishBatch(ctx, subject, batchOf(2), streams.Header("site", "plant-9")); berr != nil {
		t.Fatalf("PublishBatch: %v", berr)
	}
	p, _ := streams.AsPositioned(m)
	ch, err := p.ConsumeFrom(ctx, subject, "headers", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	for i, site := range []string{"plant-4", "plant-9", "plant-9"} {
		d := recvDelivery(t, ch, 20*time.Second)
		if d.Headers.Get("site") != site || d.Headers.Get("lang") != "go" {
			t.Errorf("delivery %d headers %v, want site %s and lang go", i, d.Headers, site)
		}
		if d.CanDelayRedelivery() {
			t.Error("a Kafka delivery claims it can delay redelivery")
		}
		_ = d.Ack(ctx)
	}

	// A stream with no defaults and a publish with none: no headers at all.
	_, plain := declare(t, s, subject)
	plainTail, err := plain.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := plain.Publish(ctx, subject, event{}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	if got := recvMessage(t, plainTail, 15*time.Second); len(got.Headers) != 0 {
		t.Errorf("a header-less publish arrived with %v", got.Headers)
	}
}

// Default headers ride in the stored declaration, and a bad one is refused at
// the call that set it, changing nothing.
func TestStreamHeadersAreStoredAndChecked(t *testing.T) {
	s := testStreams(t)
	want := streams.Headers{"lang": "go", "site": "plant-3"}
	stream, err := s.Create(t.Context(), streams.Stream{Name: "h", Subjects: []string{subject}}.WithHeaders(want))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got, _ := s.Get(t.Context(), stream.ID); !maps.Equal(got.Headers, want) {
		t.Errorf("Get headers %v, want %v", got.Headers, want)
	}
	if list, _ := s.List(t.Context()); len(list) != 1 || !maps.Equal(list[0].Headers, want) {
		t.Errorf("List = %+v", list)
	}

	updated, err := s.Update(t.Context(), stream.ID,
		streams.Stream{Name: "h", Subjects: []string{subject}}.WithHeader("lang", "rust"))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	for _, bad := range []string{"has space", "has:colon", ""} {
		def := streams.Stream{Name: "bad", Subjects: []string{subject}}.WithHeader(bad, "x")
		if _, cerr := s.Create(t.Context(), def); !errors.Is(cerr, streams.ErrInvalidHeader) {
			t.Errorf("Create %q = %v, want ErrInvalidHeader", bad, cerr)
		}
		if _, uerr := s.Update(t.Context(), stream.ID, def); !errors.Is(uerr, streams.ErrInvalidHeader) {
			t.Errorf("Update %q = %v, want ErrInvalidHeader", bad, uerr)
		}
	}
	if got, _ := s.Get(t.Context(), stream.ID); !maps.Equal(got.Headers, updated.Headers) {
		t.Errorf("a refused update changed the headers to %v", got.Headers)
	}
	if list, _ := s.List(t.Context()); len(list) != 1 {
		t.Errorf("a refused create declared something: %d streams", len(list))
	}
}

func TestPublishRefusesExpiryAndRetain(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	b, _ := streams.AsBatch(m)
	for _, opt := range []streams.Option{streams.Expires(5 * time.Second), streams.Retain()} {
		if _, err := m.Publish(t.Context(), subject, event{}, opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("Publish = %v, want ErrUnsupported", err)
		}
		if _, err := b.PublishBatch(t.Context(), subject, batchOf(1), opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("PublishBatch = %v, want ErrUnsupported", err)
		}
	}
	// A zero expiry is none, and goes through.
	if _, err := m.Publish(t.Context(), subject, event{}, streams.Expires(0)); err != nil {
		t.Errorf("Publish with a zero expiry: %v", err)
	}
}

// Every producer of the pool carries headers, an out-of-order ack commits
// nothing past an unsettled delivery, and the redelivery brings the headers
// back.
func TestThePoolCarriesHeadersAndAnUnsettledDeliveryIsRedelivered(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	ctx, cancel := context.WithCancel(t.Context())

	// Sixteen unkeyed publishes take turns over all eight producers.
	const singles = 16
	for n := range singles {
		if _, err := m.Publish(ctx, subject, event{User: "pool", Action: fmt.Sprint(n)},
			streams.Header("n", fmt.Sprint(n))); err != nil {
			t.Fatalf("Publish %d: %v", n, err)
		}
	}

	p, _ := streams.AsPositioned(m)
	ch, err := p.ConsumeFrom(ctx, subject, "pool", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	var held streams.Delivery
	for i := range singles {
		d := recvDelivery(t, ch, 20*time.Second)
		var e event
		_ = d.Decode(&e)
		if d.Headers.Get("n") != e.Action {
			t.Errorf("delivery %d carries n=%q, want %q", i, d.Headers.Get("n"), e.Action)
		}
		if i == 1 {
			held = d
			if nerr := d.NakAfter(ctx, time.Second); !errors.Is(nerr, streams.ErrUnsupported) {
				t.Errorf("NakAfter = %v, want ErrUnsupported", nerr)
			}
			continue
		}
		if aerr := d.Ack(ctx); aerr != nil {
			t.Fatalf("Ack: %v", aerr)
		}
	}
	cancel()
	// Let the consumer make its last commit and leave.
	for range ch {
	}

	again, err := p.ConsumeFrom(t.Context(), subject, "pool", streams.FromNew)
	if err != nil {
		t.Fatalf("ConsumeFrom (again): %v", err)
	}
	d := recvDelivery(t, again, 20*time.Second)
	if d.ID != held.ID || d.Headers.Get("n") != held.Headers.Get("n") {
		t.Errorf("redelivered %q n=%q, want %q n=%q", d.ID, d.Headers.Get("n"), held.ID, held.Headers.Get("n"))
	}
	if aerr := d.Ack(t.Context()); aerr != nil {
		t.Errorf("Ack: %v", aerr)
	}
	if aerr := held.Ack(t.Context()); aerr == nil {
		t.Error("an ack after its consumer stopped succeeded")
	}
}

// With a group, members share the subject and each message reaches one of
// them; nothing published after Subscribe is lost while the group settles.
func TestAGroupSharesASubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	a, err := m.Subscribe(ctx, subject, streams.Group("workers"))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := m.Subscribe(ctx, subject, streams.Group("workers"))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const total = 10
	want := map[string]bool{}
	for n := range total {
		id, perr := m.Publish(ctx, subject, event{Action: fmt.Sprint(n)})
		if perr != nil {
			t.Fatalf("Publish: %v", perr)
		}
		want[id] = true
	}
	seenA, seenB := map[string]bool{}, map[string]bool{}
	deadline := time.After(30 * time.Second)
	for len(want) > 0 {
		select {
		case msg := <-a:
			seenA[msg.ID] = true
			delete(want, msg.ID)
		case msg := <-b:
			seenB[msg.ID] = true
			delete(want, msg.ID)
		case <-deadline:
			t.Fatalf("grouped messages never arrived: %v", want)
		}
	}
	if len(seenA) == total && len(seenB) == total {
		t.Error("both members received every message: a group fanned out")
	}
}
