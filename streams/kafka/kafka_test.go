package kafka_test

// These run against a Kafka cluster started in-process by kfake, so they need
// no broker installed and no container.

import (
	"context"
	"errors"
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
func testStreams(t *testing.T, opts ...streamskafka.Option) streams.Streams {
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
