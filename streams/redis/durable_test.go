package redis_test

// Durable delivery, backed by Redis Streams. These need the same live server as
// the rest of the package and skip without one, but unlike the scheduled tests
// they need no special server flags.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
)

// quickReclaim is short enough to see a redelivery inside a test and long
// enough that a consumer which is merely slow does not have its message taken.
const quickReclaim = 300 * time.Millisecond

// newDurable returns durable streams under a prefix unique to this test.
func newDurable(t *testing.T, opts ...streamsredis.Option) streams.Streams {
	t.Helper()
	opts = append([]streamsredis.Option{
		streamsredis.WithPrefix(prefix()),
		streamsredis.WithReclaimAfter(quickReclaim),
	}, opts...)
	return streamsredis.ConnectDurable(testClient(t), opts...)
}

// bindDurable creates a stream and returns its durable half.
func bindDurable(t *testing.T, s streams.Streams, subjects ...string) streams.Durable {
	t.Helper()

	_, m := bind(t, s, subjects...)
	d, err := streams.AsDurable(m)
	if err != nil {
		t.Fatalf("AsDurable: %v", err)
	}
	return d
}

// recvDelivery waits for one delivery or fails the test.
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

// The capability is the point: a durable provider answers AsDurable and a
// pub/sub one refuses by name rather than with a bare false.
func TestAsDurableSucceedsOnlyOnTheDurableProvider(t *testing.T) {
	_, durable := bind(t, newDurable(t), subject)
	if _, err := streams.AsDurable(durable); err != nil {
		t.Errorf("AsDurable on the durable provider: %v", err)
	}
	if _, err := streams.AsPositioned(durable); err != nil {
		t.Errorf("AsPositioned on the durable provider: %v", err)
	}

	_, pubsub := bind(t, newStreams(t), subject)
	err := func() error { _, e := streams.AsDurable(pubsub); return e }()
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsDurable on pub/sub = %v, want ErrUnsupported", err)
	}
}

func TestConsumeDeliversAndAcknowledges(t *testing.T) {
	s := newDurable(t)
	d := bindDurable(t, s, subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	m, err := s.Bind(ctx, streamOf(t, s))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	want := event{User: "ada", Action: "created"}
	if _, perr := m.Publish(ctx, subject, want); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	got := recvDelivery(t, ch, 5*time.Second)
	if got.Attempt != 1 {
		t.Errorf("Attempt = %d on a first delivery, want 1", got.Attempt)
	}

	var decoded event
	if derr := got.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded != want {
		t.Errorf("decoded %+v, want %+v", decoded, want)
	}
	if aerr := got.Ack(ctx); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
}

// The whole reason to reach for Durable: a consumer that dies holding a message
// must not be the last word on it.
func TestAnUnacknowledgedMessageIsRedelivered(t *testing.T) {
	s := newDurable(t)
	d := bindDurable(t, s, subject)
	id := streamOf(t, s)

	m, err := s.Bind(t.Context(), id)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	// The group has to exist before the publish: Consume starts at FromNew, so
	// a message appended before the name was ever used is behind its position.
	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(first, subject, event{User: "grace"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	// The first consumer takes it and dies without acknowledging.
	if got := recvDelivery(t, ch, 5*time.Second); got.Attempt != 1 {
		t.Errorf("first Attempt = %d, want 1", got.Attempt)
	}
	cancelFirst()

	// A second consumer under the same name picks it back up.
	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()

	again, err := d.Consume(second, subject, "workers")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}
	got := recvDelivery(t, again, 10*time.Second)
	if got.Attempt < 2 {
		t.Errorf("Attempt = %d on a redelivery, want at least 2", got.Attempt)
	}

	var decoded event
	if derr := got.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "grace" {
		t.Errorf("redelivered %+v, want the message that was never acknowledged", decoded)
	}
}

// And the other half: an acknowledged message is not delivered again.
func TestAnAcknowledgedMessageIsNotRedelivered(t *testing.T) {
	s := newDurable(t)
	d := bindDurable(t, s, subject)

	m, err := s.Bind(t.Context(), streamOf(t, s))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(first, subject, event{User: "ada"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	got := recvDelivery(t, ch, 5*time.Second)
	if aerr := got.Ack(first); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
	cancelFirst()

	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()

	again, err := d.Consume(second, subject, "workers")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}
	select {
	case d, ok := <-again:
		if ok {
			t.Fatalf("an acknowledged message came back: %+v", d.Message)
		}
	case <-time.After(2 * time.Second):
		// Nothing arrived, which is the point.
	}
}

// A named consumer's position outlives the process reading it, so a restart
// resumes rather than replays.
func TestAConsumerResumesWhereTheNameLeftOff(t *testing.T) {
	s := newDurable(t)
	d := bindDurable(t, s, subject)

	m, err := s.Bind(t.Context(), streamOf(t, s))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(first, subject, event{User: "first"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	got := recvDelivery(t, ch, 5*time.Second)
	if aerr := got.Ack(first); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
	cancelFirst()

	// Published while nobody was consuming. The group's position is server-side,
	// so it is still waiting when the name comes back.
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
	if derr := resumed.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "second" {
		t.Errorf("resumed at %+v, want the message published while detached", decoded)
	}
}

// FromEarliest reads a log that was written before anyone was listening;
// FromNew, the zero value, does not.
func TestConsumeFromEarliestReplaysHistory(t *testing.T) {
	s := newDurable(t)
	_, m := bind(t, s, subject)

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
	got := recvDelivery(t, ch, 5*time.Second)

	var decoded event
	if derr := got.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "before" {
		t.Errorf("replayed %+v, want the message published before consuming", decoded)
	}
}

func TestConsumeFromNewSkipsHistory(t *testing.T) {
	s := newDurable(t)
	_, m := bind(t, s, subject)

	p, err := streams.AsPositioned(m)
	if err != nil {
		t.Fatalf("AsPositioned: %v", err)
	}
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

// Two processes under one name share the position and split the work rather
// than each doing all of it.
func TestOneNameSplitsTheWork(t *testing.T) {
	// A long reclaim so the two consumers cannot take each other's messages
	// while the test is running — this is about the split, not redelivery.
	s := newDurable(t, streamsredis.WithReclaimAfter(time.Minute))
	d := bindDurable(t, s, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, err := d.Consume(ctx, subject, "shared")
	if err != nil {
		t.Fatalf("Consume (first): %v", err)
	}
	second, err := d.Consume(ctx, subject, "shared")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}

	m, err := s.Bind(ctx, streamOf(t, s))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	const total = 6
	for i := range total {
		if _, perr := m.Publish(ctx, subject, event{User: "u", Action: string(rune('a' + i))}); perr != nil {
			t.Fatalf("Publish %d: %v", i, perr)
		}
	}

	seen := map[string]bool{}
	deadline := time.After(15 * time.Second)
	for len(seen) < total {
		select {
		case d := <-first:
			seen[d.ID] = true
			_ = d.Ack(ctx)
		case d := <-second:
			seen[d.ID] = true
			_ = d.Ack(ctx)
		case <-deadline:
			t.Fatalf("saw %d of %d messages across both consumers", len(seen), total)
		}
	}
}

// Subscribe on this provider tails the log without acknowledging anything.
func TestDurableSubscribeTailsTheLog(t *testing.T) {
	s := newDurable(t)
	_, m := bind(t, s, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := m.Publish(ctx, subject, event{User: "tail"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-ch:
		var decoded event
		if derr := msg.Decode(&decoded); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if decoded.User != "tail" {
			t.Errorf("received %+v, want the published message", decoded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe delivered nothing")
	}
}

// A durable stream has no timer to schedule against, and saying so beats
// appending immediately and letting the caller believe otherwise.
func TestDurableRejectsATTL(t *testing.T) {
	_, m := bind(t, newDurable(t), subject)

	_, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

// Group and the consumer name would be two names for one thing here.
func TestConsumeRejectsAGroupOption(t *testing.T) {
	d := bindDurable(t, newDurable(t), subject)

	_, err := d.Consume(t.Context(), subject, "workers", streams.Group("other"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	d := bindDurable(t, newDurable(t), subject)

	_, err := d.Consume(t.Context(), subject, "")
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRejectsAnUndeclaredSubject(t *testing.T) {
	d := bindDurable(t, newDurable(t), subject)

	_, err := d.Consume(t.Context(), "typo", "workers")
	if !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Consume on an undeclared subject = %v, want ErrUnknownSubject", err)
	}
}

// streamOf returns the id of the single stream in s, for tests that bind twice.
func streamOf(t *testing.T, s streams.Streams) string {
	t.Helper()

	list, err := s.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected exactly one stream, found %d", len(list))
	}
	return list[0].ID
}
