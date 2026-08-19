package main

import (
	"context"
	"errors"
	"github.com/the-protobuf-project/runtime-go/observability"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/nats"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const url = "nats://localhost:4222"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := observability.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(observability.LevelInfo)})))

	runCore(ctx, logger)
	runJetStream(ctx, logger)

	log.Println("done")
}

// runCore shows core NATS: delivery to whoever is listening, and nothing kept.
func runCore(ctx context.Context, logger observability.Logger) {
	log.Println("--- core NATS ---")

	const subject = "user.created"

	// Connect dials and owns the connection. Pass nats.Use(conn) instead to
	// supply one of your own — with credentials, TLS, or reconnect handlers.
	s, err := nats.Connect(ctx, url, nats.WithLogger(logger))
	if err != nil {
		log.Fatalf("NATS is not reachable: %v", err)
	}
	defer closeProvider(s)

	stream, err := s.Create(ctx, streams.Stream{
		Name:        "users",
		Description: "user lifecycle events",
		Subjects:    []string{"user.*"},
	})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	defer func() { _ = s.Delete(context.Background(), stream.ID) }()

	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	// Subscribe first: the subscription is live by the time this returns, so a
	// value published afterwards is delivered rather than raced.
	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		log.Fatalf("Subscribe: %v", err)
	}

	if _, perr := m.Publish(ctx, subject, event{User: "alice", Action: "created"}); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-msgs:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("Decode: %v", derr)
		}
		log.Printf("received %s -> %+v", msg.ID, got)
	case <-time.After(5 * time.Second):
		log.Fatal("timed out")
	}

	// The stream declares a wildcard, so it accepts any single token under
	// "user." — but a message still has to land somewhere specific.
	if _, perr := m.Publish(ctx, "user.*", event{}); errors.Is(perr, streams.ErrUnknownSubject) {
		log.Println("publishing to a wildcard was rejected, as expected")
	}

	// Core NATS keeps no log, so it refuses the durable half by name rather
	// than handing back something that cannot redeliver.
	if _, derr := streams.AsDurable(m); derr != nil {
		log.Printf("core NATS is not durable, as expected: %v", derr)
	}
}

// runJetStream shows the same contract backed by a stored log.
func runJetStream(ctx context.Context, logger observability.Logger) {
	log.Println("--- JetStream ---")

	const subject = "order.placed"

	s, err := nats.ConnectJetStream(ctx, url, nats.WithLogger(logger))
	if err != nil {
		log.Fatalf("ConnectJetStream: %v (is the server running with JetStream enabled?)", err)
	}
	defer closeProvider(s)

	stream, err := s.Create(ctx, streams.Stream{Name: "orders", Subjects: []string{"order.*"}})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	defer func() { _ = s.Delete(context.Background(), stream.ID) }()

	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	// This is the difference. A named consumer's position lives on the server,
	// so it outlives this process, and a message stays deliverable until
	// someone says otherwise.
	d, err := streams.AsDurable(m)
	if err != nil {
		log.Fatalf("AsDurable: %v", err)
	}

	// The consumer gets a context of its own so it can be wound down before the
	// deferred Delete removes the stream underneath it.
	consuming, stopConsuming := context.WithCancel(ctx)

	deliveries, err := d.Consume(consuming, subject, "billing")
	if err != nil {
		log.Fatalf("Consume: %v", err)
	}

	if _, perr := m.Publish(ctx, subject, event{User: "alice", Action: "ordered"}); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}

	// Nak returns the message for redelivery. The attempt count is the signal a
	// consumer uses to notice it is in a loop it cannot escape.
	first := receive(deliveries, "the first delivery")
	log.Printf("attempt %d; returning it with Nak", first.Attempt)
	if nerr := first.Nak(ctx); nerr != nil {
		log.Fatalf("Nak: %v", nerr)
	}

	second := receive(deliveries, "the redelivery")
	var got event
	if derr := second.Decode(&got); derr != nil {
		log.Fatalf("Decode: %v", derr)
	}
	log.Printf("redelivered on attempt %d -> %+v", second.Attempt, got)

	// Acknowledge after the work, not on receipt: acknowledging first turns
	// at-least-once into at-most-once.
	if aerr := second.Ack(ctx); aerr != nil {
		log.Fatalf("Ack: %v", aerr)
	}
	log.Println("acknowledged; it will not be delivered again")

	// Stop consuming and wait for the channel to close before returning, so the
	// deferred Delete runs against a stream nobody is reading.
	stopConsuming()
	for range deliveries {
		// Draining until the channel closes is the wait.
	}
}

// receive waits for one delivery or gives up.
func receive(ch <-chan streams.Delivery, what string) streams.Delivery {
	select {
	case d, ok := <-ch:
		if !ok {
			log.Fatalf("the channel closed before %s", what)
		}
		return d
	case <-time.After(10 * time.Second):
		log.Fatalf("timed out waiting for %s", what)
		return streams.Delivery{}
	}
}

// closeProvider releases a provider that dialed its own connection.
func closeProvider(s streams.Streams) {
	if c, ok := s.(streams.Closer); ok {
		_ = c.Close()
	}
}
