package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// event is this program's model.
type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

func main() {
	// Everything is scoped to this context. Canceling it closes every
	// subscription and releases the delivery goroutines — that is the only way
	// a subscription ends.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(telemetry.LevelDebug)})))

	// You own the connection.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
		DB:   3,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis is not reachable: %v", err)
	}

	runImmediate(ctx, rdb, logger)
	runScheduled(ctx, rdb, logger)
	runDurable(ctx, rdb, logger)

	log.Println("done")
}

// runDurable shows delivery that survives a consumer dying, over Redis Streams.
func runDurable(ctx context.Context, rdb goredis.UniversalClient, logger telemetry.Logger) {
	log.Println("--- durable ---")

	const subject = "order.placed"

	// A third constructor, because this one keeps a log where the other two
	// keep nothing: a named consumer's position lives on the server, and a
	// delivered message stays pending until it is acknowledged.
	s := streamsredis.ConnectDurable(rdb,
		streamsredis.WithPrefix("example"),
		streamsredis.WithLogger(logger),
		// Short so this demo does not wait thirty seconds to show a redelivery.
		// In a real program this belongs above your slowest handler.
		streamsredis.WithReclaimAfter(2*time.Second),
	)

	stream, err := s.Create(ctx, streams.Stream{
		Name:     "orders",
		Subjects: []string{subject},
	})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	defer func() { _ = s.Delete(context.Background(), stream.ID) }()

	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	// This is what the durable provider adds. On the other two it fails with a
	// sentence explaining why, rather than a bare false.
	d, err := streams.AsDurable(m)
	if err != nil {
		log.Fatalf("AsDurable: %v", err)
	}

	// The first consumer takes the message and then dies without
	// acknowledging, which is the case the whole capability exists for.
	first, cancelFirst := context.WithCancel(ctx)
	deliveries, err := d.Consume(first, subject, "billing")
	if err != nil {
		log.Fatalf("Consume: %v", err)
	}

	if _, perr := m.Publish(ctx, subject, event{User: "alice", Action: "ordered"}); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-deliveries:
		log.Printf("consumer one took %s on attempt %d, then died without acknowledging",
			msg.ID, msg.Attempt)
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for the first delivery")
	}
	cancelFirst()

	// A second consumer under the same name picks up what the first never
	// finished. Nothing was republished; the message was still outstanding.
	second, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()

	again, err := d.Consume(second, subject, "billing")
	if err != nil {
		log.Fatalf("Consume (second): %v", err)
	}

	select {
	case msg := <-again:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("Decode: %v", derr)
		}
		log.Printf("consumer two was handed it back on attempt %d -> %+v", msg.Attempt, got)

		// Acknowledge after the work, not on receipt: acknowledging first turns
		// at-least-once into at-most-once.
		if aerr := msg.Ack(second); aerr != nil {
			log.Fatalf("Ack: %v", aerr)
		}
		log.Println("acknowledged; it will not be delivered again")
	case <-time.After(15 * time.Second):
		log.Fatal("the unacknowledged message was never redelivered")
	}
}

// runImmediate shows delivery at publish time, over pub/sub.
func runImmediate(ctx context.Context, rdb goredis.UniversalClient, logger telemetry.Logger) {
	log.Println("--- immediate ---")

	const subject = "user.created"

	s := streamsredis.Connect(rdb,
		streamsredis.WithPrefix("example"),
		streamsredis.WithLogger(logger),
	)

	// A stream declares the subjects it accepts. Publishing or subscribing to
	// one it does not declare fails at the call that made the typo.
	stream, err := s.Create(ctx, streams.Stream{
		Name:        "users",
		Description: "user lifecycle events",
		Subjects:    []string{subject},
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

	pub := streams.ChainPublisher(m,
		streams.WithPublisherRetryMiddleware(3, 100*time.Millisecond),
		streams.WithPublisherLoggingMiddleware(logger),
	)

	id, err := pub.Publish(ctx, subject, event{User: "alice", Action: "created"})
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-msgs:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("Decode: %v", derr)
		}
		log.Printf("received %s -> %+v", msg.ID, got)
	case <-time.After(2 * time.Second):
		log.Fatalf("timed out waiting for %s", id)
	}

	// An undeclared subject is rejected rather than silently creating a topic
	// nobody reads.
	if _, perr := pub.Publish(ctx, "user.typo", event{}); errors.Is(perr, streams.ErrUnknownSubject) {
		log.Println("an undeclared subject was rejected, as expected")
	}

	// An immediate stream cannot honor a delay; it says so rather than
	// publishing now and letting you believe it was scheduled.
	if _, perr := pub.Publish(ctx, subject, event{}, streams.TTL(time.Second)); perr != nil {
		log.Println("an immediate stream rejected a TTL, as expected")
	}
}

// runScheduled shows delivery when a TTL expires, over keyspace events.
func runScheduled(ctx context.Context, rdb goredis.UniversalClient, logger telemetry.Logger) {
	log.Println("--- scheduled ---")

	const subject = "reminder"

	// A separate constructor, because the two behave differently enough to be
	// worth naming: this one requires a TTL where the immediate one rejects it.
	// Streams created here live in their own key namespace.
	n := streamsredis.ConnectScheduled(rdb,
		streamsredis.WithPrefix("example"),
		streamsredis.WithLogger(logger),
	)

	stream, err := n.Create(ctx, streams.Stream{
		Name:     "reminders",
		Subjects: []string{subject},
	})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	defer func() { _ = n.Delete(context.Background(), stream.ID) }()

	m, err := n.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	reminders, err := m.Subscribe(ctx, subject)
	if err != nil {
		log.Fatalf("Subscribe: %v", err)
	}

	// A TTL is required here: the expiry is the delivery, so a message without
	// one could never fire.
	if _, perr := m.Publish(ctx, subject, event{User: "alice", Action: "take a pill"},
		streams.TTL(time.Second)); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}
	log.Println("scheduled a reminder for 1s from now")

	select {
	case msg := <-reminders:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("Decode: %v", derr)
		}
		log.Printf("reminder fired: %+v", got)
	case <-time.After(5 * time.Second):
		log.Fatal("the reminder never fired (is --notify-keyspace-events Ex set?)")
	}
}
