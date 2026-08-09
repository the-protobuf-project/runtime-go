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

	log.Println("done")
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
