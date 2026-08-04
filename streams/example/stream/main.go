// Command stream demonstrates the runtime-go streams module against a live
// Redis: ordinary pub/sub, then a TTL-expiry notification.
//
//	docker compose -f ../../docker/compose.yaml up -d
//	go run ./example/stream
package main

import (
	"context"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

const (
	subjectLogin    = "user.login"
	subjectReminder = "reminder"
)

func main() {
	// Everything is scoped to this context. Canceling it closes every
	// subscription and releases the delivery goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The caller owns the connection.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
		DB:   9,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis is not reachable: %v", err)
	}

	p, err := streamsredis.New(streamsredis.Config{Client: rdb, Prefix: "example"})
	if err != nil {
		log.Fatalf("Failed to build provider: %v", err)
	}

	// --- ORDINARY PUB/SUB ---
	s, err := p.Create(ctx, streams.Stream{
		Name:        "events",
		Description: "user events",
		Subjects:    []string{subjectLogin},
	})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	log.Printf("created stream %s", s.ID())

	m, err := p.Bind(ctx, s.ID())
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	// Subscribe before publishing: the subscription is live by the time
	// Subscribe returns, so nothing is raced.
	msgs, err := m.Subscribe(ctx, subjectLogin)
	if err != nil {
		log.Fatalf("Subscribe: %v", err)
	}

	// Cross-cutting behavior wraps the publisher.
	pub := streams.ChainPublisher(m,
		streams.WithPublisherRetryMiddleware(3, 100*time.Millisecond),
		streams.WithPublisherTelemetryMiddleware(telemetry.NoopMeter),
	)

	if perr := pub.Publish(ctx, subjectLogin, streams.Message{
		Data: map[string]any{"user": "alice"},
	}); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-msgs:
		log.Printf("received %s -> %v", msg.ID(), msg.Data)
	case <-time.After(2 * time.Second):
		log.Fatal("timed out waiting for the published message")
	}

	// --- EXPIRY NOTIFICATIONS ---
	// Delivery happens when the TTL runs out, not when Publish is called. This
	// needs the server running with --notify-keyspace-events Ex.
	notify := p.Notifications()

	n, err := notify.Create(ctx, streams.Stream{
		Name:     "reminders",
		Subjects: []string{subjectReminder},
	})
	if err != nil {
		log.Fatalf("Notifications Create: %v", err)
	}

	nm, err := notify.Bind(ctx, n.ID())
	if err != nil {
		log.Fatalf("Notifications Bind: %v", err)
	}

	reminders, err := nm.Subscribe(ctx, subjectReminder)
	if err != nil {
		log.Fatalf("Notifications Subscribe: %v", err)
	}

	if perr := nm.Publish(ctx, subjectReminder, streams.Message{
		Data: map[string]any{"body": "take a pill"},
		TTL:  time.Second, // a zero TTL is rejected: it could never fire
	}); perr != nil {
		log.Fatalf("Notifications Publish: %v", perr)
	}
	log.Println("scheduled a reminder for 1s from now")

	select {
	case msg := <-reminders:
		log.Printf("reminder fired: %v", msg.Data)
	case <-time.After(5 * time.Second):
		log.Fatal("reminder never fired (is --notify-keyspace-events Ex set?)")
	}

	// --- CLEANUP ---
	if derr := p.Delete(ctx, s.ID()); derr != nil {
		log.Fatalf("Delete: %v", derr)
	}
	if derr := notify.Delete(ctx, n.ID()); derr != nil {
		log.Fatalf("Notifications Delete: %v", derr)
	}
	log.Println("done")
}
