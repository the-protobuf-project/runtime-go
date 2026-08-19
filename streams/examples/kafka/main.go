package main

import (
	"context"
	"github.com/the-protobuf-project/runtime-go/observability"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/kafka"
)

// order is this program's model.
type order struct {
	Account string `json:"account"`
	Action  string `json:"action"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := observability.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(observability.LevelInfo)})))

	const subject = "order.placed"

	// Kafka dials: a consumer group is chosen when a client is built, so every
	// named consumer needs a connection of its own and this package makes them.
	s, err := kafka.Connect(ctx, []string{"localhost:9092"},
		kafka.WithLogger(logger),
		kafka.WithPrefix("example"),
		// More than one partition, so PartitionKey has something to choose
		// between. With one, everything is ordered and the option is moot.
		kafka.WithPartitions(4),
	)
	if err != nil {
		log.Fatalf("Kafka is not reachable: %v", err)
	}
	defer closeProvider(s)

	stream, err := s.Create(ctx, streams.Stream{Name: "orders", Subjects: []string{subject}})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	defer func() { _ = s.Delete(context.Background(), stream.ID) }()

	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	// Kafka is the only provider where PartitionKey does anything: it orders
	// within a partition and nowhere else, so two messages that must be seen in
	// order need the same key.
	log.Println("--- ordering by partition key ---")
	for _, action := range []string{"placed", "paid", "shipped"} {
		if _, perr := m.Publish(ctx, subject,
			order{Account: "acct-1", Action: action},
			streams.PartitionKey("acct-1"),
		); perr != nil {
			log.Fatalf("Publish %s: %v", action, perr)
		}
	}
	log.Println("published three messages under one key, so they share a partition")

	// A named consumer is a Kafka consumer group. FromEarliest because the
	// messages above were published before it existed — a fresh group starting
	// at FromNew would skip them.
	p, err := streams.AsPositioned(m)
	if err != nil {
		log.Fatalf("AsPositioned: %v", err)
	}

	consuming, stopConsuming := context.WithCancel(ctx)
	defer stopConsuming()

	deliveries, err := p.ConsumeFrom(consuming, subject, "billing", streams.FromEarliest)
	if err != nil {
		log.Fatalf("ConsumeFrom: %v", err)
	}

	log.Println("--- consuming ---")
	for range 3 {
		msg := receive(deliveries)

		var got order
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("Decode: %v", derr)
		}
		log.Printf("received %s -> %+v", msg.ID, got)

		// Acknowledging commits the offset. Kafka tracks one offset per
		// partition rather than one per message, so this marks everything
		// before it in that partition as handled too — which is why a partition
		// should be processed in order.
		if aerr := msg.Ack(consuming); aerr != nil {
			log.Fatalf("Ack: %v", aerr)
		}
	}

	log.Println("done")
}

func receive(ch <-chan streams.Delivery) streams.Delivery {
	select {
	case d, ok := <-ch:
		if !ok {
			log.Fatal("the delivery channel closed early")
		}
		return d
	case <-time.After(30 * time.Second):
		log.Fatal("timed out waiting for a delivery")
		return streams.Delivery{}
	}
}

func closeProvider(s streams.Streams) {
	if c, ok := s.(streams.Closer); ok {
		_ = c.Close()
	}
}
