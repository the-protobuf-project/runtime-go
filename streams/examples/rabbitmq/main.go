package main

import (
	"context"
	"github.com/the-protobuf-project/runtime-go/observability"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

// job is this program's model.
type job struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := observability.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(observability.LevelInfo)})))

	const subject = "job.queued"

	s, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
		rabbitmq.WithLogger(logger),
		rabbitmq.WithPrefix("example"),
	)
	if err != nil {
		log.Fatalf("RabbitMQ is not reachable: %v", err)
	}
	defer closeProvider(s)

	stream, err := s.Create(ctx, streams.Stream{Name: "jobs", Subjects: []string{subject}})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	defer func() { _ = s.Delete(context.Background(), stream.ID) }()

	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	d, err := streams.AsDurable(m)
	if err != nil {
		log.Fatalf("AsDurable: %v", err)
	}

	consuming, stopConsuming := context.WithCancel(ctx)
	defer stopConsuming()

	deliveries, err := d.Consume(consuming, subject, "workers")
	if err != nil {
		log.Fatalf("Consume: %v", err)
	}

	if _, perr := m.Publish(ctx, subject, job{ID: "j-1", Kind: "resize"}); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}

	// This is what RabbitMQ has that the others do not: a true negative
	// acknowledgement. Nak hands the message straight back rather than waiting
	// out a visibility timeout, as Redis does, or doing nothing until the
	// partition moves, as Kafka does.
	log.Println("--- Nak returns it immediately ---")

	first := receive(deliveries)
	log.Printf("received %s on attempt %d; returning it with Nak", first.ID, first.Attempt)
	if nerr := first.Nak(consuming); nerr != nil {
		log.Fatalf("Nak: %v", nerr)
	}

	second := receive(deliveries)
	var got job
	if derr := second.Decode(&got); derr != nil {
		log.Fatalf("Decode: %v", derr)
	}
	log.Printf("handed straight back on attempt %d -> %+v", second.Attempt, got)

	// Acknowledge after the work, not on receipt.
	if aerr := second.Ack(consuming); aerr != nil {
		log.Fatalf("Ack: %v", aerr)
	}
	log.Println("acknowledged; it will not be delivered again")

	log.Println("done")
}

func receive(ch <-chan streams.Delivery) streams.Delivery {
	select {
	case d, ok := <-ch:
		if !ok {
			log.Fatal("the delivery channel closed early")
		}
		return d
	case <-time.After(15 * time.Second):
		log.Fatal("timed out waiting for a delivery")
		return streams.Delivery{}
	}
}

func closeProvider(s streams.Streams) {
	if c, ok := s.(streams.Closer); ok {
		_ = c.Close()
	}
}
