package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/mqtt"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// reading is this program's model.
type reading struct {
	Device string  `json:"device"`
	Value  float64 `json:"value"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(telemetry.LevelInfo)})))

	const subject = "sensor/temperature"

	s, err := mqtt.Connect(ctx, "localhost:1883",
		mqtt.WithLogger(logger),
		mqtt.WithPrefix("example"),
	)
	if err != nil {
		log.Fatalf("the MQTT broker is not reachable: %v", err)
	}
	defer closeProvider(s)

	stream, err := s.Create(ctx, streams.Stream{Name: "sensors", Subjects: []string{"sensor/+"}})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	defer func() { _ = s.Delete(context.Background(), stream.ID) }()

	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	// MQTT is durable but not positioned, and that split is the interesting
	// thing about it: a session holds what a consumer has not handled, but a
	// session is a queue rather than a log, so there is nothing to seek in.
	d, err := streams.AsDurable(m)
	if err != nil {
		log.Fatalf("AsDurable: %v", err)
	}
	if _, perr := streams.AsPositioned(m); errors.Is(perr, streams.ErrUnsupported) {
		log.Printf("MQTT keeps no log to replay, as expected: %v", perr)
	}

	log.Println("--- a session outlives the consumer ---")

	// Attach once so the broker creates the session and its subscription.
	first, cancelFirst := context.WithCancel(ctx)
	deliveries, err := d.Consume(first, subject, "recorder")
	if err != nil {
		log.Fatalf("Consume: %v", err)
	}

	if _, perr := m.Publish(first, subject, reading{Device: "kitchen", Value: 21.5}); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}

	msg := receive(deliveries)
	log.Printf("consumer one received %s", msg.ID)
	if aerr := msg.Ack(first); aerr != nil {
		log.Fatalf("Ack: %v", aerr)
	}

	// The acknowledgement is a packet on the wire, so give it a moment to land
	// before dropping the connection — otherwise the broker never hears it and
	// delivers the message again.
	time.Sleep(300 * time.Millisecond)
	cancelFirst()
	time.Sleep(500 * time.Millisecond)

	// Published while nobody is attached. A clean session would drop this; the
	// named one keeps it.
	if _, perr := m.Publish(ctx, subject, reading{Device: "kitchen", Value: 22.0}); perr != nil {
		log.Fatalf("Publish while detached: %v", perr)
	}
	log.Println("published with the consumer offline")

	second, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()

	again, err := d.Consume(second, subject, "recorder")
	if err != nil {
		log.Fatalf("Consume (second): %v", err)
	}

	resumed := receive(again)
	var got reading
	if derr := resumed.Decode(&got); derr != nil {
		log.Fatalf("Decode: %v", derr)
	}
	log.Printf("the session had kept it: %+v", got)
	if aerr := resumed.Ack(second); aerr != nil {
		log.Fatalf("Ack: %v", aerr)
	}
	time.Sleep(300 * time.Millisecond)

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
