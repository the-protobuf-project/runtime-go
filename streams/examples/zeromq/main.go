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
	"github.com/the-protobuf-project/runtime-go/streams/zeromq"
)

// tick is this program's model.
type tick struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
}

// endpoint is a ZeroMQ transport string. There is no broker: one side binds it
// and the other connects.
const endpoint = "tcp://127.0.0.1:5563"

// Both sides declare the same stream by the same id, because the id is part of
// the wire topic and two processes would have agreed on it.
const streamID = "market"

const subject = "tick"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := observability.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(observability.LevelInfo)})))

	// Publish binds; Subscribe connects. They are separate constructors because
	// calling the wrong one in ZeroMQ is a silent no-op, so the choice belongs
	// where it can be seen. In a real program these are two processes.
	pub, err := zeromq.Publish(ctx, endpoint, zeromq.WithLogger(logger))
	if err != nil {
		log.Fatalf("cannot bind %s: %v", endpoint, err)
	}
	defer closeProvider(pub)

	sub, err := zeromq.Subscribe(ctx, endpoint,
		zeromq.WithLogger(logger),
		// The subscription reaches the publisher asynchronously and is never
		// acknowledged, so Subscribe waits this long before returning. It
		// narrows the slow-joiner window; nothing closes it.
		zeromq.WithSettle(500*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("cannot connect to %s: %v", endpoint, err)
	}
	defer closeProvider(sub)

	publisher := declare(ctx, pub)
	subscriber := declare(ctx, sub)

	msgs, err := subscriber.Subscribe(ctx, subject)
	if err != nil {
		log.Fatalf("Subscribe: %v", err)
	}

	if _, perr := publisher.Publish(ctx, subject, tick{Symbol: "ACME", Price: 42.5}); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-msgs:
		var got tick
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("Decode: %v", derr)
		}
		log.Printf("received %s -> %+v", msg.ID, got)
	case <-time.After(10 * time.Second):
		log.Fatal("nothing was delivered")
	}

	// A SUB socket cannot send, and refusing beats accepting a message that
	// would go nowhere.
	if _, perr := subscriber.Publish(ctx, subject, tick{}); errors.Is(perr, streams.ErrUnsupported) {
		log.Println("the subscriber refused to publish, as expected")
	}

	// Brokerless means nothing is stored, so neither capability is on offer.
	if _, derr := streams.AsDurable(publisher); errors.Is(derr, streams.ErrUnsupported) {
		log.Println("ZeroMQ keeps nothing, so it is not durable, as expected")
	}

	log.Println("done")
}

// declare creates the stream on one side and binds a manager to it.
func declare(ctx context.Context, s streams.Streams) streams.Manager {
	if _, err := s.Create(ctx, streams.Stream{
		ID:       streamID,
		Name:     "market data",
		Subjects: []string{subject},
	}); err != nil {
		log.Fatalf("Create: %v", err)
	}

	m, err := s.Bind(ctx, streamID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}
	return m
}

func closeProvider(s streams.Streams) {
	if c, ok := s.(streams.Closer); ok {
		_ = c.Close()
	}
}
