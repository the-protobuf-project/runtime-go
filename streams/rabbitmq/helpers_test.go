package rabbitmq_test

// These need a live broker and skip without one. RabbitMQ has no embeddable Go
// server, unlike the Kafka, NATS and MQTT providers, so this is the one suite
// in the module that cannot run entirely in-process:
//
//	docker compose -f ../docker/compose.yaml up -d
//	go test ./...

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user.created"

var seq atomic.Int64

// testURL is the broker under test: RABBITMQ_TEST_URL, or one built from
// RABBITMQ_TEST_HOST and RABBITMQ_TEST_PORT.
func testURL() string {
	if url := os.Getenv("RABBITMQ_TEST_URL"); url != "" {
		return url
	}
	host, port := os.Getenv("RABBITMQ_TEST_HOST"), os.Getenv("RABBITMQ_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "5672"
	}
	return "amqp://guest:guest@" + net.JoinHostPort(host, port) + "/"
}

// prefix keeps one test's exchanges and queues away from another's.
func prefix() string {
	return fmt.Sprintf("t%d-%d", time.Now().UnixNano(), seq.Add(1))
}

// testStreams connects to the broker under test, namespaced to this test, and
// records its connection so [dropConnections] drops only that one. It skips
// when nothing is listening.
func testStreams(t *testing.T, opts ...streamsrabbit.Option) streams.Streams {
	t.Helper()

	// Fail fast when nothing is listening, rather than waiting out the AMQP
	// handshake timeout on every test in the file. The address comes from
	// the URL, parsed as Connect parses it, so the defaults agree.
	uri, err := amqp.ParseURI(testURL())
	if err != nil {
		t.Fatalf("RabbitMQ test URL: %v", err)
	}
	addr := net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Skipf("no RabbitMQ at %s (%v); skipping", addr, err)
	}
	_ = conn.Close()

	opts = append([]streamsrabbit.Option{streamsrabbit.WithPrefix(prefix())}, opts...)
	s, err := streamsrabbit.Connect(t.Context(), testURL(), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	remember(t, streamsrabbit.ConnectionName(s))
	return s
}

// declare creates a stream over subjects and binds a manager to it.
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

// recvDelivery takes one delivery from ch, failing after within.
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

// recv takes one value from ch, failing after within.
func recv[T any](t *testing.T, ch <-chan T, within time.Duration) T {
	t.Helper()

	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before a message arrived")
		}
		return v
	case <-time.After(within):
		t.Fatalf("nothing arrived within %s", within)
		var zero T
		return zero
	}
}
