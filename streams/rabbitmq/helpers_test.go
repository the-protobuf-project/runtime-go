package rabbitmq_test

// These need a live broker and skip without one. RabbitMQ has no embeddable Go
// server, unlike the Kafka, NATS and MQTT providers, so this is the one suite
// in the module that cannot run entirely in-process:
//
//	docker compose -f ../docker/compose.yaml up -d
//	go test ./...

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user.created"

var seq atomic.Int64

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

func testStreams(t *testing.T, opts ...streamsrabbit.Option) streams.Streams {
	t.Helper()

	// Fail fast when nothing is listening, rather than waiting out the AMQP
	// handshake timeout on every test in the file.
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(t.Context(), "tcp", hostPort())
	if err != nil {
		t.Skipf("no RabbitMQ at %s (%v); skipping", hostPort(), err)
	}
	_ = conn.Close()

	opts = append([]streamsrabbit.Option{streamsrabbit.WithPrefix(prefix())}, opts...)
	s, err := streamsrabbit.Connect(t.Context(), testURL(), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	return s
}

func hostPort() string {
	host, port := os.Getenv("RABBITMQ_TEST_HOST"), os.Getenv("RABBITMQ_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "5672"
	}
	return net.JoinHostPort(host, port)
}

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

// dropConnections closes every connection this package opened from the
// broker's side, which is what a network failure looks like to the client. It
// goes through the management API, and skips the test when that is not there.
func dropConnections(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		// The management API lists a connection a moment after it opens.
		if names := ourConnections(t); len(names) > 0 {
			for _, name := range names {
				mgmt(t, http.MethodDelete, "/api/connections/"+url.PathEscape(name), nil)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no connection opened by this package appeared in the management API")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ourConnections names the open connections whose connection_name marks them
// as this package's.
func ourConnections(t *testing.T) []string {
	t.Helper()

	var conns []struct {
		Name             string `json:"name"`
		ClientProperties struct {
			ConnectionName string `json:"connection_name"`
		} `json:"client_properties"`
	}
	mgmt(t, http.MethodGet, "/api/connections", &conns)

	var names []string
	for _, c := range conns {
		if strings.HasPrefix(c.ClientProperties.ConnectionName, "runtime-go-") {
			names = append(names, c.Name)
		}
	}
	return names
}

func mgmt(t *testing.T, method, path string, into any) {
	t.Helper()

	base := os.Getenv("RABBITMQ_TEST_MANAGEMENT")
	if base == "" {
		base = "http://127.0.0.1:15672"
	}
	req, err := http.NewRequestWithContext(t.Context(), method, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("guest", "guest")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("no RabbitMQ management API at %s (%v); skipping", base, err)
	}
	defer func() { _ = res.Body.Close() }()
	if into != nil {
		if err := json.NewDecoder(res.Body).Decode(into); err != nil {
			t.Fatalf("management API %s %s: %v", method, path, err)
		}
	}
}
