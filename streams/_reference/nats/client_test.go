package nats

import (
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/machanirobotics/loom/go/nats/options"
)

var (
	testSrv *server.Server // nil when using external Docker NATS
	testSvc *NatsService
)

func TestMain(m *testing.M) {
	var err error

	// If NATS_URL is set, assume an external (Docker) NATS is running with JetStream.
	if url := os.Getenv("NATS_URL"); url != "" {
		if os.Getenv("NATS_NAME") == "" {
			_ = os.Setenv("NATS_NAME", "cutlery-test")
		}
		if err = waitForNATS(url, 10*time.Second); err != nil {
			panic(err)
		}
		testSvc, err = NewNatsClient(options.NatsClientOptions{
			EnableJetStream: true, // ensure JS path is exercised
		})
		if err != nil {
			panic(err)
		}

		code := m.Run()
		teardown()
		os.Exit(code)
	}

	// Otherwise, start an embedded NATS server with JetStream.
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a free port
		JetStream: true,
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		panic(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		panic("nats-server did not become ready in time")
	}
	testSrv = s

	// Point env defaults at the embedded server so your loadDefaultConnectionOptions() picks them up.
	_ = os.Setenv("NATS_URL", s.ClientURL())
	if os.Getenv("NATS_NAME") == "" {
		_ = os.Setenv("NATS_NAME", "cutlery-test")
	}
	_ = os.Setenv("NATS_USERNAME", "")
	_ = os.Setenv("NATS_PASSWORD", "")

	testSvc, err = NewNatsClient(options.NatsClientOptions{
		EnableJetStream: true, // ensure JS path is exercised
	})
	if err != nil {
		s.Shutdown()
		panic(err)
	}

	code := m.Run()
	teardown()
	os.Exit(code)
}

func waitForNATS(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url,
			nats.Name("cutlery-wait"),
			nats.Timeout(500*time.Millisecond),
			nats.NoEcho(),
		)
		if err == nil {
			_ = nc.Drain()
			nc.Close()
			return nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = errors.New("unknown error")
	}
	return errors.New("timeout waiting for NATS at " + url + ": " + lastErr.Error())
}

func teardown() {
	if testSvc != nil && testSvc.Nats != nil && testSvc.Nats.Conn() != nil {
		_ = testSvc.Nats.Conn().Drain()
		testSvc.Nats.Conn().Close()
	}
	if testSrv != nil {
		testSrv.Shutdown()
	}
}

func getSvc(t *testing.T) *NatsService {
	t.Helper()
	if testSvc == nil || testSvc.Nats == nil || testSvc.Nats.Conn() == nil {
		t.Fatalf("shared client not initialized")
	}
	return testSvc
}

func TestNewNatsClient_ConnectsAndJetStream(t *testing.T) {
	svc := getSvc(t)

	conn := svc.Nats.Conn()
	if conn == nil || conn.Status() != nats.CONNECTED {
		t.Fatalf("expected CONNECTED, got status=%v", conn.Status())
	}

	// Jetstream is a VALUE type, so we can't compare it to nil. Check the inner handler.
	if svc.Jetstream.Store.bucketManager == nil {
		t.Fatalf("expected JetStream bucket manager to be initialized")
	}
}

func TestNewNatsClient_NoJetStreamMode(t *testing.T) {
	// Build a separate client with JS disabled.
	svc, err := NewNatsClient(options.NatsClientOptions{
		EnableJetStream: false,
	})
	if err != nil {
		t.Fatalf("NewNatsClient error: %v", err)
	}
	t.Cleanup(func() {
		if c := svc.Nats.Conn(); c != nil {
			_ = c.Drain()
			c.Close()
		}
	})

	// Jetstream is a VALUE; verify that bucket manager is NOT wired when disabled.
	if svc.Jetstream.Store.bucketManager != nil {
		t.Fatalf("expected JetStream bucket manager to be nil when JS is disabled")
	}
	if svc.Nats.Conn().Status() != nats.CONNECTED {
		t.Fatalf("expected base NATS connection to be CONNECTED")
	}
}

func TestNewNatsClient_PubSubRoundTrip(t *testing.T) {
	svc := getSvc(t)
	conn := svc.Nats.Conn()

	subj := "cutlery.test.echo"
	sub, err := conn.SubscribeSync(subj)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	if err := conn.Publish(subj, []byte("hello")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("did not receive message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("unexpected payload: %q", string(msg.Data))
	}
}

func TestNewNatsClient_InvalidURLFails(t *testing.T) {
	// Force a bad URL to ensure connect error bubbles up.
	_, err := NewNatsClient(options.NatsClientOptions{
		URL:             "nats://127.0.0.1:1", // very likely closed port
		Name:            "bad-url",
		EnableJetStream: false,
	})
	if err == nil {
		t.Fatalf("expected error for invalid URL, got nil")
	}
}
