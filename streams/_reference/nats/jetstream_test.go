package nats

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ---- Compile-time interface checks (won't run, but catch mismatches early) ----

var (
	_ bucketManager = (*jetstreamHandler)(nil)
	_ StreamManager = (*jetstreamHandler)(nil)
)

// ---- Test wiring for newJetstreamManager ----

func TestNewJetstreamManager_WiresManagers(t *testing.T) {
	// Boot a JetStream context (embedded by default; uses external if NATS_URL is set)
	nc, js, cleanup := mustJetStream(t)
	defer cleanup()

	// sanity on the live context
	if nc.Status() != nats.CONNECTED {
		t.Fatalf("expected CONNECTED, got %v", nc.Status())
	}
	if js == nil {
		t.Fatalf("nil JetStream context")
	}

	// construct manager
	mgr, err := newJetstreamManager(js)
	if err != nil {
		t.Fatalf("newJetstreamManager error: %v", err)
	}

	// wiring assertions
	if (mgr.Store == StoreManager{}) {
		t.Fatalf("Store manager should be initialized")
	}
	if mgr.Store.bucketManager == nil {
		t.Fatalf("Store.BucketManager should be initialized")
	}
	if mgr.Stream == nil {
		t.Fatalf("Stream manager should be initialized")
	}

	// type assertions (the adapter should be *jetstreamHandler underneath)
	if _, ok := mgr.Stream.(*jetstreamHandler); !ok {
		t.Fatalf("Stream should be backed by *jetstreamHandler")
	}
	if _, ok := mgr.Store.bucketManager.(*jetstreamHandler); !ok {
		t.Fatalf("Store.BucketManager should be backed by *jetstreamHandler")
	}
}

// ---- Helpers: get a live JetStream context (embedded or external) ----

func mustJetStream(t *testing.T) (*nats.Conn, jetstream.JetStream, func()) {
	t.Helper()

	// Prefer external if NATS_URL provided; otherwise embed server.
	if url := os.Getenv("NATS_URL"); url != "" {
		nc, js, err := connectJS(url)
		if err != nil {
			t.Fatalf("connect (external) failed: %v", err)
		}
		return nc, js, func() {
			_ = nc.Drain()
			nc.Close()
		}
	}

	// Embedded server with JetStream
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random
		JetStream: true,
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		t.Fatalf("server not ready in time")
	}

	nc, js, err := connectJS(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatalf("connect (embedded) failed: %v", err)
	}

	cleanup := func() {
		_ = nc.Drain()
		nc.Close()
		s.Shutdown()
	}
	return nc, js, cleanup
}

func connectJS(url string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url,
		nats.Name("jetstream-manager-test"),
		nats.Timeout(2*time.Second),
		nats.NoEcho(),
	)
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}
