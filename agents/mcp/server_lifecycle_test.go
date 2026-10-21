package mcp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Canceling the context must stop an HTTP-transport StartServer (it used to
// block in ListenAndServe forever) and must not mutate the caller's config.
func TestStartServer_HTTPShutdownOnContextCancel(t *testing.T) {
	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	cfg := &MCPServerConfig{
		Name:      "test",
		Version:   "0.0.0",
		Transport: TransportStreamableHTTP,
		Addr:      addr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- StartServer(ctx, cfg, func(s *mcp.Server) {}) }()

	// Give the listener a moment to bind, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("StartServer returned %v, want nil or context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("StartServer did not return after context cancellation")
	}

	// The caller's config must be untouched by default resolution.
	if cfg.BasePath != "" {
		t.Fatalf("caller cfg.BasePath mutated to %q", cfg.BasePath)
	}
}

// An open client session holds a standalone SSE stream: a GET that hangs
// until its request context ends. Shutdown must release it rather than wait
// out the whole drain timeout.
func TestStartServer_ShutdownReleasesOpenStreams(t *testing.T) {
	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	cfg := &MCPServerConfig{Name: "test", Version: "0.0.0", Transport: TransportStreamableHTTP, Addr: addr}
	go func() { done <- StartServer(ctx, cfg, func(s *mcp.Server) {}) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.0"}, nil)
	transport := &mcp.StreamableClientTransport{Endpoint: "http://" + addr + "/mcp", HTTPClient: &http.Client{}}
	var session *mcp.ClientSession
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if session, err = client.Connect(context.Background(), transport, nil); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect: %v", err)
		}
	}
	defer func() { _ = session.Close() }()
	// Let the client open its standalone stream.
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	cancel()
	select {
	case <-done:
		if took := time.Since(start); took > shutdownTimeout/2 {
			t.Fatalf("shutdown took %v: it waited on the open stream", took)
		}
	case <-time.After(shutdownTimeout + time.Second):
		t.Fatal("StartServer did not return after context cancellation")
	}
}
