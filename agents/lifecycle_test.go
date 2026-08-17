package agents

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestStart_ServiceFailingBeforeReady(t *testing.T) {
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: freePort(t)})
	f := newFake(MCP, Requirements{HTTP: true}, "/mcp")
	f.failBefore = true
	rt.Register(f)

	start := time.Now()
	err := rt.Start(t.Context())
	if err == nil || !strings.Contains(err.Error(), "refused to start") {
		t.Fatalf("got %v, want the service's own error", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v; a failure should not wait out the ready timeout", elapsed)
	}
}

func TestStart_ServiceThatNeverMounts(t *testing.T) {
	rt := New(Config{
		Name: "svc", Host: "127.0.0.1", Port: freePort(t),
		ReadyTimeout: 100 * time.Millisecond,
	})
	f := newFake(MCP, Requirements{HTTP: true}, "/mcp")
	f.stall = true
	rt.Register(f)

	err := rt.Start(t.Context())
	if err == nil || !strings.Contains(err.Error(), "did not mount") {
		t.Fatalf("got %v, want a mount timeout", err)
	}
}

// Serve is Start, a wait, and a drain — the shape a process that does nothing
// else wants.
func TestServe_BlocksAndDrains(t *testing.T) {
	port := freePort(t)
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: port})
	rt.Register(newFake(MCP, Requirements{HTTP: true}, "/mcp"))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- rt.Serve(ctx) }()

	url := fmt.Sprintf("http://127.0.0.1:%d/mcp", port)
	waitUntil(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return true
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancellation")
	}

	// The listener is gone, so the port is free again.
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("port %d still bound after Serve returned", port)
	}
	_ = lis.Close()
}

func TestShutdown_IsSafeUnstarted(t *testing.T) {
	rt := New(Config{Name: "svc"})
	if err := rt.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown on an unstarted runtime: %v", err)
	}
	if err := rt.Shutdown(t.Context()); err != nil {
		t.Errorf("second Shutdown: %v", err)
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not hold within 5s")
}
