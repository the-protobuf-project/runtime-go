package agents

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc"
)

func TestStart_SharedListener(t *testing.T) {
	port := freePort(t)
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: port})
	mcpSvc := newFake(MCP, Requirements{HTTP: true}, "/mcp")
	a2aSvc := newFake(A2A, Requirements{HTTP: true}, "/a2a")
	rt.Register(mcpSvc, a2aSvc)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	// Both were handed the same mux and the same address.
	mcpAt, a2aAt := mcpSvc.placed(), a2aSvc.placed()
	if mcpAt.Mux != a2aAt.Mux {
		t.Error("services sharing an address should share a mux")
	}
	if mcpAt.Addr != a2aAt.Addr {
		t.Errorf("addresses differ: %q vs %q", mcpAt.Addr, a2aAt.Addr)
	}

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	for path, want := range map[string]string{"/mcp": "mcp here", "/a2a": "a2a here"} {
		if code, body := get(t, base+path); code != http.StatusOK || body != want {
			t.Errorf("GET %s = %d %q, want 200 %q", path, code, body, want)
		}
	}

	if got := len(rt.Endpoints()); got != 2 {
		t.Errorf("got %d endpoints, want 2", got)
	}
}

// Identity is declared once and every protocol is handed the same one, so two
// of them cannot describe the process differently.
func TestStart_IdentityIsShared(t *testing.T) {
	rt := New(Config{
		Name: "svc", Description: "does things", Version: "1.2.3",
		Host: "127.0.0.1", Port: freePort(t),
	})
	mcpSvc := newFake(MCP, Requirements{HTTP: true}, "/mcp")
	a2aSvc := newFake(A2A, Requirements{HTTP: true}, "/a2a")
	rt.Register(mcpSvc, a2aSvc)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	want := Identity{Name: "svc", Description: "does things", Version: "1.2.3"}
	if got := mcpSvc.placed().Identity; got != want {
		t.Errorf("mcp identity = %+v, want %+v", got, want)
	}
	if got := a2aSvc.placed().Identity; got != want {
		t.Errorf("a2a identity = %+v, want %+v", got, want)
	}
}

// A service that names its own address gets its own listener and mux, which is
// how a caller keeps two protocols on separate ports.
func TestStart_OwnAddressIsSeparate(t *testing.T) {
	sharedPort, ownPort := freePort(t), freePort(t)
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: sharedPort})

	sharing := newFake(MCP, Requirements{HTTP: true}, "/mcp")
	apart := newFake(A2A, Requirements{
		HTTP: true, Addr: fmt.Sprintf("127.0.0.1:%d", ownPort),
	}, "/a2a")
	rt.Register(sharing, apart)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	if sharing.placed().Mux == apart.placed().Mux {
		t.Error("services on different addresses must not share a mux")
	}

	if code, body := get(t, fmt.Sprintf("http://127.0.0.1:%d/mcp", sharedPort)); code != http.StatusOK || body != "mcp here" {
		t.Errorf("shared port: %d %q", code, body)
	}
	if code, body := get(t, fmt.Sprintf("http://127.0.0.1:%d/a2a", ownPort)); code != http.StatusOK || body != "a2a here" {
		t.Errorf("own port: %d %q", code, body)
	}
	// Each is only where it asked to be.
	if code, _ := get(t, fmt.Sprintf("http://127.0.0.1:%d/a2a", sharedPort)); code != http.StatusNotFound {
		t.Errorf("a2a leaked onto the shared port: %d", code)
	}
}

// A protocol that only registers on a gRPC server needs no listener, and
// binding a port for it would leave one answering nothing.
func TestStart_GRPCOnlyBindsNothing(t *testing.T) {
	port := freePort(t)
	rt := New(Config{
		Name: "svc", Host: "127.0.0.1", Port: port,
		GRPCServer: grpc.NewServer(),
	})
	rt.Register(newFake(A2A, Requirements{GRPC: true}, ""))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("port %d was bound by a runtime with nothing to serve on it", port)
	}
	_ = lis.Close()
}

func TestStart_GRPCWithoutServerIsRefused(t *testing.T) {
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: freePort(t)})
	rt.Register(newFake(A2A, Requirements{GRPC: true}, ""))

	err := rt.Start(t.Context())
	if err == nil || !strings.Contains(err.Error(), "gRPC server") {
		t.Fatalf("got %v, want a complaint about the missing gRPC server", err)
	}
}

// A host that brought its own mux owns the server; the runtime must mount and
// bind nothing.
func TestStart_SharedMuxBindsNothing(t *testing.T) {
	port := freePort(t)
	mux := http.NewServeMux()
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: port, Mux: mux})
	svc := newFake(MCP, Requirements{HTTP: true}, "/mcp")
	rt.Register(svc)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	if got := svc.placed().Mux; got != mux {
		t.Error("the host's mux should have been handed straight through")
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("port %d was bound despite the host owning the server", port)
	}
	_ = lis.Close()
}

// A service that dies on the way up fails the start, rather than being waited
// on until the ready timeout.
