package agents_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/agents/mcp"
)

func TestRuntime_ServiceAddrOverride(t *testing.T) {
	sharedPort, mcpPort := freePort(t), freePort(t)
	rt := agents.New(agents.Config{
		Name: "my-service", Version: "1.0.0",
		Host: "127.0.0.1", Port: sharedPort,
	})

	rt.Register(
		mcp.Service(serveToolsMCP, mcp.ServiceAddr(fmt.Sprintf("127.0.0.1:%d", mcpPort))),
		a2a.Service(echoAgent()),
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	// A2A on the shared port, MCP on its own, and neither on the other's.
	if code, _ := fetch(t, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", sharedPort, a2a.AgentCardPath), ""); code != http.StatusOK {
		t.Errorf("A2A card not on the shared port: %d", code)
	}
	if code, _ := fetch(t, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/tools", mcpPort), `{}`); code == http.StatusNotFound {
		t.Error("MCP was not mounted on its own port")
	}
	if code, _ := fetch(t, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/tools", sharedPort), `{}`); code != http.StatusNotFound {
		t.Errorf("MCP leaked onto the shared port: %d", code)
	}
}

func TestRuntime_ServeDrainsBothProtocols(t *testing.T) {
	port := freePort(t)
	rt := agents.New(agents.Config{
		Name: "my-service", Version: "1.0.0",
		Host: "127.0.0.1", Port: port,
	})
	rt.Register(mcp.Service(serveToolsMCP), a2a.Service(echoAgent()))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- rt.Serve(ctx) }()

	cardURL := fmt.Sprintf("http://127.0.0.1:%d%s", port, a2a.AgentCardPath)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, cardURL, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after cancellation")
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("port %d still bound after Serve returned", port)
	}
	_ = lis.Close()
}
