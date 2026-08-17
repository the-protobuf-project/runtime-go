package agents_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/agents/mcp"
)

// Serve is the standalone shape: block until the context ends, then drain.
func serveToolsMCP(ctx context.Context, cfg *mcp.MCPServerConfig) error {
	cfg.GeneratedBasePath = "/tools"
	return mcp.StartServer(ctx, cfg, func(*mcpsdk.Server) {})
}

func echoAgent() a2a.Executor {
	return a2a.TextAgent(func(_ context.Context, text string) (string, error) {
		return "echo: " + text, nil
	})
}

func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	if err := lis.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return port
}

func fetch(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, rdr)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(out)
}

// The headline: one object, one config, one port, both protocols. MCP answers
// at its generated base path and A2A at its own, with the agent card where the
// protocol says it should be.
