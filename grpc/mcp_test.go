package grpc

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/mcp"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
)

// serveToolsMCP stands in for a generated ServeFooMCP: it takes the config the
// HybridServer built and hands it to the runtime's own StartServer.
func serveToolsMCP(ctx context.Context, cfg *MCPServerConfig) error {
	cfg.GeneratedBasePath = "/tools"
	return mcp.StartServer(ctx, cfg, func(*mcpsdk.Server) {})
}

func mcpOpts(t *testing.T) options.Options {
	t.Helper()
	ports := freePorts(t, 3)
	opts := baseOpts()
	opts.GRPC = options.GRPCOptions{Host: "127.0.0.1", Port: ports[0]}
	opts.HTTP = options.HTTPOptions{Host: "127.0.0.1", Port: ports[1]}
	opts.EnableMCP = true
	opts.MCP = options.MCPOptions{
		Host: "127.0.0.1", Port: ports[2],
		Transport: options.MCPTransportStreamableHTTP,
	}
	return opts
}

func TestHybridServer_MCPMountsAtGeneratedBasePath(t *testing.T) {
	opts := mcpOpts(t)
	s := NewHybridServer(opts, WithMCPServices(serveToolsMCP))

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := s.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	eps := endpointsFor(s, agents.MCP)
	if len(eps) != 1 {
		t.Fatalf("got %d MCP endpoints, want 1: %+v", len(eps), eps)
	}
	if eps[0].Transport != string(mcp.TransportStreamableHTTP) {
		t.Errorf("transport = %q, want streamable-http", eps[0].Transport)
	}

	// The generated base path wins over anything configured, and the endpoint
	// actually answers there.
	url := "http://127.0.0.1:" + strconv.Itoa(opts.MCP.Port) + "/tools"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, http.NoBody)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		t.Errorf("MCP was not mounted at /tools")
	}
}

// MCP and A2A on one HybridServer keep their own ports, and both come up.
func TestHybridServer_MCPAndA2ATogether(t *testing.T) {
	ports := freePorts(t, 4)
	opts := baseOpts()
	opts.GRPC = options.GRPCOptions{Host: "127.0.0.1", Port: ports[0]}
	opts.HTTP = options.HTTPOptions{Host: "127.0.0.1", Port: ports[1]}
	opts.EnableMCP = true
	opts.MCP = options.MCPOptions{
		Host: "127.0.0.1", Port: ports[2],
		Transport: options.MCPTransportStreamableHTTP,
	}
	opts.EnableA2A = true
	opts.A2A = options.A2AOptions{
		Host: "127.0.0.1", Port: ports[3],
		Transports: []options.A2ATransport{options.A2ATransportJSONRPC},
	}

	s := NewHybridServer(opts,
		WithMCPServices(serveToolsMCP),
		WithA2AAgent(echoAgent(), echoSkill()),
	)

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := s.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	if got := len(endpointsFor(s, agents.MCP)); got != 1 {
		t.Errorf("got %d MCP endpoints, want 1", got)
	}
	if got := len(endpointsFor(s, agents.A2A)); got != 1 {
		t.Errorf("got %d A2A endpoints, want 1", got)
	}

	// One runtime, but still the two ports the options asked for.
	card := fetchCard(t, fmt.Sprintf("http://127.0.0.1:%d%s", opts.A2A.Port, "/.well-known/agent-card.json"))
	if card.Name != opts.ServiceName {
		t.Errorf("card.Name = %q, want %q", card.Name, opts.ServiceName)
	}
	if portIsFree(t, fmt.Sprintf("127.0.0.1:%d", opts.MCP.Port)) {
		t.Error("the MCP port was never bound")
	}
}
