package grpc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// serveNamedMCP stands in for a generated ServeFooMCP with one tool: mounted
// at /{name} on its own, answering tool {name}-ping_v1 through the server's
// unary interceptor chain.
func serveNamedMCP(name string) MCPServiceFunc { return serveNamedMCPAt("/"+name, name) }

// serveNamedMCPAt is serveNamedMCP mounted at path.
func serveNamedMCPAt(path, name string) MCPServiceFunc {
	return func(ctx context.Context, cfg *MCPServerConfig) error {
		cfg.GeneratedBasePath = path
		interceptor := cfg.UnaryInterceptor
		return mcp.StartServer(ctx, cfg, func(s *mcp.Server) {
			tool := name + "-ping_v1"
			s.AddTool(mcp.MustCreateTool(tool, tool, `{"type":"object"}`), func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				out, err := mcp.InvokeUnary(ctx, interceptor, "/"+name+"/Ping", nil, nil, func(context.Context, any) (any, error) {
					return "pong from " + name, nil
				})
				if err != nil {
					return mcp.HandleError(err)
				}
				return mcp.TextResult(out.(string)), nil
			})
		})
	}
}

// mcpSession connects an MCP client to url.
func mcpSession(t *testing.T, url string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: url, HTTPClient: &http.Client{}}, nil)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// mounted reports whether anything answers a POST at url.
func mounted(t *testing.T, url string) bool {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, http.NoBody)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode != http.StatusNotFound
}

func startHybrid(t *testing.T, s *HybridServer) {
	t.Helper()
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
}

// WithMCPEndpoint serves every registered service as one MCP server at /mcp:
// one tool list carrying both, a call to each reaching its own service through
// the server's interceptors — while each keeps its own path too.
func TestHybridServer_MCPEndpointCombinesServices(t *testing.T) {
	opts := mcpOpts(t)
	var seen []string
	s := NewHybridServer(opts,
		WithUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			seen = append(seen, info.FullMethod)
			return handler(ctx, req)
		}),
		WithMCPServices(serveNamedMCP("alpha"), serveNamedMCP("beta")),
		WithMCPEndpoint(""),
	)
	startHybrid(t, s)

	base := fmt.Sprintf("http://127.0.0.1:%d", opts.MCP.Port)
	session := mcpSession(t, base+"/mcp")

	if info := session.InitializeResult().ServerInfo; info.Name != opts.ServiceName || info.Version != opts.Version {
		t.Errorf("server info = %+v, want %s %s", info, opts.ServiceName, opts.Version)
	}

	var names []string
	for tool, err := range session.Tools(t.Context(), nil) {
		if err != nil {
			t.Fatalf("list tools: %v", err)
		}
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	if want := []string{"alpha-ping_v1", "beta-ping_v1"}; !slices.Equal(names, want) {
		t.Fatalf("tools = %v, want %v", names, want)
	}

	for _, name := range []string{"alpha", "beta"} {
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name + "-ping_v1", Arguments: map[string]any{}})
		if err != nil {
			t.Fatalf("call %s: %v", name, err)
		}
		if got := res.Content[0].(*mcp.TextContent).Text; got != "pong from "+name {
			t.Errorf("%s-ping_v1 = %q", name, got)
		}
	}
	if want := []string{"/alpha/Ping", "/beta/Ping"}; !slices.Equal(seen, want) {
		t.Errorf("interceptor saw %v, want %v", seen, want)
	}

	// The per-service paths still answer, and the banner lists all three.
	if !mounted(t, base+"/alpha") || !mounted(t, base+"/beta") {
		t.Error("per-service paths are gone without WithoutMCPServicePaths")
	}
	if got := len(endpointsFor(s, agents.MCP)); got != 3 {
		t.Errorf("got %d MCP endpoints, want 3", got)
	}
}

// WithoutMCPServicePaths leaves the combined endpoint as the only MCP surface.
func TestHybridServer_MCPEndpointOnly(t *testing.T) {
	opts := mcpOpts(t)
	s := NewHybridServer(opts,
		WithMCPServices(serveNamedMCP("alpha"), serveNamedMCP("beta")),
		WithMCPEndpoint("/agents/mcp"),
		WithoutMCPServicePaths(),
	)
	startHybrid(t, s)

	base := fmt.Sprintf("http://127.0.0.1:%d", opts.MCP.Port)
	if mounted(t, base+"/alpha") {
		t.Error("/alpha still mounted")
	}
	if !mounted(t, base+"/agents/mcp") {
		t.Error("combined endpoint not mounted at /agents/mcp")
	}
	eps := endpointsFor(s, agents.MCP)
	if len(eps) != 1 || !strings.HasSuffix(eps[0].URL, "/agents/mcp") {
		t.Errorf("endpoints = %+v, want one at /agents/mcp", eps)
	}
}

// A tool name two services declare fails Start with an error naming it.
func TestHybridServer_MCPEndpointCollisionFailsStart(t *testing.T) {
	opts := mcpOpts(t)
	s := NewHybridServer(opts,
		WithMCPServices(serveNamedMCP("alpha"), serveNamedMCP("beta"), serveNamedMCPAt("/alpha-again", "alpha")),
		WithMCPEndpoint(""),
		WithoutMCPServicePaths(),
	)
	err := s.Start()
	if err == nil {
		_ = s.Stop()
		t.Fatal("Start accepted two services declaring one tool")
	}
	if !strings.Contains(err.Error(), `tool "alpha-ping_v1"`) {
		t.Errorf("error does not name the tool: %v", err)
	}
}

// WithStatelessMCP serves the combined endpoint without sessions: initialize
// issues no Mcp-Session-Id, a request carrying none is answered, and the
// standalone GET stream is refused.
func TestHybridServer_StatelessMCP(t *testing.T) {
	opts := mcpOpts(t)
	s := NewHybridServer(opts,
		WithMCPServices(serveNamedMCP("alpha")),
		WithMCPEndpoint(""),
		WithStatelessMCP(),
	)
	startHybrid(t, s)
	url := fmt.Sprintf("http://127.0.0.1:%d/mcp", opts.MCP.Port)

	post := func(body string) (int, http.Header, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Mcp-Protocol-Version", "2025-11-25")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		out, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		return resp.StatusCode, resp.Header, string(out)
	}

	status, header, _ := post(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"raw","version":"0"}}}`)
	if status != http.StatusOK {
		t.Fatalf("initialize status %d", status)
	}
	if id := header.Get("Mcp-Session-Id"); id != "" {
		t.Errorf("stateless server issued Mcp-Session-Id %q", id)
	}

	status, _, body := post(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"alpha-ping_v1","arguments":{}}}`)
	if status != http.StatusOK || !strings.Contains(body, "pong from alpha") {
		t.Errorf("tools/call without a session: status %d, body %s", status, body)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	get, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = get.Body.Close()
	if get.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET status %d, want 405", get.StatusCode)
	}
}

// An MCP tool call reaches the unary chain with the request's Authorization
// header as incoming metadata — by default, on the combined endpoint and each
// service's own path — and WithMCPHeaderMappings() with none turns it off.
func TestHybridServer_MCPForwardsAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra []Option
		want  string
	}{
		{"default stateful", nil, "Bearer secret"},
		{"default stateless", []Option{WithStatelessMCP()}, "Bearer secret"},
		{"mappings off", []Option{WithMCPHeaderMappings()}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := mcpOpts(t)
			var seen []string
			s := NewHybridServer(opts, append([]Option{
				WithUnaryInterceptors(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					md, _ := metadata.FromIncomingContext(ctx)
					seen = append(seen, strings.Join(md.Get("authorization"), ","))
					return handler(ctx, req)
				}),
				WithMCPServices(serveNamedMCP("alpha")),
				WithMCPEndpoint(""),
			}, tc.extra...)...)
			startHybrid(t, s)

			base := fmt.Sprintf("http://127.0.0.1:%d", opts.MCP.Port)
			for _, path := range []string{"/mcp", "/alpha"} {
				client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.0"}, nil)
				session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{
					Endpoint:   base + path,
					HTTPClient: &http.Client{Transport: authTransport("Bearer secret")},
				}, nil)
				if err != nil {
					t.Fatalf("connect %s: %v", path, err)
				}
				if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "alpha-ping_v1", Arguments: map[string]any{}}); err != nil {
					t.Fatalf("call at %s: %v", path, err)
				}
				_ = session.Close()
			}
			if want := []string{tc.want, tc.want}; !slices.Equal(seen, want) {
				t.Errorf("interceptor saw %q, want %q", seen, want)
			}
		})
	}
}

// authTransport sets Authorization on every request.
type authTransport string

func (a authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", string(a))
	return http.DefaultTransport.RoundTrip(out)
}
