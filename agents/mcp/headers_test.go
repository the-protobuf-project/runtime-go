package mcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// bearer sets Authorization on every request to whatever token holds, so a
// test can change the caller's credential between calls on one session.
type bearer struct{ token atomic.Value }

func (b *bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	out := req.Clone(req.Context())
	if tok, _ := b.token.Load().(string); tok != "" {
		out.Header.Set("Authorization", tok)
	}
	return http.DefaultTransport.RoundTrip(out)
}

// authEcho answers every tool call with the authorization metadata its
// interceptor saw, the way an auth interceptor would read it.
func authEcho(ctx context.Context, req any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get("authorization"); len(got) > 0 {
		return got[0], nil
	}
	return "<none>", nil
}

// callAs calls tool as token and returns the authorization it reached the
// interceptor with.
func callAs(t *testing.T, s *mcp.ClientSession, b *bearer, token, tool string) string {
	t.Helper()
	b.token.Store(token)
	res, err := s.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("call %s: %v", tool, err)
	}
	return res.Content[0].(*mcp.TextContent).Text
}

// Every way a service is served — at its own path or combined, with sessions
// or without — lifts each request's own Authorization header into the
// metadata the unary interceptor reads.
func TestHeaderMappings_ReachInterceptor(t *testing.T) {
	for _, tc := range []struct {
		name      string
		combined  bool
		stateless bool
	}{
		{"own path, stateful", false, false},
		{"own path, stateless", false, true},
		{"combined, stateful", true, false},
		{"combined, stateless", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &MCPServerConfig{
				Name: "test", Version: "0.0.0",
				HeaderMappings:   DefaultHeaderMappings(),
				UnaryInterceptor: authEcho,
				Stateless:        tc.stateless,
			}
			svc := fakeService("alpha", []string{"alpha-ping_v1"}, "")

			var h http.Handler
			path := "/alpha"
			if tc.combined {
				server, err := CombineServices(t.Context(), cfg, svc)
				if err != nil {
					t.Fatalf("CombineServices: %v", err)
				}
				if h, err = NewHTTPHandler(server, cfg); err != nil {
					t.Fatalf("NewHTTPHandler: %v", err)
				}
				path = DefaultBasePath
			} else {
				mux := http.NewServeMux()
				cfg.Mux = mux
				ctx, cancel := context.WithCancel(context.Background())
				ready := make(chan struct{})
				cfg.OnReady = func(*MCPServerConfig) { close(ready) }
				go func() { _ = svc(ctx, cfg) }()
				t.Cleanup(cancel)
				<-ready
				h = mux
			}
			ts := httptest.NewServer(h)
			t.Cleanup(ts.Close)

			b := &bearer{}
			client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.0"}, nil)
			session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{
				Endpoint: ts.URL + path, HTTPClient: &http.Client{Transport: b},
			}, &mcp.ClientSessionOptions{})
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			t.Cleanup(func() { _ = session.Close() })

			if got := callAs(t, session, b, "Bearer one", "alpha-ping_v1"); got != "Bearer one" {
				t.Errorf("first call: interceptor saw %q", got)
			}
			if got := callAs(t, session, b, "Bearer two", "alpha-ping_v1"); got != "Bearer two" {
				t.Errorf("second call: interceptor saw %q", got)
			}
			if got := callAs(t, session, b, "", "alpha-ping_v1"); got != "<none>" {
				t.Errorf("call without a header: interceptor saw %q", got)
			}
		})
	}
}
