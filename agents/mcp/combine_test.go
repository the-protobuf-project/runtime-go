package mcp

import (
	"context"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
)

// fakeService stands in for a generated ServeFooMCP: it registers its tools,
// a resource and (optionally) a prompt with enum completions, dispatches every
// call through cfg.UnaryInterceptor, and serves via StartServer — exactly the
// shape protoc-gen-mcp emits.
func fakeService(prefix string, tools []string, prompt string) ServiceFunc {
	return func(ctx context.Context, cfg *MCPServerConfig) error {
		cfg.GeneratedBasePath = "/" + prefix
		if prompt != "" {
			if cfg.ServerOptions == nil {
				cfg.ServerOptions = &ServerOptions{}
			}
			cfg.ServerOptions.CompletionHandler = CompletionHandlerFromEnums(map[string][]string{
				prompt + ":color": {"red", "green"},
			})
		}
		var opts []Option
		if cfg.UnaryInterceptor != nil {
			opts = append(opts, WithUnaryInterceptor(cfg.UnaryInterceptor))
		}
		return StartServer(ctx, cfg, func(s *mcp.Server) {
			c := ApplyOptions(opts...)
			for _, name := range tools {
				method := "/" + prefix + "/" + name
				s.AddTool(MustCreateTool(name, name, `{"type":"object"}`), func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					out, err := InvokeUnary(ctx, c.UnaryInterceptor, method, nil, nil, func(context.Context, any) (any, error) {
						return "answered by " + method, nil
					})
					if err != nil {
						return HandleError(err)
					}
					return TextResult(out.(string)), nil
				})
			}
			s.AddResource(&mcp.Resource{URI: AppResourceURI(prefix), Name: prefix, MIMEType: "text/html"},
				DefaultAppResourceHandler(prefix, "1.0.0", prefix))
			if prompt != "" {
				s.AddPrompt(&mcp.Prompt{Name: prompt, Arguments: []*mcp.PromptArgument{{Name: "color"}}},
					DefaultPromptHandler(prompt))
			}
		})
	}
}

// connect opens a client session against a combined server served over HTTP.
func connect(t *testing.T, server *mcp.Server, cfg *MCPServerConfig) *mcp.ClientSession {
	t.Helper()
	h, err := NewHTTPHandler(server, cfg)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: ts.URL + DefaultBasePath}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func toolNames(t *testing.T, session *mcp.ClientSession) []string {
	t.Helper()
	var names []string
	for tool, err := range session.Tools(t.Context(), nil) {
		if err != nil {
			t.Fatalf("list tools: %v", err)
		}
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	return names
}

func callText(t *testing.T, session *mcp.ClientSession, name string) string {
	t.Helper()
	res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("call %s: %v", name, err)
	}
	if res.IsError || len(res.Content) != 1 {
		t.Fatalf("call %s: unexpected result %+v", name, res)
	}
	return res.Content[0].(*mcp.TextContent).Text
}

// Two services combined answer at one path: one tool list carrying both, each
// tool reaching its own service through the caller's interceptor, under the
// identity the caller chose.
func TestCombineServices_TwoServicesOneEndpoint(t *testing.T) {
	var (
		mu   sync.Mutex
		seen []string
	)
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		seen = append(seen, info.FullMethod)
		mu.Unlock()
		return handler(ctx, req)
	}
	cfg := &MCPServerConfig{
		Name:             "combined",
		Version:          "9.9.9",
		ServerOptions:    &ServerOptions{Instructions: "every service"},
		UnaryInterceptor: interceptor,
	}

	server, err := CombineServices(t.Context(), cfg,
		fakeService("alpha", []string{"alpha-ping_v1", "alpha-pong_v1"}, ""),
		fakeService("beta", []string{"beta-echo_v1"}, "beta-greet"),
	)
	if err != nil {
		t.Fatalf("CombineServices: %v", err)
	}
	if cfg.ServerOptions.CompletionHandler != nil {
		t.Error("the caller's ServerOptions were modified")
	}

	session := connect(t, server, cfg)

	init := session.InitializeResult()
	if init.ServerInfo.Name != "combined" || init.ServerInfo.Version != "9.9.9" {
		t.Errorf("server info = %+v, want combined 9.9.9", init.ServerInfo)
	}
	if init.Instructions != "every service" {
		t.Errorf("instructions = %q", init.Instructions)
	}

	want := []string{"alpha-ping_v1", "alpha-pong_v1", "beta-echo_v1"}
	if got := toolNames(t, session); !slices.Equal(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
	if got := callText(t, session, "alpha-ping_v1"); got != "answered by /alpha/alpha-ping_v1" {
		t.Errorf("alpha-ping_v1 = %q", got)
	}
	if got := callText(t, session, "beta-echo_v1"); got != "answered by /beta/beta-echo_v1" {
		t.Errorf("beta-echo_v1 = %q", got)
	}
	mu.Lock()
	if !slices.Equal(seen, []string{"/alpha/alpha-ping_v1", "/beta/beta-echo_v1"}) {
		t.Errorf("interceptor saw %v", seen)
	}
	mu.Unlock()

	var uris []string
	for r, err := range session.Resources(t.Context(), nil) {
		if err != nil {
			t.Fatalf("list resources: %v", err)
		}
		uris = append(uris, r.URI)
	}
	slices.Sort(uris)
	if want := []string{AppResourceURI("alpha"), AppResourceURI("beta")}; !slices.Equal(uris, want) {
		t.Errorf("resources = %v, want %v", uris, want)
	}

	// The second service's completions survive being combined.
	res, err := session.Complete(t.Context(), &mcp.CompleteParams{
		Ref:      &mcp.CompleteReference{Type: "ref/prompt", Name: "beta-greet"},
		Argument: mcp.CompleteParamsArgument{Name: "color", Value: "g"},
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !slices.Equal(res.Completion.Values, []string{"green"}) {
		t.Errorf("completion = %v, want [green]", res.Completion.Values)
	}
}

// Two services declaring one tool name must fail, naming the tool and both
// services, rather than let the later silently replace the earlier.
func TestCombineServices_ToolCollisionFails(t *testing.T) {
	_, err := CombineServices(t.Context(), &MCPServerConfig{Name: "combined"},
		fakeService("alpha", []string{"shared-ping_v1"}, ""),
		fakeService("beta", []string{"beta-echo_v1"}, ""),
		fakeService("gamma", []string{"shared-ping_v1"}, ""),
	)
	if err == nil {
		t.Fatal("CombineServices accepted a duplicate tool name")
	}
	msg := err.Error()
	if !strings.Contains(msg, `tool "shared-ping_v1"`) || !strings.Contains(msg, "service #0 and service #2") {
		t.Errorf("error does not name the clash: %v", err)
	}
}

// Resources collide too: two services of one name share an app resource URI.
func TestNewCombinedServer_ResourceCollisionFails(t *testing.T) {
	sameURI := func(name string) Registration {
		return func(s *mcp.Server) {
			s.AddResource(&mcp.Resource{URI: "ui://same/app.html", Name: name}, DefaultResourceHandler())
		}
	}
	_, err := NewCombinedServer(&MCPServerConfig{}, sameURI("first"), sameURI("second"))
	if err == nil || !strings.Contains(err.Error(), `resource "ui://same/app.html"`) {
		t.Fatalf("err = %v, want a resource clash", err)
	}
}

// Only a generated ServeFooMCP can be combined; anything that returns without
// going through StartServer is refused rather than silently dropped.
func TestCombineServices_RejectsNonGeneratedFunc(t *testing.T) {
	_, err := CombineServices(t.Context(), &MCPServerConfig{},
		func(context.Context, *MCPServerConfig) error { return nil },
	)
	if err == nil || !strings.Contains(err.Error(), "without registering") {
		t.Fatalf("err = %v, want a refusal", err)
	}
}
