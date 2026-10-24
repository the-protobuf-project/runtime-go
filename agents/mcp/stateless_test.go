package mcp

import (
	"cmp"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// pods routes a client's first request to pod A and every later one to pod
// B, the way a Kubernetes Service may spread one client's requests over two
// replicas, and records which pod saw which request.
type pods struct {
	a, b *url.URL

	mu   sync.Mutex
	sent int
	seen []string // "A POST", "B GET", ...
}

func (p *pods) RoundTrip(req *http.Request) (*http.Response, error) {
	p.mu.Lock()
	target, name := p.b, "B"
	if p.sent == 0 {
		target, name = p.a, "A"
	}
	p.sent++
	p.seen = append(p.seen, name+" "+req.Method)
	p.mu.Unlock()

	out := req.Clone(req.Context())
	out.URL.Scheme, out.URL.Host, out.Host = target.Scheme, target.Host, target.Host
	return http.DefaultTransport.RoundTrip(out)
}

func (p *pods) served(name string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, s := range p.seen {
		if strings.HasPrefix(s, name+" ") {
			out = append(out, s)
		}
	}
	return out
}

// startPod builds a combined server of its own over HTTP — nothing is shared
// with any other pod — and returns its URL.
func startPod(t *testing.T, cfg *MCPServerConfig) *url.URL {
	t.Helper()
	server, err := CombineServices(t.Context(), cfg,
		fakeService("alpha", []string{"alpha-ping_v1"}, ""),
		fakeService("beta", []string{"beta-echo_v1"}, ""),
	)
	if err != nil {
		t.Fatalf("CombineServices: %v", err)
	}
	h, err := NewHTTPHandler(server, cfg)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse %s: %v", ts.URL, err)
	}
	return u
}

// sessionProtocol is the newest protocol version that still has sessions
// (Mcp-Session-Id); the clients agents run today speak it or older. From
// 2026-07-28 the protocol is sessionless whatever the server's mode.
const sessionProtocol = "2025-11-25"

// dialAcross connects a client speaking protocol that initializes on pod A
// and sends everything after to pod B.
func dialAcross(t *testing.T, cfg *MCPServerConfig, protocol string) (*mcp.ClientSession, *pods, error) {
	t.Helper()
	p := &pods{a: startPod(t, cfg), b: startPod(t, cfg)}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{
		Endpoint:   p.a.String() + DefaultBasePath,
		HTTPClient: &http.Client{Transport: p},
	}, &mcp.ClientSessionOptions{ProtocolVersion: protocol})
	if err == nil {
		t.Cleanup(func() { _ = session.Close() })
	}
	return session, p, err
}

// A stateless combined server needs no sticky routing: a client initialized on
// one pod lists and calls tools on another that has never seen it, whether it
// speaks a session protocol or the sessionless one.
func TestStateless_CombinedServerAcrossPods(t *testing.T) {
	for _, protocol := range []string{sessionProtocol, ""} {
		t.Run(cmp.Or(protocol, "latest"), func(t *testing.T) {
			session, p, err := dialAcross(t, &MCPServerConfig{Name: "combined", Version: "1.0.0", Stateless: true}, protocol)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}

			if got, want := toolNames(t, session), []string{"alpha-ping_v1", "beta-echo_v1"}; !slices.Equal(got, want) {
				t.Fatalf("tools = %v, want %v", got, want)
			}
			if got := callText(t, session, "alpha-ping_v1"); got != "answered by /alpha/alpha-ping_v1" {
				t.Errorf("alpha-ping_v1 = %q", got)
			}
			if got := callText(t, session, "beta-echo_v1"); got != "answered by /beta/beta-echo_v1" {
				t.Errorf("beta-echo_v1 = %q", got)
			}

			if got := p.served("A"); !slices.Equal(got, []string{"A POST"}) {
				t.Errorf("pod A served %v, want only the initialize POST", got)
			}
			if !slices.Contains(p.served("B"), "B POST") {
				t.Errorf("pod B served no POST: %v", p.served("B"))
			}
		})
	}
}

// Without Stateless a session-protocol client's session lives on the pod that
// initialized it, so the other pod refuses the follow-up — the default,
// unchanged.
func TestStateful_CombinedServerAcrossPodsLosesSession(t *testing.T) {
	// The first follow-up — the initialized notification — already lands on
	// pod B, which has never heard of the session.
	_, _, err := dialAcross(t, &MCPServerConfig{Name: "combined", Version: "1.0.0"}, sessionProtocol)
	if err == nil {
		t.Fatal("pod B accepted a session pod A created")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Errorf("error = %v, want session not found", err)
	}
}

// postInitialize sends a raw initialize request to url and returns the
// response's status and headers.
func postInitialize(t *testing.T, url string) (int, http.Header) {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"raw","version":"0"}}}`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode, resp.Header
}

// getStatus sends a GET to url and returns the response's status.
func getStatus(t *testing.T, url string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// Stateless reaches a per-service server mounted by StartServer on a shared
// mux: no session id is issued, and the standalone GET stream is refused.
func TestStateless_PerServiceServer(t *testing.T) {
	for _, stateless := range []bool{false, true} {
		mux := http.NewServeMux()
		ctx, cancel := context.WithCancel(t.Context())
		ready := make(chan struct{})
		cfg := &MCPServerConfig{
			Name:      "alpha",
			Mux:       mux,
			Stateless: stateless,
			OnReady:   func(*MCPServerConfig) { close(ready) },
		}
		done := make(chan error, 1)
		go func() { done <- fakeService("alpha", []string{"alpha-ping_v1"}, "")(ctx, cfg) }()
		<-ready
		// OnReady fires just before mounting; wait until the path answers.
		ts := httptest.NewServer(mux)

		var (
			status int
			header http.Header
		)
		for range 100 {
			if status, header = postInitialize(t, ts.URL+"/alpha"); status != http.StatusNotFound {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if status != http.StatusOK {
			t.Fatalf("stateless=%t: initialize status %d", stateless, status)
		}
		if id := header.Get("Mcp-Session-Id"); (id == "") != stateless {
			t.Errorf("stateless=%t: Mcp-Session-Id = %q", stateless, id)
		}
		if stateless {
			if got := getStatus(t, ts.URL+"/alpha"); got != http.StatusMethodNotAllowed {
				t.Errorf("GET status %d, want 405", got)
			}
		}

		ts.Close()
		cancel()
		<-done
	}
}

// Stateless copies the caller's streamable options rather than writing into
// them, and leaves them as given when off.
func TestStreamableOptions(t *testing.T) {
	given := &mcp.StreamableHTTPOptions{JSONResponse: true}
	if got := streamableOptions(&MCPServerConfig{StreamableHTTPOptions: given}); got != given {
		t.Error("stateful config did not pass its options through")
	}
	got := streamableOptions(&MCPServerConfig{StreamableHTTPOptions: given, Stateless: true})
	if !got.Stateless || !got.JSONResponse {
		t.Errorf("options = %+v, want Stateless and JSONResponse", got)
	}
	if given.Stateless {
		t.Error("the caller's options were modified")
	}
	if got := streamableOptions(&MCPServerConfig{Stateless: true}); got == nil || !got.Stateless {
		t.Errorf("options = %+v, want Stateless", got)
	}
}
