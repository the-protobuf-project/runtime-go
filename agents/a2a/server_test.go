package a2a

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func echoAgent() Executor {
	return TextAgent(func(_ context.Context, text string) (string, error) {
		return "echo: " + text, nil
	})
}

func TestParseTransports(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want []Transport
	}{
		{"single", "jsonrpc", []Transport{TransportJSONRPC}},
		{"several", "jsonrpc,grpc", []Transport{TransportJSONRPC, TransportGRPC}},
		{"spaced and mixed case", " JSONRPC , Grpc ", []Transport{TransportJSONRPC, TransportGRPC}},
		{"unknown dropped", "jsonrpc,carrier-pigeon,rest", []Transport{TransportJSONRPC, TransportREST}},
		{"empty", "", nil},
		{"only junk", "nope", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseTransports(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// A list that parses to nothing must not leave a server with no transport —
// StartServer falls back to the default rather than refusing to run.
func TestResolvedTransports_FallsBackToJSONRPC(t *testing.T) {
	cfg := &ServerConfig{Transports: ParseTransports("nonsense")}
	got := cfg.resolvedTransports()
	if len(got) != 1 || got[0] != TransportJSONRPC {
		t.Fatalf("got %v, want [jsonrpc]", got)
	}
}

func TestResolveBasePath(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cfg       ServerConfig
		generated string
		want      string
	}{
		{"default", ServerConfig{}, "", DefaultBasePath},
		{"configured", ServerConfig{BasePath: "/agent"}, "", "/agent"},
		{"generated field beats configured", ServerConfig{BasePath: "/agent", GeneratedBasePath: "/gen"}, "", "/gen"},
		{"generated argument beats both", ServerConfig{BasePath: "/agent", GeneratedBasePath: "/gen"}, "/arg", "/arg"},
		{"rooted", ServerConfig{BasePath: "agent"}, "", "/agent"},
		{"trailing slash trimmed", ServerConfig{BasePath: "/agent/"}, "", "/agent"},
		{"root stays root", ServerConfig{BasePath: "/"}, "", "/"},
		{"blank falls through", ServerConfig{BasePath: "   "}, "", DefaultBasePath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveBasePath(&tc.cfg, tc.generated); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStartServer_GRPCWithoutServerIsRefused(t *testing.T) {
	cfg := &ServerConfig{Name: "test", Transport: TransportGRPC}
	err := StartServer(t.Context(), cfg, echoAgent())
	if !errors.Is(err, ErrNoGRPCServer) {
		t.Fatalf("got %v, want ErrNoGRPCServer", err)
	}
}

func TestStartServer_RootBasePathConflictIsRefused(t *testing.T) {
	cfg := &ServerConfig{
		Name:       "test",
		Transports: []Transport{TransportJSONRPC, TransportREST},
		BasePath:   "/",
		Mux:        http.NewServeMux(),
	}
	err := StartServer(t.Context(), cfg, echoAgent())
	if !errors.Is(err, ErrBasePathConflict) {
		t.Fatalf("got %v, want ErrBasePathConflict", err)
	}
}
