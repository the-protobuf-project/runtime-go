package a2a

import (
	"net/http"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestBuildCard_Defaults(t *testing.T) {
	card := BuildCard(&ServerConfig{Name: "echo", Version: "1.0.0", Addr: "127.0.0.1:9000"})

	if card.Name != "echo" || card.Version != "1.0.0" {
		t.Errorf("identity: got %q/%q", card.Name, card.Version)
	}
	// Required-on-the-wire fields must never marshal as null.
	if card.Skills == nil {
		t.Error("Skills is nil; it must serialize as an empty array")
	}
	if len(card.DefaultInputModes) != 1 || card.DefaultInputModes[0] != DefaultMode {
		t.Errorf("DefaultInputModes = %v", card.DefaultInputModes)
	}
	if len(card.DefaultOutputModes) != 1 || card.DefaultOutputModes[0] != DefaultMode {
		t.Errorf("DefaultOutputModes = %v", card.DefaultOutputModes)
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("SupportedInterfaces = %+v, want one", card.SupportedInterfaces)
	}
	iface := card.SupportedInterfaces[0]
	if iface.ProtocolBinding != a2a.TransportProtocolJSONRPC {
		t.Errorf("protocol = %q, want JSONRPC", iface.ProtocolBinding)
	}
	if iface.URL != "http://127.0.0.1:9000"+DefaultBasePath {
		t.Errorf("URL = %q", iface.URL)
	}
}

// A caller-supplied card is served verbatim. Rebuilding one that was signed
// elsewhere would invalidate the signature.
func TestBuildCard_ExplicitCardWins(t *testing.T) {
	own := &Card{Name: "from-registry"}
	got := BuildCard(&ServerConfig{Name: "ignored", Card: own, Skills: []Skill{{ID: "x"}}})
	if got != own {
		t.Fatal("expected the configured card back unchanged")
	}
	if got.Name != "from-registry" || len(got.Skills) != 0 {
		t.Errorf("card was modified: %+v", got)
	}
}

// Every configured transport is declared, in order, and the first is the one a
// client should prefer.
func TestBuildCard_DeclaresEveryTransport(t *testing.T) {
	card := BuildCard(&ServerConfig{
		Name:       "echo",
		Addr:       "127.0.0.1:9000",
		Transports: []Transport{TransportGRPC, TransportJSONRPC, TransportREST},
	})

	if len(card.SupportedInterfaces) != 3 {
		t.Fatalf("got %d interfaces, want 3", len(card.SupportedInterfaces))
	}
	want := []a2a.TransportProtocol{
		a2a.TransportProtocolGRPC,
		a2a.TransportProtocolJSONRPC,
		a2a.TransportProtocolHTTPJSON,
	}
	for i, w := range want {
		if got := card.SupportedInterfaces[i].ProtocolBinding; got != w {
			t.Errorf("interface[%d] = %q, want %q", i, got, w)
		}
	}
	// gRPC is dialed by host:port, so its entry must carry no scheme.
	if got := card.SupportedInterfaces[0].URL; got != "127.0.0.1:9000" {
		t.Errorf("gRPC URL = %q, want a bare host:port", got)
	}
}

// A card is read by clients elsewhere, so it must advertise the public URL
// rather than whatever interface the process happened to bind.
func TestBuildCard_PublicURLOverridesListenAddress(t *testing.T) {
	card := BuildCard(&ServerConfig{
		Name:       "echo",
		Addr:       "0.0.0.0:9000",
		PublicURL:  "https://agents.example.com",
		BasePath:   "/echo",
		Transports: []Transport{TransportJSONRPC, TransportGRPC},
	})

	if got := card.SupportedInterfaces[0].URL; got != "https://agents.example.com/echo" {
		t.Errorf("JSON-RPC URL = %q", got)
	}
	if got := card.SupportedInterfaces[1].URL; got != "agents.example.com" {
		t.Errorf("gRPC URL = %q, want the host alone", got)
	}
}

// "0.0.0.0" is a bind instruction; a client that dials it goes nowhere.
func TestBuildCard_WildcardBindAdvertisesLoopback(t *testing.T) {
	card := BuildCard(&ServerConfig{Name: "echo", Addr: "0.0.0.0:9000"})
	if got := card.SupportedInterfaces[0].URL; got != "http://127.0.0.1:9000"+DefaultBasePath {
		t.Errorf("URL = %q, want the loopback form", got)
	}
}

func TestServerEndpoint(t *testing.T) {
	cfg := &ServerConfig{
		Name:       "echo",
		Addr:       "127.0.0.1:9000",
		Transports: []Transport{TransportJSONRPC, TransportGRPC},
	}

	ep, err := ServerEndpoint(cfg)
	if err != nil {
		t.Fatalf("ServerEndpoint: %v", err)
	}
	if ep.Transport != TransportJSONRPC {
		t.Errorf("preferred transport = %q, want jsonrpc", ep.Transport)
	}
	if ep.CardURL != "http://127.0.0.1:9000"+AgentCardPath {
		t.Errorf("CardURL = %q", ep.CardURL)
	}

	all := ServerEndpoints(cfg)
	if len(all) != 2 {
		t.Fatalf("got %d endpoints, want 2", len(all))
	}
	if all[1].Transport != TransportGRPC || all[1].URL != "127.0.0.1:9000" {
		t.Errorf("gRPC endpoint = %+v", all[1])
	}
}

// An agent whose host mounts the card reports no card URL of its own, so a
// startup summary does not claim an endpoint this server never registered.
func TestServerEndpoint_SharedMuxWithoutCardReportsNone(t *testing.T) {
	cfg := &ServerConfig{Name: "echo", Mux: http.NewServeMux(), ServeAgentCard: false}
	ep, err := ServerEndpoint(cfg)
	if err != nil {
		t.Fatalf("ServerEndpoint: %v", err)
	}
	if ep.CardURL != "" {
		t.Errorf("CardURL = %q, want empty", ep.CardURL)
	}
}
