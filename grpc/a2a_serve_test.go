package grpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
)

// Serving over JSON-RPC means an HTTP listener with the agent card on it, which
// is the first thing any client fetches.
func TestHybridServer_A2AJSONRPCServesCard(t *testing.T) {
	opts := a2aOpts(t, options.A2ATransportJSONRPC)
	s := NewHybridServer(opts, WithA2AAgent(echoAgent(), echoSkill()))

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := s.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	base := "http://127.0.0.1:" + strconv.Itoa(opts.A2A.Port)
	card := fetchCard(t, base+a2a.AgentCardPath)

	if card.Name != opts.ServiceName {
		t.Errorf("card.Name = %q, want %q", card.Name, opts.ServiceName)
	}
	if len(card.Skills) != 1 || card.Skills[0].ID != "echo" {
		t.Errorf("card.Skills = %+v", card.Skills)
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("SupportedInterfaces = %+v, want one", card.SupportedInterfaces)
	}

	eps := endpointsFor(s, agents.A2A)
	if len(eps) != 1 {
		t.Fatalf("got %d endpoints, want 1", len(eps))
	}
	if got := eps[0].Transport; got != string(a2a.TransportJSONRPC) {
		t.Errorf("transport = %q, want jsonrpc", got)
	}
}

// The gRPC transport is a service like any other on the server's own port —
// which only works because it registers before Serve is called.
func TestHybridServer_A2AGRPCRegistersOnSharedServer(t *testing.T) {
	opts := a2aOpts(t, options.A2ATransportGRPC)
	s := NewHybridServer(opts, WithA2AAgent(echoAgent(), echoSkill()))

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := s.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	if _, ok := s.grpcServer.GetServiceInfo()[a2a.GRPCServiceName]; !ok {
		t.Errorf("%s was not registered on the shared gRPC server", a2a.GRPCServiceName)
	}
	eps := endpointsFor(s, agents.A2A)
	if len(eps) != 1 {
		t.Fatalf("got %d endpoints, want 1", len(eps))
	}
	if got := eps[0].Transport; got != string(a2a.TransportGRPC) {
		t.Errorf("transport = %q, want grpc", got)
	}
	// Nothing was mounted on the HTTP mux, so no listener should have opened.
	if !portIsFree(t, fmt.Sprintf("127.0.0.1:%d", opts.A2A.Port)) {
		t.Error("a gRPC-only agent should not bind the A2A port")
	}
}

// Both transports at once: the card must declare each one, or a client picks an
// endpoint that is not there.
func TestHybridServer_A2ABothTransportsAreAdvertised(t *testing.T) {
	opts := a2aOpts(t, options.A2ATransportJSONRPC, options.A2ATransportGRPC)
	s := NewHybridServer(opts, WithA2AAgent(echoAgent(), echoSkill()))

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := s.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	card := fetchCard(t, "http://127.0.0.1:"+strconv.Itoa(opts.A2A.Port)+a2a.AgentCardPath)
	if len(card.SupportedInterfaces) != 2 {
		t.Fatalf("SupportedInterfaces = %+v, want two", card.SupportedInterfaces)
	}
	if _, ok := s.grpcServer.GetServiceInfo()[a2a.GRPCServiceName]; !ok {
		t.Error("gRPC transport was not registered")
	}
	if got := len(endpointsFor(s, agents.A2A)); got != 2 {
		t.Errorf("got %d endpoints, want 2", got)
	}
}

// Nothing A2A should run, or bind, when it is off.
func TestHybridServer_A2ADisabledStartsNothing(t *testing.T) {
	opts := a2aOpts(t, options.A2ATransportJSONRPC)
	opts.EnableA2A = false
	s := NewHybridServer(opts, WithA2AAgent(echoAgent(), echoSkill()))

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := s.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	if len(endpointsFor(s, agents.A2A)) != 0 {
		t.Error("A2A reported endpoints despite EnableA2A=false")
	}
	if !portIsFree(t, fmt.Sprintf("127.0.0.1:%d", opts.A2A.Port)) {
		t.Error("the A2A port was bound despite EnableA2A=false")
	}
	if _, ok := s.grpcServer.GetServiceInfo()[a2a.GRPCServiceName]; ok {
		t.Error("A2A registered on the gRPC server despite EnableA2A=false")
	}
}

func fetchCard(t *testing.T, url string) a2a.Card {
	t.Helper()

	var resp *http.Response
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		if resp, err = http.DefaultClient.Do(req); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if resp == nil {
		t.Fatalf("agent card at %s never became reachable", url)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("card status = %d, want 200", resp.StatusCode)
	}
	var card a2a.Card
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		t.Fatalf("decode card: %v", err)
	}
	return card
}
