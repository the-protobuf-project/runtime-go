package grpc

import (
	"context"
	"net"
	"testing"

	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
)

// endpointsFor pulls one protocol's endpoints off the server's agent runtime.
func endpointsFor(s *HybridServer, p agents.Protocol) []agents.Endpoint {
	var out []agents.Endpoint
	for _, ep := range s.agentEndpoints() {
		if ep.Protocol == p {
			out = append(out, ep)
		}
	}
	return out
}

// portIsFree reports whether nothing is listening on addr.
func portIsFree(t *testing.T, addr string) bool {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return false
	}
	_ = lis.Close()
	return true
}

func echoSkill() A2ASkill {
	return A2ASkill{ID: "echo", Name: "Echo", Description: "Repeats the input", Tags: []string{"demo"}}
}

func echoAgent() A2AAgent {
	return a2a.TextAgent(func(_ context.Context, text string) (string, error) {
		return "echo: " + text, nil
	})
}

// freePorts reserves n ports and releases them, so a test can predict addresses
// without racing the rest of the suite for fixed ones.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	var lc net.ListenConfig
	ports := make([]int, 0, n)
	listeners := make([]net.Listener, 0, n)
	for range n {
		lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		listeners = append(listeners, lis)
		ports = append(ports, lis.Addr().(*net.TCPAddr).Port)
	}
	for _, lis := range listeners {
		if err := lis.Close(); err != nil {
			t.Fatalf("release port: %v", err)
		}
	}
	return ports
}

func a2aOpts(t *testing.T, transports ...options.A2ATransport) options.Options {
	t.Helper()
	ports := freePorts(t, 3)
	opts := baseOpts()
	opts.GRPC = options.GRPCOptions{Host: "127.0.0.1", Port: ports[0]}
	opts.HTTP = options.HTTPOptions{Host: "127.0.0.1", Port: ports[1]}
	opts.EnableA2A = true
	opts.A2A = options.A2AOptions{Host: "127.0.0.1", Port: ports[2], Transports: transports}
	return opts
}

func TestWithA2AAgent_Registers(t *testing.T) {
	s := NewHybridServer(baseOpts(), WithA2AAgent(echoAgent(), echoSkill()))
	if len(s.a2aServiceFuncs) != 1 {
		t.Fatalf("got %d service funcs, want 1", len(s.a2aServiceFuncs))
	}
}

// The A2A port is derived rather than demanded, so the four layers land on
// consecutive ports without the caller assigning any.
func TestValidateOptions_A2APortDefaults(t *testing.T) {
	opts := baseOpts()
	opts.GRPC = options.GRPCOptions{Host: "127.0.0.1", Port: 50051}
	opts.HTTP.Port = 8080
	opts.EnableA2A = true
	s := NewHybridServer(opts, WithA2AAgent(echoAgent()))

	if err := s.validateOptions(); err != nil {
		t.Fatalf("validateOptions: %v", err)
	}
	if s.opts.A2A.Port != 8082 {
		t.Errorf("A2A.Port = %d, want 8082 (HTTP+1+1)", s.opts.A2A.Port)
	}
	if s.opts.A2A.Host != "0.0.0.0" {
		t.Errorf("A2A.Host = %q, want 0.0.0.0", s.opts.A2A.Host)
	}
}

func TestValidateOptions_A2APortRespectsMCP(t *testing.T) {
	opts := baseOpts()
	opts.GRPC = options.GRPCOptions{Host: "127.0.0.1", Port: 50051}
	opts.HTTP.Port = 8080
	opts.EnableMCP = true
	opts.MCP.Port = 9000
	opts.EnableA2A = true
	s := NewHybridServer(opts, WithA2AAgent(echoAgent()))

	if err := s.validateOptions(); err != nil {
		t.Fatalf("validateOptions: %v", err)
	}
	if s.opts.A2A.Port != 9001 {
		t.Errorf("A2A.Port = %d, want 9001 (MCP+1)", s.opts.A2A.Port)
	}
}
