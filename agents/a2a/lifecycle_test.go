package a2a

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Resolution happens on a copy: a caller reusing one config across servers must
// never find another server's defaults written back into it.
func TestStartServer_DoesNotMutateCallerConfig(t *testing.T) {
	cfg := &ServerConfig{Name: "test", Mux: http.NewServeMux()}

	ready := make(chan struct{})
	cfg.OnReady = func(*ServerConfig) { close(ready) }

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- StartServer(ctx, cfg, echoAgent()) }()
	<-ready
	cancel()
	<-done

	if cfg.BasePath != "" {
		t.Errorf("BasePath was written back as %q", cfg.BasePath)
	}
	if cfg.Addr != "" {
		t.Errorf("Addr was written back as %q", cfg.Addr)
	}
	if len(cfg.Transports) != 0 {
		t.Errorf("Transports was written back as %v", cfg.Transports)
	}
}

func TestStartServer_SharedMuxMountsRoutes(t *testing.T) {
	mux := http.NewServeMux()
	cfg := &ServerConfig{
		Name:           "echo",
		Description:    "Repeats what it is told",
		Version:        "1.0.0",
		Mux:            mux,
		ServeAgentCard: true,
		Skills:         []Skill{{ID: "echo", Name: "Echo", Description: "Repeats", Tags: []string{"demo"}}},
	}

	ready := make(chan *ServerConfig, 1)
	cfg.OnReady = func(resolved *ServerConfig) { ready <- resolved }

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- StartServer(ctx, cfg, echoAgent()) }()

	var resolved *ServerConfig
	select {
	case resolved = <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("OnReady was never called")
	}
	if resolved.BasePath != DefaultBasePath {
		t.Errorf("resolved BasePath = %q, want %q", resolved.BasePath, DefaultBasePath)
	}

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// The card is served, and describes the agent it was built from.
	cardReq, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+AgentCardPath, nil)
	if err != nil {
		t.Fatalf("build card request: %v", err)
	}
	resp, err := srv.Client().Do(cardReq)
	if err != nil {
		t.Fatalf("fetch card: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("card status = %d, want 200", resp.StatusCode)
	}
	var card Card
	if decErr := json.NewDecoder(resp.Body).Decode(&card); decErr != nil {
		t.Fatalf("decode card: %v", decErr)
	}
	if card.Name != "echo" {
		t.Errorf("card.Name = %q, want echo", card.Name)
	}
	if len(card.Skills) != 1 || card.Skills[0].ID != "echo" {
		t.Errorf("card.Skills = %+v", card.Skills)
	}

	// The JSON-RPC route exists. What it answers to a malformed body is the
	// SDK's business; that it is routed at all is ours.
	rpcReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		srv.URL+DefaultBasePath, strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build jsonrpc request: %v", err)
	}
	rpcReq.Header.Set("Content-Type", "application/json")
	rpc, err := srv.Client().Do(rpcReq)
	if err != nil {
		t.Fatalf("post jsonrpc: %v", err)
	}
	defer func() { _ = rpc.Body.Close() }()
	if rpc.StatusCode == http.StatusNotFound {
		t.Errorf("JSON-RPC endpoint was not mounted at %s", DefaultBasePath)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("StartServer returned %v, want nil", err)
	}
}
