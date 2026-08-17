package agents_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/agents/mcp"
)

func TestRuntime_MCPAndA2AShareOnePort(t *testing.T) {
	port := freePort(t)
	rt := agents.New(agents.Config{
		Name:        "my-service",
		Description: "does two things at once",
		Version:     "1.0.0",
		Host:        "127.0.0.1",
		Port:        port,
	})

	rt.Register(
		mcp.Service(serveToolsMCP),
		a2a.Service(echoAgent(), a2a.Skill{
			ID: "echo", Name: "Echo", Description: "Repeats the input",
			Tags: []string{"demo"},
		}),
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(t.Context()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	// The agent card is served, and carries the runtime's identity — not
	// something restated at the a2a.Service call.
	code, body := fetch(t, http.MethodGet, base+a2a.AgentCardPath, "")
	if code != http.StatusOK {
		t.Fatalf("agent card: %d %s", code, body)
	}
	var card a2a.Card
	if err := json.Unmarshal([]byte(body), &card); err != nil {
		t.Fatalf("decode card: %v", err)
	}
	if card.Name != "my-service" || card.Version != "1.0.0" {
		t.Errorf("card identity = %q/%q, want my-service/1.0.0", card.Name, card.Version)
	}
	if card.Description != "does two things at once" {
		t.Errorf("card description = %q", card.Description)
	}
	if len(card.Skills) != 1 || card.Skills[0].ID != "echo" {
		t.Errorf("card skills = %+v", card.Skills)
	}

	// Both protocols are routed, on the one port, at different paths.
	if code, _ := fetch(t, http.MethodPost, base+a2a.DefaultBasePath, `{}`); code == http.StatusNotFound {
		t.Errorf("A2A was not mounted at %s", a2a.DefaultBasePath)
	}
	if code, _ := fetch(t, http.MethodPost, base+"/tools", `{}`); code == http.StatusNotFound {
		t.Error("MCP was not mounted at its generated base path /tools")
	}

	// And the runtime can say where everything went.
	eps := rt.Endpoints()
	if len(eps) != 2 {
		t.Fatalf("got %d endpoints, want 2: %+v", len(eps), eps)
	}
	seen := map[agents.Protocol]bool{}
	for _, ep := range eps {
		seen[ep.Protocol] = true
	}
	if !seen[agents.MCP] || !seen[agents.A2A] {
		t.Errorf("endpoints did not cover both protocols: %+v", eps)
	}
}
