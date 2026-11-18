package mcp

import (
	"context"
	"errors"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

func TestEveryMCPMethodIsAnOperationNamingItsTool(t *testing.T) {
	rec := recorder.New()
	handle := observeMethods(rec.Signals())(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{}, nil
	})
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "search_books"}}
	if _, err := handle(context.Background(), "tools/call", req); err != nil {
		t.Fatal(err)
	}
	s := rec.Span("agents.tools/call")
	if s == nil || s.Failed() {
		t.Fatalf("span %+v (all %v)", s, rec.Spans())
	}
	if s.Attr("mcp.tool.name") != "search_books" || s.Kind != telemetry.SpanKindServer {
		t.Errorf("span attributes: tool %v kind %v", s.Attr("mcp.tool.name"), s.Kind)
	}
	if got := rec.Outcomes()["tools/call"]; got != "ok" {
		t.Errorf("outcome %q", got)
	}
}

func TestAToolThatAnswersIsErrorFailsItsOperation(t *testing.T) {
	rec := recorder.New()
	handle := observeMethods(rec.Signals())(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{IsError: true}, nil
	})
	_, _ = handle(context.Background(), "tools/call", &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "t"}})
	if got := rec.Outcomes()["tools/call"]; got != "error" {
		t.Errorf("outcome %q, want error", got)
	}
}

func TestAProtocolErrorFailsTheSpan(t *testing.T) {
	rec := recorder.New()
	boom := errors.New("boom")
	handle := observeMethods(rec.Signals())(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return nil, boom
	})
	if _, err := handle(context.Background(), "resources/read", &mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: "res://a"}}); !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	s := rec.Span("agents.resources/read")
	if s == nil || !errors.Is(s.Err(), boom) || s.Attr("mcp.resource.uri") != "res://a" {
		t.Errorf("span %+v", s)
	}
}
