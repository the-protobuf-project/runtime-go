// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAskRoundTrip(t *testing.T) {
	err := Ask(codes.FailedPrecondition, "Book it for 31,360 rupees?", []ElicitField{{Name: "confirmed", Type: "boolean", Required: true}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v; want FailedPrecondition", status.Code(err))
	}
	msg, schema, ok := Asked(err)
	if !ok || msg != "Book it for 31,360 rupees?" || schema.Properties["confirmed"].Type != "boolean" || schema.Required[0] != "confirmed" {
		t.Fatalf("Asked = %q, %+v, %v", msg, schema, ok)
	}
	if _, _, ok := Asked(status.Error(codes.InvalidArgument, "plain")); ok {
		t.Error("a plain error read as a question")
	}
}

func TestAskPendingWithoutElicitation(t *testing.T) {
	err := Ask(codes.InvalidArgument, "May I have your name?", []ElicitField{{Name: "phone", Type: "string"}, {Name: "guest_display_name", Type: "string"}})
	res, ok := AskPending(&CallToolRequest{}, err)
	if !ok || !res.IsError || len(res.InputRequests) != 0 {
		t.Fatalf("AskPending = %+v, %v; want a tool error for the model to ask", res, ok)
	}
	if text := res.Content[0].(*TextContent).Text; !strings.Contains(text, "May I have your name? (guest_display_name, phone)") {
		t.Errorf("text = %q", text)
	}
	if _, ok := AskPending(&CallToolRequest{}, status.Error(codes.Internal, "boom")); ok {
		t.Error("a plain error was put as a question")
	}
}

func TestAskPendingCapabilities(t *testing.T) {
	err := Ask(codes.FailedPrecondition, "Book it?", []ElicitField{{Name: "confirmed", Type: "boolean", Required: true}})
	call := func(meta mcp.Meta) *CallToolResult {
		res, _ := AskPending(&CallToolRequest{Session: &mcp.ServerSession{}, Params: &CallToolParamsRaw{Meta: meta}}, err)
		return res
	}
	if res := call(nil); !res.IsError {
		t.Errorf("nothing declared = %+v; want a tool error", res)
	}
	if res := call(mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{"elicitation": map[string]any{}}}); len(res.InputRequests) != 1 {
		t.Errorf("elicitation declared = %+v; want the question asked", res)
	}
	if res := call(mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{}}); !res.IsError {
		t.Errorf("elicitation left out = %+v; want a tool error", res)
	}
}

func TestMergeAnswers(t *testing.T) {
	req := &CallToolRequest{Params: &CallToolParamsRaw{InputResponses: mcp.InputResponseMap{
		askID("Name?"):  &ElicitResult{Action: "accept", Content: map[string]any{"guest_display_name": "Priya"}},
		ElicitRequestID: &ElicitResult{Action: "accept", Content: map[string]any{"ignored": true}},
	}}}
	merged, declined := MergeAnswers(req, json.RawMessage(`{"venue":"venues/v"}`))
	var got map[string]any
	_ = json.Unmarshal(merged, &got)
	if declined || got["guest_display_name"] != "Priya" || got["venue"] != "venues/v" || got["ignored"] != nil {
		t.Errorf("merged = %s, declined %v", merged, declined)
	}
	req.Params.InputResponses[askID("Book?")] = &ElicitResult{Action: "decline"}
	if _, declined := MergeAnswers(req, merged); !declined {
		t.Error("a declined question was not reported")
	}
}

func TestMissingElicitFields(t *testing.T) {
	fields := []ElicitField{{Name: "name"}, {Name: "phone"}, {Name: "email"}}
	got := MissingElicitFields(json.RawMessage(`{"name":"Priya","phone":""}`), fields)
	if len(got) != 2 || got[0].Name != "phone" || got[1].Name != "email" {
		t.Errorf("missing = %+v; want phone and email", got)
	}
}
