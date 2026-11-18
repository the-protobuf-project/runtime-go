package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// observeMethods makes every MCP method the server receives one operation of
// component "agents", backend "mcp", named for the method (tools/call,
// resources/read, prompts/get, initialize and the rest: a set the protocol
// fixes, so it is a safe metric label). A tool call's tool, a resource's URI
// and a prompt's name are span attributes, never labels. A tool that answers
// with IsError fails its operation: the call reached the tool and the tool
// failed. The span is the child of the HTTP request's, which continued the
// caller's trace.
func observeMethods(signals telemetry.Signals) mcp.Middleware {
	obs := signals.Observer("agents")
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			ctx, op := obs.Start(ctx, "mcp", method, telemetry.SpanKindServer,
				telemetry.WithSpanAttributes(methodAttributes(req)))
			res, err := next(ctx, method, req)
			outcome := telemetry.OutcomeOf(err)
			if err != nil && outcome.IsError() {
				op.Span().SetError(err)
			} else if r, ok := res.(*mcp.CallToolResult); ok && r != nil && r.IsError {
				op.Span().SetAttribute("mcp.tool.is_error", true)
				outcome = telemetry.OutcomeError
			}
			op.Finish(outcome)
			return res, err
		}
	}
}

// methodAttributes names what a request is about, for its span.
func methodAttributes(req mcp.Request) map[string]any {
	attrs := map[string]any{"rpc.system": "mcp"}
	if req == nil {
		return attrs
	}
	switch p := req.GetParams().(type) {
	case *mcp.CallToolParamsRaw:
		attrs["mcp.tool.name"] = p.Name
	case *mcp.CallToolParams:
		attrs["mcp.tool.name"] = p.Name
	case *mcp.ReadResourceParams:
		attrs["mcp.resource.uri"] = p.URI
	case *mcp.GetPromptParams:
		attrs["mcp.prompt.name"] = p.Name
	}
	return attrs
}
