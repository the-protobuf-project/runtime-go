package mcp

import (
	"context"
	"fmt"
	"strings"

	mcppb "buf.build/gen/go/the-protobuf-project/mcp/protocolbuffers/go/mcp/protobuf"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// SendProgressFromProto sends an MCP progress notification from an MCPProgress proto.
// If token or p is nil, it returns nil. Used by generated streaming tool handlers.
func SendProgressFromProto(ctx context.Context, session *mcpsdk.ServerSession, token any, p *mcppb.MCPProgress) error {
	if token == nil || p == nil || session == nil {
		return nil
	}
	params := &mcpsdk.ProgressNotificationParams{
		ProgressToken: token,
		Progress:      p.Progress,
		Message:       p.Message,
	}
	if p.Total != nil {
		params.Total = *p.Total
	}
	return session.NotifyProgress(ctx, params)
}

// SendDoneProgress sends a final MCP progress notification (progress=1, total=1)
// with resultJSON as the message, signaling to the MCP client that the streaming
// operation has completed. Generated non-blocking streaming handlers call this
// when the result chunk arrives from the gRPC server method.
func SendDoneProgress(ctx context.Context, session *mcpsdk.ServerSession, token any, resultJSON string) error {
	if token == nil || session == nil {
		return nil
	}
	one := 1.0
	return SendProgressFromProto(ctx, session, token, &mcppb.MCPProgress{
		Progress: 1.0,
		Total:    &one,
		Message:  resultJSON,
	})
}

// DefaultPromptHandler returns a prompt handler that produces a single user
// message containing the prompt description. It is used as a placeholder for
// prompts declared via MCP proto options. Replace it by calling
// server.RemovePrompts / server.AddPrompt with your own handler.
func DefaultPromptHandler(description string) func(context.Context, *mcpsdk.GetPromptRequest) (*mcpsdk.GetPromptResult, error) {
	return func(_ context.Context, req *mcpsdk.GetPromptRequest) (*mcpsdk.GetPromptResult, error) {
		return &mcpsdk.GetPromptResult{
			Description: description,
			Messages: []*mcpsdk.PromptMessage{
				{
					Role:    "user",
					Content: &mcpsdk.TextContent{Text: description},
				},
			},
		}, nil
	}
}

// DefaultResourceHandler returns a resource handler that returns an empty JSON
// object. It is used as a placeholder for resources declared via MCP proto
// options. Replace it by calling server.RemoveResources / server.AddResource
// with your own handler.
func DefaultResourceHandler() func(context.Context, *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
	return func(_ context.Context, req *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
		return &mcpsdk.ReadResourceResult{
			Contents: []*mcpsdk.ResourceContents{
				{URI: req.Params.URI, Text: "{}"},
			},
		}, nil
	}
}

// AppResourceURI returns the canonical ui:// resource URI for a service app.
func AppResourceURI(serviceName string) string {
	return fmt.Sprintf("ui://%s/app.html", strings.ToLower(serviceName))
}

// SetToolAppMeta returns a shallow clone of tool with _meta.ui.resourceUri set,
// which makes the tool show up as an MCP App in supporting hosts.
func SetToolAppMeta(tool *mcpsdk.Tool, resourceURI string) *mcpsdk.Tool {
	cloned := *tool
	cloned.Meta = mcpsdk.Meta{
		"ui": map[string]any{
			"resourceUri": resourceURI,
		},
	}
	return &cloned
}

// DefaultAppHTML returns a minimal HTML page for an MCP App placeholder.
func DefaultAppHTML(appName, version, description string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>%s</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; color: #333; }
  h1 { font-size: 1.5rem; } p { color: #666; } .version { font-size: 0.85rem; color: #999; }
</style>
</head>
<body>
  <h1>%s</h1>
  <p class="version">v%s</p>
  <p>%s</p>
  <p>This is a generated MCP App placeholder. Replace this resource with your own UI.</p>
</body>
</html>`, appName, appName, version, description)
}

// DefaultAppResourceHandler returns a resource handler that serves the default
// app HTML page. The returned handler is suitable for registration with
// server.AddResource for the ui:// resource URI.
func DefaultAppResourceHandler(appName, version, description string) func(context.Context, *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
	html := DefaultAppHTML(appName, version, description)
	return func(_ context.Context, req *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
		return &mcpsdk.ReadResourceResult{
			Contents: []*mcpsdk.ResourceContents{
				{URI: req.Params.URI, MIMEType: "text/html", Text: html},
			},
		}, nil
	}
}

// CompletionHandlerFromEnums builds a CompletionHandler that serves autocomplete
// values for prompt arguments. The enumValues map is keyed by "promptName:argName".
func CompletionHandlerFromEnums(enumValues map[string][]string) func(context.Context, *mcpsdk.CompleteRequest) (*mcpsdk.CompleteResult, error) {
	return func(_ context.Context, req *mcpsdk.CompleteRequest) (*mcpsdk.CompleteResult, error) {
		if req.Params.Ref.Type != "ref/prompt" {
			return &mcpsdk.CompleteResult{Completion: mcpsdk.CompletionResultDetails{Values: []string{}}}, nil
		}
		key := req.Params.Ref.Name + ":" + req.Params.Argument.Name
		values, ok := enumValues[key]
		if !ok {
			return &mcpsdk.CompleteResult{Completion: mcpsdk.CompletionResultDetails{Values: []string{}}}, nil
		}
		prefix := strings.ToLower(req.Params.Argument.Value)
		filtered := []string{}
		for _, v := range values {
			if strings.HasPrefix(strings.ToLower(v), prefix) {
				filtered = append(filtered, v)
			}
		}
		return &mcpsdk.CompleteResult{
			Completion: mcpsdk.CompletionResultDetails{
				Values:  filtered,
				Total:   len(filtered),
				HasMore: false,
			},
		}, nil
	}
}
