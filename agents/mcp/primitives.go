package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/types/known/structpb"
)

// ProgressMessage is the shape of a generated mcp.v1.MCPProgress value.
//
// It is an interface rather than the generated struct so that this module does
// not depend on any one build of the MCP annotations. A generated *MCPProgress
// satisfies it whatever version of the schema produced it, and — because Go's
// protobuf registry rejects two packages claiming the same extension numbers —
// depending on a specific build here would make a binary that links this
// runtime alongside a differently-built MCPProgress panic during package init.
//
// Total reports 0 when unset, which is also what the MCP spec means by an
// unknown total, so the two are indistinguishable on the wire.
type ProgressMessage interface {
	GetProgress() float64
	GetMessage() string
	GetTotal() float64
}

// progressTokenCarrier is implemented by an MCPProgress whose schema has the
// progress_token oneof, letting a chunk name the progress stream it belongs to
// rather than relying on the token from the MCP request.
type progressTokenCarrier interface {
	GetTokenString() string
	GetTokenInt() int64
}

// progressMetaCarrier is implemented by an MCPProgress whose schema has the
// meta map, carrying arbitrary metadata alongside the progress update.
type progressMetaCarrier interface {
	GetMeta() map[string]*structpb.Struct
}

// SendProgressFromProto sends an MCP progress notification from an MCPProgress
// proto. If token or p is nil, it returns nil. Used by generated streaming tool
// handlers.
//
// When p carries its own progress token it wins over token, so a gRPC server
// fanning one stream out to several MCP requests can tag each chunk.
func SendProgressFromProto(ctx context.Context, session *mcp.ServerSession, token any, p ProgressMessage) error {
	if token == nil || p == nil || session == nil {
		return nil
	}
	if carrier, ok := p.(progressTokenCarrier); ok {
		if s := carrier.GetTokenString(); s != "" {
			token = s
		} else if i := carrier.GetTokenInt(); i != 0 {
			token = i
		}
	}
	params := &mcp.ProgressNotificationParams{
		ProgressToken: token,
		Progress:      p.GetProgress(),
		Message:       p.GetMessage(),
		Total:         p.GetTotal(),
	}
	if carrier, ok := p.(progressMetaCarrier); ok {
		if meta := carrier.GetMeta(); len(meta) > 0 {
			params.Meta = make(mcp.Meta, len(meta))
			for k, v := range meta {
				params.Meta[k] = v.AsMap()
			}
		}
	}
	return session.NotifyProgress(ctx, params)
}

// doneProgress is the final progress update SendDoneProgress sends. It exists
// so that this package can report completion without constructing a generated
// protobuf value, which would reintroduce the schema dependency
// [ProgressMessage] removes.
type doneProgress struct{ message string }

func (d doneProgress) GetProgress() float64 { return 1.0 }
func (d doneProgress) GetMessage() string   { return d.message }
func (d doneProgress) GetTotal() float64    { return 1.0 }

// SendDoneProgress sends a final MCP progress notification (progress=1, total=1)
// with resultJSON as the message, signaling to the MCP client that the streaming
// operation has completed. Generated non-blocking streaming handlers call this
// when the result chunk arrives from the gRPC server method.
func SendDoneProgress(ctx context.Context, session *mcp.ServerSession, token any, resultJSON string) error {
	if token == nil || session == nil {
		return nil
	}
	return SendProgressFromProto(ctx, session, token, doneProgress{message: resultJSON})
}

// DefaultPromptHandler returns a prompt handler that produces a single user
// message containing the prompt description. It is used as a placeholder for
// prompts declared via MCP proto options. Replace it by calling
// server.RemovePrompts / server.AddPrompt with your own handler.
func DefaultPromptHandler(description string) func(context.Context, *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	return func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{
			Description: description,
			Messages: []*mcp.PromptMessage{
				{
					Role:    "user",
					Content: &mcp.TextContent{Text: description},
				},
			},
		}, nil
	}
}

// DefaultResourceHandler returns a resource handler that returns an empty JSON
// object. It is used as a placeholder for resources declared via MCP proto
// options. Replace it by calling server.RemoveResources / server.AddResource
// with your own handler.
func DefaultResourceHandler() func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	return func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
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
func SetToolAppMeta(tool *mcp.Tool, resourceURI string) *mcp.Tool {
	cloned := *tool
	cloned.Meta = mcp.Meta{
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
func DefaultAppResourceHandler(appName, version, description string) func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	html := DefaultAppHTML(appName, version, description)
	return func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{URI: req.Params.URI, MIMEType: "text/html", Text: html},
			},
		}, nil
	}
}

// CompletionHandlerFromEnums builds a CompletionHandler that serves autocomplete
// values for prompt arguments. The enumValues map is keyed by "promptName:argName".
func CompletionHandlerFromEnums(enumValues map[string][]string) func(context.Context, *mcp.CompleteRequest) (*mcp.CompleteResult, error) {
	return func(_ context.Context, req *mcp.CompleteRequest) (*mcp.CompleteResult, error) {
		if req.Params.Ref.Type != "ref/prompt" {
			return &mcp.CompleteResult{Completion: mcp.CompletionResultDetails{Values: []string{}}}, nil
		}
		key := req.Params.Ref.Name + ":" + req.Params.Argument.Name
		values, ok := enumValues[key]
		if !ok {
			return &mcp.CompleteResult{Completion: mcp.CompletionResultDetails{Values: []string{}}}, nil
		}
		prefix := strings.ToLower(req.Params.Argument.Value)
		filtered := []string{}
		for _, v := range values {
			if strings.HasPrefix(strings.ToLower(v), prefix) {
				filtered = append(filtered, v)
			}
		}
		return &mcp.CompleteResult{
			Completion: mcp.CompletionResultDetails{
				Values:  filtered,
				Total:   len(filtered),
				HasMore: false,
			},
		}, nil
	}
}
