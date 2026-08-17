package mcp

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// assertSameType fails to compile unless both arguments have the same type.
// Inference must unify the two parameters to a single T, which it can only do
// when the re-export and the SDK type really are one type.
//
// This is a compile-time assertion; the body never needs to run. It replaces
// the more obvious `var x *Server = (*mcp.Server)(nil)`, which asserts the same
// thing but reads to staticcheck as a redundant type on a declaration (ST1023).
func assertSameType[T any](_, _ T) {}

// The re-exports must be aliases, not defined types. A defined type would still
// compile everywhere inside this package but would stop a caller passing a
// value to the SDK, which is the whole reason they exist.
func TestReExportsAreAliasesOfTheSDK(_ *testing.T) {
	assertSameType((*Server)(nil), (*mcp.Server)(nil))
	assertSameType((*ServerOptions)(nil), (*mcp.ServerOptions)(nil))
	assertSameType((*Implementation)(nil), (*mcp.Implementation)(nil))
	assertSameType((*Resource)(nil), (*mcp.Resource)(nil))
	assertSameType((*ResourceTemplate)(nil), (*mcp.ResourceTemplate)(nil))
	assertSameType((*ResourceContents)(nil), (*mcp.ResourceContents)(nil))
	assertSameType((*ReadResourceRequest)(nil), (*mcp.ReadResourceRequest)(nil))
	assertSameType((*ReadResourceParams)(nil), (*mcp.ReadResourceParams)(nil))
	assertSameType((*ReadResourceResult)(nil), (*mcp.ReadResourceResult)(nil))
	assertSameType((*Annotations)(nil), (*mcp.Annotations)(nil))
	assertSameType((*Icon)(nil), (*mcp.Icon)(nil))
	assertSameType((*Prompt)(nil), (*mcp.Prompt)(nil))
	assertSameType((*PromptArgument)(nil), (*mcp.PromptArgument)(nil))
	assertSameType((*PromptMessage)(nil), (*mcp.PromptMessage)(nil))
	assertSameType((*GetPromptRequest)(nil), (*mcp.GetPromptRequest)(nil))
	assertSameType((*GetPromptResult)(nil), (*mcp.GetPromptResult)(nil))
	assertSameType((*CallToolRequest)(nil), (*mcp.CallToolRequest)(nil))
	assertSameType((*CallToolResult)(nil), (*mcp.CallToolResult)(nil))
	assertSameType((*Tool)(nil), (*mcp.Tool)(nil))
	assertSameType((*TextContent)(nil), (*mcp.TextContent)(nil))
	assertSameType((*ElicitParams)(nil), (*mcp.ElicitParams)(nil))
	assertSameType((*ElicitResult)(nil), (*mcp.ElicitResult)(nil))
	assertSameType((*InputRequestMap)(nil), (*mcp.InputRequestMap)(nil))
	assertSameType((*ServerSession)(nil), (*mcp.ServerSession)(nil))
	assertSameType((*ProgressNotificationParams)(nil), (*mcp.ProgressNotificationParams)(nil))
	assertSameType((*Client)(nil), (*mcp.Client)(nil))
	assertSameType((*ClientOptions)(nil), (*mcp.ClientOptions)(nil))
	assertSameType((*ClientSession)(nil), (*mcp.ClientSession)(nil))
	assertSameType((*StreamableClientTransport)(nil), (*mcp.StreamableClientTransport)(nil))
	assertSameType((*StreamableHTTPHandler)(nil), (*mcp.StreamableHTTPHandler)(nil))
	assertSameType((*StreamableHTTPOptions)(nil), (*mcp.StreamableHTTPOptions)(nil))
	assertSameType((*ResourceHandler)(nil), (*mcp.ResourceHandler)(nil))
	assertSameType((*Role)(nil), (*mcp.Role)(nil))
	assertSameType((*IconTheme)(nil), (*mcp.IconTheme)(nil))
}

// The exported constants must carry the wire values the MCP spec defines, since
// generated code emits them into resource and prompt registrations.
func TestExportedConstantsMatchTheWireValues(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"RoleUser", string(RoleUser), "user"},
		{"RoleAssistant", string(RoleAssistant), "assistant"},
		{"IconThemeLight", string(IconThemeLight), "light"},
		{"IconThemeDark", string(IconThemeDark), "dark"},
		{"ElicitModeForm", ElicitModeForm, "form"},
		{"ElicitModeURL", ElicitModeURL, "url"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

// A server built through this package must accept the re-exported types, since
// that is exactly what generated Register*MCPHandler code does.
func TestReExportedServerAcceptsReExportedTypes(_ *testing.T) {
	s := NewMCPServer(&MCPServerConfig{Name: "alias-test", Version: "1.0.0"})

	s.AddResource(&Resource{
		URI:      "test://doc.md",
		Name:     "doc",
		Title:    "A document",
		MIMEType: "text/markdown",
		Size:     3,
		Annotations: &Annotations{
			Audience: []Role{RoleUser, RoleAssistant},
			Priority: 0.5,
		},
		Icons: []Icon{{Source: "https://example.com/i.svg", Theme: IconThemeLight}},
	}, DefaultResourceHandler())

	s.AddResourceTemplate(&ResourceTemplate{
		URITemplate: "test://docs/{doc}",
		Name:        "docs",
		MIMEType:    "text/markdown",
	}, DefaultResourceHandler())

	s.AddPrompt(&Prompt{
		Name:      "summarize",
		Arguments: []*PromptArgument{{Name: "topic", Required: true}},
	}, DefaultPromptHandler("summarize"))
}

// WithResourceHandler must win over the generated placeholder, and only for the
// URI it names.
func TestResourceHandlerFor(t *testing.T) {
	marker := "override"
	custom := func(_ context.Context, _ *ReadResourceRequest) (*ReadResourceResult, error) {
		return &ReadResourceResult{Contents: []*ResourceContents{{URI: marker}}}, nil
	}
	fallback := func(_ context.Context, _ *ReadResourceRequest) (*ReadResourceResult, error) {
		return &ReadResourceResult{Contents: []*ResourceContents{{URI: "fallback"}}}, nil
	}

	cfg := ApplyOptions(WithResourceHandler("test://doc.md", custom))

	call := func(h ResourceHandler) string {
		res, err := h(context.Background(), &ReadResourceRequest{})
		if err != nil {
			t.Fatalf("handler: %v", err)
		}
		return res.Contents[0].URI
	}

	if got := call(ResourceHandlerFor(cfg, "test://doc.md", fallback)); got != marker {
		t.Errorf("configured URI: got %q, want the override %q", got, marker)
	}
	if got := call(ResourceHandlerFor(cfg, "test://other.md", fallback)); got != "fallback" {
		t.Errorf("unconfigured URI: got %q, want the fallback", got)
	}
	if got := call(ResourceHandlerFor(nil, "test://doc.md", fallback)); got != "fallback" {
		t.Errorf("nil config: got %q, want the fallback", got)
	}
}
