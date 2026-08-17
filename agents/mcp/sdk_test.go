package mcp

import (
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The re-exports must be aliases, not defined types. A defined type would still
// compile at every use inside this package but would stop a caller passing a
// value to the SDK, which is the whole reason they exist. Assigning across the
// two spellings only compiles when they name the same type.
func TestReExportsAreAliasesOfTheSDK(t *testing.T) {
	var (
		server   *Server                     = (*mcp.Server)(nil)
		resource *Resource                   = (*mcp.Resource)(nil)
		template *ResourceTemplate           = (*mcp.ResourceTemplate)(nil)
		contents *ResourceContents           = (*mcp.ResourceContents)(nil)
		annots   *Annotations                = (*mcp.Annotations)(nil)
		icon     *Icon                       = (*mcp.Icon)(nil)
		prompt   *Prompt                     = (*mcp.Prompt)(nil)
		promptCT *PromptMessage              = (*mcp.PromptMessage)(nil)
		callReq  *CallToolRequest            = (*mcp.CallToolRequest)(nil)
		callRes  *CallToolResult             = (*mcp.CallToolResult)(nil)
		elicitP  *ElicitParams               = (*mcp.ElicitParams)(nil)
		elicitR  *ElicitResult               = (*mcp.ElicitResult)(nil)
		session  *ServerSession              = (*mcp.ServerSession)(nil)
		progress *ProgressNotificationParams = (*mcp.ProgressNotificationParams)(nil)
	)
	// The assignments above are the assertion; these keep them live.
	_ = server
	_ = resource
	_ = template
	_ = contents
	_ = annots
	_ = icon
	_ = prompt
	_ = promptCT
	_ = callReq
	_ = callRes
	_ = elicitP
	_ = elicitR
	_ = session
	_ = progress

	// And back the other way, for the types generated code constructs directly.
	var sdkRole mcp.Role = RoleUser
	if sdkRole != "user" {
		t.Errorf("RoleUser = %q, want %q", sdkRole, "user")
	}
	var sdkTheme mcp.IconTheme = IconThemeDark
	if sdkTheme != "dark" {
		t.Errorf("IconThemeDark = %q, want %q", sdkTheme, "dark")
	}
	var reqs InputRequestMap = mcp.InputRequestMap{}
	if reqs == nil {
		t.Error("InputRequestMap: got nil, want an empty map")
	}
}

// A server built through this package must be usable with the re-exported
// types, since that is exactly what generated Register*MCPHandler code does.
func TestReExportedServerAcceptsReExportedTypes(t *testing.T) {
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
