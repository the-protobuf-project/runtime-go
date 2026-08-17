package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This file is the package's protocol surface: the upstream SDK types a caller
// legitimately needs, re-exported so that generated code and service binaries
// import this package alone.
//
// Without it, every file protoc-gen-mcp emits imports both this package and the
// SDK, which forces two problems on the caller. Their go.mod grows a direct
// dependency on the SDK, so generated code and the runtime can drift onto
// different SDK versions and fail with type mismatches that name neither
// package. And because both packages are called mcp, one of them has to be
// import-aliased at every call site.
//
// These are type aliases, not defined types, and deliberately so: an alias is
// the same type as the SDK's, so a value crosses between this package and the
// SDK with no conversion, and a caller who does reach for the SDK directly
// still interoperates. A defined type would break both.
//
// The SDK is still an exported dependency of this module — the alias makes it a
// detail of one file rather than of every generated file in every consumer.

// Server is the MCP server generated Register*MCPHandler functions add tools,
// prompts, and resources to. Build one with [NewMCPServer].
type Server = mcp.Server

// ServerOptions configures a [Server]. Set it on [MCPServerConfig.ServerOptions].
type ServerOptions = mcp.ServerOptions

// Implementation identifies a client or server by name and version in the MCP
// initialize handshake.
type Implementation = mcp.Implementation

// Resource is a resource the server can read, addressed by a fixed URI.
// Generated code registers one per resource declared in the proto.
type Resource = mcp.Resource

// ResourceTemplate is a family of resources addressed by an RFC 6570 URI
// template. Generated code registers one per resource `pattern` in the proto.
type ResourceTemplate = mcp.ResourceTemplate

// ResourceContents is the body of a resource read. Set Text for textual media
// types and Blob for binary ones; Blob is base64-encoded on the wire.
type ResourceContents = mcp.ResourceContents

// ReadResourceRequest is the request passed to a resource handler.
type ReadResourceRequest = mcp.ReadResourceRequest

// ReadResourceParams carries the URI being read.
type ReadResourceParams = mcp.ReadResourceParams

// ReadResourceResult is what a resource handler returns.
type ReadResourceResult = mcp.ReadResourceResult

// Annotations describe a resource's intended audience and priority.
type Annotations = mcp.Annotations

// Icon is an icon a client may display for a resource.
type Icon = mcp.Icon

// IconTheme is the background an icon is designed for: [IconThemeLight] or
// [IconThemeDark].
type IconTheme = mcp.IconTheme

// Role is the sender or recipient of a message: [RoleUser] or [RoleAssistant].
type Role = mcp.Role

// Prompt is a prompt template the server offers.
type Prompt = mcp.Prompt

// PromptArgument is one argument of a [Prompt], derived from the fields of the
// proto message named by the prompt's schema.
type PromptArgument = mcp.PromptArgument

// PromptMessage is a single message in a prompt result.
type PromptMessage = mcp.PromptMessage

// GetPromptRequest is the request passed to a prompt handler.
type GetPromptRequest = mcp.GetPromptRequest

// GetPromptResult is what a prompt handler returns.
type GetPromptResult = mcp.GetPromptResult

// CallToolRequest is the request passed to a tool handler.
type CallToolRequest = mcp.CallToolRequest

// CallToolResult is what a tool handler returns. Build the common cases with
// [TextResult] and [ErrorResult].
type CallToolResult = mcp.CallToolResult

// Tool is an MCP tool descriptor. Build one with [MustCreateTool].
type Tool = mcp.Tool

// TextContent is a plain-text content block.
type TextContent = mcp.TextContent

// ElicitParams asks the client to collect input before a tool runs, either as a
// form or by sending the user to a URL.
type ElicitParams = mcp.ElicitParams

// ElicitResult is the client's answer to an [ElicitParams]. Action is "accept",
// "decline", or "cancel".
type ElicitResult = mcp.ElicitResult

// InputRequestMap carries pending input requests on a [CallToolResult], keyed
// by request ID. Generated elicitation code uses [ElicitRequestID].
type InputRequestMap = mcp.InputRequestMap

// ServerSession is a live connection between the server and one client.
type ServerSession = mcp.ServerSession

// ProgressNotificationParams reports progress against a request's progress
// token. Generated streaming code sends these via [SendProgressFromProto].
type ProgressNotificationParams = mcp.ProgressNotificationParams

// Roles a message may be addressed to or from.
const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// Icon themes. The zero value (unset) means the icon suits any background.
const (
	IconThemeLight IconTheme = "light"
	IconThemeDark  IconTheme = "dark"
)

// Elicitation modes, set on [ElicitParams.Mode]. When unset the SDK infers the
// mode: url if a URL or elicitation ID is present, otherwise form.
const (
	ElicitModeForm = "form"
	ElicitModeURL  = "url"
)

// Client-side types, for tests and for processes that drive an MCP server.

// Client connects to an MCP server. Build one with [NewClient].
type Client = mcp.Client

// ClientOptions configures a [Client].
type ClientOptions = mcp.ClientOptions

// ClientSession is a live connection from a [Client] to a server.
type ClientSession = mcp.ClientSession

// StreamableClientTransport connects a [Client] to a server over streamable
// HTTP. Set Endpoint to the server's URL.
type StreamableClientTransport = mcp.StreamableClientTransport

// StreamableHTTPHandler serves a [Server] over streamable HTTP.
type StreamableHTTPHandler = mcp.StreamableHTTPHandler

// StreamableHTTPOptions configures a [StreamableHTTPHandler].
type StreamableHTTPOptions = mcp.StreamableHTTPOptions

// NewClient returns a client identified by impl. Pass nil options for defaults.
func NewClient(impl *Implementation, options *ClientOptions) *Client {
	return mcp.NewClient(impl, options)
}

// NewStreamableHTTPHandler returns an HTTP handler serving the server that
// getServer returns for each request. Pass nil opts for defaults.
//
// [StartServer] wires this up for a whole service; reach for this directly when
// you are mounting an MCP server into an HTTP server you already own.
func NewStreamableHTTPHandler(getServer func(*http.Request) *Server, opts *StreamableHTTPOptions) *StreamableHTTPHandler {
	return mcp.NewStreamableHTTPHandler(getServer, opts)
}
