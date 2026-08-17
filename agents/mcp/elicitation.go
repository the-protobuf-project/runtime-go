package mcp

import (
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ElicitField describes a field for an elicitation (confirmation) request.
// Used with RunElicitation to build a form shown to the user before tool execution.
type ElicitField struct {
	Name        string   // JSON property name
	Description string   // Shown in the form
	Required    bool     // If true, user must provide a value
	Type        string   // JSON Schema type: "string", "number", "boolean"
	EnumValues  []string // Optional: friendly names shown in the elicitation form
	ProtoValues []string // Optional: proto enum names, parallel to EnumValues, for reverse-mapping after accept
}

// MergeElicitResult overlays the accepted elicitation result content onto the
// original LLM tool args JSON. Enum fields whose ElicitField has ProtoValues
// are reverse-mapped from their friendly UI names back to their protobuf enum
// names so that protojson.Unmarshal decodes them correctly. The returned bytes
// are always valid JSON.
func MergeElicitResult(args json.RawMessage, content map[string]any, fields []ElicitField) json.RawMessage {
	if len(content) == 0 {
		return args
	}
	// Build friendly-name → proto-name lookup per field.
	protoMap := make(map[string]map[string]string, len(fields))
	for _, f := range fields {
		if len(f.ProtoValues) > 0 && len(f.ProtoValues) == len(f.EnumValues) {
			m := make(map[string]string, len(f.EnumValues))
			for i, friendly := range f.EnumValues {
				m[friendly] = f.ProtoValues[i]
			}
			protoMap[f.Name] = m
		}
	}
	// Unmarshal existing args.
	var merged map[string]any
	if err := json.Unmarshal(args, &merged); err != nil || merged == nil {
		merged = make(map[string]any)
	}
	// Overlay elicitation content, reverse-mapping enum values where needed.
	for k, v := range content {
		if m, ok := protoMap[k]; ok {
			if s, ok := v.(string); ok {
				if proto, ok := m[s]; ok {
					v = proto
				}
			}
		}
		merged[k] = v
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return args
	}
	return out
}

// ElicitRequestID is the input-request ID under which generated tool handlers
// attach their confirmation elicitation to a CallToolResult, and under which
// the client echoes the answer back in CallToolParams.InputResponses.
const ElicitRequestID = "elicitation"

// ElicitSchema builds the JSON Schema an elicitation form is rendered from.
func ElicitSchema(fields []ElicitField) *jsonschema.Schema {
	props := make(map[string]*jsonschema.Schema, len(fields))
	var required []string
	for _, f := range fields {
		sch := &jsonschema.Schema{Type: f.Type, Description: f.Description}
		if len(f.EnumValues) > 0 {
			for _, v := range f.EnumValues {
				sch.Enum = append(sch.Enum, v)
			}
		}
		props[f.Name] = sch
		if f.Required {
			required = append(required, f.Name)
		}
	}
	return &jsonschema.Schema{
		Type:       "object",
		Properties: props,
		Required:   required,
	}
}

// RunElicitation advances the multi round-trip elicitation handshake
// (SEP-2322) for one tool call, building a form from the given fields.
//
// Protocol version 2026-07-28 forbids a server from issuing an elicitation
// request while it is serving a request. A handler instead returns a result
// carrying an InputRequests map; the client fulfills it and retries the same
// tool call with the answers in Params.InputResponses, so the handler runs
// twice. Clients on older protocol versions never see this: the go-sdk's
// server middleware performs the round trip on their behalf and reinvokes the
// handler, so a handler only needs the one code path.
//
// When err is nil, exactly one of the first two results is non-nil:
//   - result: the client answered. Inspect result.Action; anything other than
//     "accept" means the user declined.
//   - pending: no answer yet. Return it from the handler unchanged to ask the
//     client for input.
func RunElicitation(req *mcp.CallToolRequest, message string, fields []ElicitField) (result *mcp.ElicitResult, pending *mcp.CallToolResult, err error) {
	if req == nil || req.Params == nil {
		return nil, nil, fmt.Errorf("elicitation %q: no tool call request", message)
	}
	if resp, ok := req.Params.InputResponses[ElicitRequestID]; ok {
		res, ok := resp.(*mcp.ElicitResult)
		if !ok {
			return nil, nil, fmt.Errorf("elicitation %q: input response is %T, want *mcp.ElicitResult", message, resp)
		}
		return res, nil, nil
	}
	return nil, &mcp.CallToolResult{
		InputRequests: mcp.InputRequestMap{
			ElicitRequestID: &mcp.ElicitParams{
				Message:         message,
				RequestedSchema: ElicitSchema(fields),
			},
		},
	}, nil
}
