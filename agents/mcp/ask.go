// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package mcp

// ask.go is elicitation a service raises: a question only the handler can
// ask, because only it knows what is missing or what is at stake -- "Book Sea
// View Deluxe, 12 to 14 December, for 31,360 rupees?". (mcp.v1.elicitation
// is the static kind, written in the proto.)
//
// The handler returns Ask's error: a gRPC status carrying a
// google.rpc.ErrorInfo whose reason is ELICITATION_REQUIRED and whose
// metadata holds the question and the fields it asks for, as the JSON Schema
// MCP elicitation takes. Every surface sees the same error -- a REST or gRPC
// client reads it with Asked and shows a form or a confirmation -- and a
// generated MCP tool turns it into an elicitation (AskPending), then, when
// the client calls again with the answers, overlays them on the arguments
// (MergeAnswers) and calls the handler again. A field asked for is a field of
// the request, which is how the answer reaches the handler.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The ErrorInfo an Ask error carries.
const (
	AskReason = "ELICITATION_REQUIRED"
	AskDomain = "mcp.the-protobuf-project.org"
)

// askPrefix starts the input-request ID of a question a handler raised; the
// rest is a hash of the question, so each question has its own answer.
const askPrefix = "ask:"

// Ask is the error a handler returns to ask the caller for fields -- each a
// field of its request -- before it can go on: code is how a client that
// cannot ask reads it (InvalidArgument for missing details,
// FailedPrecondition for a consent), message the question.
func Ask(code codes.Code, message string, fields []ElicitField) error {
	schema, err := json.Marshal(ElicitSchema(fields))
	if err != nil {
		return status.Error(codes.Internal, "mcp: ask: "+err.Error())
	}
	st, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
		Reason: AskReason, Domain: AskDomain,
		Metadata: map[string]string{"message": message, "requested_schema": string(schema)},
	})
	if err != nil {
		return status.Error(codes.Internal, "mcp: ask: "+err.Error())
	}
	return st.Err()
}

// Asked is the question err asks, when it is an Ask error.
func Asked(err error) (message string, schema *jsonschema.Schema, ok bool) {
	st, isStatus := status.FromError(err)
	if err == nil || !isStatus {
		return "", nil, false
	}
	for _, d := range st.Details() {
		info, isInfo := d.(*errdetails.ErrorInfo)
		if !isInfo || info.GetReason() != AskReason || info.GetDomain() != AskDomain {
			continue
		}
		schema = &jsonschema.Schema{}
		if json.Unmarshal([]byte(info.GetMetadata()["requested_schema"]), schema) != nil {
			return "", nil, false
		}
		return info.GetMetadata()["message"], schema, true
	}
	return "", nil, false
}

// AskPending is the tool result that puts err's question to the client, when
// err is an Ask error: an elicitation, when the client can answer one; the
// question as a tool error otherwise, for the model to ask in its own words
// and call again with the answer.
func AskPending(req *CallToolRequest, err error) (*CallToolResult, bool) {
	message, schema, ok := Asked(err)
	if !ok {
		return nil, false
	}
	if !canElicit(req) {
		return ErrorResult(message + " (" + strings.Join(names(schema), ", ") + ")"), true
	}
	return &CallToolResult{InputRequests: InputRequestMap{askID(message): &ElicitParams{Message: message, RequestedSchema: schema}}}, true
}

// MergeAnswers overlays on args the answers req carries to questions a
// handler raised. declined is true when the user declined or cancelled one:
// the call should stop, not run on without the answer.
func MergeAnswers(req *CallToolRequest, args json.RawMessage) (merged json.RawMessage, declined bool) {
	merged = args
	if req == nil || req.Params == nil {
		return merged, false
	}
	for id, resp := range req.Params.InputResponses {
		r, ok := resp.(*ElicitResult)
		if !strings.HasPrefix(id, askPrefix) || !ok {
			continue
		}
		if r.Action != "accept" {
			return merged, true
		}
		merged = MergeElicitResult(merged, r.Content, nil)
	}
	return merged, false
}

// canElicit reports whether the client said it answers form elicitations:
// in the request's _meta, as a client of the stateless 2026-07-28 protocol
// does, or in its session's initialize. A stateless server has no initialize
// from an older client, which so cannot be asked.
func canElicit(req *CallToolRequest) bool {
	if req == nil || req.Session == nil {
		return false
	}
	caps := req.ClientCapabilities()
	return caps != nil && caps.Elicitation != nil
}

// askID is the input-request ID of message.
func askID(message string) string {
	sum := sha256.Sum256([]byte(message))
	return askPrefix + hex.EncodeToString(sum[:6])
}

// names are the fields schema asks for, in order.
func names(schema *jsonschema.Schema) []string {
	return slices.Sorted(maps.Keys(schema.Properties))
}
