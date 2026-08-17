package mcp

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

// basicProgress is an MCPProgress from a schema with neither the progress_token
// oneof nor the meta map — what a proto built against an older published schema
// looks like.
type basicProgress struct {
	progress float64
	message  string
	total    float64
}

func (p basicProgress) GetProgress() float64 { return p.progress }
func (p basicProgress) GetMessage() string   { return p.message }
func (p basicProgress) GetTotal() float64    { return p.total }

// fullProgress additionally carries the token oneof and meta map, as the
// current mcp.v1 schema generates.
type fullProgress struct {
	basicProgress
	tokenString string
	tokenInt    int64
	meta        map[string]*structpb.Struct
}

func (p fullProgress) GetTokenString() string               { return p.tokenString }
func (p fullProgress) GetTokenInt() int64                   { return p.tokenInt }
func (p fullProgress) GetMeta() map[string]*structpb.Struct { return p.meta }

// A schema without the token oneof must fall back to the token from the MCP
// request, or progress notifications would not correlate with anything.
func TestProgressParamsUsesRequestTokenWhenChunkHasNone(t *testing.T) {
	params := progressParams("req-token", basicProgress{progress: 2, message: "working", total: 10})

	if got := params.ProgressToken; got != "req-token" {
		t.Errorf("ProgressToken = %v, want the request's token", got)
	}
	if params.Progress != 2 || params.Total != 10 || params.Message != "working" {
		t.Errorf("params = %+v, want progress 2, total 10, message %q", params, "working")
	}
	if params.Meta != nil {
		t.Errorf("Meta = %v, want nil for a schema with no meta map", params.Meta)
	}
}

// A chunk naming its own stream must win, so one gRPC stream can serve several
// MCP requests.
func TestProgressParamsChunkTokenOverridesRequestToken(t *testing.T) {
	tests := []struct {
		name  string
		chunk fullProgress
		want  any
	}{
		{
			name:  "string token",
			chunk: fullProgress{tokenString: "chunk-token"},
			want:  "chunk-token",
		},
		{
			name:  "int token",
			chunk: fullProgress{tokenInt: 42},
			want:  int64(42),
		},
		{
			name:  "string wins when both are set",
			chunk: fullProgress{tokenString: "chunk-token", tokenInt: 42},
			want:  "chunk-token",
		},
		{
			name:  "unset oneof falls back",
			chunk: fullProgress{},
			want:  "req-token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := progressParams("req-token", tt.chunk).ProgressToken; got != tt.want {
				t.Errorf("ProgressToken = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// meta must reach the client; dropping it was the gap that made the field
// unusable from a gRPC stream.
func TestProgressParamsForwardsMeta(t *testing.T) {
	stage, err := structpb.NewStruct(map[string]any{"name": "upload", "index": 3.0})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	params := progressParams("req-token", fullProgress{meta: map[string]*structpb.Struct{"stage": stage}})

	got, ok := params.Meta["stage"].(map[string]any)
	if !ok {
		t.Fatalf("Meta[stage] = %#v, want a map", params.Meta["stage"])
	}
	if got["name"] != "upload" {
		t.Errorf("Meta[stage][name] = %v, want %q", got["name"], "upload")
	}
	if got["index"] != 3.0 {
		t.Errorf("Meta[stage][index] = %v, want 3", got["index"])
	}
}

// An empty meta map must not produce an empty _meta object on the wire.
func TestProgressParamsOmitsEmptyMeta(t *testing.T) {
	params := progressParams("req-token", fullProgress{meta: map[string]*structpb.Struct{}})
	if params.Meta != nil {
		t.Errorf("Meta = %v, want nil for an empty map", params.Meta)
	}
}

// An unset total and an explicit zero are the same thing to the MCP spec, and
// both must serialize as omitted rather than as a bogus total of 0.
func TestProgressParamsTreatsZeroTotalAsUnknown(t *testing.T) {
	if got := progressParams("req-token", basicProgress{progress: 1}).Total; got != 0 {
		t.Errorf("Total = %v, want 0 (omitted on the wire)", got)
	}
}

// SendProgressFromProto must be a no-op rather than a panic when the call has
// nothing to send to: generated handlers ignore its error.
func TestSendProgressFromProtoNoOpsOnNilInputs(t *testing.T) {
	ctx := context.Background()
	if err := SendProgressFromProto(ctx, nil, "token", basicProgress{}); err != nil {
		t.Errorf("nil session: %v, want nil", err)
	}
	if err := SendProgressFromProto(ctx, nil, nil, basicProgress{}); err != nil {
		t.Errorf("nil token: %v, want nil", err)
	}
	if err := SendProgressFromProto(ctx, nil, "token", nil); err != nil {
		t.Errorf("nil progress: %v, want nil", err)
	}
	if err := SendDoneProgress(ctx, nil, "token", "{}"); err != nil {
		t.Errorf("SendDoneProgress with nil session: %v, want nil", err)
	}
	if err := SendDoneProgress(ctx, nil, nil, "{}"); err != nil {
		t.Errorf("SendDoneProgress with nil token: %v, want nil", err)
	}
}

// The completion signal a client waits on is progress == total; it must not
// depend on the schema, since doneProgress is built here rather than generated.
func TestDoneProgressSignalsCompletion(t *testing.T) {
	params := progressParams("req-token", doneProgress{message: `{"ok":true}`})

	if params.Progress != 1.0 || params.Total != 1.0 {
		t.Errorf("progress/total = %v/%v, want 1/1", params.Progress, params.Total)
	}
	if params.Message != `{"ok":true}` {
		t.Errorf("Message = %q, want the result JSON", params.Message)
	}
	if params.ProgressToken != "req-token" {
		t.Errorf("ProgressToken = %v, want the request's token", params.ProgressToken)
	}
}
