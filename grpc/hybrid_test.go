package grpc

import (
	"testing"

	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"google.golang.org/protobuf/encoding/protojson"
)

func baseOpts() options.Options {
	return options.Options{
		ServiceName: "test",
		Version:     "0.0.0",
		Environment: options.Development,
	}
}

func TestNewHybridServer_DefaultCamelCase(t *testing.T) {
	s := NewHybridServer(baseOpts())
	if s.restMarshal.UseProtoNames {
		t.Error("expected camelCase (UseProtoNames=false) by default")
	}
	if !s.restMarshal.EmitUnpopulated {
		t.Error("expected EmitUnpopulated=true by default")
	}
	if s.mux == nil {
		t.Error("expected mux to be built")
	}
}

func TestWithRESTSnakeCase(t *testing.T) {
	s := NewHybridServer(baseOpts(), WithRESTSnakeCase())
	if !s.restMarshal.UseProtoNames {
		t.Error("WithRESTSnakeCase should set UseProtoNames=true")
	}
	// EmitUnpopulated default must be preserved.
	if !s.restMarshal.EmitUnpopulated {
		t.Error("WithRESTSnakeCase should not disturb EmitUnpopulated")
	}
}

func TestWithRESTMarshaler_Override(t *testing.T) {
	s := NewHybridServer(baseOpts(), WithRESTMarshaler(
		protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false},
		protojson.UnmarshalOptions{DiscardUnknown: false},
	))
	if !s.restMarshal.UseProtoNames {
		t.Error("override should set UseProtoNames=true")
	}
	if s.restMarshal.EmitUnpopulated {
		t.Error("override should set EmitUnpopulated=false")
	}
	if s.restUnmarshal.DiscardUnknown {
		t.Error("override should set DiscardUnknown=false")
	}
}
