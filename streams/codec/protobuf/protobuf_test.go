package protobuf_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

func TestRoundTrip(t *testing.T) {
	want := wrapperspb.String("hello")

	body, err := protobuf.Codec.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got wrapperspb.StringValue
	if err := protobuf.Codec.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.GetValue() != "hello" {
		t.Errorf("decoded %q, want %q", got.GetValue(), "hello")
	}
}

// The constraint this codec adds is real, so it has to be enforced at the call
// that broke it rather than producing a payload nobody can read.
func TestRejectsAValueThatIsNotAProtoMessage(t *testing.T) {
	_, err := protobuf.Codec.Marshal(struct{ User string }{User: "ada"})
	if err == nil {
		t.Fatal("Marshal accepted a plain struct")
	}
	if !strings.Contains(err.Error(), "proto.Message") {
		t.Errorf("error %q does not say what was required", err)
	}

	if derr := protobuf.Codec.Unmarshal(nil, &struct{}{}); derr == nil {
		t.Error("Unmarshal accepted a destination that is not a proto.Message")
	}
}

// The name travels on the wire, so changing it would orphan every message
// already published.
func TestNameIsStable(t *testing.T) {
	if got := protobuf.Codec.Name(); got != "protobuf" {
		t.Errorf("Name() = %q, want %q — changing this orphans published messages", got, "protobuf")
	}
}

// The point of the seam: a proto payload goes through the same frame every
// provider writes, and comes back out decodable.
func TestThroughTheWireFrame(t *testing.T) {
	body, err := core.Pack(protobuf.Codec, "msg-1", wrapperspb.Int64(42))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	msg, err := core.Unpack(streams.NewRegistry(protobuf.Codec), "counts", body)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if msg.ID != "msg-1" {
		t.Errorf("ID = %q, want %q", msg.ID, "msg-1")
	}

	var got wrapperspb.Int64Value
	if derr := msg.Decode(&got); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if got.GetValue() != 42 {
		t.Errorf("decoded %d, want 42", got.GetValue())
	}
}

// A program that has not been given this codec must refuse the message rather
// than hand back a zero value.
func TestAProgramWithoutTheCodecRefuses(t *testing.T) {
	body, err := core.Pack(protobuf.Codec, "msg-1", wrapperspb.String("hello"))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	if _, err := core.Unpack(streams.NewRegistry(), "counts", body); err == nil {
		t.Fatal("a registry without the protobuf codec decoded a protobuf message")
	}
}

// Proto is the reason to reach for this codec, so the saving should be visible.
func TestProtoIsSmallerThanJSON(t *testing.T) {
	value := wrapperspb.String("a reasonably typical payload value")

	asProto, err := core.Pack(protobuf.Codec, "msg-1", value)
	if err != nil {
		t.Fatalf("Pack (protobuf): %v", err)
	}
	asJSON, err := core.Pack(streams.JSON, "msg-1", value)
	if err != nil {
		t.Fatalf("Pack (json): %v", err)
	}

	if len(asProto) >= len(asJSON) {
		t.Errorf("protobuf frame is %d bytes and JSON is %d; expected protobuf to be smaller",
			len(asProto), len(asJSON))
	}
}
