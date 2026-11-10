package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/the-protobuf-project/runtime-go/streams"
)

func TestPackWithHeadersRoundTrip(t *testing.T) {
	headers := streams.Headers{"lang": "go", "site": "plant-3"}
	body, err := PackWith(streams.JSON, "msg-1", order{User: "ada", Total: 42}, headers)
	if err != nil {
		t.Fatalf("PackWith: %v", err)
	}
	if body[1] != frameVersionHeaders {
		t.Fatalf("version = %d, want %d", body[1], frameVersionHeaders)
	}

	msg, err := Unpack(streams.NewRegistry(), "orders.placed", body)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if msg.Headers.Get("lang") != "go" || msg.Headers.Get("site") != "plant-3" || len(msg.Headers) != 2 {
		t.Errorf("headers = %v, want %v", msg.Headers, headers)
	}
	var got order
	if err := msg.Decode(&got); err != nil || got.User != "ada" {
		t.Fatalf("Decode = %+v, %v", got, err)
	}
}

// Without headers PackWith writes version 1 byte for byte, so a reader that
// knows only version 1 still reads it.
func TestPackWithoutHeadersIsVersionOne(t *testing.T) {
	plain, _ := Pack(streams.JSON, "msg-1", order{User: "ada"})
	with, _ := PackWith(streams.JSON, "msg-1", order{User: "ada"}, streams.Headers{})
	if !bytes.Equal(plain, with) {
		t.Fatalf("PackWith with no headers = %x, want %x", with, plain)
	}
	if plain[1] != frameVersion {
		t.Fatalf("version = %d, want %d", plain[1], frameVersion)
	}
}

// The bytes runtime-rs writes for the same message, so the two runtimes
// read each other's headers.
func TestPackWithMatchesTheRustLayout(t *testing.T) {
	body, err := PackWith(rawCodec{}, "i", []byte("d"), streams.Headers{"b": "2", "a": "1"})
	if err != nil {
		t.Fatalf("PackWith: %v", err)
	}
	want := []byte{0xB7, 0x02, 3, 'r', 'a', 'w', 1, 'i', 2, 1, 'a', 1, '1', 1, 'b', 1, '2', 'd'}
	if !bytes.Equal(body, want) {
		t.Fatalf("frame = %x, want %x (keys in order)", body, want)
	}
}

func TestUnpackRefusesTooManyHeaders(t *testing.T) {
	body := []byte{0xB7, 0x02, 4, 'j', 's', 'o', 'n', 1, 'i', 0xff, 0xff, 0x03}
	if _, err := Unpack(streams.NewRegistry(), "s", body); err == nil {
		t.Fatal("Unpack accepted a frame declaring 65535 headers")
	}
}

func TestOutgoingLaysThePublishOverTheStream(t *testing.T) {
	s := streams.Stream{Name: "telemetry"}.WithHeader("lang", "go").WithHeader("site", "plant-3")
	o := streams.NewOptions(streams.Header("site", "plant-4"), streams.Header("traceparent", "00-ab"))
	got, err := Outgoing(s, o)
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	want := streams.Headers{"lang": "go", "site": "plant-4", "traceparent": "00-ab"}
	if len(got) != len(want) {
		t.Fatalf("Outgoing = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	if s.Headers.Get("site") != "plant-3" {
		t.Error("Outgoing changed the stream's own headers")
	}

	_, err = Outgoing(s, streams.NewOptions(streams.Header("bad key", "x")))
	if !errors.Is(err, streams.ErrInvalidHeader) {
		t.Fatalf("Outgoing with an invalid key = %v, want ErrInvalidHeader", err)
	}
}

// rawCodec writes bytes as they are, so a test can spell out a whole frame.
type rawCodec struct{}

func (rawCodec) Name() string                    { return "raw" }
func (rawCodec) Marshal(v any) ([]byte, error)   { return v.([]byte), nil }
func (rawCodec) Unmarshal(b []byte, v any) error { *(v.(*[]byte)) = b; return nil }
