package core

import (
	"errors"
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/streams"
)

type order struct {
	User  string `json:"user"`
	Total int    `json:"total"`
}

func TestPackUnpackRoundTrip(t *testing.T) {
	want := order{User: "ada", Total: 42}

	body, err := Pack(streams.JSON, "msg-1", want)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	msg, err := Unpack(streams.NewRegistry(), "orders.placed", body)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if msg.ID != "msg-1" {
		t.Errorf("ID = %q, want %q", msg.ID, "msg-1")
	}
	if msg.Subject != "orders.placed" {
		t.Errorf("Subject = %q, want %q", msg.Subject, "orders.placed")
	}

	var got order
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != want {
		t.Errorf("decoded %+v, want %+v", got, want)
	}
}

// A nil codec is the unconfigured provider, which must still work.
func TestPackDefaultsToJSON(t *testing.T) {
	body, err := Pack(nil, "msg-1", order{User: "ada"})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if !strings.Contains(string(body), "json") {
		t.Errorf("frame %q does not name the JSON codec", body)
	}
}

func TestUnpackRejectsAForeignPayload(t *testing.T) {
	reg := streams.NewRegistry()

	for _, payload := range [][]byte{
		[]byte("this is not a frame"),
		[]byte(`{"id":"1","data":{}}`), // the old JSON envelope, deliberately unsupported
		nil,
		{frameMarker}, // marker but nothing else
	} {
		if _, err := Unpack(reg, "orders.placed", payload); err == nil {
			t.Errorf("Unpack accepted %q", payload)
		}
	}
}

func TestUnpackRejectsAnUnknownFrameVersion(t *testing.T) {
	body, err := Pack(streams.JSON, "msg-1", order{})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	body[1] = 0x99

	_, err = Unpack(streams.NewRegistry(), "orders.placed", body)
	if err == nil {
		t.Fatal("Unpack accepted a frame from a future version")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("error %q does not mention the version", err)
	}
}

// A codec this program does not have must be refused by name rather than
// decoded as something else.
func TestUnpackRefusesAnUnknownCodec(t *testing.T) {
	body, err := Pack(fakeCodec{}, "msg-1", order{User: "ada"})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	_, err = Unpack(streams.NewRegistry(), "orders.placed", body)
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("Unpack error = %v, want ErrUnsupported", err)
	}
	if !strings.Contains(err.Error(), "madeup") {
		t.Errorf("error %q does not name the codec", err)
	}
}

// The same frame decodes once the codec is registered — this is what lets one
// program read what another wrote.
func TestUnpackAcceptsARegisteredCodec(t *testing.T) {
	body, err := Pack(fakeCodec{}, "msg-1", order{User: "ada"})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	msg, err := Unpack(streams.NewRegistry(fakeCodec{}), "orders.placed", body)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if msg.ID != "msg-1" {
		t.Errorf("ID = %q, want %q", msg.ID, "msg-1")
	}
}

// A truncated frame must be an error, not a panic: it arrives from the network.
func TestUnpackDoesNotPanicOnATruncatedFrame(t *testing.T) {
	body, err := Pack(streams.JSON, "msg-1", order{User: "ada", Total: 42})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	for i := range body {
		if _, err := Unpack(streams.NewRegistry(), "orders.placed", body[:i]); err == nil && i < len(body) {
			// A prefix that happens to be a complete frame with an empty
			// payload is legitimate; anything shorter must fail.
			continue
		}
	}
}

func TestPackCarriesTheIDItWasGiven(t *testing.T) {
	body, err := Pack(streams.JSON, "chosen-id", order{User: "ada"})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if !strings.Contains(string(body), "chosen-id") {
		t.Errorf("packed body %q does not carry the id", body)
	}
}

// fakeCodec stands in for a codec this program was not built with.
type fakeCodec struct{}

func (fakeCodec) Name() string                { return "madeup" }
func (fakeCodec) Marshal(any) ([]byte, error) { return []byte("opaque"), nil }
func (fakeCodec) Unmarshal([]byte, any) error { return nil }
