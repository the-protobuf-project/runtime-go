package core

import (
	"strings"
	"testing"
)

type order struct {
	User  string `json:"user"`
	Total int    `json:"total"`
}

func TestPackUnpackRoundTrip(t *testing.T) {
	want := order{User: "ada", Total: 42}

	body, err := Pack("msg-1", want)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	msg, err := Unpack("orders.placed", body)
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

func TestUnpackRejectsMalformedPayload(t *testing.T) {
	if _, err := Unpack("orders.placed", []byte("this is not json")); err == nil {
		t.Fatal("Unpack accepted a payload that is not JSON")
	}
}

func TestUnpackRejectsAPayloadThatIsNotAnEnvelope(t *testing.T) {
	// Valid JSON, wrong shape. Unknown fields are ignored by encoding/json, so
	// without an explicit check this would decode into a message with an empty
	// id and look like a successful delivery.
	_, err := Unpack("orders.placed", []byte(`{"user":"ada","total":42}`))
	if err == nil {
		t.Fatal("Unpack accepted JSON that is not an envelope")
	}
	if !strings.Contains(err.Error(), "orders.placed") {
		t.Errorf("error %q does not name the subject it arrived on", err)
	}
}

func TestPackCarriesTheIDItWasGiven(t *testing.T) {
	body, err := Pack("chosen-id", order{User: "ada"})
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if !strings.Contains(string(body), "chosen-id") {
		t.Errorf("packed body %q does not carry the id", body)
	}
}
