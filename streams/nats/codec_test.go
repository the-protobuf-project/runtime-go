package nats_test

// The codec seam, exercised through a real provider rather than only through
// core: a protobuf payload has to survive the whole path — publish, broker,
// deliver, decode.

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	streamsnats "github.com/the-protobuf-project/runtime-go/streams/nats"
)

func TestProtobufPayloadThroughJetStream(t *testing.T) {
	s, err := streamsnats.UseJetStream(testConn(t), streamsnats.WithCodec(protobuf.Codec))
	if err != nil {
		t.Fatalf("UseJetStream: %v", err)
	}
	_, m := declare(t, s, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if _, perr := m.Publish(ctx, subject, wrapperspb.String("ada")); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-ch:
		var got wrapperspb.StringValue
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got.GetValue() != "ada" {
			t.Errorf("decoded %q, want %q", got.GetValue(), "ada")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}
}

// A provider configured for protobuf still reads JSON, because the message
// carries the codec that wrote it. This is what lets one side of a deployment
// switch before the other.
func TestAProtobufProviderStillReadsJSON(t *testing.T) {
	nc := testConn(t)

	writer := streamsnats.Use(nc) // default JSON
	reader := streamsnats.Use(nc, streamsnats.WithCodec(protobuf.Codec))

	const id = "mixed"
	decl := streams.Stream{ID: id, Name: "mixed", Subjects: []string{subject}}
	if _, err := writer.Create(t.Context(), decl); err != nil {
		t.Fatalf("Create (writer): %v", err)
	}
	if _, err := reader.Create(t.Context(), decl); err != nil {
		t.Fatalf("Create (reader): %v", err)
	}

	wm, err := writer.Bind(t.Context(), id)
	if err != nil {
		t.Fatalf("Bind (writer): %v", err)
	}
	rm, err := reader.Bind(t.Context(), id)
	if err != nil {
		t.Fatalf("Bind (reader): %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := rm.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := wm.Publish(ctx, subject, event{User: "ada", Action: "created"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-ch:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got.User != "ada" {
			t.Errorf("decoded %+v, want the JSON message", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a protobuf-configured provider did not read a JSON message")
	}
}

// The batch fallback: NATS has no batch primitive, so this publishes in turn —
// but the contract is uniform, so a caller writes one shape of code.
func TestPublishBatchThroughTheFallback(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)

	b, err := streams.AsBatch(m)
	if err != nil {
		t.Fatalf("AsBatch: %v", err)
	}

	values := []any{
		event{User: "one", Action: "created"},
		event{User: "two", Action: "created"},
		event{User: "three", Action: "created"},
	}
	ids, err := b.PublishBatch(t.Context(), subject, values)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(ids) != len(values) {
		t.Fatalf("got %d ids, want %d", len(ids), len(values))
	}
	for i, id := range ids {
		if id == "" {
			t.Errorf("entry %d has no id", i)
		}
	}
}

// An undeclared subject fails every entry rather than some of them.
func TestPublishBatchFallbackReportsFailures(t *testing.T) {
	_, m := declare(t, jetStream(t), subject)
	b, _ := streams.AsBatch(m)

	ids, err := b.PublishBatch(t.Context(), "typo", []any{event{}, event{}})
	if err == nil {
		t.Fatal("PublishBatch accepted an undeclared subject")
	}
	// The slice is still one entry per value, so a caller can line them up.
	if len(ids) != 2 {
		t.Errorf("got %d ids, want one per value even on failure", len(ids))
	}
}
