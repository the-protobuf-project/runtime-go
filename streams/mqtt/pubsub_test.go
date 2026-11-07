package mqtt_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

func TestLifecycle(t *testing.T) {
	s := testStreams(t)
	ctx := t.Context()

	created, err := s.Create(ctx, streams.Stream{
		Name: "users", Description: "before", Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == "" {
		t.Fatal("Create did not assign an id")
	}

	if _, uerr := s.Update(ctx, created.ID, streams.Stream{
		Name: "users", Description: "after", Subjects: []string{subject},
	}); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}
	if after, _ := s.Get(ctx, created.ID); after.Description != "after" {
		t.Errorf("Update did not take: %q", after.Description)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("List returned %d streams, want 1", len(list))
	}

	if derr := s.Delete(ctx, created.ID); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}
	if _, gerr := s.Get(ctx, created.ID); !errors.Is(gerr, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", gerr)
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := event{User: "ada", Action: "created"}
	id, err := m.Publish(ctx, subject, want)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := recv(t, ch, 10*time.Second)
	if msg.ID != id {
		t.Errorf("delivered id %q, want the published %q", msg.ID, id)
	}
	var got event
	if derr := msg.Decode(&got); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if got != want {
		t.Errorf("decoded %+v, want %+v", got, want)
	}
}

// A stream may declare a filter; a message still has to land somewhere specific.
func TestWildcardSubjects(t *testing.T) {
	_, m := declare(t, testStreams(t), "user/+")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, "user/+")
	if err != nil {
		t.Fatalf("Subscribe to the filter: %v", err)
	}
	if _, perr := m.Publish(ctx, "user/created", event{User: "ada"}); perr != nil {
		t.Fatalf("Publish to a subject the filter covers: %v", perr)
	}
	recv(t, ch, 10*time.Second)

	if _, perr := m.Publish(ctx, "user/+", event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish to a wildcard = %v, want ErrUnknownSubject", perr)
	}
}

func TestPublishRejectsAnUndeclaredSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := m.Publish(t.Context(), "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish = %v, want ErrUnknownSubject", err)
	}
}

func TestPublishRejectsATTL(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	_, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

func TestConnectRejectsAnEmptyAddress(t *testing.T) {
	if _, err := streamsmqtt.Connect(t.Context(), ""); err == nil {
		t.Error("Connect accepted an empty address")
	}
}

// Connect makes the first connection itself, so an unreachable broker is an
// error at the call rather than a connection retrying in the background.
func TestConnectFailsWhenNothingIsListening(t *testing.T) {
	// A port bound and released, so nothing is listening on it.
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	if _, err := streamsmqtt.Connect(t.Context(), addr); err == nil {
		t.Error("Connect succeeded against a closed broker")
	}
}
