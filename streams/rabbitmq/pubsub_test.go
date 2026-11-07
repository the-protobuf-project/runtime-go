package rabbitmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
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

	select {
	case msg := <-ch:
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
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}
}

// A stream may declare a binding pattern; a message still needs a concrete key.
func TestWildcardSubjects(t *testing.T) {
	_, m := declare(t, testStreams(t), "user.*")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, "user.*")
	if err != nil {
		t.Fatalf("Subscribe to the pattern: %v", err)
	}
	if _, perr := m.Publish(ctx, "user.created", event{User: "ada"}); perr != nil {
		t.Fatalf("Publish to a subject the pattern covers: %v", perr)
	}

	select {
	case msg := <-ch:
		if msg.Subject != "user.created" {
			t.Errorf("Subject = %q, want the concrete routing key", msg.Subject)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}

	if _, perr := m.Publish(ctx, "user.*", event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish to a wildcard = %v, want ErrUnknownSubject", perr)
	}
}
