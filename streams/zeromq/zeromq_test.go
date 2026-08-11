package zeromq_test

// ZeroMQ is brokerless, so these need nothing running: the publisher binds a
// socket in this process and the subscriber connects to it.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamszmq "github.com/the-protobuf-project/runtime-go/streams/zeromq"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user.created"

var seq atomic.Int64

// endpoint returns an inproc address unique to one test.
//
// inproc rather than tcp so the tests need no port and cannot collide with
// anything else on the machine.
func endpoint() string {
	return fmt.Sprintf("inproc://streams-test-%d", seq.Add(1))
}

// pair returns a publisher and a subscriber over one endpoint, each with its
// own declaration of the same stream — which is what two processes would have.
func pair(t *testing.T, subjects ...string) (streams.Manager, streams.Manager) {
	t.Helper()

	ep := endpoint()
	ctx := t.Context()

	pub, err := streamszmq.Publish(ctx, ep, streamszmq.WithSettle(150*time.Millisecond))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { _ = pub.(streams.Closer).Close() })

	sub, err := streamszmq.Subscribe(ctx, ep, streamszmq.WithSettle(150*time.Millisecond))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.(streams.Closer).Close() })

	// The same id on both sides, because the id is part of the wire topic and
	// two processes would have agreed on it.
	const id = "s-1"
	decl := streams.Stream{ID: id, Name: "test", Subjects: subjects}

	if _, cerr := pub.Create(ctx, decl); cerr != nil {
		t.Fatalf("Create (publisher): %v", cerr)
	}
	if _, cerr := sub.Create(ctx, decl); cerr != nil {
		t.Fatalf("Create (subscriber): %v", cerr)
	}

	pm, err := pub.Bind(ctx, id)
	if err != nil {
		t.Fatalf("Bind (publisher): %v", err)
	}
	sm, err := sub.Bind(ctx, id)
	if err != nil {
		t.Fatalf("Bind (subscriber): %v", err)
	}
	return pm, sm
}

func TestPublishAndSubscribe(t *testing.T) {
	pm, sm := pair(t, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := sm.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := event{User: "ada", Action: "created"}
	id, err := pm.Publish(ctx, subject, want)
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

// The topic frame is what a SUB socket filters on, so a subscriber must not see
// another subject's messages.
func TestSubjectsAreIsolated(t *testing.T) {
	pm, sm := pair(t, "a", "b")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	onlyA, err := sm.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if _, perr := pm.Publish(ctx, "b", event{User: "for b"}); perr != nil {
		t.Fatalf("Publish to b: %v", perr)
	}
	if _, perr := pm.Publish(ctx, "a", event{User: "for a"}); perr != nil {
		t.Fatalf("Publish to a: %v", perr)
	}

	select {
	case msg := <-onlyA:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got.User != "for a" {
			t.Errorf("received %+v on subject a", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered on a")
	}
}

// PUB/SUB fans out: two subscribers each see every message, which is the
// behavior Group would have to change and cannot.
func TestEverySubscriberSeesEveryMessage(t *testing.T) {
	pm, sm := pair(t, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, err := sm.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe (first): %v", err)
	}
	second, err := sm.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe (second): %v", err)
	}

	if _, perr := pm.Publish(ctx, subject, event{User: "ada"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	for i, ch := range []<-chan streams.Message{first, second} {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("subscriber %d never received the message", i+1)
		}
	}
}

// ZeroMQ keeps nothing, so it must refuse the durable half by name.
func TestZeroMQIsNeitherDurableNorPositioned(t *testing.T) {
	_, sm := pair(t, subject)

	if _, err := streams.AsDurable(sm); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsDurable = %v, want ErrUnsupported", err)
	}
	if _, err := streams.AsPositioned(sm); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned = %v, want ErrUnsupported", err)
	}
}

// A SUB socket cannot send, and refusing beats accepting a message that would
// go nowhere.
func TestASubscriberCannotPublish(t *testing.T) {
	_, sm := pair(t, subject)

	_, err := sm.Publish(t.Context(), subject, event{})
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish on a subscriber = %v, want ErrUnsupported", err)
	}
}

func TestPublishRejectsAnUndeclaredSubject(t *testing.T) {
	pm, _ := pair(t, subject)

	if _, err := pm.Publish(t.Context(), "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish = %v, want ErrUnknownSubject", err)
	}
}

func TestPublishRejectsATTL(t *testing.T) {
	pm, _ := pair(t, subject)

	_, err := pm.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

// Fan-out is the topology, so sharing a subject is not something an option can
// ask for here.
func TestPublishRejectsAGroup(t *testing.T) {
	pm, _ := pair(t, subject)

	_, err := pm.Publish(t.Context(), subject, event{}, streams.Group("workers"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a Group = %v, want ErrUnsupported", err)
	}
}

func TestLifecycle(t *testing.T) {
	ctx := t.Context()

	s, err := streamszmq.Publish(ctx, endpoint())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })

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

func TestConnectRejectsAnEmptyEndpoint(t *testing.T) {
	if _, err := streamszmq.Publish(t.Context(), ""); err == nil {
		t.Error("Publish accepted an empty endpoint")
	}
	if _, err := streamszmq.Subscribe(t.Context(), ""); err == nil {
		t.Error("Subscribe accepted an empty endpoint")
	}
}

// Canceling the context is the only way a subscription ends, and it must close
// the channel rather than leave the goroutine on the socket.
func TestCancelingClosesTheSubscription(t *testing.T) {
	_, sm := pair(t, subject)

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := sm.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cancel()
	select {
	case _, open := <-ch:
		if open {
			// A message already in flight is fine; the close is what matters.
			select {
			case _, stillOpen := <-ch:
				if stillOpen {
					t.Error("the channel stayed open after cancellation")
				}
			case <-time.After(5 * time.Second):
				t.Error("the channel never closed after cancellation")
			}
		}
	case <-time.After(5 * time.Second):
		t.Error("the channel never closed after cancellation")
	}
}
