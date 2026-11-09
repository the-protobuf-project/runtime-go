package zeromq_test

// The cases runtime-rs's tests/zeromq.rs adds: wire layout, prefix isolation,
// close, reconnect, headers, expiry and retain.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamszmq "github.com/the-protobuf-project/runtime-go/streams/zeromq"
)

const (
	streamID = "s-1"
	settle   = 150 * time.Millisecond
	wait     = 10 * time.Second
)

// pairOn returns a publisher bound at ep and a subscriber connected to wherever
// it actually bound, both declaring decl and bound to it.
func pairOn(t *testing.T, ep string, decl streams.Stream) (pub, sub streams.Streams, pm, sm streams.Manager) {
	t.Helper()
	ctx := t.Context()

	pub, err := streamszmq.Publish(ctx, ep, streamszmq.WithSettle(settle))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { _ = pub.(streams.Closer).Close() })
	sub, err = streamszmq.Subscribe(ctx, streamszmq.Endpoint(pub), streamszmq.WithSettle(settle))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.(streams.Closer).Close() })

	for _, s := range []streams.Streams{pub, sub} {
		if _, cerr := s.Create(ctx, decl); cerr != nil {
			t.Fatalf("Create: %v", cerr)
		}
	}
	if pm, err = pub.Bind(ctx, decl.ID); err != nil {
		t.Fatalf("Bind (publisher): %v", err)
	}
	if sm, err = sub.Bind(ctx, decl.ID); err != nil {
		t.Fatalf("Bind (subscriber): %v", err)
	}
	return pub, sub, pm, sm
}

// declaration is stream s-1 over subjects.
func declaration(subjects ...string) streams.Stream {
	return streams.Stream{ID: streamID, Name: "test", Subjects: subjects}
}

// receive waits for one message, failing the test on a timeout or a closed channel.
func receive(t *testing.T, ch <-chan streams.Message) streams.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("the subscription ended")
		}
		return msg
	case <-time.After(wait):
		t.Fatal("nothing was delivered")
		return streams.Message{}
	}
}

// user decodes an event message's user.
func user(t *testing.T, msg streams.Message) string {
	t.Helper()
	var got event
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return got.User
}

func TestEndpointResolvesAnEphemeralPort(t *testing.T) {
	pub, _, _, _ := pairOn(t, "tcp://127.0.0.1:0", declaration(subject))
	ep := streamszmq.Endpoint(pub)
	if ep == "tcp://127.0.0.1:0" || ep == "" {
		t.Fatalf("Endpoint = %q, want the port the system chose", ep)
	}
}

func TestWorksOverTCPAndIPC(t *testing.T) {
	dir, err := os.MkdirTemp("", "zmq")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	for _, ep := range []string{"tcp://127.0.0.1:0", "ipc://" + filepath.Join(dir, "feed.sock")} {
		t.Run(ep[:3], func(t *testing.T) {
			_, _, pm, sm := pairOn(t, ep, declaration(subject))
			ch, err := sm.Subscribe(t.Context(), subject)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			id, err := pm.Publish(t.Context(), subject, event{User: "ada"})
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if got := receive(t, ch); got.ID != id || got.Subject != subject {
				t.Fatalf("got %q on %q, want %q on %q", got.ID, got.Subject, id, subject)
			}
		})
	}
}

// ZeroMQ filters by prefix; a subject that extends another must still not leak
// into it.
func TestALongerSubjectIsNotAMatch(t *testing.T) {
	pm, sm := pair(t, "user", "users")
	ch, err := sm.Subscribe(t.Context(), "user")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := pm.Publish(t.Context(), "users", event{User: "plural"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := pm.Publish(t.Context(), "user", event{User: "singular"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := user(t, receive(t, ch)); got != "singular" {
		t.Fatalf("received %q on user", got)
	}
}

func TestBatchesPublishEachInOrder(t *testing.T) {
	pm, sm := pair(t, subject)
	ch, err := sm.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := streams.AsBatch(pm)
	if err != nil {
		t.Fatalf("AsBatch: %v", err)
	}
	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for _, id := range ids {
		if got := receive(t, ch); got.ID != id {
			t.Fatalf("got %q, want %q", got.ID, id)
		}
	}
	if _, err := b.PublishBatch(t.Context(), subject, []any{event{}}, streams.ID("one")); !errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("PublishBatch with an id = %v, want ErrUnsupported", err)
	}
}

func TestSubscribeRejectsAGroupAndAnUndeclaredSubject(t *testing.T) {
	_, sm := pair(t, subject)
	if _, err := sm.Subscribe(t.Context(), subject, streams.Group("workers")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Subscribe with a Group = %v, want ErrUnsupported", err)
	}
	if _, err := sm.Subscribe(t.Context(), "typo"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe = %v, want ErrUnknownSubject", err)
	}
}

func TestLifecycleRefusesWhatIsGone(t *testing.T) {
	ctx := t.Context()
	s, err := streamszmq.Publish(ctx, endpoint())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	created, err := s.Create(ctx, streams.Stream{Name: "users", Subjects: []string{subject}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created.Active || len(created.ID) < len(":users") || created.ID[len(created.ID)-len(":users"):] != ":users" {
		t.Errorf("Create = %+v, want an active stream with an id ending :users", created)
	}
	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, created.ID); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Delete again = %v, want ErrNotFound", err)
	}
	if _, err := s.Update(ctx, created.ID, streams.Stream{}); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Update after Delete = %v, want ErrNotFound", err)
	}
	c := s.(streams.Closer)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close again: %v", err)
	}
}

// Close is how a provider releases the subscriptions it opened, so a channel
// whose context is still live must close rather than wait forever.
func TestClosingEndsLiveSubscriptions(t *testing.T) {
	pub, sub, pm, sm := pairOn(t, endpoint(), declaration(subject))
	ch, err := sm.Subscribe(context.Background(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := sub.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case _, open := <-ch:
		if open {
			t.Fatal("a message arrived instead of the end")
		}
	case <-time.After(wait):
		t.Fatal("the channel never closed")
	}
	if _, err := sm.Subscribe(t.Context(), subject); err == nil {
		t.Error("Subscribe after Close succeeded")
	}

	if err := pub.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := pm.Publish(t.Context(), subject, event{}); err == nil {
		t.Error("Publish after Close succeeded")
	}
}

// Canceling one subscription closes its socket, and the publisher carries on
// serving the subscriptions that remain.
func TestDroppingASubscriptionLeavesTheOthers(t *testing.T) {
	pm, sm := pair(t, subject)
	dropped, cancel := context.WithCancel(t.Context())
	if _, err := sm.Subscribe(dropped, subject); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	kept, err := sm.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	time.Sleep(settle)

	id, err := pm.Publish(t.Context(), subject, event{User: "ada"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := receive(t, kept); got.ID != id {
		t.Fatalf("got %q, want %q", got.ID, id)
	}
}

// A publisher that goes away and comes back on the same endpoint is found
// again: the subscriber's socket redials it and resends the subscription.
func TestASubscriptionSurvivesAPublisherRestart(t *testing.T) {
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep := fmt.Sprintf("tcp://%s", l.Addr())
	_ = l.Close()

	pub, _, pm, sm := pairOn(t, ep, declaration(subject))
	ch, err := sm.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, serr := pm.Publish(t.Context(), subject, event{User: "before"}); serr != nil {
		t.Fatalf("Publish: %v", serr)
	}
	if got := user(t, receive(t, ch)); got != "before" {
		t.Fatalf("got %q", got)
	}
	if serr := pub.(streams.Closer).Close(); serr != nil {
		t.Fatalf("Close: %v", serr)
	}

	again, err := streamszmq.Publish(t.Context(), ep)
	if err != nil {
		t.Fatalf("Publish again: %v", err)
	}
	t.Cleanup(func() { _ = again.(streams.Closer).Close() })
	if _, serr := again.Create(t.Context(), declaration(subject)); serr != nil {
		t.Fatalf("Create: %v", serr)
	}
	pm2, err := again.Bind(t.Context(), streamID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	// Publish until the redialed subscription reaches the new publisher; one
	// sent before it has arrived is dropped, as any slow joiner's is.
	deadline := time.After(wait)
	for {
		if _, err := pm2.Publish(t.Context(), subject, event{User: "after"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Fatal("the subscription ended with the publisher")
			}
			if got := user(t, msg); got != "after" {
				t.Fatalf("got %q", got)
			}
			return
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("the subscription never found the restarted publisher")
		}
	}
}

// rawSub is a plain SUB socket on ep, subscribed to everything.
func rawSub(t *testing.T, ep string) zmq4.Socket {
	t.Helper()
	raw := zmq4.NewSub(t.Context())
	t.Cleanup(func() { _ = raw.Close() })
	if err := raw.Dial(ep); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := raw.SetOption(zmq4.OptionSubscribe, ""); err != nil {
		t.Fatalf("SetOption: %v", err)
	}
	time.Sleep(settle)
	return raw
}

// rawRecv receives one message from raw within wait.
func rawRecv(t *testing.T, raw zmq4.Socket) zmq4.Msg {
	t.Helper()
	got := make(chan zmq4.Msg, 1)
	go func() {
		if msg, err := raw.Recv(); err == nil {
			got <- msg
		}
	}()
	select {
	case msg := <-got:
		return msg
	case <-time.After(wait):
		t.Fatal("nothing arrived on the raw socket")
		return zmq4.Msg{}
	}
}

// The frames on the wire are a topic frame and a version-1 packed frame, read
// by a plain SUB socket; headers turn the packed frame into version 2.
func TestWireLayout(t *testing.T) {
	pub, _, pm, _ := pairOn(t, "tcp://127.0.0.1:0", declaration(subject))
	raw := rawSub(t, streamszmq.Endpoint(pub))

	value := event{User: "ada"}
	if _, err := pm.Publish(t.Context(), subject, value, streams.ID("msg-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := rawRecv(t, raw)
	want, _ := core.Pack(streams.JSON, "msg-1", value)
	if len(msg.Frames) != 2 || string(msg.Frames[0]) != streamID+"|"+subject || !bytes.Equal(msg.Frames[1], want) {
		t.Fatalf("frames = %q, want [%q %q]", msg.Frames, streamID+"|"+subject, want)
	}

	headers := streams.Headers{"lang": "go"}
	if _, err := pm.Publish(t.Context(), subject, value, streams.ID("msg-1"), streams.WithHeaders(headers)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg = rawRecv(t, raw)
	want, _ = core.PackWith(streams.JSON, "msg-1", value, headers)
	if msg.Frames[1][1] != 2 || !bytes.Equal(msg.Frames[1], want) {
		t.Fatalf("frame = %q, want the version-2 %q", msg.Frames[1], want)
	}
}

// A subscriber reads the frames another publisher sends, and drops a payload
// that was not framed by the contract.
func TestReadsTheWireLayout(t *testing.T) {
	raw := zmq4.NewPub(t.Context())
	t.Cleanup(func() { _ = raw.Close() })
	if err := raw.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	sub, err := streamszmq.Subscribe(t.Context(), "tcp://"+raw.Addr().String(), streamszmq.WithSettle(settle))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.(streams.Closer).Close() })
	if _, serr := sub.Create(t.Context(), declaration(subject)); serr != nil {
		t.Fatalf("Create: %v", serr)
	}
	sm, _ := sub.Bind(t.Context(), streamID)
	ch, err := sm.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	topic := []byte(streamID + "|" + subject)
	if err := raw.SendMulti(zmq4.NewMsgFrom(topic, []byte("not a frame"))); err != nil {
		t.Fatal(err)
	}
	// What runtime-rs's provider::pack("id", json "x") writes.
	if err := raw.SendMulti(zmq4.NewMsgFrom(topic, []byte("\xB7\x01\x04json\x02id\"x\""))); err != nil {
		t.Fatal(err)
	}
	msg := receive(t, ch)
	var got string
	if err := msg.Decode(&got); err != nil || msg.ID != "id" || got != "x" {
		t.Fatalf("got %q %q (%v), want id and x", msg.ID, got, err)
	}
}

func TestStreamAndPublishHeadersArrive(t *testing.T) {
	decl := declaration(subject).WithHeader("lang", "go").WithHeader("site", "plant-3")
	pub, _, pm, sm := pairOn(t, endpoint(), decl)
	if got, _ := pub.Get(t.Context(), streamID); got.Headers.Get("site") != "plant-3" || len(got.Headers) != 2 {
		t.Fatalf("Get = %v, want the declared headers", got.Headers)
	}
	ch, err := sm.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if _, serr := pm.Publish(t.Context(), subject, event{User: "ada"}); serr != nil {
		t.Fatalf("Publish: %v", serr)
	}
	got := receive(t, ch)
	if len(got.Headers) != 2 || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-3" {
		t.Fatalf("headers = %v, want the stream's", got.Headers)
	}
	if user(t, got) != "ada" {
		t.Fatal("wrong value")
	}

	opts := []streams.Option{streams.Header("site", "plant-9"), streams.Header("trace", "t-1")}
	want := streams.Headers{"lang": "go", "site": "plant-9", "trace": "t-1"}
	same := func(h streams.Headers) bool {
		if len(h) != len(want) {
			return false
		}
		for k, v := range want {
			if h.Get(k) != v {
				return false
			}
		}
		return true
	}
	if _, serr := pm.Publish(t.Context(), subject, event{User: "grace"}, opts...); serr != nil {
		t.Fatalf("Publish: %v", serr)
	}
	if got := receive(t, ch); !same(got.Headers) {
		t.Fatalf("headers = %v, want %v", got.Headers, want)
	}

	b, _ := streams.AsBatch(pm)
	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}}, opts...)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for _, id := range ids {
		got := receive(t, ch)
		if got.ID != id || !same(got.Headers) {
			t.Fatalf("got %q with %v, want %q with %v", got.ID, got.Headers, id, want)
		}
	}
}

func TestInvalidHeadersAreRefusedWhereTheyAreSet(t *testing.T) {
	pub, sub, pm, _ := pairOn(t, endpoint(), declaration(subject))
	ctx := t.Context()
	invalid := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("%s = %v, want ErrInvalidHeader", what, err)
		}
	}
	_, err := pub.Create(ctx, streams.Stream{Name: "x", Subjects: []string{subject}}.WithHeader("a:b", "1"))
	invalid("Create", err)
	_, err = sub.Create(ctx, streams.Stream{Name: "x", Subjects: []string{subject}}.WithHeader("", "1"))
	invalid("Create (subscriber)", err)
	_, err = pub.Update(ctx, streamID, streams.Stream{Name: "x"}.WithHeader("bad key", "1"))
	invalid("Update", err)

	bad := streams.Header("k", "line\nbreak")
	_, err = pm.Publish(ctx, subject, event{}, bad)
	invalid("Publish", err)
	b, _ := streams.AsBatch(pm)
	_, err = b.PublishBatch(ctx, subject, []any{event{}}, bad)
	invalid("PublishBatch", err)
}

func TestExpiryAndRetainAreRefused(t *testing.T) {
	pm, _ := pair(t, subject)
	ctx := t.Context()
	b, _ := streams.AsBatch(pm)
	for _, opt := range []streams.Option{streams.Expires(5 * time.Second), streams.Retain()} {
		if _, err := pm.Publish(ctx, subject, event{}, opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("Publish = %v, want ErrUnsupported", err)
		}
		if _, err := b.PublishBatch(ctx, subject, []any{event{}}, opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("PublishBatch = %v, want ErrUnsupported", err)
		}
	}
	// A zero expiry is no expiry.
	if _, err := pm.Publish(ctx, subject, event{}, streams.Expires(0)); err != nil {
		t.Errorf("Publish with a zero expiry: %v", err)
	}
}
