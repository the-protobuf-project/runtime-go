//go:build live

package nats_test

// The NATS providers against a real server, core and JetStream on the same one.
//
// Start it with `docker compose -p tpp-streams-live-nats -f docker/compose.nats.yaml up -d --wait`
// (from the streams module); NATS_URL overrides the default
// nats://127.0.0.1:14222. Every test declares streams under fresh ids and
// subjects, so runs do not see each other's messages and need no cleanup to
// be repeatable. Without a server the tests skip.

import (
	"context"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamsnats "github.com/the-protobuf-project/runtime-go/streams/nats"
)

// within is how long a live test waits for something that should arrive.
const within = 5 * time.Second

// liveConn connects to NATS_URL, skipping the test when nothing answers.
func liveConn(t *testing.T) *gonats.Conn {
	t.Helper()

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:14222"
	}
	nc, err := gonats.Connect(url, gonats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("no NATS at %s (%v); see docker/compose.nats.yaml", url, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// unique is a subject, or id, unique to this run under prefix.
func unique(prefix string) string {
	return prefix + "." + core.NewID()
}

// liveJetStream is a JetStream provider on nc, failing if the server lacks
// JetStream.
func liveJetStream(t *testing.T, nc *gonats.Conn, opts ...streamsnats.Option) streams.Streams {
	t.Helper()

	s, err := streamsnats.UseJetStream(nc, opts...)
	if err != nil {
		t.Fatalf("UseJetStream: %v", err)
	}
	return s
}

// liveDeclare creates a stream over subjects, binds it, and deletes it when the
// test ends.
func liveDeclare(t *testing.T, s streams.Streams, in streams.Stream) (streams.Stream, streams.Manager) {
	t.Helper()

	stream, err := s.Create(t.Context(), in)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = s.Delete(context.WithoutCancel(t.Context()), stream.ID) })
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return stream, m
}

// next receives one item within d, failing on a timeout or a closed channel.
func next[T any](t *testing.T, ch <-chan T, d time.Duration) T {
	t.Helper()

	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("the channel closed before anything arrived")
		}
		return v
	case <-time.After(d):
		t.Fatalf("nothing arrived within %s", d)
	}
	var zero T
	return zero
}

// nothing asserts nothing arrives within d; a closed channel also passes.
func nothing[T any](t *testing.T, ch <-chan T, d time.Duration) {
	t.Helper()

	select {
	case v, ok := <-ch:
		if ok {
			t.Fatalf("something arrived that should not have: %+v", v)
		}
	case <-time.After(d):
	}
}

// merge joins two channels into one, until ctx is done.
func merge[T any](ctx context.Context, a, b <-chan T) <-chan T {
	out := make(chan T)
	for _, ch := range []<-chan T{a, b} {
		go func() {
			for v := range ch {
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return out
}

// ---------------------------------------------------------------- core NATS

func TestLivePlainPublishAndSubscribe(t *testing.T) {
	s := streamsnats.Use(liveConn(t))
	subj := unique(subject)
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})

	ch, err := m.Subscribe(t.Context(), subj)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := m.Publish(t.Context(), subj, event{User: "ada"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := next(t, ch, within)
	var e event
	if got.ID != id || got.Subject != subj || got.Decode(&e) != nil || e.User != "ada" {
		t.Errorf("delivered %q on %q as %+v", got.ID, got.Subject, e)
	}
}

func TestLivePlainWildcardSubjects(t *testing.T) {
	s := streamsnats.Use(liveConn(t))
	root := unique("orders")
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{root + ".*"}})

	ch, _ := m.Subscribe(t.Context(), root+".*")
	if _, err := m.Publish(t.Context(), root+".placed", event{User: "ada"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := next(t, ch, within); got.Subject != root+".placed" {
		t.Errorf("Subject = %q", got.Subject)
	}
	if _, err := m.Publish(t.Context(), root+".*", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish to the wildcard = %v, want ErrUnknownSubject", err)
	}
}

func TestLivePlainQueueGroupSharesTheSubject(t *testing.T) {
	s := streamsnats.Use(liveConn(t), streamsnats.WithQueueGroup("workers"))
	subj := unique(subject)
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})

	first, _ := m.Subscribe(t.Context(), subj)
	second, _ := m.Subscribe(t.Context(), subj)
	for i := range 20 {
		if _, err := m.Publish(t.Context(), subj, event{User: strconv.Itoa(i)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	both := merge(t.Context(), first, second)
	for range 20 {
		next(t, both, within)
	}
	// Shared, not fanned out: twenty published, twenty delivered in total.
	nothing(t, both, 300*time.Millisecond)
}

func TestLivePlainWithoutAGroupFansOut(t *testing.T) {
	s := streamsnats.Use(liveConn(t))
	subj := unique(subject)
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})

	first, _ := m.Subscribe(t.Context(), subj)
	second, _ := m.Subscribe(t.Context(), subj)
	id, _ := m.Publish(t.Context(), subj, event{User: "ada"})
	if next(t, first, within).ID != id || next(t, second, within).ID != id {
		t.Error("a subscriber missed the message")
	}
}

func TestLivePlainDropsAForeignPayloadAndKeepsGoing(t *testing.T) {
	nc := liveConn(t)
	s := streamsnats.Use(nc)
	subj := unique(subject)
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})

	ch, _ := m.Subscribe(t.Context(), subj)
	if err := nc.Publish(subj, []byte("not a frame")); err != nil {
		t.Fatal(err)
	}
	id, _ := m.Publish(t.Context(), subj, event{User: "ada"})
	if got := next(t, ch, within); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
}

func TestLivePlainCancelUnsubscribes(t *testing.T) {
	nc := liveConn(t)
	s := streamsnats.Use(nc)
	subj := unique(subject)
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})

	ctx, cancel := context.WithCancel(t.Context())
	ch, _ := m.Subscribe(ctx, subj)
	cancel()
	for range ch {
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	// With the interest gone the server has no one to route to, which a
	// request with no responders reports at once.
	if _, err := nc.Request(subj, []byte("ping"), time.Second); !errors.Is(err, gonats.ErrNoResponders) {
		t.Errorf("Request = %v, want ErrNoResponders: the canceled subscription still had interest", err)
	}
}

func TestLivePlainBatchAndCapabilities(t *testing.T) {
	s := streamsnats.Use(liveConn(t))
	subj := unique(subject)
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})
	if _, err := streams.AsDurable(m); err == nil {
		t.Error("core NATS claimed to be durable")
	}
	if _, err := streams.AsPositioned(m); err == nil {
		t.Error("core NATS claimed to be positioned")
	}

	ch, _ := m.Subscribe(t.Context(), subj)
	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(t.Context(), subj, []any{event{User: "a"}, event{User: "b"}})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for _, id := range ids {
		if got := next(t, ch, within); got.ID != id {
			t.Errorf("delivered %q, want %q", got.ID, id)
		}
	}
}

// ---------------------------------------------------------------- JetStream

func TestLiveJetStreamLifecycle(t *testing.T) {
	s := liveJetStream(t, liveConn(t))
	ctx := t.Context()
	subj := unique(subject)

	created, err := s.Create(ctx, streams.Stream{Name: "users.v1", Description: "before", Subjects: []string{subj}, UserID: "ada"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// The generated id carries the name, made safe for a JetStream name.
	if !strings.HasSuffix(created.ID, ":users-v1") {
		t.Errorf("id = %q", created.ID)
	}
	got, err := s.Get(ctx, created.ID)
	if err != nil || got.Name != created.Name || got.UserID != "ada" || !got.Active || got.Description != "before" {
		t.Errorf("Get = %+v, %v", got, err)
	}
	updated, err := s.Update(ctx, created.ID, streams.Stream{Name: "users.v1", Description: "after", Subjects: []string{subj}, UserID: "ada"})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got, _ := s.Get(ctx, created.ID); got.Description != "after" {
		t.Errorf("Get after Update = %+v", got)
	}
	list, _ := s.List(ctx)
	if !slices.ContainsFunc(list, func(x streams.Stream) bool { return x.ID == updated.ID }) {
		t.Error("List is missing the stream")
	}

	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, gerr := s.Get(ctx, created.ID)
	_, berr := s.Bind(ctx, created.ID)
	_, uerr := s.Update(ctx, created.ID, updated)
	for _, err := range []error{gerr, berr, s.Delete(ctx, created.ID), uerr} {
		if !errors.Is(err, streams.ErrNotFound) {
			t.Errorf("after Delete: %v, want ErrNotFound", err)
		}
	}
}

func TestLiveJetStreamRefusesWhatItCannotMean(t *testing.T) {
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, liveConn(t)), streams.Stream{Name: "live", Subjects: []string{subj}})

	if _, err := m.Publish(t.Context(), subj, event{}, streams.TTL(time.Second)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v", err)
	}
	d, _ := streams.AsDurable(m)
	if _, err := d.Consume(t.Context(), subj, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v", err)
	}
	if _, err := d.Consume(t.Context(), subj, "workers", streams.Group("g")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a group = %v", err)
	}
}

func TestLiveJetStreamSubscribeStartsAtNew(t *testing.T) {
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, liveConn(t)), streams.Stream{Name: "live", Subjects: []string{subj}})

	if _, err := m.Publish(t.Context(), subj, event{User: "old"}); err != nil {
		t.Fatal(err)
	}
	ch, _ := m.Subscribe(t.Context(), subj)
	id, _ := m.Publish(t.Context(), subj, event{User: "new"})
	if got := next(t, ch, within); got.ID != id {
		t.Errorf("delivered %q, want the new %q", got.ID, id)
	}
}

func TestLiveJetStreamDurableRedeliversANak(t *testing.T) {
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, liveConn(t)), streams.Stream{Name: "live", Subjects: []string{subj}})
	d, _ := streams.AsDurable(m)
	ctx := t.Context()

	ch, _ := d.Consume(ctx, subj, "workers")
	id, _ := m.Publish(ctx, subj, event{User: "ada"})
	first := next(t, ch, within)
	if first.ID != id || first.Attempt != 1 {
		t.Errorf("first: %q attempt %d", first.ID, first.Attempt)
	}
	_ = first.Nak(ctx)
	second := next(t, ch, within)
	if second.ID != id || second.Attempt != 2 {
		t.Errorf("second: %q attempt %d", second.ID, second.Attempt)
	}
	_ = second.Ack(ctx)
	nothing(t, ch, 500*time.Millisecond)
}

func TestLiveJetStreamDurableResumesUnderItsName(t *testing.T) {
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, liveConn(t)), streams.Stream{Name: "live", Subjects: []string{subj}})
	d, _ := streams.AsDurable(m)

	first, cancel := context.WithCancel(t.Context())
	ch, _ := d.Consume(first, subj, "resumer")
	_, _ = m.Publish(first, subj, event{User: "one"})
	_ = next(t, ch, within).Ack(first)
	cancel()

	// Published while nobody is attached: the name, not the process, holds the
	// position, so it arrives on re-attach.
	id, _ := m.Publish(t.Context(), subj, event{User: "two"})
	again, _ := d.Consume(t.Context(), subj, "resumer")
	got := next(t, again, within)
	if got.ID != id {
		t.Errorf("resumed at %q, want %q", got.ID, id)
	}
	_ = got.Ack(t.Context())
}

func TestLiveJetStreamPositionedReplaysFromEarliest(t *testing.T) {
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, liveConn(t)), streams.Stream{Name: "live", Subjects: []string{subj}})

	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(t.Context(), subj, []any{event{User: "a"}, event{User: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := streams.AsPositioned(m)
	ch, _ := p.ConsumeFrom(t.Context(), subj, "replayer", streams.FromEarliest, streams.Prefetch(1))
	for _, id := range ids {
		d := next(t, ch, within)
		if d.ID != id {
			t.Errorf("replayed %q, want %q", d.ID, id)
		}
		_ = d.Ack(t.Context())
	}
}

// A pipelined batch several times its in-flight window, and larger than the
// provider's 4,000 outstanding acks, finishes and is stored in order.
func TestLiveJetStreamALargeBatchLandsInOrder(t *testing.T) {
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, liveConn(t)), streams.Stream{Name: "live", Subjects: []string{subj}})

	values := make([]any, 6000)
	for i := range values {
		values[i] = event{User: strconv.Itoa(i)}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, subj, values)
	if err != nil || len(ids) != len(values) {
		t.Fatalf("PublishBatch: %d ids, %v", len(ids), err)
	}
	p, _ := streams.AsPositioned(m)
	ch, _ := p.ConsumeFrom(t.Context(), subj, "in-order", streams.FromEarliest)
	for i, id := range ids {
		d := next(t, ch, within)
		if d.ID != id {
			t.Fatalf("entry %d arrived as %q, want %q", i, d.ID, id)
		}
		_ = d.Ack(t.Context())
	}
}

func TestLiveJetStreamTerminatesAForeignPayload(t *testing.T) {
	nc := liveConn(t)
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, nc), streams.Stream{Name: "live", Subjects: []string{subj}})
	d, _ := streams.AsDurable(m)

	ch, _ := d.Consume(t.Context(), subj, "strict")
	js, _ := jetstream.New(nc)
	if _, err := js.Publish(t.Context(), subj, []byte("not a frame")); err != nil {
		t.Fatal(err)
	}
	id, _ := m.Publish(t.Context(), subj, event{User: "ada"})
	got := next(t, ch, within)
	if got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
	_ = got.Ack(t.Context())
	nothing(t, ch, 500*time.Millisecond)
}

// What a runtime-rs peer relies on: the id in Nats-Msg-Id, the frame as the
// body, the declaration in runtime-go's metadata keys, and the server
// collapsing a retried publish.
func TestLiveJetStreamIsWireCompatibleWithRust(t *testing.T) {
	nc := liveConn(t)
	subj := unique(subject)
	stream, m := liveDeclare(t, liveJetStream(t, nc), streams.Stream{Name: "live", Subjects: []string{subj}})

	for range 2 {
		if _, err := m.Publish(t.Context(), subj, event{User: "ada"}, streams.ID("retried-1")); err != nil {
			t.Fatal(err)
		}
	}
	js, _ := jetstream.New(nc)
	handle, _ := js.Stream(t.Context(), stream.ID)
	info, _ := handle.Info(t.Context())
	if info.State.Msgs != 1 {
		t.Errorf("stored %d, want the duplicate collapsed", info.State.Msgs)
	}
	if info.Config.Metadata["runtime-go.name"] != "live" || info.Config.Metadata["runtime-go.active"] != "true" {
		t.Errorf("metadata = %v", info.Config.Metadata)
	}
	if _, ok := info.Config.Metadata["runtime-go.headers"]; ok {
		t.Error("a stream without headers wrote the headers key")
	}
	raw, err := handle.GetMsg(t.Context(), info.State.FirstSeq)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Header.Get("Nats-Msg-Id") != "retried-1" || len(raw.Header) != 1 {
		t.Errorf("headers = %v, want only Nats-Msg-Id", raw.Header)
	}
	msg, err := core.Unpack(nil, subj, raw.Data)
	if err != nil || msg.ID != "retried-1" || msg.Headers != nil {
		t.Errorf("frame = %+v, %v", msg, err)
	}
}

// ---------------------------------------------------------------- headers

// liveHeaded is a declaration over subj with default headers lang and site.
func liveHeaded(subj string) streams.Stream {
	return streams.Stream{Name: "headed", Subjects: []string{subj}}.WithHeader("lang", "go").WithHeader("site", "plant-3")
}

// The stream's headers arrive, a publish's own win, and both are NATS headers
// another client reads, outside the payload. A message without any goes out
// as a plain PUB, as a runtime-rs publisher's does.
func TestLivePlainHeadersTravelAsNatsHeaders(t *testing.T) {
	nc := liveConn(t)
	s := streamsnats.Use(nc)
	subj := unique(subject)
	stream, m := liveDeclare(t, s, liveHeaded(subj))
	if got, _ := s.Get(t.Context(), stream.ID); got.Headers.Get("lang") != "go" {
		t.Errorf("Get = %v", got.Headers)
	}
	raw, _ := nc.SubscribeSync(subj)
	ch, _ := m.Subscribe(t.Context(), subj)

	id, _ := m.Publish(t.Context(), subj, event{User: "ada"}, streams.Header("site", "plant-4"), streams.Header("trace", "00-ab"))
	got := next(t, ch, within)
	if got.ID != id || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-4" || got.Headers.Get("trace") != "00-ab" {
		t.Errorf("delivered %q with %v", got.ID, got.Headers)
	}
	native, err := raw.NextMsg(within)
	if err != nil || native.Header.Get("site") != "plant-4" {
		t.Fatalf("native = %v, %v", native, err)
	}
	if framed, _ := core.Unpack(nil, subj, native.Data); framed.Headers != nil {
		t.Error("headers went into the frame")
	}

	_, bare := liveDeclare(t, s, streams.Stream{Name: "bare", Subjects: []string{subj}})
	_, _ = bare.Publish(t.Context(), subj, event{User: "go"})
	if got := next(t, ch, within); got.Headers != nil {
		t.Errorf("a bare publish arrived with %v", got.Headers)
	}
	if native, _ := raw.NextMsg(within); native == nil || native.Header != nil {
		t.Error("a header-less publish was not a plain PUB")
	}

	b, _ := streams.AsBatch(m)
	ids, _ := b.PublishBatch(t.Context(), subj, []any{event{User: "a"}, event{User: "b"}})
	for _, id := range ids {
		if got := next(t, ch, within); got.ID != id || got.Headers.Get("lang") != "go" {
			t.Errorf("batch entry %q arrived as %q with %v", id, got.ID, got.Headers)
		}
	}
}

func TestLivePlainRefusesBadHeadersExpiryAndRetain(t *testing.T) {
	s := streamsnats.Use(liveConn(t))
	subj := unique(subject)
	if _, err := s.Create(t.Context(), liveHeaded(subj).WithHeader("Nats-Msg-Id", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create = %v", err)
	}
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})
	for _, opt := range []streams.Option{streams.Header("Nats-TTL", "1"), streams.Header("a b", "1")} {
		if _, err := m.Publish(t.Context(), subj, event{}, opt); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Publish = %v, want ErrInvalidHeader", err)
		}
	}
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Expires(5*time.Second)); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "expire") {
		t.Errorf("Publish with Expires = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "retain") {
		t.Errorf("Publish with Retain = %v", err)
	}
}

// The stream's headers live in its metadata, so another provider binding it
// stamps the same ones; they arrive on deliveries, redeliveries included, and
// the stored message carries them as NATS headers.
func TestLiveJetStreamHeadersAreStoredAndRedelivered(t *testing.T) {
	nc := liveConn(t)
	s := liveJetStream(t, nc)
	subj := unique(subject)
	stream, _ := liveDeclare(t, s, liveHeaded(subj))
	other := liveJetStream(t, nc)
	if got, _ := other.Get(t.Context(), stream.ID); got.Headers.Get("site") != "plant-3" {
		t.Errorf("another provider read %v", got.Headers)
	}
	m, _ := other.Bind(t.Context(), stream.ID)
	d, _ := streams.AsDurable(m)
	ch, _ := d.Consume(t.Context(), subj, "headed")

	site := streams.Header("site", "plant-4")
	id, _ := m.Publish(t.Context(), subj, event{User: "ada"}, site)
	first := next(t, ch, within)
	if first.ID != id || first.Headers.Get("lang") != "go" || first.Headers.Get("site") != "plant-4" {
		t.Errorf("first %q with %v", first.ID, first.Headers)
	}
	if _, ok := first.Headers["Nats-Msg-Id"]; ok {
		t.Errorf("the server's header leaked: %v", first.Headers)
	}
	_ = first.Nak(t.Context())
	second := next(t, ch, within)
	if second.ID != id || second.Attempt != 2 || second.Headers.Get("site") != "plant-4" {
		t.Errorf("second %q attempt %d with %v", second.ID, second.Attempt, second.Headers)
	}
	_ = second.Ack(t.Context())

	b, _ := streams.AsBatch(m)
	ids, _ := b.PublishBatch(t.Context(), subj, []any{event{User: "a"}, event{User: "b"}}, site)
	for _, id := range ids {
		got := next(t, ch, within)
		if got.ID != id || got.Headers.Get("site") != "plant-4" {
			t.Errorf("batch entry %q arrived as %q with %v", id, got.ID, got.Headers)
		}
		_ = got.Ack(t.Context())
	}

	js, _ := jetstream.New(nc)
	handle, _ := js.Stream(t.Context(), stream.ID)
	raw, err := handle.GetMsg(t.Context(), 1)
	if err != nil || raw.Header.Get("lang") != "go" || raw.Header.Get("Nats-Msg-Id") != id {
		t.Errorf("stored = %v, %v", raw, err)
	}

	// An update replaces the set; one without headers leaves none behind.
	updated, err := s.Update(t.Context(), stream.ID, streams.Stream{Name: "headed", Subjects: []string{subj}})
	if err != nil || updated.Headers != nil {
		t.Errorf("Update = %v, %v", updated.Headers, err)
	}
	if got, _ := s.Get(t.Context(), stream.ID); got.Headers != nil {
		t.Errorf("Get after Update = %v", got.Headers)
	}
	if _, err := s.Update(t.Context(), stream.ID, liveHeaded(subj).WithHeader("Nats-Expected-Stream", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Update with a reserved key = %v", err)
	}
}

func TestLiveJetStreamRefusesBadHeadersAndRetain(t *testing.T) {
	s := liveJetStream(t, liveConn(t))
	subj := unique(subject)
	if _, err := s.Create(t.Context(), liveHeaded(subj).WithHeader("Nats-TTL", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create = %v", err)
	}
	if _, err := s.Create(t.Context(), liveHeaded(subj).WithHeader("", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create with an empty key = %v", err)
	}
	_, m := liveDeclare(t, s, streams.Stream{Name: "live", Subjects: []string{subj}})
	reserved := streams.Header("Nats-Msg-Id", "x")
	if _, err := m.Publish(t.Context(), subj, event{}, reserved); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish = %v", err)
	}
	b, _ := streams.AsBatch(m)
	if _, err := b.PublishBatch(t.Context(), subj, []any{event{}}, reserved); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("PublishBatch = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "retain") {
		t.Errorf("Publish with Retain = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Expires(500*time.Millisecond)); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "whole seconds") {
		t.Errorf("Publish with a sub-second expiry = %v", err)
	}
}

// Nats-TTL: the server removes the message once its expiry passes, so a
// consumer that arrives later never sees it, while one without an expiry
// stays.
func TestLiveJetStreamExpiryRemovesTheMessage(t *testing.T) {
	nc := liveConn(t)
	subj := unique(subject)
	stream, m := liveDeclare(t, liveJetStream(t, nc), streams.Stream{Name: "live", Subjects: []string{subj}})

	gone, err := m.Publish(t.Context(), subj, event{User: "late"}, streams.Expires(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	kept, _ := m.Publish(t.Context(), subj, event{User: "kept"})
	js, _ := jetstream.New(nc)
	handle, _ := js.Stream(t.Context(), stream.ID)
	raw, err := handle.GetMsg(t.Context(), 1)
	if err != nil || raw.Header.Get("Nats-TTL") != "1" {
		t.Errorf("stored = %v, %v; want Nats-TTL 1", raw, err)
	}
	deadline := time.Now().Add(within)
	for {
		info, _ := handle.Info(t.Context())
		if info.State.Msgs == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the message did not expire")
		}
		time.Sleep(200 * time.Millisecond)
	}
	p, _ := streams.AsPositioned(m)
	ch, _ := p.ConsumeFrom(t.Context(), subj, "late", streams.FromEarliest)
	d := next(t, ch, within)
	if d.ID == gone || d.ID != kept {
		t.Errorf("delivered %q, want %q", d.ID, kept)
	}
	_ = d.Ack(t.Context())
}

// A delayed nak: the server holds the redelivery back for the delay, rather
// than the client sleeping.
func TestLiveJetStreamNakAfterHoldsTheRedeliveryBack(t *testing.T) {
	subj := unique(subject)
	_, m := liveDeclare(t, liveJetStream(t, liveConn(t)), streams.Stream{Name: "live", Subjects: []string{subj}})
	d, _ := streams.AsDurable(m)
	ch, _ := d.Consume(t.Context(), subj, "patient")
	id, _ := m.Publish(t.Context(), subj, event{User: "ada"})

	first := next(t, ch, within)
	if !first.CanDelayRedelivery() {
		t.Error("JetStream cannot delay a redelivery")
	}
	naked := time.Now()
	if err := first.NakAfter(t.Context(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	nothing(t, ch, 1500*time.Millisecond)
	second := next(t, ch, within)
	if time.Since(naked) < 2*time.Second || second.ID != id || second.Attempt != 2 {
		t.Errorf("redelivered %q attempt %d after %s", second.ID, second.Attempt, time.Since(naked))
	}
	_ = second.Ack(t.Context())
}

// A stream some other tool created without allow_msg_ttl refuses an expiry by
// name, rather than as the server's error, until it is updated through this
// provider.
func TestLiveJetStreamExpiryNeedsAllowMsgTTL(t *testing.T) {
	nc := liveConn(t)
	s := liveJetStream(t, nc)
	subj := unique(subject)
	id := strings.NewReplacer(".", "-").Replace(core.NewStreamID("foreign"))
	js, _ := jetstream.New(nc)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: id, Subjects: []string{subj}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(context.WithoutCancel(t.Context()), id) })

	m, _ := s.Bind(t.Context(), id)
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Expires(5*time.Second)); !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "allow_msg_ttl") {
		t.Errorf("Publish with Expires = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{}); err != nil {
		t.Errorf("Publish: %v", err)
	}
	if _, err := s.Update(t.Context(), id, streams.Stream{Name: "foreign", Subjects: []string{subj}}); err != nil {
		t.Fatal(err)
	}
	m, _ = s.Bind(t.Context(), id)
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Expires(5*time.Second)); err != nil {
		t.Errorf("Publish with Expires after the update: %v", err)
	}
}
