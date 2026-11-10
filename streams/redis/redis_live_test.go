//go:build live

package redis_test

// The provider against a throwaway plaintext server, for what the untagged
// tests do not already cover: headers, the refused options, delayed
// redelivery, batches, coalesced acks, and a consumer stopping mid-read.
//
//	docker compose -p tpp-streams-live-redis -f ../docker/compose.redis.yaml up -d --wait
//	go test -race -tags live ./redis -run Live
//	docker compose -p tpp-streams-live-redis -f ../docker/compose.redis.yaml down -v
//
// REDIS_LIVE_ADDR moves it (default 127.0.0.1:16380); the tests skip when
// nothing answers there. Every test runs under its own key prefix in database
// 3, so tests share a server without reading each other's streams, and
// nothing is flushed.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
)

// within is how long a live test waits for something that should happen.
const within = 5 * time.Second

func liveAddr() string {
	if addr := os.Getenv("REDIS_LIVE_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:16380"
}

// liveClient is a client on database 3 of the live server, skipping the test
// when there is none. Scheduled delivery needs keyspace expiry events, which
// the compose file turns on; this asks for them too in case the server was
// started some other way.
func liveClient(t *testing.T) *goredis.Client {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: liveAddr(), DB: 3})
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); start docker/compose.redis.yaml", liveAddr(), err)
	}
	if err := rdb.ConfigSet(t.Context(), "notify-keyspace-events", "Ex").Err(); err != nil {
		t.Fatalf("CONFIG SET: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// liveMode is one of the three constructors.
type liveMode func(goredis.UniversalClient, ...streamsredis.Option) streams.Streams

// live returns streams of mode under a fresh prefix, which it also returns,
// with quickReclaim so a redelivery happens within a test.
func live(t *testing.T, mode liveMode, opts ...streamsredis.Option) (streams.Streams, string) {
	t.Helper()
	p := prefix()
	opts = append([]streamsredis.Option{
		streamsredis.WithPrefix(p),
		streamsredis.WithReclaimAfter(quickReclaim),
	}, opts...)
	return mode(liveClient(t), opts...), p
}

// bindStream creates stream as given and binds a manager to it.
func bindStream(t *testing.T, s streams.Streams, stream streams.Stream) (streams.Stream, streams.Manager) {
	t.Helper()
	created, err := s.Create(t.Context(), stream)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return created, m
}

// withDefaults is a stream on subject stamping lang and site on everything.
func withDefaults() streams.Stream {
	return streams.Stream{Name: "headers", Subjects: []string{subject}}.
		WithHeader("lang", "go").
		WithHeader("site", "plant-3")
}

// defaults are the headers withDefaults stamps.
func defaults() streams.Headers { return streams.Headers{"lang": "go", "site": "plant-3"} }

// overridden is what a publish with overrideOpts arrives with.
func overridden() streams.Headers {
	return streams.Headers{"lang": "go", "site": "plant-9", "trace": "t-1"}
}

// overrideOpts overrides site and adds trace on top of the stream's defaults.
func overrideOpts() []streams.Option {
	return []streams.Option{streams.Header("site", "plant-9"), streams.Header("trace", "t-1")}
}

// recvMessage waits for one message or fails the test.
func recvMessage(t *testing.T, ch <-chan streams.Message, wait time.Duration) streams.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("subscription closed before a message arrived")
		}
		return msg
	case <-time.After(wait):
		t.Fatalf("no message within %s", wait)
		return streams.Message{}
	}
}

// silent fails the test if anything arrives on ch within quiet; a closed
// channel also passes.
func silent[T any](t *testing.T, ch <-chan T, quiet time.Duration) {
	t.Helper()
	select {
	case got, ok := <-ch:
		if ok {
			t.Fatalf("expected nothing, received %+v", got)
		}
	case <-time.After(quiet):
	}
}

// eventually polls check every 50ms until it holds, failing after within.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s did not happen in time", what)
}

// sameHeaders fails the test unless got equals want.
func sameHeaders(t *testing.T, what string, got, want streams.Headers) {
	t.Helper()
	if !maps.Equal(got, want) {
		t.Errorf("%s: headers = %v, want %v", what, got, want)
	}
}

// userOf decodes msg as an event and returns its user.
func userOf(t *testing.T, msg streams.Message) string {
	t.Helper()
	var e event
	if err := msg.Decode(&e); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return e.User
}

func asBatch(t *testing.T, m streams.Manager) streams.Batch {
	t.Helper()
	b, err := streams.AsBatch(m)
	if err != nil {
		t.Fatalf("AsBatch: %v", err)
	}
	return b
}

func asDurable(t *testing.T, m streams.Manager) streams.Durable {
	t.Helper()
	d, err := streams.AsDurable(m)
	if err != nil {
		t.Fatalf("AsDurable: %v", err)
	}
	return d
}

// --- lifecycle and modes -----------------------------------------------------

func TestLiveModesKeepSeparateNamespaces(t *testing.T) {
	rdb := liveClient(t)
	p := prefix()
	all := []streams.Streams{
		streamsredis.Use(rdb, streamsredis.WithPrefix(p)),
		streamsredis.UseScheduled(rdb, streamsredis.WithPrefix(p)),
		streamsredis.UseDurable(rdb, streamsredis.WithPrefix(p)),
	}
	for _, s := range all {
		bind(t, s, "s")
	}
	for _, s := range all {
		list, err := s.List(t.Context())
		if err != nil || len(list) != 1 {
			t.Errorf("List = %d streams, %v; want 1", len(list), err)
		}
	}
}

// --- immediate ---------------------------------------------------------------

func TestLiveAGroupIsRefusedRatherThanFannedOut(t *testing.T) {
	for _, mode := range []liveMode{streamsredis.Use, streamsredis.UseScheduled, streamsredis.UseDurable} {
		s, _ := live(t, mode)
		_, m := bind(t, s, subject)
		if _, err := m.Subscribe(t.Context(), subject, streams.Group("workers")); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("Subscribe with a group = %v, want ErrUnsupported", err)
		}
	}
}

// Ending the subscription has to release the server-side subscription with it.
func TestLiveCancelingASubscriptionUnsubscribes(t *testing.T) {
	s, p := live(t, streamsredis.Use)
	stream, m := bind(t, s, subject)
	rdb := liveClient(t)
	channel := fmt.Sprintf("%s:stream:ch:%s:%s", p, stream.ID, subject)
	subscribers := func() int64 {
		n, err := rdb.PubSubNumSub(t.Context(), channel).Result()
		if err != nil {
			t.Fatalf("PUBSUB NUMSUB: %v", err)
		}
		return n[channel]
	}

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := m.Subscribe(ctx, subject); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := subscribers(); got != 1 {
		t.Fatalf("subscribers = %d, want 1", got)
	}
	cancel()
	eventually(t, "the server-side unsubscribe", func() bool { return subscribers() == 0 })
}

func TestLiveAnImmediateBatchIsOnePipeline(t *testing.T) {
	s, _ := live(t, streamsredis.Use)
	_, m := bind(t, s, subject)
	msgs, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b := asBatch(t, m)
	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}})
	if err != nil || len(ids) != 3 {
		t.Fatalf("PublishBatch = %v, %v", ids, err)
	}
	for _, id := range ids {
		if got := recvMessage(t, msgs, within).ID; got != id {
			t.Errorf("delivered %q, want %q in order", got, id)
		}
	}
	if _, err := b.PublishBatch(t.Context(), subject, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("a batch under one id = %v, want ErrUnsupported", err)
	}
	if ids, err := b.PublishBatch(t.Context(), subject, nil); err != nil || len(ids) != 0 {
		t.Errorf("an empty batch = %v, %v", ids, err)
	}
}

func TestLivePubSubHasNoDurableHalf(t *testing.T) {
	s, _ := live(t, streamsredis.Use)
	_, m := bind(t, s, subject)
	if _, err := streams.AsDurable(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsDurable = %v, want ErrUnsupported", err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned = %v, want ErrUnsupported", err)
	}
}

// --- scheduled -----------------------------------------------------------------

// One expiry delivers one message even with two subscribers racing for it.
func TestLiveOneExpiryIsClaimedOnce(t *testing.T) {
	s, _ := live(t, streamsredis.UseScheduled)
	_, m := bind(t, s, "reminder")
	a, err := m.Subscribe(t.Context(), "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := m.Subscribe(t.Context(), "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := m.Publish(t.Context(), "reminder", event{User: "once"}, streams.TTL(200*time.Millisecond)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := 0
	deadline := time.After(within)
	quiet := time.After(within + 700*time.Millisecond)
	for {
		select {
		case <-a:
			got++
		case <-b:
			got++
		case <-deadline:
			if got == 0 {
				t.Fatal("the scheduled message never fired")
			}
			deadline = nil
			continue
		case <-quiet:
			if got != 1 {
				t.Fatalf("one expiry delivered %d times", got)
			}
			return
		}
		if got == 1 {
			deadline, quiet = nil, time.After(700*time.Millisecond)
		}
	}
}

func TestLiveAScheduledBatchFiresWhole(t *testing.T) {
	s, _ := live(t, streamsredis.UseScheduled)
	_, m := bind(t, s, "reminder")
	msgs, err := m.Subscribe(t.Context(), "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ids, err := asBatch(t, m).PublishBatch(t.Context(), "reminder", []any{event{User: "a"}, event{User: "b"}}, streams.TTL(200*time.Millisecond))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		seen[recvMessage(t, msgs, within).ID] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("%s never fired", id)
		}
	}
}

// --- durable -------------------------------------------------------------------

// A nak leaves the message pending, so it comes back with its attempt count
// rising rather than as a fresh copy.
func TestLiveANakedMessageComesBack(t *testing.T) {
	s, _ := live(t, streamsredis.UseDurable)
	_, m := bind(t, s, subject)
	deliveries, err := asDurable(t, m).Consume(t.Context(), subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	id, err := m.Publish(t.Context(), subject, event{User: "retry"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := recvDelivery(t, deliveries, within).Nak(t.Context()); err != nil {
		t.Fatalf("Nak: %v", err)
	}
	second := recvDelivery(t, deliveries, 10*time.Second)
	if second.ID != id || second.Attempt < 2 {
		t.Errorf("redelivered %q at attempt %d, want %q at 2 or more", second.ID, second.Attempt, id)
	}
	_ = second.Ack(t.Context())
}

// With unconfirmed acks an Ack returns before its XACK is answered, and many
// go out as one multi-id XACK; every one still lands, so the group ends with
// nothing pending and a fresh consumer under the name sees nothing again.
func TestLiveUnconfirmedAcksStillSettleEveryMessage(t *testing.T) {
	s, p := live(t, streamsredis.UseDurable, streamsredis.WithConfirmAcks(false))
	stream, m := bind(t, s, subject)
	d := asDurable(t, m)
	ctx, cancel := context.WithCancel(t.Context())
	first, err := d.Consume(ctx, subject, "workers", streams.Prefetch(256))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	const total = 500
	values := make([]any, total)
	for i := range values {
		values[i] = event{User: "u", Action: strconv.Itoa(i)}
	}
	if _, err = asBatch(t, m).PublishBatch(t.Context(), subject, values); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	seen := map[string]bool{}
	for len(seen) < total {
		got := recvDelivery(t, first, within)
		if got.Attempt != 1 {
			t.Fatalf("attempt %d before anything was naked", got.Attempt)
		}
		seen[got.ID] = true
		if err = got.Ack(t.Context()); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	key := fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)
	rdb := liveClient(t)
	eventually(t, "every ack to land", func() bool {
		n, cerr := rdb.XPending(t.Context(), key, "workers").Result()
		return cerr == nil && n.Count == 0
	})
	cancel()

	again, err := d.Consume(t.Context(), subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	silent(t, again, time.Second)
}

// A consumer stopped while its read is blocked must not strand a message
// published in that moment: the read it abandoned would otherwise leave the
// entry pending with a consumer that no longer exists until the reclaim
// interval (30s here) passes.
func TestLiveAMessagePublishedAsAConsumerDropsIsNotStranded(t *testing.T) {
	s, _ := live(t, streamsredis.UseDurable, streamsredis.WithReclaimAfter(30*time.Second))
	for round := range 20 {
		_, m := bind(t, s, subject)
		d := asDurable(t, m)
		ctx, cancel := context.WithCancel(t.Context())
		if _, err := d.Consume(ctx, subject, "workers"); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		// Let the loop reach its blocking read before the consumer goes.
		time.Sleep(20 * time.Millisecond)
		cancel()
		if _, err := m.Publish(t.Context(), subject, event{User: "racing"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		next, err := d.Consume(t.Context(), subject, "workers")
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		select {
		case got, ok := <-next:
			if !ok {
				t.Fatalf("round %d: the consumer ended", round)
			}
			if user := userOf(t, got.Message); user != "racing" {
				t.Fatalf("round %d: got %q", round, user)
			}
			_ = got.Ack(t.Context())
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the message was stranded", round)
		}
	}
}

// Stopping a consumer removes its identity from the group once it holds
// nothing, so the roster does not grow with every restart.
func TestLiveStoppingAConsumerLeavesTheGroup(t *testing.T) {
	s, p := live(t, streamsredis.UseDurable)
	stream, m := bind(t, s, subject)
	key := fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)
	rdb := liveClient(t)
	consumers := func() int {
		got, err := rdb.XInfoConsumers(t.Context(), key, "workers").Result()
		if err != nil {
			return -1
		}
		return len(got)
	}

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := asDurable(t, m).Consume(ctx, subject, "workers"); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	eventually(t, "the consumer to join", func() bool { return consumers() == 1 })
	cancel()
	eventually(t, "the consumer to leave", func() bool { return consumers() == 0 })
}

// The retention cap trims the log, approximately.
func TestLiveMaxLenCapsTheLog(t *testing.T) {
	s, p := live(t, streamsredis.UseDurable, streamsredis.WithMaxLen(10))
	stream, m := bind(t, s, subject)
	b := asBatch(t, m)
	values := make([]any, 100)
	for i := range values {
		values[i] = event{User: "u", Action: strconv.Itoa(i)}
	}
	for range 10 {
		if _, err := b.PublishBatch(t.Context(), subject, values); err != nil {
			t.Fatalf("PublishBatch: %v", err)
		}
	}
	n, err := liveClient(t).XLen(t.Context(), fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)).Result()
	if err != nil || n >= 1000 {
		t.Errorf("the log kept %d entries (%v)", n, err)
	}
}

// --- headers, expiry, retain, delayed nak --------------------------------------

func TestLivePubSubCarriesStreamAndPublishHeaders(t *testing.T) {
	s, _ := live(t, streamsredis.Use)
	_, m := bindStream(t, s, withDefaults())
	msgs, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if _, err := m.Publish(t.Context(), subject, event{User: "a"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := recvMessage(t, msgs, within)
	sameHeaders(t, "stream defaults", got.Headers, defaults())
	if userOf(t, got) != "a" {
		t.Error("the payload did not survive the headers")
	}

	if _, err := m.Publish(t.Context(), subject, event{User: "b"}, overrideOpts()...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	sameHeaders(t, "overridden", recvMessage(t, msgs, within).Headers, overridden())

	if _, err := asBatch(t, m).PublishBatch(t.Context(), subject, []any{event{User: "c"}, event{User: "d"}}, overrideOpts()...); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for range 2 {
		sameHeaders(t, "batch", recvMessage(t, msgs, within).Headers, overridden())
	}
}

// A message without headers is the version-1 frame every reader understands,
// byte for byte; one with headers is version 2.
func TestLivePubSubFramesStayVersion1WithoutHeaders(t *testing.T) {
	s, p := live(t, streamsredis.Use)
	stream, m := bind(t, s, subject)
	raw := liveClient(t).Subscribe(t.Context(), fmt.Sprintf("%s:stream:ch:%s:%s", p, stream.ID, subject))
	defer func() { _ = raw.Close() }()
	if _, err := raw.Receive(t.Context()); err != nil {
		t.Fatalf("SUBSCRIBE: %v", err)
	}
	frame := func() []byte {
		select {
		case msg := <-raw.Channel():
			return []byte(msg.Payload)
		case <-time.After(within):
			t.Fatal("no frame arrived")
			return nil
		}
	}

	payload := event{User: "go"}
	id, err := m.Publish(t.Context(), subject, payload)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	want, err := core.Pack(streams.JSON, id, payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := frame(); !bytes.Equal(got, want) || got[1] != 1 {
		t.Errorf("a header-less frame is %x, want the version-1 frame %x", got, want)
	}

	if _, err := m.Publish(t.Context(), subject, payload, streams.Header("lang", "go")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := frame(); got[1] != 2 {
		t.Errorf("a frame with headers is version %d, want 2", got[1])
	}
}

func TestLiveScheduledCarriesHeaders(t *testing.T) {
	s, _ := live(t, streamsredis.UseScheduled)
	_, m := bindStream(t, s, withDefaults())
	msgs, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	opts := append(overrideOpts(), streams.TTL(200*time.Millisecond))
	if _, err := m.Publish(t.Context(), subject, event{User: "later"}, opts...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := recvMessage(t, msgs, within)
	sameHeaders(t, "scheduled", got.Headers, overridden())
	if userOf(t, got) != "later" {
		t.Error("the payload did not survive the headers")
	}
}

// Durable headers are fields of the entry, beside an unchanged version-1 "msg"
// frame, and every delivery reads them back, a redelivery included.
func TestLiveDurableHeadersAreEntryFields(t *testing.T) {
	s, p := live(t, streamsredis.UseDurable)
	stream, m := bindStream(t, s, withDefaults())
	tail, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	deliveries, err := asDurable(t, m).Consume(t.Context(), subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	id, err := m.Publish(t.Context(), subject, event{User: "d"}, overrideOpts()...)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	key := fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)
	entries, err := liveClient(t).XRange(t.Context(), key, "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("XRANGE = %v, %v", entries, err)
	}
	fields := slices.Sorted(maps.Keys(entries[0].Values))
	if want := []string{"h:lang", "h:site", "h:trace", "msg"}; !slices.Equal(fields, want) {
		t.Errorf("entry fields = %v, want %v", fields, want)
	}
	if v := entries[0].Values["h:site"]; v != "plant-9" {
		t.Errorf("h:site = %v, want plant-9", v)
	}
	if frame := entries[0].Values["msg"].(string); frame[1] != 1 {
		t.Errorf("the payload frame is version %d, want 1", frame[1])
	}

	sameHeaders(t, "tail", recvMessage(t, tail, within).Headers, overridden())

	first := recvDelivery(t, deliveries, within)
	if first.ID != id {
		t.Fatalf("delivered %q, want %q", first.ID, id)
	}
	sameHeaders(t, "first delivery", first.Headers, overridden())
	_ = first.Nak(t.Context())
	again := recvDelivery(t, deliveries, 10*time.Second)
	if again.ID != id || again.Attempt < 2 {
		t.Fatalf("redelivered %q at attempt %d", again.ID, again.Attempt)
	}
	sameHeaders(t, "a redelivery", again.Headers, overridden())
	_ = again.Ack(t.Context())

	if _, err := asBatch(t, m).PublishBatch(t.Context(), subject, []any{event{User: "e"}}); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	next := recvDelivery(t, deliveries, within)
	sameHeaders(t, "batch", next.Headers, defaults())
	_ = next.Ack(t.Context())
}

// Stream headers are stored with the metadata; a stream without them is
// stored with no "headers" field at all.
func TestLiveStreamHeadersAreStoredAndOmittedWhenEmpty(t *testing.T) {
	for _, mode := range []liveMode{streamsredis.Use, streamsredis.UseScheduled, streamsredis.UseDurable} {
		s, _ := live(t, mode)
		created, err := s.Create(t.Context(), withDefaults())
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		read, err := s.Get(t.Context(), created.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		sameHeaders(t, "stored", read.Headers, defaults())
		updated, err := s.Update(t.Context(), created.ID, withDefaults().WithHeader("team", "billing"))
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		reread, err := s.Get(t.Context(), created.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		sameHeaders(t, "updated", reread.Headers, updated.Headers)
		if updated.Headers.Get("team") != "billing" {
			t.Errorf("update lost its header: %v", updated.Headers)
		}
		if err := s.Delete(t.Context(), created.ID); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	s, p := live(t, streamsredis.UseDurable)
	stream, _ := bind(t, s, subject)
	meta, err := liveClient(t).XRange(t.Context(), fmt.Sprintf("%s:durable:meta:%s", p, stream.ID), "-", "+").Result()
	if err != nil || len(meta) != 1 {
		t.Fatalf("XRANGE meta = %v, %v", meta, err)
	}
	if json := meta[0].Values["meta"].(string); strings.Contains(json, "headers") {
		t.Errorf("a stream without headers stored %s", json)
	}
}

// everyMode is the three constructors, each with the publish options it
// otherwise accepts: a scheduled stream needs a TTL.
func everyMode() []struct {
	mode liveMode
	base []streams.Option
} {
	return []struct {
		mode liveMode
		base []streams.Option
	}{
		{streamsredis.Use, nil},
		{streamsredis.UseScheduled, []streams.Option{streams.TTL(200 * time.Millisecond)}},
		{streamsredis.UseDurable, nil},
	}
}

func TestLiveInvalidHeadersAreRefusedWhereTheyAreSet(t *testing.T) {
	invalid := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("%s = %v, want ErrInvalidHeader", what, err)
		}
	}
	for _, c := range everyMode() {
		s, _ := live(t, c.mode)
		_, err := s.Create(t.Context(), streams.Stream{Name: "x", Subjects: []string{subject}}.WithHeader("a:b", "1"))
		invalid("Create with a ':' key", err)
		_, err = s.Create(t.Context(), streams.Stream{Name: "x", Subjects: []string{subject}}.WithHeader("", "1"))
		invalid("Create with an empty key", err)
		stream, m := bind(t, s, subject)
		_, err = s.Update(t.Context(), stream.ID, streams.Stream{Name: "x"}.WithHeader("bad key", "1"))
		invalid("Update", err)
		bad := append(slices.Clone(c.base), streams.Header("ok", "line\nbreak"))
		_, err = m.Publish(t.Context(), subject, event{}, bad...)
		invalid("Publish", err)
		_, err = asBatch(t, m).PublishBatch(t.Context(), subject, []any{event{}}, bad...)
		invalid("PublishBatch", err)
		// Nothing reserved: a header may even be called msg or meta.
		fine := append(slices.Clone(c.base), streams.Header("msg", "fine"))
		if _, err := m.Publish(t.Context(), subject, event{}, fine...); err != nil {
			t.Errorf("a header called msg was refused: %v", err)
		}
	}
}

func TestLiveExpiryAndRetainAreRefusedInEveryMode(t *testing.T) {
	for _, c := range everyMode() {
		s, _ := live(t, c.mode)
		_, m := bind(t, s, subject)
		b := asBatch(t, m)
		for _, opt := range []streams.Option{streams.Expires(5 * time.Second), streams.Retain()} {
			opts := append(slices.Clone(c.base), opt)
			if _, err := m.Publish(t.Context(), subject, event{}, opts...); !errors.Is(err, streams.ErrUnsupported) {
				t.Errorf("Publish = %v, want ErrUnsupported", err)
			}
			if _, err := b.PublishBatch(t.Context(), subject, []any{event{}}, opts...); !errors.Is(err, streams.ErrUnsupported) {
				t.Errorf("PublishBatch = %v, want ErrUnsupported", err)
			}
		}
		// A zero expiry is no expiry, as a zero TTL is no TTL.
		if _, err := m.Publish(t.Context(), subject, event{}, append(slices.Clone(c.base), streams.Expires(0))...); err != nil {
			t.Errorf("a zero expiry was refused: %v", err)
		}
	}
}

// A delayed nak holds the redelivery back for the delay and no longer: well
// before a plain nak's reclaim interval, never before the delay, and with the
// attempt count of one redelivery.
func TestLiveADelayedNakWaitsForItsDelay(t *testing.T) {
	const (
		reclaim = 8 * time.Second
		delay   = 1500 * time.Millisecond
	)
	s, _ := live(t, streamsredis.UseDurable, streamsredis.WithReclaimAfter(reclaim))
	_, m := bindStream(t, s, withDefaults())
	deliveries, err := asDurable(t, m).Consume(t.Context(), subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	id, err := m.Publish(t.Context(), subject, event{User: "busy"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	first := recvDelivery(t, deliveries, within)
	if !first.CanDelayRedelivery() || first.MaxRedeliveryDelay() != reclaim {
		t.Fatalf("CanDelayRedelivery = %v, MaxRedeliveryDelay = %v", first.CanDelayRedelivery(), first.MaxRedeliveryDelay())
	}
	nakedAt := time.Now()
	if err := first.NakAfter(t.Context(), delay); err != nil {
		t.Fatalf("NakAfter: %v", err)
	}

	silent(t, deliveries, delay-100*time.Millisecond)
	again := recvDelivery(t, deliveries, within)
	waited := time.Since(nakedAt)
	if again.ID != id {
		t.Fatalf("redelivered %q, want %q", again.ID, id)
	}
	if waited < delay {
		t.Errorf("redelivered after %v, before the %v delay", waited, delay)
	}
	if waited >= reclaim-2*time.Second {
		t.Errorf("redelivered after %v, no sooner than a plain nak", waited)
	}
	if again.Attempt != 2 {
		t.Errorf("attempt %d, want 2", again.Attempt)
	}
	sameHeaders(t, "a delayed redelivery", again.Headers, defaults())
	_ = again.Ack(t.Context())
}

func TestLiveADelayedNakBeyondTheReclaimIntervalIsRefused(t *testing.T) {
	s, _ := live(t, streamsredis.UseDurable)
	_, m := bind(t, s, subject)
	deliveries, err := asDurable(t, m).Consume(t.Context(), subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, err = m.Publish(t.Context(), subject, event{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := recvDelivery(t, deliveries, within)
	if err = first.NakAfter(t.Context(), 2*quickReclaim); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("NakAfter past the reclaim interval = %v, want ErrUnsupported", err)
	}
	// It stays pending and comes back as a plain nak's would.
	again := recvDelivery(t, deliveries, within)
	if again.Attempt < 2 {
		t.Errorf("attempt %d, want 2 or more", again.Attempt)
	}
	_ = again.Ack(t.Context())

	// With reclaiming off, nothing would redeliver: no delayed nak at all.
	off, _ := live(t, streamsredis.UseDurable, streamsredis.WithReclaimAfter(0))
	_, m = bind(t, off, subject)
	deliveries, err = asDurable(t, m).Consume(t.Context(), subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, err := m.Publish(t.Context(), subject, event{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first = recvDelivery(t, deliveries, within)
	if first.CanDelayRedelivery() {
		t.Error("a delivery with reclaiming off claims it can delay its redelivery")
	}
	if err := first.NakAfter(t.Context(), 10*time.Millisecond); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("NakAfter with reclaiming off = %v, want ErrUnsupported", err)
	}
}
