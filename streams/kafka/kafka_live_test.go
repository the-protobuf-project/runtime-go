//go:build live

package kafka_test

// The Kafka provider against a real broker.
//
// Start one from streams/ with
//
//	docker compose -p tpp-streams-live-kafka -f docker/compose.kafka.yaml up -d --wait
//
// then go test -race -tags live ./kafka -run Live. Point it elsewhere with
// KAFKA_ADDR, and at another container for the console-consumer checks with
// KAFKA_CONTAINER. Every test runs under its own topic prefix, so they share a
// broker without reading each other's streams. Tests skip, rather than fail,
// when the broker is not reachable.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamskafka "github.com/the-protobuf-project/runtime-go/streams/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// within bounds every wait on a live broker.
const within = 30 * time.Second

// liveAddr is the plaintext broker, KAFKA_ADDR or the compose default.
func liveAddr() string {
	if addr := os.Getenv("KAFKA_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:19092"
}

// reachable skips the test unless something listens on addr.
func reachable(t testing.TB, addr string) {
	t.Helper()
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Skipf("no broker on %s (%v); start the compose file named in this file's header", addr, err)
	}
	_ = conn.Close()
}

// freshPrefix is a topic prefix no other test uses.
func freshPrefix(kind string) string {
	return kind + "-" + strings.ToLower(core.NewID())
}

// liveProvider connects to the plaintext broker under a fresh prefix and
// returns the provider and that prefix.
func liveProvider(t testing.TB, opts ...streamskafka.Option) (streams.Streams, string) {
	t.Helper()
	reachable(t, liveAddr())
	prefix := freshPrefix("live")
	s, err := streamskafka.Connect(t.Context(), []string{liveAddr()},
		append([]streamskafka.Option{streamskafka.WithPrefix(prefix)}, opts...)...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	return s, prefix
}

// declareWithHeaders creates a stream over subject with default headers.
func declareWithHeaders(t testing.TB, s streams.Streams) (streams.Stream, streams.Manager) {
	t.Helper()
	stream, err := s.Create(t.Context(), streams.Stream{Name: "headers", Subjects: []string{subject}}.
		WithHeaders(streams.Headers{"lang": "go", "site": "plant-3"}))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return stream, m
}

// rawRecord is one record as Kafka holds it.
type rawRecord struct {
	headers []kgo.RecordHeader
	value   []byte
}

// rawRecords reads the first want records of topic's partition 0 from the
// start, with a plain franz-go client and opts.
func rawRecords(t testing.TB, seeds []string, topic string, want int, opts ...kgo.Opt) []rawRecord {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
	}, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), within)
	defer cancel()
	var out []rawRecord
	for len(out) < want {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("only %d raw records", len(out))
		}
		fetches.EachRecord(func(r *kgo.Record) {
			out = append(out, rawRecord{headers: r.Headers, value: r.Value})
		})
	}
	return out
}

// consoleHeaders is what Kafka's own console consumer, run inside container
// on its in-container listener, prints for the headers of the first want
// records on topic.
func consoleHeaders(t testing.TB, container, bootstrap, topic string, want int) []string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "docker", "exec", container,
		"/opt/kafka/bin/kafka-console-consumer.sh",
		"--bootstrap-server", bootstrap, "--topic", topic,
		"--from-beginning", "--max-messages", fmt.Sprint(want),
		"--timeout-ms", "20000",
		"--property", "print.headers=true",
		"--property", "print.value=false",
	).Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("kafka-console-consumer failed: %v: %s", err, exit.Stderr)
		}
		t.Fatalf("kafka-console-consumer: %v", err)
	}
	return strings.Split(strings.TrimRight(string(out), "\n"), "\n")
}

// liveContainer is the plaintext broker's container, for docker exec.
func liveContainer() string {
	if c := os.Getenv("KAFKA_CONTAINER"); c != "" {
		return c
	}
	return "tpp-streams-kafka"
}

func TestLiveLifecycle(t *testing.T) {
	s, _ := liveProvider(t)
	ctx := t.Context()
	created, err := s.Create(ctx, streams.Stream{
		Name: "users", Description: "before", Subjects: []string{subject}, UserID: "u-1",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get(ctx, created.ID)
	if err != nil || got.Name != "users" || got.UserID != "u-1" || got.Description != "before" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	if _, err := s.Update(ctx, created.ID, streams.Stream{
		Name: "users", Description: "after", Subjects: []string{subject},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if after, _ := s.Get(ctx, created.ID); after.Description != "after" {
		t.Errorf("Update did not take: %q", after.Description)
	}
	if list, _ := s.List(ctx); len(list) != 1 {
		t.Errorf("List returned %d streams, want 1", len(list))
	}
	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, created.ID); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestLiveAMissingStreamIsNotFound(t *testing.T) {
	s, _ := liveProvider(t)
	if _, err := s.Get(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
	if _, err := s.Bind(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Bind = %v, want ErrNotFound", err)
	}
}

// The tail is pinned at the end before Subscribe returns, so no sleep.
func TestLivePublishAndSubscribe(t *testing.T) {
	s, _ := liveProvider(t)
	_, m := declare(t, s, subject)
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
	msg := recvMessage(t, ch, within)
	var got event
	if derr := msg.Decode(&got); derr != nil || msg.ID != id || got != want {
		t.Errorf("delivered %q %+v (%v), want %q %+v", msg.ID, got, derr, id, want)
	}
}

func TestLiveConsumeDeliversAndAcknowledges(t *testing.T) {
	s, _ := liveProvider(t)
	_, m := declare(t, s, subject)
	if _, err := m.Publish(t.Context(), subject, event{User: "grace"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, _ := streams.AsPositioned(m)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ch, err := p.ConsumeFrom(ctx, subject, "billing", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	d := recvDelivery(t, ch, within)
	var e event
	if derr := d.Decode(&e); derr != nil || e.User != "grace" {
		t.Errorf("decoded %+v, %v", e, derr)
	}
	if d.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0 where the provider cannot count", d.Attempt)
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// drain waits until a consumer's channel closes, which is after its last
// commit and its leaving the group.
func drain[T any](ch <-chan T) {
	for range ch {
	}
}

// A committed offset is the position that survives the process.
func TestLiveAConsumerResumesWhereTheNameLeftOff(t *testing.T) {
	s, _ := liveProvider(t)
	_, m := declare(t, s, subject)
	for _, who := range []string{"first", "second"} {
		if _, err := m.Publish(t.Context(), subject, event{User: who}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	p, _ := streams.AsPositioned(m)
	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := p.ConsumeFrom(first, subject, "billing", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	if ierr := recvDelivery(t, ch, within).Ack(first); ierr != nil {
		t.Fatalf("Ack: %v", ierr)
	}
	cancelFirst()
	drain(ch)

	d, _ := streams.AsDurable(m)
	again, err := d.Consume(t.Context(), subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	var e event
	_ = recvDelivery(t, again, within).Decode(&e)
	if e.User != "second" {
		t.Errorf("resumed at %+v, want the message after the acknowledged one", e)
	}
}

// Messages sharing a key land on one partition and keep their order.
func TestLiveAPartitionKeyKeepsOrder(t *testing.T) {
	s, _ := liveProvider(t, streamskafka.WithPartitions(4))
	_, m := declare(t, s, subject)
	for _, action := range []string{"one", "two"} {
		if _, err := m.Publish(t.Context(), subject, event{User: "ada", Action: action},
			streams.PartitionKey("ada")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	p, _ := streams.AsPositioned(m)
	ch, err := p.ConsumeFrom(t.Context(), subject, "ordering", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	for _, want := range []string{"one", "two"} {
		d := recvDelivery(t, ch, within)
		var e event
		_ = d.Decode(&e)
		if e.Action != want {
			t.Fatalf("received %q, want %q", e.Action, want)
		}
		_ = d.Ack(t.Context())
	}
}

func TestLiveABatchDeliversEveryEntry(t *testing.T) {
	s, _ := liveProvider(t)
	_, m := declare(t, s, subject)
	b, _ := streams.AsBatch(m)
	const total = 25
	ids, err := b.PublishBatch(t.Context(), subject, batchOf(total))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	unique := map[string]bool{}
	for _, id := range ids {
		unique[id] = true
	}
	if len(unique) != total {
		t.Fatalf("%d unique ids, want %d", len(unique), total)
	}
	p, _ := streams.AsPositioned(m)
	ch, err := p.ConsumeFrom(t.Context(), subject, "batch-reader", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	for range total {
		d := recvDelivery(t, ch, within)
		if !unique[d.ID] {
			t.Errorf("an id nobody published: %q", d.ID)
		}
		_ = d.Ack(t.Context())
	}
}

// Stream defaults arrive, a publish's own win, and they travel as Kafka record
// headers: outside the payload, which stays the version-1 frame, and visible
// to Kafka's own tools.
func TestLiveHeadersTravelAsRecordHeaders(t *testing.T) {
	s, prefix := liveProvider(t)
	stream, m := declareWithHeaders(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	tail, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := m.Publish(ctx, subject, event{User: "ada", Action: "one"},
		streams.Header("site", "plant-4"), streams.Header("traceparent", "00-abc-def-01")); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	want := streams.Headers{"lang": "go", "site": "plant-4", "traceparent": "00-abc-def-01"}
	if got := recvMessage(t, tail, within); !maps.Equal(got.Headers, want) {
		t.Errorf("headers %v, want %v", got.Headers, want)
	}
	if _, perr := m.Publish(ctx, subject, event{User: "ada", Action: "two"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	if got := recvMessage(t, tail, within); !maps.Equal(got.Headers, streams.Headers{"lang": "go", "site": "plant-3"}) {
		t.Errorf("headers %v, want only the stream's defaults", got.Headers)
	}

	topic := prefix + "." + stream.ID + "." + subject
	raw := rawRecords(t, []string{liveAddr()}, topic, 2)
	keys := make([]string, 0, len(raw[0].headers))
	for _, h := range raw[0].headers {
		keys = append(keys, h.Key+"="+string(h.Value))
	}
	if !slices.Equal(keys, []string{"lang=go", "site=plant-4", "traceparent=00-abc-def-01"}) {
		t.Errorf("record headers %v", keys)
	}
	for _, r := range raw {
		if r.value[0] != 0xB7 || r.value[1] != 0x01 {
			t.Errorf("the payload is not a version-1 frame: % x", r.value[:2])
		}
		if strings.Contains(string(r.value), "plant") || strings.Contains(string(r.value), "lang") {
			t.Error("headers leaked into the payload")
		}
	}

	printed := consoleHeaders(t, liveContainer(), "localhost:29092", topic, 2)
	if !slices.Equal(printed, []string{"lang:go,site:plant-4,traceparent:00-abc-def-01", "lang:go,site:plant-3"}) {
		t.Errorf("console consumer printed %q", printed)
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// A message with no headers has no header block on its record.
func TestLiveAMessageWithoutHeadersHasNoRecordHeaders(t *testing.T) {
	s, prefix := liveProvider(t)
	stream, m := declare(t, s, subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tail, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := m.Publish(ctx, subject, event{User: "go", Action: "peer"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recvMessage(t, tail, within); got.ID != id || len(got.Headers) != 0 {
		t.Errorf("delivered %q with %v", got.ID, got.Headers)
	}
	topic := prefix + "." + stream.ID + "." + subject
	if raw := rawRecords(t, []string{liveAddr()}, topic, 1); len(raw[0].headers) != 0 {
		t.Errorf("a header-less publish wrote headers: %v", raw[0].headers)
	}
	if printed := consoleHeaders(t, liveContainer(), "localhost:29092", topic, 1); !slices.Equal(printed, []string{"NO_HEADERS"}) {
		t.Errorf("console consumer printed %q", printed)
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// Every producer in the pool and every record of a batch carries the headers,
// and a durable redelivery brings them back.
func TestLiveTheProducerPoolBatchesAndRedeliveriesCarryHeaders(t *testing.T) {
	s, _ := liveProvider(t)
	stream, m := declareWithHeaders(t, s)

	// Sixteen concurrent unkeyed publishes take turns over all eight producers.
	const singles, batch = 16, 10
	var wg sync.WaitGroup
	errs := make([]error, singles)
	for n := range singles {
		wg.Go(func() {
			_, errs[n] = m.Publish(t.Context(), subject, event{User: "pool", Action: fmt.Sprint(n)},
				streams.Header("n", fmt.Sprint(n)))
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	b, _ := streams.AsBatch(m)
	values := make([]any, batch)
	for n := range values {
		values[n] = event{User: "batch", Action: fmt.Sprint(n)}
	}
	if _, err := b.PublishBatch(t.Context(), subject, values, streams.Header("site", "plant-9")); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}

	p, _ := streams.AsPositioned(m)
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := p.ConsumeFrom(ctx, subject, "headers", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	var held streams.Delivery
	for i := range singles + batch {
		d := recvDelivery(t, ch, within)
		var e event
		_ = d.Decode(&e)
		if d.Headers.Get("lang") != "go" {
			t.Errorf("%+v lost the stream's lang", e)
		}
		if e.User == "pool" {
			if d.Headers.Get("n") != e.Action || d.Headers.Get("site") != "plant-3" {
				t.Errorf("%+v carries %v", e, d.Headers)
			}
		} else if d.Headers.Get("site") != "plant-9" {
			t.Errorf("%+v carries %v", e, d.Headers)
		}
		if d.CanDelayRedelivery() {
			t.Error("a Kafka delivery claims it can delay redelivery")
		}
		// The second, so the first's ack gives the name a committed offset.
		if i == 1 {
			held = d
			if nerr := d.NakAfter(ctx, time.Second); !errors.Is(nerr, streams.ErrUnsupported) {
				t.Errorf("NakAfter = %v, want ErrUnsupported", nerr)
			}
			continue
		}
		if aerr := d.Ack(ctx); aerr != nil {
			t.Fatalf("Ack: %v", aerr)
		}
	}
	cancel()
	drain(ch)

	// The unsettled second record holds the partition: the name reads it
	// again, headers and all.
	d, _ := streams.AsDurable(m)
	again, err := d.Consume(t.Context(), subject, "headers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	redelivered := recvDelivery(t, again, within)
	if redelivered.ID != held.ID || !maps.Equal(redelivered.Headers, held.Headers) {
		t.Errorf("redelivered %q %v, want %q %v", redelivered.ID, redelivered.Headers, held.ID, held.Headers)
	}
	_ = redelivered.Ack(t.Context())
	_ = s.Delete(t.Context(), stream.ID)
}

// Default headers ride in the stored declaration, and a bad one is refused.
func TestLiveStreamHeadersAreStoredAndChecked(t *testing.T) {
	s, _ := liveProvider(t)
	stream, _ := declareWithHeaders(t, s)
	want := streams.Headers{"lang": "go", "site": "plant-3"}
	if !maps.Equal(stream.Headers, want) {
		t.Errorf("Create headers %v", stream.Headers)
	}
	if got, _ := s.Get(t.Context(), stream.ID); !maps.Equal(got.Headers, want) {
		t.Errorf("Get headers %v", got.Headers)
	}
	if list, _ := s.List(t.Context()); len(list) != 1 || !maps.Equal(list[0].Headers, want) {
		t.Errorf("List = %+v", list)
	}
	updated, err := s.Update(t.Context(), stream.ID,
		streams.Stream{Name: "headers", Subjects: []string{subject}}.WithHeader("lang", "rust"))
	if err != nil || !maps.Equal(updated.Headers, streams.Headers{"lang": "rust"}) {
		t.Fatalf("Update = %v, %v", updated.Headers, err)
	}
	for _, bad := range []string{"has space", "has:colon", ""} {
		def := streams.Stream{Name: "bad", Subjects: []string{subject}}.WithHeader(bad, "x")
		if _, err := s.Create(t.Context(), def); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Create %q = %v", bad, err)
		}
		if _, err := s.Update(t.Context(), stream.ID, def); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Update %q = %v", bad, err)
		}
	}
	if got, _ := s.Get(t.Context(), stream.ID); !maps.Equal(got.Headers, updated.Headers) {
		t.Errorf("a refused update changed the headers to %v", got.Headers)
	}
	if list, _ := s.List(t.Context()); len(list) != 1 {
		t.Errorf("a refused create declared something: %d streams", len(list))
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// What Kafka cannot do per message is refused by name, and a bad header is
// refused before anything is sent.
func TestLiveExpiryRetainAndBadHeadersAreRefused(t *testing.T) {
	s, prefix := liveProvider(t)
	stream, m := declare(t, s, subject)
	b, _ := streams.AsBatch(m)
	for says, opt := range map[string]streams.Option{
		"expiry": streams.Expires(5 * time.Second),
		"retain": streams.Retain(),
	} {
		_, perr := m.Publish(t.Context(), subject, event{}, opt)
		_, berr := b.PublishBatch(t.Context(), subject, batchOf(1), opt)
		for _, err := range []error{perr, berr} {
			if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), says) {
				t.Errorf("%s: %v, want ErrUnsupported naming it", says, err)
			}
		}
	}
	for _, bad := range []streams.Option{
		streams.Header("has space", "x"),
		streams.Header("lang", "two\nlines"),
		streams.Header("big", strings.Repeat("v", streams.MaxHeaderValue+1)),
	} {
		_, perr := m.Publish(t.Context(), subject, event{}, bad)
		_, berr := b.PublishBatch(t.Context(), subject, batchOf(1), bad)
		for _, err := range []error{perr, berr} {
			if !errors.Is(err, streams.ErrInvalidHeader) {
				t.Errorf("%v, want ErrInvalidHeader", err)
			}
		}
	}
	// A zero expiry is none, and goes through; it is the only record.
	if _, err := m.Publish(t.Context(), subject, event{Action: "zero"}, streams.Expires(0)); err != nil {
		t.Fatalf("Publish with a zero expiry: %v", err)
	}
	topic := prefix + "." + stream.ID + "." + subject
	if raw := rawRecords(t, []string{liveAddr()}, topic, 1); len(raw) != 1 {
		t.Errorf("%d records, want 1", len(raw))
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// With a group, members share the subject; nothing published after Subscribe
// is lost while the group settles, since a partition the group never
// committed starts at the end noted on Subscribe.
func TestLiveAGroupLosesNothingWhileItSettles(t *testing.T) {
	s, _ := liveProvider(t)
	stream, m := declare(t, s, subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ch, err := m.Subscribe(ctx, subject, streams.Group("settling"))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Published at once, before the group has had time to form.
	want := map[string]bool{}
	for n := range 5 {
		id, perr := m.Publish(ctx, subject, event{Action: fmt.Sprint(n)})
		if perr != nil {
			t.Fatalf("Publish: %v", perr)
		}
		want[id] = true
	}
	for len(want) > 0 {
		delete(want, recvMessage(t, ch, within).ID)
	}
	cancel()
	drain(ch)
	_ = s.Delete(t.Context(), stream.ID)
}
