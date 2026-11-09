package kafka

// These test the pieces that need no broker: naming, the producer pool, the
// commit ledger, and how headers map to record headers. franz-go connects
// lazily, so clients pointed at an unreachable address are built and inspected
// without any network I/O.

import (
	"context"
	"encoding/json"
	"errors"
	"hash/maphash"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
)

// nowhere is an address nothing listens on.
var nowhere = []string{"127.0.0.1:1"}

// store builds a streamStore whose producers point nowhere.
func store(t *testing.T, opts ...Option) *streamStore {
	t.Helper()
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.producers < 1 {
		cfg.producers = 1
	}
	producers, err := buildProducers(nowhere, cfg)
	if err != nil {
		t.Fatalf("buildProducers: %v", err)
	}
	s := &streamStore{seeds: nowhere, cfg: cfg, cl: producers[0], log: telemetry.NoopLogger, producers: producers, keys: maphash.MakeSeed()}
	t.Cleanup(s.closeProducers)
	return s
}

func TestSafeNameMatchesRust(t *testing.T) {
	for in, want := range map[string]string{
		"01J:orders/eu": "01J-orders-eu",
		"a.b-c_D9":      "a.b-c_D9",
		// One dash per character, not per byte.
		"café": "caf-",
	} {
		if got := safeName(in); got != want {
			t.Errorf("safeName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTopicsAreNamedAsRustNamesThem(t *testing.T) {
	plain := &streamStore{}
	if got := plain.topic("01J:orders", "order.placed"); got != "01J-orders.order.placed" {
		t.Errorf("topic = %q", got)
	}
	if got := plain.metaTopic(); got != "streams_meta" {
		t.Errorf("metaTopic = %q", got)
	}
	prefixed := &streamStore{cfg: config{prefix: "ex"}}
	if got := prefixed.topic("s1", "a.b"); got != "ex.s1.a.b" {
		t.Errorf("topic = %q", got)
	}
	spaced := &streamStore{cfg: config{prefix: "my app"}}
	if got := spaced.metaTopic(); got != "my-app_streams_meta" {
		t.Errorf("metaTopic = %q", got)
	}
}

func TestConfigDefaultsToOnePartitionReplicaAndEightProducers(t *testing.T) {
	cfg := defaultConfig()
	if cfg.partitions != 1 || cfg.replicas != 1 || cfg.prefix != "" || cfg.producers != 8 {
		t.Errorf("defaults = partitions %d, replicas %d, prefix %q, producers %d",
			cfg.partitions, cfg.replicas, cfg.prefix, cfg.producers)
	}
}

func TestTheProducerIsDurableUnlessToldOtherwise(t *testing.T) {
	s := store(t)
	p := s.producers[0]
	if acks := p.OptValue(kgo.RequiredAcks); acks != kgo.AllISRAcks() {
		t.Errorf("acks = %v, want all in-sync replicas", acks)
	}
	if off := p.OptValue(kgo.DisableIdempotentWrite); off != false {
		t.Errorf("idempotence disabled = %v, want enabled", off)
	}
	if linger := p.OptValue(kgo.ProducerLinger); linger != time.Millisecond {
		t.Errorf("linger = %v, want 1ms", linger)
	}

	// The caller's own options win.
	own := store(t, WithClientOptions(kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite()))
	if acks := own.producers[0].OptValue(kgo.RequiredAcks); acks != kgo.LeaderAck() {
		t.Errorf("acks = %v, want the caller's leader ack", acks)
	}
}

func TestAKeyAlwaysTakesTheSameProducerAndNoneTakesTurns(t *testing.T) {
	s := store(t)
	if len(s.producers) != 8 {
		t.Fatalf("%d producers, want 8", len(s.producers))
	}
	keyed := s.producer("order-7")
	for range 20 {
		if s.producer("order-7") != keyed {
			t.Fatal("a partition key moved between producers")
		}
	}
	turns := map[*kgo.Client]bool{}
	for range 8 {
		turns[s.producer("")] = true
	}
	if len(turns) != 8 {
		t.Errorf("8 unkeyed publishes used %d producers, want all 8", len(turns))
	}
}

func TestZeroProducersIsOne(t *testing.T) {
	if n := len(store(t, WithProducers(0)).producers); n != 1 {
		t.Errorf("%d producers, want 1", n)
	}
}

func TestExpiryAndRetainAreRefusedByName(t *testing.T) {
	refused := func(opts ...streams.Option) string {
		err := checkOptions(streams.NewOptions(opts...))
		if !errors.Is(err, streams.ErrUnsupported) {
			t.Fatalf("checkOptions = %v, want ErrUnsupported", err)
		}
		return err.Error()
	}
	if why := refused(streams.Expires(5 * time.Second)); !strings.Contains(why, "expiry") {
		t.Errorf("expiry refused as %q", why)
	}
	if why := refused(streams.Retain()); !strings.Contains(why, "compacted") {
		t.Errorf("retain refused as %q", why)
	}
	// A zero expiry is none, as a zero TTL is.
	if err := checkOptions(streams.NewOptions(streams.Expires(0))); err != nil {
		t.Errorf("a zero expiry was refused: %v", err)
	}
}

func TestHeadersMapToRecordHeadersAndBack(t *testing.T) {
	sent := streams.Headers{"traceparent": "00-ab-cd-01", "lang": "go"}
	rec := &kgo.Record{Headers: recordHeaders(sent)}
	if len(rec.Headers) != 2 || rec.Headers[0].Key != "lang" || string(rec.Headers[0].Value) != "go" {
		t.Errorf("record headers = %+v, want key order", rec.Headers)
	}
	if got := received(context.Background(), telemetry.NoopLogger, subjectName, rec); !maps.Equal(got, sent) {
		t.Errorf("received %v, want %v", got, sent)
	}
	if recordHeaders(nil) != nil {
		t.Error("no headers wrote a header block")
	}
	if got := received(context.Background(), telemetry.NoopLogger, subjectName, &kgo.Record{}); len(got) != 0 {
		t.Errorf("a header-less record read as %v", got)
	}
}

func TestAForeignRecordHeaderIsReadAsKafkaReadsIt(t *testing.T) {
	rec := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "site", Value: []byte("plant-3")},
		{Key: "site", Value: []byte("plant-4")},
		{Key: "flag", Value: nil},
		{Key: "binary", Value: []byte{0xff, 0xfe}},
		{Key: string([]byte{0xff}), Value: []byte("x")},
	}}
	got := received(context.Background(), telemetry.NoopLogger, subjectName, rec)
	if got["site"] != "plant-4" {
		t.Errorf("site = %q, want the last value", got["site"])
	}
	if v, ok := got["flag"]; !ok || v != "" {
		t.Errorf("flag = %q, %v; want a null value read as empty", v, ok)
	}
	if _, ok := got["binary"]; ok || len(got) != 2 {
		t.Errorf("non-UTF-8 headers were kept: %v", got)
	}
}

func TestADeliveryCannotDelayItsRedelivery(t *testing.T) {
	c := newCommits("t")
	c.delivered(0, 1)
	d := streams.NewDelivery(streams.Message{ID: "id"}, 0, &kafkaAck{commits: c, offset: 1})
	if d.CanDelayRedelivery() {
		t.Error("a Kafka delivery claims it can delay redelivery")
	}
	if err := d.NakAfter(context.Background(), time.Second); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("NakAfter = %v, want ErrUnsupported", err)
	}
}

func TestADeclarationKeepsItsHeadersAndLeavesNoneOutOfTheJSON(t *testing.T) {
	with, err := json.Marshal(streams.Stream{ID: "s", Headers: streams.Headers{"lang": "go"}})
	if err != nil {
		t.Fatal(err)
	}
	var back streams.Stream
	if err := json.Unmarshal(with, &back); err != nil || back.Headers["lang"] != "go" {
		t.Errorf("headers did not round-trip: %s", with)
	}
	without, _ := json.Marshal(streams.Stream{ID: "s"})
	if strings.Contains(string(without), "headers") {
		t.Errorf("a header-less declaration wrote headers: %s", without)
	}
	for _, key := range []string{`"id"`, `"name"`, `"description"`, `"subjects"`, `"user_id"`, `"active"`} {
		if !strings.Contains(string(without), key) {
			t.Errorf("%s missing from %s", key, without)
		}
	}
}

func TestCreateAndUpdateRefuseABadDefaultHeader(t *testing.T) {
	s := store(t)
	bad := streams.Stream{Name: "n", Subjects: []string{subjectName}}.WithHeader("has space", "x")
	// Refused before any network I/O: the broker here is unreachable.
	if _, err := s.Create(t.Context(), bad); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create = %v, want ErrInvalidHeader", err)
	}
	if _, err := s.Update(t.Context(), "s", bad); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Update = %v, want ErrInvalidHeader", err)
	}
}

func TestPublishAndBatchRefuseExpiryRetainAndBadHeadersBeforeSending(t *testing.T) {
	m := &manager{store: store(t), stream: streams.Stream{ID: "s-1", Subjects: []string{subjectName}}}
	for _, opt := range []streams.Option{streams.Expires(5 * time.Second), streams.Retain()} {
		if _, err := m.Publish(t.Context(), subjectName, "x", opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("Publish = %v, want ErrUnsupported", err)
		}
		if _, err := m.PublishBatch(t.Context(), subjectName, []any{"x"}, opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("PublishBatch = %v, want ErrUnsupported", err)
		}
	}
	bad := streams.Header("lang", "two\nlines")
	if _, err := m.Publish(t.Context(), subjectName, "x", bad); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish = %v, want ErrInvalidHeader", err)
	}
	if _, err := m.PublishBatch(t.Context(), subjectName, []any{"x"}, bad); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("PublishBatch = %v, want ErrInvalidHeader", err)
	}
}

const subjectName = "user.created"

// committed takes what the ledger would commit now, as sorted
// partition/offset pairs.
func committed(c *commits) [][2]int64 {
	offsets, _ := c.take(nil)
	out := make([][2]int64, 0, len(offsets))
	for p, o := range offsets {
		out = append(out, [2]int64{int64(p), o})
	}
	slices.SortFunc(out, func(a, b [2]int64) int { return int(a[0] - b[0]) })
	return out
}

func TestAnAckCommitsTheNextOffset(t *testing.T) {
	c := newCommits("t")
	c.delivered(2, 41)
	c.ack(2, 41)
	if got := committed(c); !slices.Equal(got, [][2]int64{{2, 42}}) {
		t.Errorf("committed %v, want [2 42]", got)
	}
	// Nothing new, nothing to commit.
	if got := committed(c); len(got) != 0 {
		t.Errorf("committed %v again", got)
	}
}

func TestNothingIsCommittedPastAnUnackedDelivery(t *testing.T) {
	c := newCommits("t")
	for offset := int64(10); offset < 13; offset++ {
		c.delivered(0, offset)
	}
	c.delivered(1, 5)
	// Out of order: 11 and 12 are handled, 10 is not.
	c.ack(0, 12)
	c.ack(0, 11)
	c.ack(1, 5)
	if got := committed(c); !slices.Equal(got, [][2]int64{{1, 6}}) {
		t.Errorf("committed %v, want only [1 6]", got)
	}
	// A nak holds the partition where it is: nothing is recorded.
	if got := committed(c); len(got) != 0 {
		t.Errorf("committed %v past a naked delivery", got)
	}
	// Reassigned, the partition is read again from 10.
	for offset := int64(10); offset < 13; offset++ {
		c.delivered(0, offset)
		c.ack(0, offset)
	}
	if got := committed(c); !slices.Equal(got, [][2]int64{{0, 13}}) {
		t.Errorf("committed %v, want [0 13]", got)
	}
}

func TestARewoundPartitionStartsOver(t *testing.T) {
	c := newCommits("t")
	c.delivered(0, 7)
	c.delivered(0, 8)
	// Read again from 7: the old 7 and 8 are being redelivered, not lost.
	c.delivered(0, 7)
	c.ack(0, 7)
	if got := committed(c); !slices.Equal(got, [][2]int64{{0, 8}}) {
		t.Errorf("committed %v, want [0 8]", got)
	}
}

func TestAFailedCommitIsRetried(t *testing.T) {
	c := newCommits("t")
	c.delivered(0, 3)
	c.ack(0, 3)
	offsets, _ := c.take(nil)
	c.retry(offsets)
	if got := committed(c); !slices.Equal(got, [][2]int64{{0, 4}}) {
		t.Errorf("committed %v, want [0 4]", got)
	}
}

func TestCommitsAreLimitedToOwnedPartitions(t *testing.T) {
	c := newCommits("t")
	c.assign([]int32{0, 1})
	c.delivered(0, 1)
	c.delivered(1, 1)
	c.ack(0, 1)
	c.ack(1, 1)
	c.revoke([]int32{1})
	offsets, _ := c.take(c.owned())
	if len(offsets) != 1 || offsets[0] != 2 {
		t.Errorf("committed %v, want only the owned partition 0 at 2", offsets)
	}
}

func TestAnAckAfterTheConsumerIsGoneFailsLoudly(t *testing.T) {
	c := newCommits("t")
	c.delivered(0, 7)
	c.close()
	err := (&kafkaAck{commits: c, offset: 7, subject: subjectName}).Ack(context.Background())
	if err == nil || !strings.Contains(err.Error(), "redelivered") {
		t.Errorf("Ack after close = %v, want an error saying it will be redelivered", err)
	}
}
