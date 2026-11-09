package rabbitmq

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

// These need no broker: the mapping between headers and AMQP properties, and
// the names and settings derived from configuration, are pure.

// Headers go out as an AMQP table of strings and come back as they went.
func TestHeadersBecomeLongStringsAndComeBack(t *testing.T) {
	sent := streams.Headers{"lang": "go", "traceparent": "00-ab-cd-01"}
	tbl := table(sent)
	if v, ok := tbl["lang"].(string); !ok || v != "go" {
		t.Errorf("table[lang] = %#v, want the string go", tbl["lang"])
	}
	if got := received(tbl); !reflect.DeepEqual(got, sent) {
		t.Errorf("received = %v, want %v", got, sent)
	}
}

// A publish without headers sends no table, and a delivery without one has
// no headers.
func TestNoHeadersSendNoTable(t *testing.T) {
	if tbl := table(nil); tbl != nil {
		t.Errorf("table(nil) = %v, want nil", tbl)
	}
	if h := received(nil); h != nil {
		t.Errorf("received(nil) = %v, want nil", h)
	}
}

// Only string values the publisher set are headers: the broker's bookkeeping
// and other clients' non-string values are left out.
func TestReceivedKeepsStringsAndDropsTheBrokersBookkeeping(t *testing.T) {
	got := received(amqp.Table{
		"lang":                "go",
		"site":                "p3",
		"x-delivery-count":    int64(2),
		"x-first-death-queue": "q",
		"CC":                  "k",
		"count":               int32(3),
		"binary":              []byte{0xff, 0xfe},
	})
	want := streams.Headers{"lang": "go", "site": "p3"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("received = %v, want %v", got, want)
	}
}

// RabbitMQ's own names are refused; only the exact names and the lower-case
// prefix mean anything to the broker.
func TestReservedNamesAreRefused(t *testing.T) {
	for _, key := range []string{"x-delivery-count", "x-death", "x-delay", "CC", "BCC"} {
		if err := checkHeaders(streams.Headers{key: "v"}); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("checkHeaders(%q) = %v, want ErrInvalidHeader", key, err)
		}
	}
	if err := checkHeaders(streams.Headers{"X-Request-Id": "1", "cc": "x", "CCX": "y"}); err != nil {
		t.Errorf("checkHeaders refused ordinary keys: %v", err)
	}
	long := strings.Repeat("k", 256)
	if err := checkHeaders(streams.Headers{long: "v"}); !errors.Is(err, streams.ErrInvalidHeader) || !strings.Contains(err.Error(), "255") {
		t.Errorf("checkHeaders(256-byte key) = %v, want ErrInvalidHeader naming 255", err)
	}
	if err := checkHeaders(streams.Headers{strings.Repeat("k", 255): "v"}); err != nil {
		t.Errorf("checkHeaders(255-byte key) = %v", err)
	}
}

// A stream's default headers are validated and checked for RabbitMQ's names.
func TestDefaultHeadersAreChecked(t *testing.T) {
	if err := checkStream(streams.Stream{Name: "s"}.WithHeader("lang", "go")); err != nil {
		t.Errorf("checkStream(lang) = %v", err)
	}
	for _, key := range []string{"x-death", "CC", "a b"} {
		err := checkStream(streams.Stream{Name: "s"}.WithHeader(key, "v"))
		if !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("checkStream(%q) = %v, want ErrInvalidHeader", key, err)
		}
	}
}

// Expiry is whole milliseconds rounded up, and refused past RabbitMQ's range.
func TestExpirationIsWholeMillisecondsRoundedUp(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{5 * time.Second, "5000"},
		{time.Microsecond, "1"},
		{1500 * time.Microsecond, "2"},
		{math.MaxUint32 * time.Millisecond, strconv.FormatUint(math.MaxUint32, 10)},
	}
	for _, c := range cases {
		if got, err := expiration(c.in); err != nil || got != c.want {
			t.Errorf("expiration(%s) = %q, %v, want %q", c.in, got, err, c.want)
		}
	}
	if _, err := expiration((math.MaxUint32 + 1) * time.Millisecond); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("expiration past the limit = %v, want ErrUnsupported", err)
	}
}

// The attempt comes from a quorum queue's count of any integer width, then
// from the redelivered flag.
func TestAttemptReadsTheQuorumCountThenTheFlag(t *testing.T) {
	cases := []struct {
		redelivered bool
		count       any
		want        int
	}{
		{false, nil, 1},
		{true, nil, 2},
		{true, int64(3), 4},
		{false, int32(0), 1},
		{true, uint8(1), 2},
		{true, int16(5), 6},
		{true, "x", 2},
		{true, int64(-1), 2},
	}
	for _, c := range cases {
		d := amqp.Delivery{Redelivered: c.redelivered}
		if c.count != nil {
			d.Headers = amqp.Table{"x-delivery-count": c.count}
		}
		if got := attempt(&d); got != c.want {
			t.Errorf("attempt(redelivered %v, count %#v) = %d, want %d", c.redelivered, c.count, got, c.want)
		}
	}
}

// A call's prefetch overrides the store's, zero or less falls back to it, and
// more than AMQP can say is clamped.
func TestQosFallsBackAndClamps(t *testing.T) {
	cases := []struct{ call, want int }{{0, 32}, {-1, 32}, {5, 5}, {1 << 20, math.MaxUint16}}
	for _, c := range cases {
		if got := qos(c.call, 32); got != c.want {
			t.Errorf("qos(%d, 32) = %d, want %d", c.call, got, c.want)
		}
	}
}

// A name over 255 bytes is refused rather than sent half-written.
func TestOverlongNamesAreRefused(t *testing.T) {
	if err := short("exchange name", strings.Repeat("x", 255)); err != nil {
		t.Errorf("short(255) = %v", err)
	}
	if err := short("exchange name", strings.Repeat("x", 256)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("short(256) = %v, want ErrUnsupported", err)
	}
}

// Exchange and queue names agree with runtime-rs's, so the two share streams
// and consumers.
func TestNamesMatchRuntimeRs(t *testing.T) {
	s := &store{cfg: config{prefix: "app"}}
	if got := s.exchange("s-1"); got != "app.s-1" {
		t.Errorf("exchange = %q", got)
	}
	if got := s.queue("s-1", safeName("billing.eu"), safeName("user.*")); got != "app.s-1.billing-eu.user--" {
		t.Errorf("queue = %q", got)
	}
	bare := &store{}
	if got := bare.queue("s-1", "workers", safeName("job.queued")); got != "s-1.workers.job-queued" {
		t.Errorf("queue without a prefix = %q", got)
	}
}

// A generated id carries the stream's name without its routing structure.
func TestGeneratedIDsCannotChangeTheTopology(t *testing.T) {
	id := safeName(core.NewStreamID("user events.v2"))
	if !strings.HasSuffix(id, ":user-events-v2") || strings.ContainsAny(id, ".*# ") {
		t.Errorf("generated id %q", id)
	}
}

// A declaration is active under its id and keeps the caller's fields,
// headers included, as copies.
func TestDeclarationMarksActiveAndKeepsFields(t *testing.T) {
	in := streams.Stream{ID: "ignored", Name: "n", Subjects: []string{"a.b"}}.WithHeader("lang", "go")
	out := declaration("id-1", in)
	if out.ID != "id-1" || !out.Active || !reflect.DeepEqual(out.Subjects, []string{"a.b"}) || out.Headers.Get("lang") != "go" {
		t.Errorf("declaration = %+v", out)
	}
	in.Headers["lang"] = "changed"
	if out.Headers.Get("lang") != "go" {
		t.Error("the declaration shares the caller's headers")
	}
}

// Single publishes take the publishing channels in turn; zero channels is one.
func TestPublishChannelsAreTakenInTurn(t *testing.T) {
	if cfg := newConfig(); cfg.channels != defaultPublishChannels || cfg.prefetch != defaultPrefetch {
		t.Errorf("defaults: %d channels, prefetch %d", cfg.channels, cfg.prefetch)
	}
	if cfg := newConfig(WithPublishChannels(0)); cfg.channels != 1 {
		t.Errorf("WithPublishChannels(0) = %d channels, want 1", cfg.channels)
	}

	s := newStore(newConfig(WithPublishChannels(4)), false)
	seen := map[int]int{}
	for range 8 {
		seen[s.nextSlot()]++
	}
	if len(seen) != 4 {
		t.Errorf("8 publishes took slots %v, want each of 4 twice", seen)
	}
	one := newStore(newConfig(WithPublishChannels(1)), false)
	for range 3 {
		if one.nextSlot() != 0 {
			t.Error("a single channel is not always slot 0")
		}
	}
}
