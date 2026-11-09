package redis

// In-process tests of the pieces that need no server: the key layout, how an
// entry decodes, the delayed-nak arithmetic, and which commands may be sent
// twice.

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

func TestKeysMatchTheLayout(t *testing.T) {
	cases := []struct{ got, want string }{
		{newKeys("app", kindStream).stream("s1"), "app:stream:meta:s1"},
		{newKeys("app", kindStream).streamPattern(), "app:stream:meta:*"},
		{newKeys("app", kindStream).channel("s1", "user.created"), "app:stream:ch:s1:user.created"},
		{newKeys("app", kindDurable).subject("s1", "order.placed"), "app:durable:s:s1:order.placed"},
		{newKeys("app", kindNotify).pending("s1", "reminder", "m1"), "app:notify:pending:s1:reminder:m1"},
		{newKeys("app", kindNotify).pendingPrefix("s1", "reminder"), "app:notify:pending:s1:reminder:"},
		{newKeys("app", kindNotify).payload("m1"), "app:notify:payload:m1"},
		// An empty prefix adds no separator.
		{newKeys("", kindStream).stream("s1"), "stream:meta:s1"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("key = %q, want %q", c.got, c.want)
		}
	}
}

func TestKindsNeverShareAKey(t *testing.T) {
	a := newKeys("p", kindStream).stream("x")
	b := newKeys("p", kindNotify).stream("x")
	c := newKeys("p", kindDurable).stream("x")
	if a == b || b == c || a == c {
		t.Errorf("kinds collide: %q %q %q", a, b, c)
	}
}

func TestKeysRecoverAStreamID(t *testing.T) {
	k := newKeys("p", kindStream)
	id := "01J0000000000000000000000:users"
	if got := k.idFromStream(k.stream(id)); got != id {
		t.Errorf("idFromStream = %q, want %q", got, id)
	}
}

func TestConsumerNamesAreDistinctWithinAProcess(t *testing.T) {
	a, b := consumerName(), consumerName()
	if a == b {
		t.Errorf("two consumer names are both %q", a)
	}
	if !strings.Contains(a, strconv.Itoa(os.Getpid())) {
		t.Errorf("consumer name %q does not carry the pid", a)
	}
}

func TestDecodeEntryReadsThePayloadField(t *testing.T) {
	frame, err := core.Pack(streams.JSON, "m1", "x")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := decodeEntry(nil, "s", goredis.XMessage{ID: "1-0", Values: map[string]any{payloadField: string(frame)}})
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	if msg.ID != "m1" || msg.Headers != nil {
		t.Errorf("decoded %+v", msg)
	}

	if _, err := decodeEntry(nil, "s", goredis.XMessage{ID: "2-0"}); err == nil {
		t.Error("an entry without a payload field decoded")
	}
}

func TestDecodeEntryReadsHeaderFieldsAndNothingElse(t *testing.T) {
	frame, err := core.Pack(streams.JSON, "m1", "x")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := decodeEntry(nil, "s", goredis.XMessage{ID: "1-0", Values: map[string]any{
		payloadField:                   string(frame),
		headerFieldPrefix + "lang":     "go",
		headerFieldPrefix + "msg":      "a header called msg",
		"other":                        "not a header",
		headerFieldPrefix + "not-text": 42,
	}})
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	want := streams.Headers{"lang": "go", "msg": "a header called msg"}
	if len(msg.Headers) != len(want) || msg.Headers["lang"] != "go" || msg.Headers["msg"] != want["msg"] {
		t.Errorf("headers = %v, want %v", msg.Headers, want)
	}
	var got string
	if err := msg.Decode(&got); err != nil || got != "x" {
		t.Errorf("payload = %q (%v), want x", got, err)
	}
}

// A header key may not contain ':', so no header maps onto "msg", "meta", or
// another header's field.
func TestHeaderFieldsCannotCollideWithTheDriverFields(t *testing.T) {
	for _, own := range []string{payloadField, metadataField} {
		if strings.HasPrefix(own, headerFieldPrefix) {
			t.Errorf("driver field %q starts with the header prefix", own)
		}
	}
	if err := (streams.Headers{"h:x": "1"}).Validate(); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("a key with ':' validated: %v", err)
	}
}

func TestADelayedNakSetsBackTheIdleTime(t *testing.T) {
	reclaim := 30 * time.Second
	cases := []struct {
		delay time.Duration
		want  int64
	}{
		{0, 30_000},
		{5 * time.Second, 25_000},
		// Rounded so the delay is never shortened.
		{1500 * time.Microsecond, 29_998},
		{reclaim, 0},
	}
	for _, c := range cases {
		got, err := idleForDelay(reclaim, c.delay)
		if err != nil || got != c.want {
			t.Errorf("idleForDelay(%v, %v) = %d, %v; want %d", reclaim, c.delay, got, err, c.want)
		}
	}
	if _, err := idleForDelay(reclaim, reclaim+time.Millisecond); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("a delay past the reclaim interval = %v, want ErrUnsupported", err)
	}
	if _, err := idleForDelay(0, time.Second); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("a delay with reclaiming off = %v, want ErrUnsupported", err)
	}
}

// A PUBLISH, an XADD of a message, or a scheduled message is never repeated by
// the client, and by sendOnce only when the connection was refused outright.
func TestOnceCommandsAreRepeatedOnlyWhenRefused(t *testing.T) {
	if !newOnce(context.Background(), "PUBLISH", "c", "x").NoRetry() {
		t.Error("a once command lets the client repeat it")
	}

	dropped := []error{io.EOF, io.ErrUnexpectedEOF, syscall.ECONNRESET, syscall.EPIPE}
	for _, err := range dropped {
		if refused(err) {
			t.Errorf("%v counts as refused, so a publish would be sent twice", err)
		}
	}
	answers := []error{
		&net.OpError{Op: "read", Err: syscall.ETIMEDOUT},
		errors.New("NOPERM this user has no permissions"),
		errors.New("WRONGPASS invalid username-password pair"),
	}
	for _, err := range answers {
		if refused(err) {
			t.Errorf("%v counts as refused", err)
		}
	}
	for _, err := range []error{
		&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED},
		syscall.ECONNREFUSED,
	} {
		if !refused(err) {
			t.Errorf("%v does not count as refused", err)
		}
	}
}

func TestDisconnectedTellsAGoneConnectionFromARefusal(t *testing.T) {
	for _, err := range []error{
		io.EOF, syscall.ECONNRESET, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED},
		errors.New("LOADING Redis is loading the dataset in memory"),
	} {
		if !disconnected(err) {
			t.Errorf("%v is not seen as a lost connection", err)
		}
	}
	for _, err := range []error{
		errors.New("NOPERM this user has no permissions"),
		errors.New("NOGROUP No such key or consumer group"),
		goredis.ErrClosed,
	} {
		if disconnected(err) {
			t.Errorf("%v is seen as a lost connection, so the loop would retry it forever", err)
		}
	}
}

func TestExpiryAndRetainAreRefused(t *testing.T) {
	for _, o := range []streams.Options{
		streams.NewOptions(streams.Expires(5 * time.Second)),
		streams.NewOptions(streams.Retain()),
	} {
		if err := refuseUnhonored(o); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("refuseUnhonored(%+v) = %v, want ErrUnsupported", o, err)
		}
	}
	// A zero expiry is no expiry, as a zero TTL is no TTL.
	if err := refuseUnhonored(streams.NewOptions(streams.Expires(0))); err != nil {
		t.Errorf("a zero expiry was refused: %v", err)
	}
}
