package streams

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestHeadersValidateNamesWhatIsWrong(t *testing.T) {
	if err := (Headers{"lang": "go", "content-type": "application/arrow"}).Validate(); err != nil {
		t.Fatalf("valid headers refused: %v", err)
	}
	for _, c := range []struct{ key, value, says string }{
		{"", "x", "empty"},
		{"has space", "x", "visible ASCII"},
		{"has:colon", "x", "visible ASCII"},
		{"lang", "two\nlines", "control"},
		{strings.Repeat("k", MaxHeaderKey+1), "v", "limit"},
	} {
		err := (Headers{c.key: c.value}).Validate()
		if !errors.Is(err, ErrInvalidHeader) || !strings.Contains(err.Error(), c.says) {
			t.Errorf("%q: %v, want ErrInvalidHeader saying %q", c.key, err, c.says)
		}
	}
	big := Headers{}
	for i := range 5 {
		big[string(rune('a'+i))] = strings.Repeat("v", MaxHeaderValue)
	}
	if err := big.Validate(); err == nil || !strings.Contains(err.Error(), "total") {
		t.Fatalf("oversized headers: %v", err)
	}
}

func TestHeadersRejectReserved(t *testing.T) {
	err := (Headers{"Nats-Msg-Id": "x"}).RejectReserved("NATS", "Nats-")
	if !errors.Is(err, ErrInvalidHeader) || !strings.Contains(err.Error(), "Nats-") {
		t.Fatalf("RejectReserved = %v", err)
	}
}

// A stream's headers are stored as a plain JSON object, and left out when
// empty, so a stream written before headers existed still reads the same.
func TestStreamHeadersJSON(t *testing.T) {
	b, _ := json.Marshal(Stream{ID: "s"})
	if strings.Contains(string(b), "headers") {
		t.Fatalf("empty headers serialized: %s", b)
	}
	b, _ = json.Marshal(Stream{ID: "s"}.WithHeader("lang", "go"))
	if !strings.Contains(string(b), `"headers":{"lang":"go"}`) {
		t.Fatalf("headers serialized as %s", b)
	}
}

func TestWithPublisherRetryGivesUpOnInvalidHeader(t *testing.T) {
	f := &fakePublisher{errs: []error{ErrInvalidHeader, ErrInvalidHeader}}
	if _, err := WithPublisherRetry(f, 3, time.Millisecond).Publish(context.Background(), "s", 1); !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("err = %v", err)
	}
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("calls = %d, want 1", n)
	}
}

type countingAcker struct{ naks int }

func (a *countingAcker) Ack(context.Context) error { return nil }
func (a *countingAcker) Nak(context.Context) error { a.naks++; return nil }

type delayingAcker struct {
	countingAcker
	delay time.Duration
}

func (a *delayingAcker) NakAfter(_ context.Context, d time.Duration) error { a.delay = d; return nil }
func (a *delayingAcker) MaxNakDelay() time.Duration                        { return 30 * time.Second }

// Where the provider cannot delay, NakAfter still returns the message, then
// says it did not wait.
func TestNakAfterFallsBackToNak(t *testing.T) {
	a := &countingAcker{}
	d := NewDelivery(Message{}, 1, a)
	if d.CanDelayRedelivery() {
		t.Fatal("CanDelayRedelivery on a plain acker")
	}
	if err := d.NakAfter(context.Background(), time.Second); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("NakAfter = %v, want ErrUnsupported", err)
	}
	if a.naks != 1 {
		t.Fatalf("naks = %d, want 1", a.naks)
	}
}

func TestNakAfterDelays(t *testing.T) {
	a := &delayingAcker{}
	d := NewDelivery(Message{}, 1, a)
	if !d.CanDelayRedelivery() || d.MaxRedeliveryDelay() != 30*time.Second {
		t.Fatal("delaying acker not reported")
	}
	if err := d.NakAfter(context.Background(), 5*time.Second); err != nil || a.delay != 5*time.Second || a.naks != 0 {
		t.Fatalf("NakAfter = %v, delay %v, naks %d", err, a.delay, a.naks)
	}
}
