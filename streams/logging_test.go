package streams

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

type capturedRecord struct {
	level  telemetry.Level
	msg    string
	err    error
	fields telemetry.Fields
}

// captureLogger records everything written to it. It is safe for the one
// goroutine the subscriber decorator starts because the test drains before
// reading.
type captureLogger struct {
	records *[]capturedRecord
	bound   telemetry.Fields
	done    chan struct{}
}

func newCaptureLogger() (*captureLogger, *[]capturedRecord) {
	recs := &[]capturedRecord{}
	return &captureLogger{records: recs, done: make(chan struct{})}, recs
}

func (c *captureLogger) add(level telemetry.Level, msg string, err error, fields telemetry.Fields) {
	merged := telemetry.Fields{}
	maps.Copy(merged, c.bound)
	maps.Copy(merged, fields)
	*c.records = append(*c.records, capturedRecord{level: level, msg: msg, err: err, fields: merged})
}

func (c *captureLogger) Debug(_ context.Context, msg string, f telemetry.Fields) {
	c.add(telemetry.LevelDebug, msg, nil, f)
}
func (c *captureLogger) Info(_ context.Context, msg string, f telemetry.Fields) {
	c.add(telemetry.LevelInfo, msg, nil, f)
	if msg == "subscription closed" {
		close(c.done)
	}
}
func (c *captureLogger) Warn(_ context.Context, msg string, f telemetry.Fields) {
	c.add(telemetry.LevelWarn, msg, nil, f)
}
func (c *captureLogger) Error(_ context.Context, msg string, err error, f telemetry.Fields) {
	c.add(telemetry.LevelError, msg, err, f)
}
func (c *captureLogger) Enabled(context.Context, telemetry.Level) bool { return true }
func (c *captureLogger) With(f telemetry.Fields) telemetry.Logger {
	merged := telemetry.Fields{}
	maps.Copy(merged, c.bound)
	maps.Copy(merged, f)
	return &captureLogger{records: c.records, bound: merged, done: c.done}
}

func find(recs []capturedRecord, msg string) (capturedRecord, bool) {
	for _, r := range recs {
		if r.msg == msg {
			return r, true
		}
	}
	return capturedRecord{}, false
}

func TestPublisherLoggingRecordsSuccessAtDebug(t *testing.T) {
	log, recs := newCaptureLogger()
	p := WithPublisherLogging(&fakePublisher{}, log)

	if _, err := p.Publish(t.Context(), "user.login", nil, ID("m-1"), TTL(time.Minute)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rec, ok := find(*recs, "published")
	if !ok {
		t.Fatalf("no publish record: %+v", *recs)
	}
	if rec.level != telemetry.LevelDebug {
		t.Errorf("level = %v, want debug", rec.level)
	}
	if rec.fields["subject"] != "user.login" {
		t.Errorf("subject = %v", rec.fields["subject"])
	}
	if rec.fields["id"] != "m-1" {
		t.Errorf("id = %v, want m-1", rec.fields["id"])
	}
	// TTL is the whole point on the notification path, so it must be recorded.
	if rec.fields["ttl"] != time.Minute.String() {
		t.Errorf("ttl = %v, want 1m0s", rec.fields["ttl"])
	}
}

// An undeclared subject is a programming mistake and the message went nowhere,
// so it belongs at error.
func TestPublisherLoggingRecordsUnknownSubjectAtError(t *testing.T) {
	log, recs := newCaptureLogger()
	p := WithPublisherLogging(&fakePublisher{errs: []error{ErrUnknownSubject}}, log)

	if _, err := p.Publish(t.Context(), "typo", nil); !errors.Is(err, ErrUnknownSubject) {
		t.Fatalf("Publish error = %v", err)
	}

	rec, ok := find(*recs, "publish rejected: subject not declared by this stream")
	if !ok {
		t.Fatalf("no rejection record: %+v", *recs)
	}
	if rec.level != telemetry.LevelError {
		t.Errorf("level = %v, want error", rec.level)
	}
}

func TestPublisherLoggingToleratesNilLogger(t *testing.T) {
	p := WithPublisherLogging(&fakePublisher{}, nil)

	if _, err := p.Publish(t.Context(), "s", nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// The close record is how a leaked consumer is spotted, so it must be written
// when the upstream channel ends — and it must carry the delivered count.
func TestSubscriberLoggingRecordsOpenDeliverAndClose(t *testing.T) {
	log, recs := newCaptureLogger()
	upstream := make(chan Message, 2)
	s := WithSubscriberLogging(&fakeSubscriber{ch: upstream}, log)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	out, err := s.Subscribe(ctx, "user.login")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	upstream <- Message{ID: "m-1", Subject: "user.login"}
	upstream <- Message{ID: "m-2", Subject: "user.login"}
	close(upstream)

	for range out {
	}

	select {
	case <-log.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the subscription close was never logged")
	}

	if _, ok := find(*recs, "subscribed"); !ok {
		t.Errorf("no subscribe record: %+v", *recs)
	}
	closed, ok := find(*recs, "subscription closed")
	if !ok {
		t.Fatalf("no close record: %+v", *recs)
	}
	if closed.fields["delivered"] != 2 {
		t.Errorf("delivered = %v, want 2", closed.fields["delivered"])
	}
}

func TestSubscriberLoggingRecordsFailureAtError(t *testing.T) {
	boom := errors.New("broker down")
	log, recs := newCaptureLogger()
	s := WithSubscriberLogging(&fakeSubscriber{err: boom}, log)

	if _, err := s.Subscribe(t.Context(), "s"); !errors.Is(err, boom) {
		t.Fatalf("Subscribe error = %v", err)
	}
	rec, ok := find(*recs, "subscribe failed")
	if !ok {
		t.Fatalf("no failure record: %+v", *recs)
	}
	if !errors.Is(rec.err, boom) {
		t.Errorf("logged error = %v, want %v", rec.err, boom)
	}
}

// fakeSubscriber returns a channel the test controls.
type fakeSubscriber struct {
	ch  chan Message
	err error
}

func (f *fakeSubscriber) Subscribe(context.Context, string) (<-chan Message, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.ch, nil
}
