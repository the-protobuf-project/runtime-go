package rabbitmq_test

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

func TestPublishRejectsAnUndeclaredSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := m.Publish(t.Context(), "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish = %v, want ErrUnknownSubject", err)
	}
}

func TestPublishRejectsATTL(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	_, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

func TestConnectRejectsAnEmptyURL(t *testing.T) {
	if _, err := streamsrabbit.Connect(t.Context(), ""); err == nil {
		t.Error("Connect accepted an empty URL")
	}
}

func TestConsumeRejectsAGroupOption(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	_, err := d.Consume(t.Context(), subject, "billing", streams.Group("other"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}

// A TLS configuration with a plain URL would connect in the clear, so it is
// refused before anything is dialed.
func TestConnectRejectsTLSForAPlainURL(t *testing.T) {
	_, err := streamsrabbit.Connect(t.Context(), "amqp://guest:guest@127.0.0.1:5672/",
		streamsrabbit.WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}))
	if err == nil {
		t.Error("Connect accepted a TLS configuration for an amqp:// URL")
	}
}
