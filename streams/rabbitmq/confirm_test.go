package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

// A batch is sent whole and confirmed once, and every value in it arrives.
func TestPublishBatchIsConfirmed(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	ch, err := d.Consume(ctx, subject, "ledger")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	values := make([]any, 50)
	for i := range values {
		values[i] = event{User: "u", Action: "batch"}
	}
	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, subject, values)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}

	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	for len(want) > 0 {
		got := recv(t, ch, 10*time.Second)
		delete(want, got.ID)
		_ = got.Ack(ctx)
	}
}

// The broker closes a channel that publishes to a missing exchange. That is an
// error for that publish, named by the broker, and not the end of publishing.
func TestPublishToAMissingExchangeFailsAlone(t *testing.T) {
	s := testStreams(t)
	ctx := t.Context()

	gone, m := declare(t, s, subject)
	if err := s.Delete(ctx, gone.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := m.Publish(ctx, subject, event{}); err == nil {
		t.Fatal("publishing to a deleted stream's exchange succeeded")
	}

	_, again := declare(t, s, subject)
	if _, err := again.Publish(ctx, subject, event{}); err != nil {
		t.Fatalf("Publish after the broker closed the last channel: %v", err)
	}
}

// Use shares the caller's connection, does not close it, and does not redial
// it once it has gone.
func TestUseSharesTheCallersConnection(t *testing.T) {
	testStreams(t) // skips without a broker
	conn, err := amqp.Dial(testURL())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	s := streamsrabbit.Use(conn, streamsrabbit.WithPrefix(prefix()))
	_, m := declare(t, s, subject)
	if _, err := m.Publish(t.Context(), subject, event{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if err := s.(streams.Closer).Close(); err != nil || conn.IsClosed() {
		t.Fatalf("Close = %v, closed the caller's connection = %v", err, conn.IsClosed())
	}

	_ = conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := m.Publish(ctx, subject, event{}); err == nil {
		t.Fatal("Publish succeeded on a closed connection")
	}
	if time.Since(start) > 2*time.Second {
		t.Error("Publish waited for a connection that nothing will redial")
	}
}
