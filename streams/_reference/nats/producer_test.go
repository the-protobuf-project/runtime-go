package nats

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/types"
)

func startEmbeddedNATSForProducer(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create embedded nats-server: %v", err)
	}
	go s.Start()

	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		t.Fatalf("nats-server did not become ready in time")
	}

	nc, err := nats.Connect(s.ClientURL(), nats.Name("producer-e2e-tests"))
	if err != nil {
		s.Shutdown()
		t.Fatalf("failed to connect to embedded nats: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		_ = nc.Drain()
		nc.Close()
		s.Shutdown()
		t.Fatalf("failed to create jetstream context: %v", err)
	}

	t.Cleanup(func() {
		_ = nc.Drain()
		nc.Close()
		s.Shutdown()
	})

	return s, nc, js
}

// createTestStream creates a JS stream that captures the given subjects.
func createTestStream(t *testing.T, js jetstream.JetStream, name string, subjects []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Using your `types.StreamConfig` surface (as your code expects).
	cfg := types.StreamConfig{
		Name:     name,
		Subjects: subjects,
	}
	if _, err := js.CreateStream(ctx, cfg); err != nil {
		// If already exists, try update
		if !strings.Contains(strings.ToLower(err.Error()), "already") {
			t.Fatalf("CreateStream failed: %v", err)
		}
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			t.Fatalf("UpdateStream after exists failed: %v", err)
		}
	}
}

// newProducer wires your producer handler over the live JS context.
func newProducer(js jetstream.JetStream) *producerHandler {
	return &producerHandler{stream: js}
}

// waitForMsg waits for a message on sub within a timeout and returns its data.
func waitForMsg(t *testing.T, sub *nats.Subscription, timeout time.Duration) []byte {
	t.Helper()
	msg, err := sub.NextMsg(timeout)
	if err != nil {
		t.Fatalf("did not receive message: %v", err)
	}
	return msg.Data
}

// --- tests ---

func TestProducer_Publish_Sync_E2E(t *testing.T) {
	_, nc, js := startEmbeddedNATSForProducer(t)

	streamName := fmt.Sprintf("e2e_pub_sync_%d", time.Now().UnixNano())
	subject := streamName + ".subject"

	createTestStream(t, js, streamName, []string{streamName + ".>"})

	// Live subscriber (core NATS)
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// Producer
	prod := newProducer(js)

	ack, err := prod.Publish(types.JetStreamPublishRequest{
		Subject: subject,
		Data:    []byte("sync-hello"),
		Async:   false,
	}, helpers.NatsContext{}) // using default cancellable context
	if err != nil {
		t.Fatalf("Publish (sync) failed: %v", err)
	}
	if ack == nil || ack.Sequence == 0 {
		t.Fatalf("expected non-nil ack with non-zero sequence")
	}

	data := waitForMsg(t, sub, 2*time.Second)
	if string(data) != "sync-hello" {
		t.Fatalf("unexpected payload: got %q want %q", string(data), "sync-hello")
	}
}

func TestProducer_Publish_Async_E2E(t *testing.T) {
	_, nc, js := startEmbeddedNATSForProducer(t)

	streamName := fmt.Sprintf("e2e_pub_async_%d", time.Now().UnixNano())
	subject := streamName + ".subject"

	createTestStream(t, js, streamName, []string{streamName + ".>"})

	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	prod := newProducer(js)

	ack, err := prod.Publish(types.JetStreamPublishRequest{
		Subject: subject,
		Data:    []byte("async-hello"),
		Async:   true,
	})
	if err != nil {
		t.Fatalf("Publish (async) failed: %v", err)
	}
	if ack == nil || ack.Sequence == 0 {
		t.Fatalf("expected non-nil ack with non-zero sequence (async)")
	}

	data := waitForMsg(t, sub, 2*time.Second)
	if string(data) != "async-hello" {
		t.Fatalf("unexpected payload: got %q want %q", string(data), "async-hello")
	}
}

func TestProducer_PublishRaw_Sync_E2E(t *testing.T) {
	_, nc, js := startEmbeddedNATSForProducer(t)

	streamName := fmt.Sprintf("e2e_pubraw_sync_%d", time.Now().UnixNano())
	subject := streamName + ".subject"

	createTestStream(t, js, streamName, []string{streamName + ".>"})

	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	prod := newProducer(js)

	ack, err := prod.PublishRaw(types.JetStreamPublishRequest{
		Subject: subject,
		Data:    []byte("raw-sync"),
		Async:   false,
	})
	if err != nil {
		t.Fatalf("PublishRaw (sync) failed: %v", err)
	}
	if ack == nil || ack.Sequence == 0 {
		t.Fatalf("expected non-nil ack with non-zero sequence (raw sync)")
	}

	data := waitForMsg(t, sub, 2*time.Second)
	if string(data) != "raw-sync" {
		t.Fatalf("unexpected payload: got %q want %q", string(data), "raw-sync")
	}
}

func TestProducer_PublishRaw_Async_E2E(t *testing.T) {
	_, nc, js := startEmbeddedNATSForProducer(t)

	streamName := fmt.Sprintf("e2e_pubraw_async_%d", time.Now().UnixNano())
	subject := streamName + ".subject"

	createTestStream(t, js, streamName, []string{streamName + ".>"})

	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	prod := newProducer(js)

	ack, err := prod.PublishRaw(types.JetStreamPublishRequest{
		Subject: subject,
		Data:    []byte("raw-async"),
		Async:   true,
	})
	if err != nil {
		t.Fatalf("PublishRaw (async) failed: %v", err)
	}
	if ack == nil || ack.Sequence == 0 {
		t.Fatalf("expected non-nil ack with non-zero sequence (raw async)")
	}

	data := waitForMsg(t, sub, 2*time.Second)
	if string(data) != "raw-async" {
		t.Fatalf("unexpected payload: got %q want %q", string(data), "raw-async")
	}
}

func TestProducer_PublishState_And_Clean(t *testing.T) {
	_, _, js := startEmbeddedNATSForProducer(t)

	streamName := fmt.Sprintf("e2e_pub_state_%d", time.Now().UnixNano())
	subject := streamName + ".subject"

	createTestStream(t, js, streamName, []string{streamName + ".>"})
	prod := newProducer(js)

	// Kick off a few async publishes.
	const n = 5
	for i := 0; i < n; i++ {
		_, err := prod.Publish(types.JetStreamPublishRequest{
			Subject: subject,
			Data:    []byte(fmt.Sprintf("msg-%d", i)),
			Async:   true,
		})
		if err != nil {
			t.Fatalf("async Publish #%d failed: %v", i, err)
		}
	}

	// Monitor state until complete=true (with a timeout).
	stateCh, err := prod.PublishState()
	if err != nil {
		t.Fatalf("PublishState failed: %v", err)
	}

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	complete := false
	for !complete {
		select {
		case st, ok := <-stateCh:
			if !ok {
				t.Fatalf("state channel closed unexpectedly")
			}
			// We don't assert exact pending curve; just ensure we eventually complete.
			if st.Complete {
				complete = true
			}
		case <-timeout.C:
			t.Fatalf("timed out waiting for publish completion")
		}
	}

	// Cleanup should be safe (no-op if nothing pending).
	if err := prod.Clean(); err != nil {
		t.Fatalf("Clean failed: %v", err)
	}
}
