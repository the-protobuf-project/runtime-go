package nats

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/machanirobotics/loom/go/nats/types"
)

func startEmbeddedNATSForStreams(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create nats-server: %v", err)
	}
	go s.Start()

	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		t.Fatalf("nats-server did not become ready in time")
	}

	nc, err := nats.Connect(s.ClientURL(), nats.Name("stream-manager-tests"))
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

// getStreamsHandler exposes the stream methods (implemented on *jetstreamHandler)
// from the public jetstreamManager. We’re in package nats, so this cast is safe.
func getStreamsHandler(t *testing.T, jm jetstreamManager) *jetstreamHandler {
	t.Helper()
	bm := jm.Store.bucketManager
	if bm == nil {
		t.Fatalf("BucketManager was not wired; JetStream manager initialization failed")
	}
	jh, ok := bm.(*jetstreamHandler)
	if !ok {
		t.Fatalf("BucketManager is not *jetstreamHandler (got %T)", bm)
	}
	return jh
}

func TestStream_Create_Get_List_Delete(t *testing.T) {
	_, _, js := startEmbeddedNATSForStreams(t)

	jm, err := newJetstreamManager(js)
	if err != nil {
		t.Fatalf("newJetstreamManager error: %v", err)
	}
	jh := getStreamsHandler(t, jm)

	streamName := fmt.Sprintf("stream_ut_%d", time.Now().UnixNano())
	cfg := types.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName + ".>"},
	}

	// Create
	s, err := jh.CreateStream(cfg)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if s == nil {
		t.Fatalf("CreateStream returned nil stream")
	}

	// Get
	got, err := jh.GetStream(streamName)
	if err != nil {
		t.Fatalf("GetStream failed: %v", err)
	}
	if got == nil {
		t.Fatalf("GetStream returned nil stream")
	}

	// List should include our stream
	names, err := jh.ListStreams()
	if err != nil {
		t.Fatalf("ListStreams failed: %v", err)
	}
	found := false
	for _, n := range names {
		if n == streamName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ListStreams did not include %q; got=%v", streamName, names)
	}

	// Delete
	if err := jh.DeleteStream(streamName); err != nil {
		t.Fatalf("DeleteStream failed: %v", err)
	}

	// Get after delete should be not found
	_, err = jh.GetStream(streamName)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "not found") {
		t.Fatalf("expected not found after delete, got: %v", err)
	}
}

func TestStream_CreateIsIdempotent_UsesUpdatePath(t *testing.T) {
	_, _, js := startEmbeddedNATSForStreams(t)

	jm, err := newJetstreamManager(js)
	if err != nil {
		t.Fatalf("newJetstreamManager error: %v", err)
	}
	jh := getStreamsHandler(t, jm)

	streamName := fmt.Sprintf("stream_ut_idemp_%d", time.Now().UnixNano())
	cfg := types.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName + ".>"},
	}

	// First create should create…
	if s, err := jh.CreateStream(cfg); err != nil || s == nil {
		t.Fatalf("first CreateStream failed: %v (stream=%v)", err, s)
	}

	// Second create should hit the "exists → update" path and still succeed.
	if s, err := jh.CreateStream(cfg); err != nil || s == nil {
		t.Fatalf("second CreateStream (idempotent update) failed: %v (stream=%v)", err, s)
	}

	// Cleanup
	if err := jh.DeleteStream(streamName); err != nil {
		t.Fatalf("DeleteStream failed: %v", err)
	}
}

func TestStream_UpdateChangesConfig_Succeeds(t *testing.T) {
	_, _, js := startEmbeddedNATSForStreams(t)

	jm, err := newJetstreamManager(js)
	if err != nil {
		t.Fatalf("newJetstreamManager error: %v", err)
	}
	jh := getStreamsHandler(t, jm)

	streamName := fmt.Sprintf("stream_ut_update_%d", time.Now().UnixNano())

	// Create with one subject
	if _, err := jh.CreateStream(types.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName + ".a"},
	}); err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Update to add another subject
	updated, err := jh.UpdateStream(types.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName + ".a", streamName + ".b"},
	})
	if err != nil {
		t.Fatalf("UpdateStream failed: %v", err)
	}
	if updated == nil {
		t.Fatalf("UpdateStream returned nil stream")
	}

	// (Optionally) we could publish to the new subject and ensure it routes;
	// for now, just ensure Get still works.
	if _, err := jh.GetStream(streamName); err != nil {
		t.Fatalf("GetStream after update failed: %v", err)
	}

	// Cleanup
	if err := jh.DeleteStream(streamName); err != nil {
		t.Fatalf("DeleteStream failed: %v", err)
	}
}

func TestStream_DeleteNonExistent_ReturnsError(t *testing.T) {
	_, _, js := startEmbeddedNATSForStreams(t)

	jm, err := newJetstreamManager(js)
	if err != nil {
		t.Fatalf("newJetstreamManager error: %v", err)
	}
	jh := getStreamsHandler(t, jm)

	name := fmt.Sprintf("stream_ut_missing_%d", time.Now().UnixNano())
	if err := jh.DeleteStream(name); err == nil {
		t.Fatalf("expected error when deleting non-existent stream")
	}
}
