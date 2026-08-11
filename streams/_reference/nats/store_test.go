// kv_store_test.go
package nats

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/machanirobotics/loom/go/nats/types"
)

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
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

	nc, err := nats.Connect(s.ClientURL(), nats.Name("kv-store-tests"))
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

// --- tests ---

func TestBucketManager_Create_Get_Set_Delete(t *testing.T) {
	_, _, js := startEmbeddedNATS(t)

	jm, err := newJetstreamManager(js)
	if err != nil {
		t.Fatalf("newJetstreamManager error: %v", err)
	}

	if jm.Store.bucketManager == nil {
		t.Fatalf("expected Store.BucketManager to be initialized")
	}

	bucket := fmt.Sprintf("kv-ut-%d", time.Now().UnixNano())

	kvMgr, err := jm.Store.New(types.KeyValueConfig{Bucket: bucket, History: 10})
	if err != nil {
		t.Fatalf("Create bucket failed: %v", err)
	}
	if kvMgr == nil {
		t.Fatalf("Create returned nil KVManager")
	}

	kvMgr2, err := jm.Store.Get(bucket)
	if err != nil {
		t.Fatalf("Get (bind) failed: %v", err)
	}
	if kvMgr2 == nil {
		t.Fatalf("Get returned nil KVManager")
	}

	kvMgr3, err := jm.Store.Set(bucket)
	if err != nil {
		t.Fatalf("Set (alias of Get) failed: %v", err)
	}
	if kvMgr3 == nil {
		t.Fatalf("Set returned nil KVManager")
	}

	if err := jm.Store.Delete(bucket); err != nil {
		t.Fatalf("Delete bucket failed: %v", err)
	}
}

func TestKVManager_Create_Get_Delete_History_Update_Watch(t *testing.T) {
	_, _, js := startEmbeddedNATS(t)

	jm, err := newJetstreamManager(js)
	if err != nil {
		t.Fatalf("newJetstreamManager error: %v", err)
	}

	bucket := fmt.Sprintf("kv-ut2-%d", time.Now().UnixNano())

	kv, err := jm.Store.New(types.KeyValueConfig{Bucket: bucket, History: 10})
	if err != nil {
		t.Fatalf("Create bucket failed: %v", err)
	}

	// Use VALID subject-token keys: letters/digits, '-', '_', '.'
	const key = "user.123"
	const val1 = "Alice"

	rev1, err := kv.Create(key, []byte(val1))
	if err != nil {
		t.Fatalf("KV Create failed: %v", err)
	}
	if rev1 == 0 {
		t.Fatalf("expected non-zero rev after Create")
	}

	got1, gotRev1, err := kv.Get(key)
	if err != nil {
		t.Fatalf("KV Get failed: %v", err)
	}
	if string(got1) != val1 {
		t.Fatalf("unexpected value: got %q want %q", string(got1), val1)
	}
	if gotRev1 != rev1 {
		t.Fatalf("unexpected rev: got %d want %d", gotRev1, rev1)
	}

	// Update (no change)
	revSame, err := kv.Update(key, []byte(val1))
	if err != nil {
		t.Fatalf("KV Update (no change) failed: %v", err)
	}
	if revSame != rev1 {
		t.Fatalf("expected same revision on no-op update: got %d want %d", revSame, rev1)
	}

	// Update (change)
	const val2 = "Alice v2"
	rev2, err := kv.Update(key, []byte(val2))
	if err != nil {
		t.Fatalf("KV Update (change) failed: %v", err)
	}
	if rev2 == 0 || rev2 == rev1 {
		t.Fatalf("expected new revision after update: got %d (old %d)", rev2, rev1)
	}

	// History
	hist, err := kv.History(key)
	if err != nil {
		t.Fatalf("KV History failed: %v", err)
	}
	if len(hist) < 2 {
		t.Fatalf("expected at least 2 history entries, got %d", len(hist))
	}

	// Watch (smoke)
	w, err := kv.Watch(">")
	if err != nil {
		t.Fatalf("KV Watch failed: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Another valid key
	const key2 = "user.456"
	if _, err := kv.Create(key2, []byte("Bob")); err != nil {
		t.Fatalf("KV Create (for watch) failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // best-effort; we just verify watcher can run

	// Delete keys
	if err := kv.Delete(key); err != nil {
		t.Fatalf("KV Delete failed: %v", err)
	}
	if err := kv.Delete(key2); err != nil {
		t.Fatalf("KV Delete (key2) failed: %v", err)
	}

	// Cleanup bucket
	if err := jm.Store.Delete(bucket); err != nil {
		t.Fatalf("Delete bucket failed: %v", err)
	}
}
