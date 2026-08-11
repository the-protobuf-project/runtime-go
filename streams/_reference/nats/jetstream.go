package nats

import (
	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/nats-io/nats.go/jetstream"
)

// jetstreamManager is the public façade exposed by NatsService for JetStream.
// It groups higher-level store abstractions (e.g., KV buckets) behind a single
// entry point.
//
// Example:
//
//	client, err := nats.NewNatsClient()
//	if err != nil {
//	    log.Fatalf("failed to connect: %v", err)
//	}
//	defer client.Close()
//
//	// Create a KV bucket and put a value
//	kv, err := client.Jetstream.Store.Create(types.KeyValueConfig{Name: "example"})
//	if err != nil {
//	    log.Fatalf("failed to create bucket: %v", err)
//	}
//
//	_, err = kv.Create("user.1", []byte("alice"))
//	if err != nil {
//	    log.Fatalf("failed to create entry: %v", err)
//	}
type jetstreamManager struct {
	// Store groups storage-related managers (e.g., Key-Value/Bucket APIs).
	// Additional store types can be added here in the future.
	Store StoreManager

	// Stream provides access to stream management operations.
	// Deprecated: Use StreamManager directly via client.Jetstream.Stream instead.
	Stream StreamManager
}

// jetstreamHandler is the concrete adapter that forwards operations
// to the underlying jetstream.JetStream client. It implements BucketManager.
type jetstreamHandler struct {
	client jetstream.JetStream // Underlying JetStream client.
}

// newJetstreamManager wraps a jetstream.JetStream into a jetstreamManager,
// wiring all store-level managers (e.g., BucketManager).
//
// It does not allocate network resources by itself; it only binds helpers
// around the already-live JetStream context.
//
// Logging:
//   - Debug when the manager is constructed successfully.
func newJetstreamManager(js jetstream.JetStream) (jetstreamManager, error) {
	shared.Pulse.Logger.Debug("nats: initializing jetstream manager")
	jh := &jetstreamHandler{client: js}
	// Wire sub-managers here.
	mgr := jetstreamManager{
		Store: StoreManager{
			// jetstreamHandler implements BucketManager (see store.go).
			bucketManager: jh,
		},
		Stream: jh, // jetstreamHandler implements StreamManager (see streams.go).
	}

	shared.Pulse.Logger.Debug("nats: jetstream manager initialized")
	return mgr, nil
}
