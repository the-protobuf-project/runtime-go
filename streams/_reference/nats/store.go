package nats

import (
	"fmt"

	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/machanirobotics/loom/go/nats/types"
)

// StoreManager groups storage-related managers for JetStream (e.g., KV).
// It currently exposes a BucketManager to create/bind/delete KV buckets.
type StoreManager struct {
	bucketManager // Embedded interface for direct access.
}

// BucketManager manages JetStream Key-Value (KV) buckets.
type bucketManager interface {
	// New creates a new KV bucket according to cfg and returns a bound KVManager.
	//
	// If a bucket with the same name exists, the underlying client may return an error.
	// Callers should handle idempotency at a higher layer if needed.
	New(cfg types.KeyValueConfig, context ...helpers.NatsContext) (KVManager, error)

	// Get binds to an existing KV bucket and returns a KVManager bound to it.
	//
	// The returned KVManager is ready for entry operations (Create/Get/Delete/etc.).
	// If the bucket does not exist or the client cannot bind, an error is returned.
	Get(name string, context ...helpers.NatsContext) (KVManager, error)

	// Delete permanently deletes a KV bucket by name.
	//
	// Note: Deleting a bucket tombstones all contained keys. This operation is irreversible.
	Delete(name string, context ...helpers.NatsContext) error

	// Set is an alias for Get. It binds to an existing bucket and returns a KVManager.
	// Provided for API readability in workflows that “set” the active bucket explicitly.
	Set(name string) (KVManager, error)
}

// bucketInstance implements KVManager by binding operations to a specific KV bucket.
type bucketInstance struct {
	currentBucket types.KeyValue // the bound KV bucket
}

// KVManager performs entry-level operations within a bound KV bucket.
type KVManager interface {
	// Create stores a value for key and returns the new revision.
	//
	// If the key already exists, semantics follow JetStream KV Put (new revision).
	// To enforce optimistic concurrency, prefer a CAS approach at a higher layer.
	Create(key string, v any, context ...helpers.NatsContext) (uint64, error)

	// Get fetches the latest value and its revision for key.
	// Returns (nil, 0, error) if the key does not exist or retrieval fails.
	Get(key string, context ...helpers.NatsContext) ([]byte, uint64, error)

	// Delete tombstones key in the bucket. JetStream keeps tombstones in history.
	Delete(key string, context ...helpers.NatsContext) error

	// Watch streams updates for subjects matching the given filter.
	// The returned watcher must be drained/closed by the caller.
	Watch(subject string, opts ...types.WatchOpt) (types.KeyWatcher, error)

	// History returns all revisions for key in ascending order (oldest→newest).
	// For very long histories, consider iterating via lower-level streaming APIs.
	History(key string, context ...helpers.NatsContext) ([]types.KeyValueEntry, error)

	// Update replaces a key’s value only if its content changed.
	// Returns the current or new revision accordingly.
	Update(key string, v any, context ...helpers.NatsContext) (uint64, error)
}

// New creates a KV bucket with cfg and returns a KVManager bound to it.
func (j *jetstreamHandler) New(cfg types.KeyValueConfig, context ...helpers.NatsContext) (KVManager, error) {
	ctx := helpers.HandleContext(context...)
	shared.Pulse.Logger.Debugf("creating KV bucket %q", cfg.Bucket)

	kv, err := j.client.CreateKeyValue(ctx.Ctx, cfg)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to create KV bucket %q: %v", cfg.Bucket, err)
		return nil, fmt.Errorf("create bucket %q: %w", cfg.Bucket, err)
	}

	shared.Pulse.Logger.Debugf("created KV bucket %q successfully", cfg.Bucket)
	return &bucketInstance{currentBucket: kv}, nil
}

// Get binds to a KV bucket by name and returns a KVManager bound to it.
func (j *jetstreamHandler) Get(name string, context ...helpers.NatsContext) (KVManager, error) {
	ctx := helpers.HandleContext(context...)
	shared.Pulse.Logger.Debugf("binding to KV bucket %q", name)

	kv, err := j.client.KeyValue(ctx.Ctx, name)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to bind to KV bucket %q: %v", name, err)
		return nil, fmt.Errorf("bind bucket %q: %w", name, err)
	}

	shared.Pulse.Logger.Debugf("bound to KV bucket %q successfully", name)
	return &bucketInstance{currentBucket: kv}, nil
}

// Delete deletes a KV bucket by name.
func (j *jetstreamHandler) Delete(name string, context ...helpers.NatsContext) error {
	ctx := helpers.HandleContext(context...)
	shared.Pulse.Logger.Debugf("deleting KV bucket %q", name)

	if err := j.client.DeleteKeyValue(ctx.Ctx, name); err != nil {
		shared.Pulse.Logger.Errorf("failed to delete KV bucket %q: %v", name, err)
		return fmt.Errorf("delete bucket %q: %w", name, err)
	}

	shared.Pulse.Logger.Debugf("deleted KV bucket %q successfully", name)
	return nil
}

// Set binds to a KV bucket (alias for Get).
func (j *jetstreamHandler) Set(name string) (KVManager, error) {
	shared.Pulse.Logger.Debugf("selecting KV bucket %q", name)
	return j.Get(name)
}

// Create stores a value for key and returns the new revision.
func (j *bucketInstance) Create(key string, v any, context ...helpers.NatsContext) (uint64, error) {
	ctx := helpers.HandleContext(context...)
	b := j.currentBucket.Bucket()
	shared.Pulse.Logger.Debugf("creating entry key=%q in bucket=%q", key, b)

	bytesData, err := ConvertAnyDataToBytes(v)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to encode entry for key=%q in bucket=%q: %v", key, b, err)
		return 0, fmt.Errorf("encode value for %q: %w", key, err)
	}

	rev, err := j.currentBucket.Put(ctx.Ctx, key, bytesData)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to create entry key=%q in bucket=%q: %v", key, b, err)
		return 0, fmt.Errorf("put %q in %q: %w", key, b, err)
	}

	shared.Pulse.Logger.Debugf("created entry key=%q in bucket=%q (rev=%d)", key, b, rev)
	return rev, nil
}

// Get returns the latest value and revision for key.
func (j *bucketInstance) Get(key string, context ...helpers.NatsContext) ([]byte, uint64, error) {
	ctx := helpers.HandleContext(context...)
	b := j.currentBucket.Bucket()
	shared.Pulse.Logger.Debugf("fetching entry key=%q from bucket=%q", key, b)

	e, err := j.currentBucket.Get(ctx.Ctx, key)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to fetch entry key=%q from bucket=%q: %v", key, b, err)
		return nil, 0, fmt.Errorf("get %q from %q: %w", key, b, err)
	}

	shared.Pulse.Logger.Debugf("fetched entry key=%q from bucket=%q (rev=%d)", key, b, e.Revision())
	return e.Value(), e.Revision(), nil
}

// Delete tombstones key in the bound bucket.
func (j *bucketInstance) Delete(key string, context ...helpers.NatsContext) error {
	ctx := helpers.HandleContext(context...)
	b := j.currentBucket.Bucket()
	shared.Pulse.Logger.Debugf("deleting entry key=%q from bucket=%q", key, b)

	if err := j.currentBucket.Delete(ctx.Ctx, key); err != nil {
		shared.Pulse.Logger.Errorf("failed to delete entry key=%q from bucket=%q: %v", key, b, err)
		return fmt.Errorf("delete %q from %q: %w", key, b, err)
	}

	shared.Pulse.Logger.Debugf("deleted entry key=%q from bucket=%q", key, b)
	return nil
}

// Watch opens a watcher that streams updates matching subject.
// The caller is responsible for draining/closing the returned KeyWatcher.
func (j *bucketInstance) Watch(subject string, opts ...types.WatchOpt) (types.KeyWatcher, error) {
	ctx := helpers.HandleContext()
	b := j.currentBucket.Bucket()
	shared.Pulse.Logger.Debugf("watching entries in bucket=%q with subject=%q", b, subject)

	w, err := j.currentBucket.Watch(ctx.Ctx, subject, opts...)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to watch entries in bucket=%q with subject=%q: %v", b, subject, err)
		return nil, fmt.Errorf("watch %q in %q: %w", subject, b, err)
	}

	shared.Pulse.Logger.Debugf("started watcher in bucket=%q with subject=%q", b, subject)
	return w, nil
}

// History returns all revisions for key in ascending order.
//
// Note: If your histories are large, consider using the lower-level streaming
// iterator APIs instead of materializing everything upfront.
func (j *bucketInstance) History(key string, context ...helpers.NatsContext) ([]types.KeyValueEntry, error) {
	ctx := helpers.HandleContext(context...)
	b := j.currentBucket.Bucket()
	shared.Pulse.Logger.Debugf("fetching history for key=%q in bucket=%q", key, b)

	entries, err := j.currentBucket.History(ctx.Ctx, key)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to fetch history for key=%q in bucket=%q: %v", key, b, err)
		return nil, fmt.Errorf("history %q in %q: %w", key, b, err)
	}

	shared.Pulse.Logger.Debugf("fetched history for key=%q in bucket=%q (entries=%d)", key, b, len(entries))
	return entries, nil
}

// Update replaces a key’s value only if its content changed.
// Returns the current revision (no-op) or the new revision after write.
func (j *bucketInstance) Update(key string, v any, context ...helpers.NatsContext) (uint64, error) {
	ctx := helpers.HandleContext(context...)
	b := j.currentBucket.Bucket()
	shared.Pulse.Logger.Debugf("updating entry key=%q in bucket=%q", key, b)

	// Read current value
	e, err := j.currentBucket.Get(ctx.Ctx, key)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to read current value for key=%q in bucket=%q: %v", key, b, err)
		return 0, fmt.Errorf("get %q from %q: %w", key, b, err)
	}
	currentRev := e.Revision()
	currentValue := e.Value()

	// Encode new value
	newValue, err := ConvertAnyDataToBytes(v)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to encode new value for key=%q in bucket=%q: %v", key, b, err)
		return 0, fmt.Errorf("encode value for %q: %w", key, err)
	}

	// Compare bytes (raw equality)
	if string(currentValue) == string(newValue) {
		shared.Pulse.Logger.Debugf("no update needed for key=%q in bucket=%q (rev=%d unchanged)", key, b, currentRev)
		return currentRev, nil
	}

	// Write new value (reuse same context)
	rev, err := j.Create(key, newValue, ctx)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to update entry key=%q in bucket=%q: %v", key, b, err)
		return 0, fmt.Errorf("update %q in %q: %w", key, b, err)
	}

	shared.Pulse.Logger.Debugf("updated entry key=%q in bucket=%q (rev=%d)", key, b, rev)
	return rev, nil
}
