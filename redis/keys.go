package redis

import "strings"

// kind distinguishes the handlers sharing a database, so a cache entry, a
// record, a stream, and a notification never collide on a key.
type kind string

const (
	kindCache  kind = "cache"
	kindKV     kind = "kv"
	kindStream kind = "stream"
	kindNotify kind = "notify"
)

// keys builds every Redis key one handler uses.
//
// The prefix and kind are baked in at construction, so an operation asks for
// "the entry key for this id" and cannot accidentally address another handler's
// keyspace.
type keys struct {
	// base is the prefix and kind already joined with their separator, so each
	// helper is a single concatenation.
	base string
}

func newKeys(prefix string, k kind) keys {
	var b strings.Builder
	if prefix != "" {
		b.WriteString(prefix)
		b.WriteByte(':')
	}
	b.WriteString(string(k))
	b.WriteByte(':')
	return keys{base: b.String()}
}

// entry holds a stored value.
func (k keys) entry(id string) string { return k.base + "entry:" + id }

// index is the set of every id this handler holds.
//
// Redis cannot enumerate a logical group without scanning the whole keyspace,
// so the handlers maintain this themselves.
func (k keys) index() string { return k.base + "index" }

// byContent maps a content hash to the id holding it, which is what makes the
// KV store content-addressed.
func (k keys) byContent(hash string) string { return k.base + "content:" + hash }

// contentOf maps an id back to the hash it holds, so a write can release the
// old reservation without rehashing the stored value.
func (k keys) contentOf(id string) string { return k.base + "hash:" + id }

// stream holds a stream's metadata.
func (k keys) stream(id string) string { return k.base + "meta:" + id }

// streamPattern matches every stream metadata key in this handler's namespace.
func (k keys) streamPattern() string { return k.base + "meta:*" }

// idFromStream recovers a stream id from a key built by [keys.stream].
func (k keys) idFromStream(key string) string {
	return strings.TrimPrefix(key, k.base+"meta:")
}

// channel is the pub/sub channel a subject's messages travel on.
func (k keys) channel(streamID, subject string) string {
	return k.base + "ch:" + streamID + ":" + subject
}

// pending carries the TTL whose expiry is the notification.
//
// The stream and subject are in the key because a keyspace expiry event
// delivers only a key name — everything needed to route it has to be
// recoverable from there. The message id makes each key unique, so two
// notifications on one subject do not overwrite each other and reset one
// another's timer.
func (k keys) pending(streamID, subject, msgID string) string {
	return k.pendingPrefix(streamID, subject) + msgID
}

// pendingPrefix is what a subscriber matches an expiry event against.
func (k keys) pendingPrefix(streamID, subject string) string {
	return k.base + "pending:" + streamID + ":" + subject + ":"
}

// payload holds a notification's body, which outlives the TTL key announcing it
// — by the time the expiry fires, that key's value is already gone.
func (k keys) payload(msgID string) string { return k.base + "payload:" + msgID }
