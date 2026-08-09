package redis

import "strings"

// kind separates the two delivery modes so an immediate stream and a scheduled
// one never collide on a key, even in the same database under the same prefix.
type kind string

const (
	kindStream kind = "stream"
	kindNotify kind = "notify"
)

// metadataField is the field a stream's metadata is stored under, inside the
// Redis stream key that represents it.
const metadataField = "meta"

// keys builds every Redis key one handler uses. The prefix and kind are baked
// in at construction, so an operation asks for "the key for this id" and cannot
// reach another handler's keyspace.
type keys struct {
	base string
}

func newKeys(prefix string, k kind) keys {
	if prefix != "" {
		prefix += ":"
	}
	return keys{base: prefix + string(k) + ":"}
}

// stream holds a stream's metadata.
func (k keys) stream(id string) string { return k.base + "meta:" + id }

// streamPattern matches every stream metadata key in this namespace.
func (k keys) streamPattern() string { return k.base + "meta:*" }

// idFromStream recovers a stream id from a key built by [keys.stream].
func (k keys) idFromStream(key string) string {
	return strings.TrimPrefix(key, k.base+"meta:")
}

// channel is the pub/sub channel a subject's messages travel on.
func (k keys) channel(streamID, subject string) string {
	return k.base + "ch:" + streamID + ":" + subject
}

// pending carries the TTL whose expiry is the delivery.
//
// The stream and subject are in the key because a keyspace expiry event
// delivers only a key name — everything needed to route it has to be
// recoverable from there. The message id makes each key unique, so two
// scheduled messages on one subject do not overwrite each other and reset one
// another's timer.
func (k keys) pending(streamID, subject, msgID string) string {
	return k.pendingPrefix(streamID, subject) + msgID
}

// pendingPrefix is what a subscriber matches an expiry event against.
func (k keys) pendingPrefix(streamID, subject string) string {
	return k.base + "pending:" + streamID + ":" + subject + ":"
}

// payload holds a scheduled message's body, which outlives the TTL key
// announcing it — by the time the expiry fires, that key's value is gone.
func (k keys) payload(msgID string) string { return k.base + "payload:" + msgID }
