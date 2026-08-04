package streams

import "strings"

// Field names for the metadata entry stored inside each stream key.
const (
	fieldStreamInfo = "stream-info"
	fieldNotifyInfo = "notify-info"
)

// keys builds the Redis key names for one set of streams, namespaced by an
// optional prefix. Two providers sharing a Redis database but configured with
// different prefixes do not collide.
//
// The kind segment ("stream" or "notify") keeps ordinary streams and expiry
// notifications in separate namespaces, so listing one never turns up the other.
type redisKeys struct {
	prefix string
	kind   string
}

func newRedisKeys(prefix, kind string) redisKeys {
	if prefix != "" {
		prefix += ":"
	}
	return redisKeys{prefix: prefix, kind: kind}
}

// stream returns the key holding a stream's metadata.
// Data type: stream (one entry, carrying the metadata field).
func (k redisKeys) stream(id string) string { return k.prefix + k.kind + ":" + id }

// streamPattern matches every stream key in this namespace, for List.
func (k redisKeys) streamPattern() string { return k.prefix + k.kind + ":*" }

// idFromStreamKey recovers the stream ID from a key produced by [keys.stream].
func (k redisKeys) idFromStreamKey(key string) string {
	return strings.TrimPrefix(key, k.prefix+k.kind+":")
}

// channel returns the pub/sub channel a subject's messages are published on.
func (k redisKeys) channel(streamID, subject string) string {
	return k.prefix + k.kind + ":ch:" + streamID + ":" + subject
}

// pending returns the TTL-bearing key whose expiry *is* the notification.
//
// The stream ID and subject are part of the key because a Redis keyspace
// expiry event carries only the key name — everything the subscriber needs to
// route the event has to be recoverable from it. The message ID makes the key
// unique per message, so two notifications on one subject do not overwrite each
// other and reset one another's timer.
func (k redisKeys) pending(streamID, subject, msgID string) string {
	return k.prefix + k.kind + ":pending:" + streamID + ":" + subject + ":" + msgID
}

// pendingPattern matches the pending keys for one stream and subject. It is
// what a subscriber uses to decide whether an expiry event is addressed to it.
func (k redisKeys) pendingPattern(streamID, subject string) string {
	return k.prefix + k.kind + ":pending:" + streamID + ":" + subject + ":"
}

// payload returns the durable copy of a notification body.
//
// The TTL key's value is gone by the time its expiry fires, so the body lives
// under a second key that outlives it and is deleted once delivered.
func (k redisKeys) payload(msgID string) string {
	return k.prefix + k.kind + ":payload:" + msgID
}

// expiryChannel is the keyspace notification channel for expired keys in a
// database. Redis requires `--notify-keyspace-events Ex` for these to fire.
func expiryChannel(db int) string {
	return "__keyevent@" + itoa(db) + "__:expired"
}
