package topic

import (
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// BatchPolicy is when a [BatchSink] flushes: whichever limit is reached first.
type BatchPolicy struct {
	// MaxSize flushes once this many values are buffered. Zero is treated as
	// one.
	MaxSize int

	// MaxDelay flushes once the oldest buffered value has waited this long.
	MaxDelay time.Duration
}

// FieldFunc reads a header value or partition key out of a message. It reports
// false when a message on the way to the field is unset, and the header or key
// is then left off rather than sent empty.
type FieldFunc[T any] func(T) (string, bool)

// HeaderField is a header whose value is read from each published message.
type HeaderField[T any] struct {
	Header string
	Field  FieldFunc[T]
}

// Spec is the static half of a topic: its subject and the defaults every
// publish and receive starts from.
//
// protoc-gen-streams writes one per topic from streams.yaml, as a package-level
// value a [Topic] points at, so no call copies its headers. Change one only
// before the topics using it are made: a deployment's overrides are applied to
// the specs once, at startup.
type Spec[T any] struct {
	// Subject is the subject the topic publishes and subscribes on.
	Subject string

	// Consumer is the durable consumer name [Consumer.Name] suggests.
	Consumer string

	// Group is the subscription group every subscriber and consumer joins by
	// default; empty for none.
	Group string

	// Headers are on every publish, under the stream's own.
	Headers streams.Headers

	// HeaderFields are headers read from each published message, over the
	// static ones.
	HeaderFields []HeaderField[T]

	// PartitionKey is read from each published message; nil for none.
	PartitionKey FieldFunc[T]

	// Expiry drops a message not delivered within this time; see
	// [streams.Options.Expires].
	Expiry time.Duration

	// Retain keeps each message as the subject's last value; see
	// [streams.Options.Retain].
	Retain bool

	// Prefetch is how many messages a provider may hold ahead of the reader;
	// see [streams.Options.Prefetch].
	Prefetch int

	// Batch is the flush policy of [NewBatchSink].
	Batch BatchPolicy

	// Codec is the topic's wire format; nil for [JSON].
	Codec Codec[T]
}
