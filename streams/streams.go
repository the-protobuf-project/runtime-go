// Package streams defines the backend-agnostic contract for runtime-go's
// messaging layer.
//
// Providers live in subpackages — [github.com/the-protobuf-project/runtime-go/streams/redis]
// and friends — and each exposes a typed Config plus a New constructor:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 0})
//	s, err := streamsredis.New(streamsredis.Config{Client: rdb, Prefix: "app"})
//
// A provider never opens its own connection. The caller builds the client, so
// its lifetime, pooling, and shutdown stay the caller's to control and this
// package holds no process-wide connection state.
//
// # Lifetime is the context's
//
// [Subscriber.Subscribe] returns a channel that is closed when its context ends.
// That is the only way to stop a subscription, and it is deliberate: a consumer
// that walks away without canceling would otherwise leak the delivery
// goroutine and its server-side subscription for the life of the process.
//
//	ctx, cancel := context.WithCancel(ctx)
//	defer cancel()                    // stops delivery, closes msgs
//	msgs, err := sub.Subscribe(ctx, "user.login")
//
// For storage rather than messaging see the sibling
// [github.com/the-protobuf-project/runtime-go/cache] and
// [github.com/the-protobuf-project/runtime-go/database] modules.
package streams

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a stream does not exist. Providers wrap it
	// with the stream ID for context, so callers test with errors.Is rather
	// than comparing formatted error strings.
	ErrNotFound = errors.New("streams: not found")

	// ErrUnknownSubject is returned when publishing or subscribing to a subject
	// the stream does not declare.
	//
	// Subjects are fixed at creation so that a typo fails loudly at the call
	// that made it, instead of silently producing a topic nobody reads.
	ErrUnknownSubject = errors.New("streams: subject not declared by this stream")
)

// Message is one payload published on a stream.
type Message struct {
	id   string
	Data any `json:"data"`

	// TTL is how long a notification waits before it fires. It is only
	// meaningful for [Notifier] messages, where delivery happens *when the TTL
	// expires* rather than immediately, and a provider rejects a zero TTL on
	// that path — see [Notifier].
	//
	// Ordinary [Publisher.Publish] delivers immediately and ignores this field.
	TTL time.Duration `json:"ttl,omitempty"`
}

// NewMessage builds a Message with its ID already set. Providers use it to
// return delivered messages: id is unexported, so it cannot be filled in from a
// struct literal outside this package.
func NewMessage(id string, data any, ttl time.Duration) Message {
	return Message{id: id, Data: data, TTL: ttl}
}

// ID returns the ID of the message. Providers assign one at publish time.
func (m *Message) ID() string {
	return m.id
}

// SetID sets the ID of the message, to publish under an ID of your own choosing
// rather than a generated one.
func (m *Message) SetID(id string) {
	m.id = id
}

// Stream is the metadata describing a stream and the subjects it accepts.
type Stream struct {
	id          string
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Subjects    []string `json:"subjects"`
	UserID      string   `json:"user_id"`
	Active      bool     `json:"active"`
}

// NewStream builds a Stream with its ID already set.
func NewStream(id string, s Stream) Stream {
	s.id = id
	return s
}

// ID returns the ID of the stream.
func (s *Stream) ID() string {
	return s.id
}

// SetID sets the ID of the stream.
func (s *Stream) SetID(id string) {
	s.id = id
}

// Publisher sends messages to a subject on a stream.
type Publisher interface {
	// Publish sends a message on a subject, exactly once. It does not block
	// waiting for subscribers, and it returns an error wrapping
	// [ErrUnknownSubject] when the stream does not declare the subject.
	Publish(ctx context.Context, subject string, msg Message) error
}

// Subscriber receives messages from a subject on a stream.
type Subscriber interface {
	// Subscribe returns a channel of messages for a subject.
	//
	// The subscription is active before Subscribe returns, so a message
	// published afterwards is delivered rather than raced. The channel is
	// closed when ctx is done — cancel it to stop delivery and release the
	// server-side subscription.
	//
	// It returns an error wrapping [ErrUnknownSubject] when the stream does not
	// declare the subject.
	Subscribe(ctx context.Context, subject string) (<-chan Message, error)
}

// Manager is a publisher and subscriber bound to one stream.
type Manager interface {
	Publisher
	Subscriber
}

// Streams is the lifecycle contract every provider satisfies.
type Streams interface {
	// Create declares a new stream and returns it with its assigned ID.
	Create(ctx context.Context, s Stream) (Stream, error)

	// Get retrieves a stream by ID, returning an error wrapping [ErrNotFound]
	// when no such stream exists.
	Get(ctx context.Context, id string) (Stream, error)

	// Bind returns a publisher and subscriber for an existing stream. It
	// returns an error wrapping [ErrNotFound] when the stream does not exist.
	Bind(ctx context.Context, id string) (Manager, error)

	// Update replaces a stream's configuration.
	Update(ctx context.Context, id string, s Stream) (Stream, error)

	// Delete removes a stream.
	Delete(ctx context.Context, id string) error

	// List returns every stream.
	List(ctx context.Context) ([]Stream, error)
}

// Notifier is implemented by providers that can deliver a message when its TTL
// expires rather than when it is published — a scheduled reminder, a lease
// timeout, a delayed retry.
//
// Delivery is the point of expiry, so a message with a zero TTL is rejected:
// it would never fire, and silently accepting it would strand the caller
// waiting for a notification that cannot arrive.
type Notifier interface {
	// Notifications returns the lifecycle interface for expiry-driven channels.
	// Streams created through it are bound with [Streams.Bind] exactly like
	// ordinary ones; the difference is only in when delivery happens.
	Notifications() Streams
}

// Closer is implemented by providers holding a resource of their own that must
// be released. Providers do not own the client they were handed, so most have
// nothing to close — closing that client stays the caller's job.
type Closer interface {
	Close() error
}
