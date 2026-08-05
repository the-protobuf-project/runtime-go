package streams

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a stream does not exist. Providers wrap it
	// with the stream id for context, so callers test with errors.Is rather
	// than comparing formatted error strings.
	ErrNotFound = errors.New("streams: not found")

	// ErrUnknownSubject is returned when publishing or subscribing to a subject
	// the stream does not declare.
	//
	// Subjects are fixed at creation so a typo fails loudly at the call that
	// made it, instead of silently producing a topic nobody reads.
	ErrUnknownSubject = errors.New("streams: subject not declared by this stream")

	// ErrUnsupported is returned when a provider cannot do what was asked —
	// scheduling a delivery on a stream that only publishes immediately, for
	// one.
	//
	// It is a distinct sentinel because it is a settled answer: unlike a
	// dropped connection, retrying cannot make the provider capable, so
	// [WithPublisherRetry] gives up on it immediately.
	ErrUnsupported = errors.New("streams: unsupported by this provider")
)

// Stream is the metadata describing a stream and the subjects it accepts.
//
// This is configuration, not payload — it stays a defined type because every
// provider needs the same facts to create a stream.
type Stream struct {
	// ID identifies the stream. Providers assign one when Create is given an
	// empty ID.
	ID string `json:"id"`

	Name        string   `json:"name"`
	Description string   `json:"description"`
	Subjects    []string `json:"subjects"`
	UserID      string   `json:"user_id"`
	Active      bool     `json:"active"`
}

// Message is one delivered payload.
//
// Data is left encoded so a subscriber decodes into its own model with
// [Message.Decode] rather than receiving a shape this package chose.
type Message struct {
	// ID is the publisher-assigned identifier.
	ID string

	// Subject is the subject it arrived on.
	Subject string

	// Data is the encoded payload. Prefer [Message.Decode] over reading it.
	Data []byte
}

// Decode unmarshals the payload into dest, which must be a non-nil pointer.
func (m Message) Decode(dest any) error {
	return decode(m.Data, dest)
}

// Options are the per-operation settings a provider understands. A provider
// ignores what it cannot honor, so a call written against one backend still
// compiles and runs against another.
type Options struct {
	// TTL delays delivery until it elapses, for providers that can schedule.
	// Zero publishes immediately.
	TTL time.Duration

	// ID publishes under a chosen identifier instead of a generated one.
	ID string
}

// Option configures one operation.
type Option func(*Options)

// TTL delays delivery until the duration elapses — a scheduled reminder, a
// lease timeout. Providers that cannot schedule reject a non-zero TTL rather
// than silently publishing immediately.
func TTL(d time.Duration) Option {
	return func(o *Options) { o.TTL = d }
}

// ID publishes under a chosen identifier instead of a generated one.
func ID(id string) Option {
	return func(o *Options) { o.ID = id }
}

// NewOptions folds opts into a single Options. Providers call this rather than
// re-implementing the same loop.
func NewOptions(opts ...Option) Options {
	var o Options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Publisher sends values to a subject on a stream.
type Publisher interface {
	// Publish sends a value on a subject, exactly once. It does not block
	// waiting for subscribers, and returns an error wrapping
	// [ErrUnknownSubject] when the stream does not declare the subject.
	Publish(ctx context.Context, subject string, value any, opts ...Option) (string, error)
}

// Subscriber receives messages from a subject on a stream.
type Subscriber interface {
	// Subscribe returns a channel of messages for a subject.
	//
	// The subscription is active before Subscribe returns, so a value published
	// afterwards is delivered rather than raced. The channel is closed when ctx
	// is done — cancel it to stop delivery and release the server-side
	// subscription.
	Subscribe(ctx context.Context, subject string) (<-chan Message, error)
}

// Manager is a publisher and subscriber bound to one stream.
type Manager interface {
	Publisher
	Subscriber
}

// Streams is the lifecycle contract every provider satisfies.
type Streams interface {
	// Create declares a stream and returns it with its assigned ID.
	Create(ctx context.Context, s Stream) (Stream, error)

	// Get retrieves a stream by id, returning an error wrapping [ErrNotFound]
	// when no such stream exists.
	Get(ctx context.Context, id string) (Stream, error)

	// Bind returns a publisher and subscriber for an existing stream.
	Bind(ctx context.Context, id string) (Manager, error)

	// Update replaces a stream's configuration.
	Update(ctx context.Context, id string, s Stream) (Stream, error)

	// Delete removes a stream.
	Delete(ctx context.Context, id string) error

	// List returns every stream.
	List(ctx context.Context) ([]Stream, error)
}

// Closer is implemented by providers holding a resource of their own that must
// be released. Providers do not own the client they were handed, so most have
// nothing to close — closing that client stays the caller's job.
type Closer interface {
	Close() error
}
