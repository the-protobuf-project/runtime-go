package options

import "time"

// RegularStreamRetention configures JetStream max_age / max_bytes.
// Zero MaxAge and MaxBytes means an unbounded stream (no retention limits).
type RegularStreamRetention struct {
	MaxAge   time.Duration
	MaxBytes int64
}

// NatsClientOptions configures the NATS client connection.
type NatsClientOptions struct {
	// URL is the NATS server URL (e.g., nats://localhost:4222).
	// This field is required.
	URL string

	// Name is an optional client/service name shown on the NATS server.
	Name string

	// Auth holds basic username/password authentication.
	Auth NatsAuthOptions

	// EnableJetStream indicates whether callers intend to use JetStream.
	// (This does not change the base connection options; it’s a hint for callers.)
	EnableJetStream bool

	// RegularStreamRetention sets JetStream limits. All zero → unbounded.
	RegularStreamRetention RegularStreamRetention
}

// NatsAuthOptions contains authentication details for NATS.
type NatsAuthOptions struct {
	// Username for NATS authentication.
	Username string
	// Password for NATS authentication.
	Password string
}
