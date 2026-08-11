package redis

import "time"

// Document represents a user-defined data payload along with configuration
// options such as TTL. The Data field can contain any type that is JSON-
// serializable.
type Document struct {
	id   string        // ID of the document
	Data interface{}   `json:"data"`          // Data payload of the document
	TTL  time.Duration `json:"ttl,omitempty"` // Time-to-live in seconds; 0 means no expiration
}

// ID returns the ID of the document.
func (d *Document) ID() string {
	return d.id
}

// SetID sets the ID of the document.
func (d *Document) SetID(id string) {
	d.id = id
}

// Stream represents a Redis stream with its metadata and configuration.
// It is used to manage the lifecycle of streams and their associated publishers/subscribers.
type Stream struct {
	id          string   // ID of the stream
	Name        string   `json:"name"`        // Name of the stream
	Description string   `json:"description"` // Description of the stream
	Subjects    []string `json:"subjects"`    // Subjects associated with the stream
	UserID      string   `json:"user_id"`     // User ID associated with the stream
	Active      bool     `json:"active"`      // Whether the stream is active
}

// ID returns the ID of the stream.
func (s *Stream) ID() string {
	return s.id
}

// SetID sets the ID of the stream.
func (s *Stream) SetID(id string) {
	s.id = id
}
