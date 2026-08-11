package core

import "github.com/the-protobuf-project/runtime-go/ulid"

// NewID returns a message id.
//
// It is time-ordered so that ids sort into the order they were created, which
// is what makes a listing readable and a log greppable without a separate
// timestamp to sort on.
func NewID() string { return ulid.Generate().GetTimeCode() }

// NewStreamID returns a stream id, suffixed with name when one is given.
//
// The suffix is for whoever is reading the keyspace in redis-cli or the stream
// list in nats-cli: a time code alone identifies a stream but says nothing
// about what it carries.
func NewStreamID(name string) string {
	id := ulid.Generate().GetTimeCode()
	if name != "" {
		id += ":" + name
	}
	return id
}
