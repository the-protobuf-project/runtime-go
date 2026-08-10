package streams

import (
	"context"
	"fmt"
)

// Providers are not equally capable, and pretending otherwise is how a contract
// starts lying. [Publisher] and [Subscriber] are the honest intersection — send
// a value, receive it, no promise that it survives a restart — because that is
// what Redis Pub/Sub and core NATS actually do.
//
// What follows is everything past that intersection. A provider implements one
// only if its backend has it; a caller reaching for one a provider lacks gets
// [ErrUnsupported] naming the provider, rather than a silent downgrade to
// weaker delivery than it asked for. That downgrade is the failure worth
// designing against: a program that believes it has at-least-once delivery and
// actually has at-most-once loses messages under exactly the conditions it
// added the durability for.

// Durable is implemented by a provider that remembers a consumer's position and
// redelivers until a message is acknowledged.
//
// This is the difference between Redis Pub/Sub and everything else. A plain
// [Subscriber] hands over a channel: a message read from it is gone, and a
// subscriber that dies mid-work loses whatever it was holding. A Durable
// consumer is named, its position outlives the process, and a message stays
// deliverable until someone says otherwise.
//
// NATS JetStream and Kafka both have it. Redis Pub/Sub does not — it has no
// stored log to resume from, so the capability is absent rather than
// approximated with a client-side buffer that would lose the same messages one
// layer higher up.
type Durable interface {
	// Consume delivers messages under a named consumer whose position the
	// server keeps.
	//
	// The name is the identity that survives a restart: two processes consuming
	// under one name share the position and split the work, and a process that
	// dies and comes back resumes where the name left off rather than where the
	// process did. It is required for that reason — a generated one would make
	// every restart a new consumer and every restart replay or skip.
	//
	// The channel closes when ctx is done. Messages in flight at that point are
	// not acknowledged, so they are redelivered — which is the contract doing
	// what it promised rather than a leak.
	Consume(ctx context.Context, subject, consumer string, opts ...Option) (<-chan Delivery, error)
}

// Delivery is a message that has to be settled.
//
// It is a distinct type from [Message] because the difference is the whole
// point: a Message has been handed over, and a Delivery has been lent. A
// consumer that treats one as the other either loses work or replays it
// forever, and a compiler that cannot tell them apart offers no help.
type Delivery struct {
	Message

	// Ack reports the message as handled, so it is not delivered again.
	//
	// Call it after the work is done rather than on receipt: acknowledging
	// first turns at-least-once into at-most-once, which is the guarantee a
	// caller was avoiding by reaching for [Durable].
	Ack func(ctx context.Context) error

	// Nak returns the message for redelivery, for work that failed in a way a
	// retry could fix.
	//
	// A message naked forever is a poison message, and neither backend here
	// decides on its own when to stop trying. A consumer that cannot make
	// progress on a message should Ack it and record the failure somewhere it
	// will be seen.
	Nak func(ctx context.Context) error

	// Attempt is how many times this message has been delivered, starting at 1.
	//
	// It is the one signal a consumer needs to break a redelivery loop it cannot
	// otherwise detect: the same bytes arriving for the fifth time look exactly
	// like the first. Zero where a provider cannot count.
	Attempt int
}

// Positioned is implemented by a provider whose stored log can be read from
// somewhere other than now.
//
// Kafka and JetStream both retain messages and can replay them; Redis Pub/Sub
// has nothing to replay. Separate from [Durable] because they are separate
// questions — a consumer can be durable and still only ever want new messages,
// and a one-off backfill can want history without a named consumer.
type Positioned interface {
	// ConsumeFrom is [Durable.Consume] starting at a chosen position.
	ConsumeFrom(ctx context.Context, subject, consumer string, at Position, opts ...Option) (<-chan Delivery, error)
}

// Position is where a consumer starts reading.
type Position int

const (
	// FromNew starts at messages published after the consumer attaches, and is
	// the zero value because it is the only position every backend can offer and
	// the only one that cannot surprise a caller with a flood of history.
	FromNew Position = iota

	// FromEarliest starts at the oldest message still retained.
	//
	// How far back that reaches is the server's retention, not this contract's:
	// a topic keeping an hour and one keeping a year both answer to this, and
	// the difference is not something a provider can report.
	FromEarliest
)

// AsDurable returns m's durable-consumer half.
//
// It exists so a provider that cannot do this fails with a sentence rather than
// a bare false — the caller asked for redelivery, and "this provider does not
// keep a log to redeliver from" is the answer, not `ok == false` fifty lines
// from the wiring that chose the provider.
func AsDurable(m Manager) (Durable, error) {
	d, ok := m.(Durable)
	if !ok {
		return nil, fmt.Errorf(
			"%w: this provider delivers a message once and does not remember whether it was handled; "+
				"reach for NATS JetStream or Kafka where redelivery matters", ErrUnsupported)
	}
	return d, nil
}

// AsPositioned returns m's replay half, on the same terms as [AsDurable].
func AsPositioned(m Manager) (Positioned, error) {
	p, ok := m.(Positioned)
	if !ok {
		return nil, fmt.Errorf(
			"%w: this provider keeps no log to read from, so there is no position but now", ErrUnsupported)
	}
	return p, nil
}
