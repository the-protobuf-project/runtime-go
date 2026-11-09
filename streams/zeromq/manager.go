package zeromq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// manager publishes to and subscribes from one stream.
type manager struct {
	store  *store
	stream streams.Stream
}

var _ streams.Manager = (*manager)(nil)

// checkSubject rejects a subject the stream does not declare.
//
// A ZeroMQ subscription is a byte prefix rather than a pattern, so a subject is
// matched by name — as on Redis and Kafka.
func (m *manager) checkSubject(ctx context.Context, subject string) error {
	if core.Declares(m.stream.Subjects, subject) {
		return nil
	}
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// envelope is the wire form: the topic frame first, so a SUB socket can filter
// on it without decoding the payload, then the packed message.
//
// Two frames rather than one prefixed blob so a SUB socket filters on the
// topic without touching the payload. ZeroMQ's filter is still a byte prefix,
// so "s|user" also passes "s|users"; the receive loop compares the whole frame.
func (m *manager) topic(subject string) string {
	return m.stream.ID + "|" + subject
}

// prepare checks everything a publish or batch on subject with o depends on,
// and returns the headers to send: the stream's defaults with the publish's
// own on top.
//
// Each refusal is made before anything is sent, so a refused batch sends
// nothing.
func (m *manager) prepare(ctx context.Context, subject string, o streams.Options) (streams.Headers, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if m.store.pub == nil {
		return nil, fmt.Errorf("%w: this provider was built with Subscribe and can only receive; use Publish to bind an endpoint", streams.ErrUnsupported)
	}
	if o.TTL > 0 {
		return nil, fmt.Errorf("%w: ZeroMQ delivers on send and cannot schedule", streams.ErrUnsupported)
	}
	if o.Group != "" {
		// PUB/SUB fans out; every subscriber gets every message. Sharing a
		// subject would need a different socket pair (PUSH/PULL), which is a
		// different topology rather than an option on this one.
		return nil, fmt.Errorf("%w: ZeroMQ PUB/SUB fans out to every subscriber and cannot share a subject", streams.ErrUnsupported)
	}
	if o.Expires > 0 {
		return nil, fmt.Errorf("%w: ZeroMQ has no queue: a message reaches the subscribers connected when it is sent or nobody, so there is nothing to expire (%v)", streams.ErrUnsupported, o.Expires)
	}
	if o.Retain {
		return nil, fmt.Errorf("%w: ZeroMQ keeps nothing once a message is sent, so it cannot retain a last value for later subscribers", streams.ErrUnsupported)
	}
	return core.Outgoing(m.stream, o)
}

// send writes one value as a topic frame and a packed frame: version 1, as
// before, unless there are headers to carry, which ride in a version-2 frame
// because ZeroMQ frames carry bytes and nothing else.
func (m *manager) send(ctx context.Context, subject, id string, value any, headers streams.Headers) error {
	if m.store.closed() {
		return errClosed
	}
	body, err := core.PackWith(m.store.codec, id, value, headers)
	if err != nil {
		return err
	}

	if err := m.store.pub.SendMulti(zmq4.NewMsgFrom([]byte(m.topic(subject)), body)); err != nil {
		m.store.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return fmt.Errorf("zeromq: cannot publish on %q: %w", subject, err)
	}

	m.store.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return nil
}

// Publish sends a value on a subject, with its headers in the frame (version
// 2) when there are any.
//
// It returns [streams.ErrUnsupported] on a provider built by [Subscribe]: a SUB
// socket cannot send, and refusing beats accepting a message that would go
// nowhere. A TTL, a group, an expiry and retain are refused the same way: there
// is no queue to hold a message in.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	o := streams.NewOptions(opts...)
	headers, err := m.prepare(ctx, subject, o)
	if err != nil {
		return "", err
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}
	if err := m.send(ctx, subject, id, value, headers); err != nil {
		return "", err
	}
	return id, nil
}

// Subscribe returns a channel of messages for a subject.
//
// The channel is closed when ctx is done or the provider is closed, either of
// which releases the socket. A dropped publisher is a pause rather than the
// end: the socket redials it and resends the subscription.
//
// A value published in the moment after this returns may still be missed; see
// the package documentation on the slow-joiner problem, and [WithSettle].
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	o := streams.NewOptions(opts...)
	if o.Group != "" {
		return nil, fmt.Errorf("%w: ZeroMQ PUB/SUB fans out to every subscriber and cannot share a subject", streams.ErrUnsupported)
	}
	if m.store.closed() {
		return nil, errClosed
	}

	// Redial without end once a peer drops: ZeroMQ's own sockets reconnect,
	// and a restarted publisher should find its subscribers again. Closing
	// the socket cancels its context, which is what stops the redialing.
	sock := zmq4.NewSub(ctx, zmq4.WithAutomaticReconnect(true), zmq4.WithDialerMaxRetries(-1))
	if err := sock.Dial(m.store.endpoint); err != nil {
		_ = sock.Close()
		return nil, fmt.Errorf("zeromq: cannot connect to %s: %w", m.store.endpoint, err)
	}

	topic := m.topic(subject)
	if err := sock.SetOption(zmq4.OptionSubscribe, topic); err != nil {
		_ = sock.Close()
		return nil, fmt.Errorf("zeromq: cannot subscribe to %q: %w", subject, err)
	}
	if !m.store.track(sock) {
		return nil, errClosed
	}

	// The subscription is on its way to the publisher and nothing will confirm
	// it arrived. Waiting narrows the window; see the package documentation.
	select {
	case <-time.After(m.store.cfg.settle):
	case <-ctx.Done():
		_ = sock.Close()
		return nil, ctx.Err()
	case <-m.store.done:
		return nil, errClosed
	}

	m.store.log.Info(ctx, "subscribed", telemetry.Fields{
		"subject": subject, "endpoint": m.store.endpoint, "topic": topic,
	})

	out := make(chan streams.Message, core.Prefetch(o))
	go func() {
		defer close(out)

		// Recv blocks with no context of its own, so cancellation has to reach
		// it by closing the socket underneath it.
		stop := context.AfterFunc(ctx, func() { _ = sock.Close() })
		defer stop()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			msg, err := sock.Recv()
			if err != nil {
				// A closed socket is how a canceled subscription ends.
				if ctx.Err() != nil || m.store.closed() || errors.Is(err, context.Canceled) {
					return
				}
				// A dropped peer: the socket redials it on its own, so this
				// is a pause rather than the end of the subscription.
				m.store.log.Debug(ctx, "receive failed", telemetry.Fields{"subject": subject, "error": err.Error()})
				continue
			}
			if len(msg.Frames) < 2 {
				m.store.log.Warn(ctx, "dropping a message with no payload frame",
					telemetry.Fields{"subject": subject, "frames": len(msg.Frames)})
				continue
			}
			// SUB filtering is by prefix, so subscribing to "s|user" also
			// receives "s|users". Comparing the whole frame keeps the subject
			// exact without changing what goes on the wire.
			if string(msg.Frames[0]) != topic {
				continue
			}

			decoded, derr := core.Unpack(m.store.registry, subject, msg.Frames[1])
			if derr != nil {
				// One bad message is not a reason to tear down a healthy
				// subscription.
				m.store.log.Warn(ctx, "dropping a malformed message",
					telemetry.Fields{"subject": subject, "error": derr.Error()})
				continue
			}

			delivered++
			select {
			case out <- decoded:
			case <-ctx.Done():
				return
			case <-m.store.done:
				return
			}
		}
	}()
	return out, nil
}

// PublishBatch sends several values on a subject, in order, after checking the
// options and headers once.
//
// ZeroMQ sends without waiting for a subscriber, so publishing in turn does not
// block per message.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	headers, err := m.prepare(ctx, subject, o)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(values))
	var failures []error
	for i, value := range values {
		ids[i] = core.NewID()
		if serr := m.send(ctx, subject, ids[i], value, headers); serr != nil {
			failures = append(failures, fmt.Errorf("entry %d: %w", i, serr))
		}
	}
	return ids, core.BatchError(subject, len(values), failures)
}
