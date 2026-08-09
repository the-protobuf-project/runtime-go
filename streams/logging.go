package streams

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// loggingPublisher records every publish.
type loggingPublisher struct {
	next Publisher
	log  telemetry.Logger
}

// WithPublisherLogging logs each publish: a debug record on success and an
// error record on failure.
//
// The logger is injected rather than resolved from a package-level global, so a
// binary that never wires logging pays nothing. Pass [telemetry.NoopLogger] to
// disable logging without unwrapping.
//
//	pub = streams.WithPublisherLogging(m, log.With(telemetry.Fields{"component": "streams"}))
func WithPublisherLogging(next Publisher, log telemetry.Logger) Publisher {
	if log == nil {
		log = telemetry.NoopLogger
	}
	return &loggingPublisher{next: next, log: log}
}

// WithPublisherLoggingMiddleware is [WithPublisherLogging] as a middleware.
func WithPublisherLoggingMiddleware(log telemetry.Logger) PublisherMiddleware {
	return func(p Publisher) Publisher { return WithPublisherLogging(p, log) }
}

// Publish logs the outcome.
//
// An undeclared subject is logged at error rather than warn: it is a
// programming mistake — the stream's subjects are fixed at creation — and the
// message was not delivered anywhere.
func (l *loggingPublisher) Publish(ctx context.Context, subject string, value any, opts ...Option) (string, error) {
	start := time.Now()
	id, err := l.next.Publish(ctx, subject, value, opts...)

	fields := telemetry.Fields{
		"subject":  subject,
		"duration": time.Since(start).String(),
	}
	if id != "" {
		fields["id"] = id
	}
	if o := NewOptions(opts...); o.TTL > 0 {
		// Present only on the scheduled path, where it is the whole point: the
		// message fires when this elapses.
		fields["ttl"] = o.TTL.String()
	}

	switch {
	case err == nil:
		l.log.Debug(ctx, "published", fields)
	case errors.Is(err, ErrUnknownSubject):
		l.log.Error(ctx, "publish rejected: subject not declared by this stream", err, fields)
	default:
		l.log.Error(ctx, "publish failed", err, fields)
	}
	return id, err
}

// loggingSubscriber records subscription lifecycle and delivery.
type loggingSubscriber struct {
	next Subscriber
	log  telemetry.Logger
}

// WithSubscriberLogging logs when a subscription opens, each message it
// delivers, and when it closes.
//
// The close record is the useful one: a subscription ends only when its context
// is canceled, so its absence in a log is how you spot a consumer that leaked.
func WithSubscriberLogging(next Subscriber, log telemetry.Logger) Subscriber {
	if log == nil {
		log = telemetry.NoopLogger
	}
	return &loggingSubscriber{next: next, log: log}
}

func (l *loggingSubscriber) Subscribe(ctx context.Context, subject string) (<-chan Message, error) {
	msgs, err := l.next.Subscribe(ctx, subject)
	if err != nil {
		l.log.Error(ctx, "subscribe failed", err, telemetry.Fields{"subject": subject})
		return nil, err
	}
	l.log.Info(ctx, "subscribed", telemetry.Fields{"subject": subject})

	// The messages are relayed through a goroutine so delivery can be counted
	// and the close observed. It inherits the caller's cancellation: when the
	// upstream channel closes, this one does too, so the decorator cannot be
	// the thing that leaks.
	out := make(chan Message)
	go func() {
		defer close(out)

		delivered := 0
		defer func() {
			l.log.Info(ctx, "subscription closed", telemetry.Fields{
				"subject":   subject,
				"delivered": delivered,
			})
		}()

		for msg := range msgs {
			delivered++
			if l.log.Enabled(ctx, telemetry.LevelDebug) {
				l.log.Debug(ctx, "delivered", telemetry.Fields{
					"subject": subject,
					"id":      msg.ID,
				})
			}
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
