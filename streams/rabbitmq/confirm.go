package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// publish sends every message to exchange under key and waits for the broker
// to confirm each, returning one error per message.
//
// They are all sent before any confirmation is awaited, so a batch costs one
// round trip rather than one per message — confirms arrive as the broker
// writes, not in lockstep with the sends.
//
// A message whose channel died before it was confirmed is sent again on the
// next connection when it was the connection that dropped: the broker may or
// may not have it, and at-least-once means assuming it does not. When the
// connection is still up the broker closed the channel itself — most often
// because the exchange is gone — and sending again would fail the same way, so
// that is returned with the broker's reason instead.
func (s *store) publish(ctx context.Context, exchange, key string, msgs []amqp.Publishing) []error {
	errs := make([]error, len(msgs))
	pending := make([]int, len(msgs))
	for i := range pending {
		pending[i] = i
	}

	for len(pending) > 0 {
		pub, err := s.publisher(ctx)
		if err != nil {
			for _, i := range pending {
				errs[i] = err
			}
			return errs
		}

		confirms := make([]*amqp.DeferredConfirmation, len(pending))
		for j, i := range pending {
			confirms[j], errs[i] = pub.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false, msgs[i])
		}

		var again []int
		for j, i := range pending {
			if errs[i] != nil {
				if pub.conn.IsClosed() && ctx.Err() == nil {
					again = append(again, i)
				}
				continue
			}
			acked, werr := confirms[j].WaitContext(ctx)
			switch {
			case werr != nil:
				errs[i] = werr
			case acked:
			case pub.conn.IsClosed():
				again = append(again, i)
			case pub.ch.IsClosed():
				errs[i] = fmt.Errorf("the broker closed the channel before confirming: %w", pub.reason())
			default:
				errs[i] = fmt.Errorf("the broker refused it (negative confirm)")
			}
		}
		pending = again
	}
	return errs
}

// publisher returns the channel publishes and declarations go through, opening
// one in confirm mode when there is none on the current connection.
//
// It is reopened rather than reused once closed, which also happens with the
// connection up: the broker closes a channel that publishes to an exchange
// that does not exist, and that one mistake must not end every publish after.
//
// A connection this package dialed can drop between being handed out and the
// channel opening on it; that waits for the next connection rather than
// failing the publish.
func (s *store) publisher(ctx context.Context) (*pubChannel, error) {
	for {
		conn, err := s.connection(ctx)
		if err != nil {
			return nil, err
		}
		pub, err := s.openPublisher(conn)
		if err == nil || !s.owned || !conn.IsClosed() {
			return pub, err
		}
	}
}

// openPublisher returns the publishing channel on conn, opening it if needed.
func (s *store) openPublisher(conn *amqp.Connection) (*pubChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pub != nil && !s.pub.ch.IsClosed() && s.pub.conn == conn {
		return s.pub, nil
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: cannot open a channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("rabbitmq: cannot enable publisher confirms: %w", err)
	}
	s.pub = &pubChannel{ch: ch, conn: conn, closed: ch.NotifyClose(make(chan *amqp.Error, 1))}
	return s.pub, nil
}

// pubChannel is the publishing channel with what is needed to explain its
// death: the connection it was opened on, and the broker's reason for closing
// it.
type pubChannel struct {
	ch     *amqp.Channel
	conn   *amqp.Connection
	closed <-chan *amqp.Error
}

// reason is why the broker closed the channel, when it said.
func (p *pubChannel) reason() error {
	select {
	case e, ok := <-p.closed:
		if ok && e != nil {
			return e
		}
	default:
	}
	return amqp.ErrClosed
}
