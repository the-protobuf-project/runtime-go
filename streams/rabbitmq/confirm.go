package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
//
// Every call takes the next publishing channel in turn and keeps to it, so
// concurrent callers' confirms do not queue behind one another in one broker
// channel process, while one call's messages, a batch, stay in order.
func (s *store) publish(ctx context.Context, exchange, key string, msgs []amqp.Publishing) []error {
	slot := s.nextSlot()
	errs := make([]error, len(msgs))
	pending := make([]int, len(msgs))
	for i := range pending {
		pending[i] = i
	}

	for len(pending) > 0 {
		pub, err := s.publisher(ctx, slot)
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

// nextSlot is the publishing channel the next publish takes, round-robin.
func (s *store) nextSlot() int {
	n := len(s.pubs)
	if n == 1 {
		return 0
	}
	return int(s.next.Add(1) % uint64(n))
}

// publisher returns publishing channel slot, opening one in confirm mode when
// there is none on the current connection. Slot 0 also declares and deletes
// exchanges.
//
// It is reopened rather than reused once closed, which also happens with the
// connection up: the broker closes a channel that publishes to an exchange
// that does not exist, and that one mistake must not end every publish after.
// Each slot is reopened on its own, so such a mistake costs the publishes in
// flight on one channel rather than on all of them.
//
// A connection this package dialed can drop between being handed out and the
// channel opening on it; that waits for the next connection rather than
// failing the publish.
func (s *store) publisher(ctx context.Context, slot int) (*pubChannel, error) {
	for {
		conn, err := s.connection(ctx)
		if err != nil {
			return nil, err
		}
		pub, err := s.openPublisher(conn, slot)
		if err == nil || !s.owned || !conn.IsClosed() {
			return pub, err
		}
	}
}

// openPublisher returns publishing channel slot on conn, opening it if needed.
//
// The usual case, a channel already open, takes only a read lock, so
// concurrent publishes on different channels do not wait on each other here.
func (s *store) openPublisher(conn *amqp.Connection, slot int) (*pubChannel, error) {
	s.mu.RLock()
	pub := s.pubs[slot]
	s.mu.RUnlock()
	if pub.usable(conn) {
		return pub, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, errClosed
	}
	if pub := s.pubs[slot]; pub.usable(conn) {
		// Another caller reopened it first; theirs wins.
		return pub, nil
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: cannot open a channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("rabbitmq: cannot enable publisher confirms: %w", err)
	}
	s.pubs[slot] = &pubChannel{ch: ch, conn: conn, closed: ch.NotifyClose(make(chan *amqp.Error, 1))}
	return s.pubs[slot], nil
}

// closePublishers closes every publishing channel, for Close. The connection
// is left alone: on a store built by [Use] it is the caller's.
func (s *store) closePublishers() error {
	s.mu.Lock()
	pubs := slices.Clone(s.pubs)
	clear(s.pubs)
	s.mu.Unlock()

	var errs []error
	for _, pub := range pubs {
		if pub == nil {
			continue
		}
		// Already shut, by the broker or the connection going away, is what
		// closing wanted.
		if err := pub.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, fmt.Errorf("rabbitmq: cannot close a publishing channel: %w", err))
		}
	}
	return errors.Join(errs...)
}

// pubChannel is the publishing channel with what is needed to explain its
// death: the connection it was opened on, and the broker's reason for closing
// it.
type pubChannel struct {
	ch     *amqp.Channel
	conn   *amqp.Connection
	closed <-chan *amqp.Error
}

// usable reports whether p is an open channel on conn.
func (p *pubChannel) usable(conn *amqp.Connection) bool {
	return p != nil && p.conn == conn && !p.ch.IsClosed()
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
