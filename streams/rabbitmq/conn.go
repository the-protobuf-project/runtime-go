package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// errClosed is a store that has been closed.
var errClosed = errors.New("rabbitmq: closed")

// errGone is a connection [Use] was given that has closed. This package did
// not dial it, so it cannot dial it again.
var errGone = errors.New("rabbitmq: the connection given to Use has closed, and is not this package's to redial")

// dial opens a connection and registers for its close, before anything else
// can close it unobserved.
//
// amqp091 can recover a connection on its own, and that is deliberately not
// used. It recovers channels in place, and the broker numbers deliveries on a
// recovered channel from one again — so acknowledging a message received
// before the drop would acknowledge a different message received after it,
// losing the one that was never handled. Every reconnection here opens fresh
// channels instead, and an acknowledgement for an old delivery fails on its
// closed channel while the broker hands that message out again.
func (s *store) dial(ctx context.Context) (*amqp.Connection, <-chan *amqp.Error, error) {
	cfg := amqp.Config{
		TLSClientConfig: s.cfg.tls,
		Properties:      amqp.Table{"connection_name": s.name},
		// amqp.Dial has no context form, so ctx bounds the TCP dial and a
		// deadline on the connection bounds the TLS and AMQP handshake after
		// it. amqp091 clears that deadline once the connection is open.
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: dialTimeout}
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if err := conn.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		},
	}
	if s.cfg.username != "" {
		cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: s.cfg.username, Password: s.cfg.password}}
	}

	conn, err := amqp.DialConfig(s.url, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("rabbitmq: cannot reach the broker: %w", err)
	}
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	return conn, closed, nil
}

// supervise waits for a connection to drop and dials until a new one is up,
// for as long as the store is open.
//
// While it is down, publishers and consumers waiting in [store.connection]
// block on their own contexts rather than failing into the gap.
func (s *store) supervise(closed <-chan *amqp.Error) {
	for {
		reason := <-closed

		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			return
		}
		s.up = make(chan struct{})
		s.mu.Unlock()

		fields := telemetry.Fields{"reason": fmt.Sprint(reason)}
		s.log.Warn(context.Background(), "connection lost; dialing again", fields)

		for {
			select {
			case <-s.done:
				return
			case <-time.After(s.cfg.retry):
			}

			ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
			conn, next, err := s.dial(ctx)
			cancel()
			if err != nil {
				s.log.Warn(context.Background(), "could not reconnect", telemetry.Fields{"error": err.Error()})
				continue
			}

			s.mu.Lock()
			if s.closing {
				s.mu.Unlock()
				_ = conn.Close()
				return
			}
			s.conn, s.pub, closed = conn, nil, next
			close(s.up)
			s.mu.Unlock()

			s.log.Info(context.Background(), "reconnected", nil)
			break
		}
	}
}

// connection returns the current connection, waiting for one while the store
// is reconnecting.
func (s *store) connection(ctx context.Context) (*amqp.Connection, error) {
	for {
		s.mu.RLock()
		conn, up, closing := s.conn, s.up, s.closing
		s.mu.RUnlock()

		switch {
		case closing:
			return nil, errClosed
		case !s.owned && conn.IsClosed():
			return nil, errGone
		}

		select {
		case <-up:
			if !conn.IsClosed() || !s.owned {
				return conn, nil
			}
			// Closed, and the supervisor has not noticed yet; it will
			// replace up, so wait for it to.
			select {
			case <-time.After(10 * time.Millisecond):
			case <-s.done:
				return nil, errClosed
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case <-s.done:
			return nil, errClosed
		case <-ctx.Done():
			return nil, fmt.Errorf("rabbitmq: not connected: %w", ctx.Err())
		}
	}
}
