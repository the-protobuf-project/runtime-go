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
