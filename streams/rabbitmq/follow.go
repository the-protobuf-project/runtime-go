package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// follow hands every delivery to handle until ctx is done, attaching again
// whenever the channel it was reading from closes.
//
// A channel closes under a consumer when the connection drops, or when the
// broker closes it — a queue deleted from under it, a consumer cancelled by an
// operator. In each case the consumer is still wanted, so it waits for a
// connection and attaches again rather than ending a channel the caller is
// ranging over.
//
// It returns once ctx is done, a handler reports it, the store has been
// closed, or the connection given to [Use] has closed — which nothing here can
// bring back.
func (m *manager) follow(ctx context.Context, subject string, a attachment,
	attach func() (attachment, error), handle func(amqp.Delivery) bool,
) {
	delivered := 0
	defer func() {
		m.store.log.Info(ctx, "consumer stopped", telemetry.Fields{"subject": subject, "delivered": delivered})
	}()

	for {
		for d := range a.deliveries {
			delivered++
			if !handle(d) {
				_ = a.ch.Close()
				return
			}
		}
		_ = a.ch.Close()
		if ctx.Err() != nil {
			return
		}

		m.store.log.Warn(ctx, "consumer interrupted; attaching again", telemetry.Fields{"subject": subject})
		for {
			var err error
			if a, err = attach(); err == nil {
				break
			}
			if ctx.Err() != nil || errors.Is(err, errGone) || errors.Is(err, errClosed) {
				return
			}
			m.store.log.Warn(ctx, "could not attach again", telemetry.Fields{"subject": subject, "error": err.Error()})
			select {
			case <-ctx.Done():
				return
			case <-time.After(m.store.cfg.retry):
			}
		}
	}
}
