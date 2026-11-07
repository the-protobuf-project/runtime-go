package mqtt

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// open starts a connection that keeps itself up, and returns once the first
// one is made and, when filter is set, subscribed.
//
// resume says whether the first connection picks up a session the broker
// already holds for this id — what a named consumer does on restart — and
// expiry is how long the broker keeps the session after a connection drops.
// Kept, a session carries the subscription and every QoS 1 message in either
// direction that was not yet acknowledged, so a reconnect loses nothing.
//
// Every time the connection comes up, the broker says whether it still had the
// session. When it did, the subscription came back with it and subscribing
// again is not just redundant — it races the messages the broker is
// redelivering, and a broker that shares one packet-identifier space across
// both directions rejects it as an identifier already in use. When it did not
// — a clean start, an undurable session after a drop, an expired one — the
// subscription is made again. That is what lets a subscriber survive a broker
// restart without the caller noticing.
//
// This is safe because a client id identifies a consumer *and* a subject (see
// consumerID): a session that exists is a session already subscribed to
// exactly this filter.
//
// The first attempt is not retried: a broker that is unreachable or refuses
// the credentials is an error now, at the call that asked for it, rather than
// a connection retrying in the background forever.
func (s *store) open(ctx context.Context, clientID string, resume bool, expiry uint32, filter string,
	onPublish func(paho.PublishReceived) (bool, error),
) (*autopaho.ConnectionManager, error) {
	if s.server == nil {
		return nil, fmt.Errorf("%w: subscribing needs a connection of its own; pass WithServer to Use", streams.ErrUnsupported)
	}

	// The first result decides open; later ones land in the buffer, unread.
	first := make(chan error, 1)
	report := func(err error) {
		select {
		case first <- err:
		default:
		}
	}

	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{s.server},
		TlsCfg:                        s.cfg.tls,
		KeepAlive:                     30,
		CleanStartOnInitialConnection: !resume,
		SessionExpiryInterval:         expiry,
		ReconnectBackoff:              autopaho.NewConstantBackoff(s.cfg.retry),
		ConnectTimeout:                s.cfg.dial,
		ConnectUsername:               s.cfg.username,
		ConnectPassword:               s.cfg.password,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ack *paho.Connack) {
			// It must not block, and subscribing waits on the broker.
			go func() { report(s.restore(cm, ack, filter, clientID)) }()
		},
		OnConnectError: func(err error) {
			s.log.Warn(context.Background(), "could not connect", telemetry.Fields{
				"client_id": clientID, "server": s.server.Redacted(), "error": err.Error(),
			})
			report(err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			// Acknowledgement belongs to the consumer, after the work. Without
			// this paho acknowledges on receipt, which would turn at-least-once
			// into at-most-once behind the caller's back.
			EnableManualAcknowledgment: true,
		},
	}
	if onPublish != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPublish}
	}

	// The connection outlives this call, so it is not bound to ctx; it ends at
	// disconnect.
	cm, err := autopaho.NewConnection(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("mqtt: cannot configure a connection to %s: %w", s.server.Redacted(), err)
	}

	wait, cancel := context.WithTimeout(ctx, s.cfg.dial)
	defer cancel()
	select {
	case err = <-first:
	case <-wait.Done():
		err = wait.Err()
	}
	if err != nil {
		_ = s.disconnect(cm)
		return nil, fmt.Errorf("mqtt: cannot connect to %s: %w", s.server.Redacted(), err)
	}
	return cm, nil
}

// restore subscribes a connection that has just come up, unless the broker
// kept its session and so its subscription.
func (s *store) restore(cm *autopaho.ConnectionManager, ack *paho.Connack, filter, clientID string) error {
	if filter == "" {
		return nil
	}
	if ack.SessionPresent {
		s.log.Debug(context.Background(), "resumed a session; its subscription was restored",
			telemetry.Fields{"filter": filter, "client_id": clientID})
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.dial)
	defer cancel()
	suback, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: s.cfg.qos}},
	})
	if err != nil {
		// The broker's reason code says why, and paho's error text does not
		// carry it — a refused filter and a quota exceeded look identical
		// without this.
		if suback != nil && len(suback.Reasons) > 0 {
			err = fmt.Errorf("broker reason %d: %w", suback.Reasons[0], err)
		}
		s.log.Error(ctx, "could not subscribe", err, telemetry.Fields{"filter": filter, "client_id": clientID})
		return fmt.Errorf("cannot subscribe to %q: %w", filter, err)
	}
	return nil
}

// disconnect ends a connection and stops it reconnecting.
func (s *store) disconnect(cm *autopaho.ConnectionManager) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cm.Disconnect(ctx); err != nil {
		return fmt.Errorf("mqtt: cannot disconnect: %w", err)
	}
	return nil
}
