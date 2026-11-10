package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/internal/batchconn"
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
// Every time the connection comes up, the filter is subscribed, whether or not
// the broker kept the session. A client id names a consumer and a subject (see
// consumerID), not a stream, so a session kept from an earlier stream under the
// same consumer and subject, by this process, another, or runtime-rs, is
// subscribed to that stream's topic, not this one; skipping the SUBSCRIBE
// because a session exists would leave this consumer deaf. Subscribing again to
// a filter the session already holds replaces it in place and keeps the queued
// messages. When the broker kept nothing (a clean start, an undurable session
// after a drop, an expired one) the same SUBSCRIBE is what lets a subscriber
// survive a broker restart without the caller noticing.
//
// The first attempt is not retried: a broker that is unreachable or refuses
// the credentials is an error now, at the call that asked for it, rather than
// a connection retrying in the background forever.
func (s *store) open(ctx context.Context, clientID string, resume bool, expiry uint32, filter string,
	onPublish func(paho.PublishReceived) (bool, error), tune tuning,
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

	// Set once the broker hands the session to another connection, when
	// tune.onTakenOver asks for that to end this one.
	var taken atomic.Bool

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
			go func() { report(s.restore(cm, ack, filter, clientID, tune.retainHandling)) }()
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
			// The publish timeout bounds how long a QoS 1 publish waits for
			// its PUBACK once it is on the wire.
			PacketTimeout: s.cfg.publishTimeout,
		},
	}
	// Writes are gathered across packets, as runtime-rs's client gathers
	// them: paho writes each packet on its own, which over TLS is two records
	// and two system calls per packet, PUBACKs included.
	if dial := batchconn.Dialer(s.server, schemes[s.server.Scheme], s.cfg.tls); dial != nil {
		cfg.AttemptConnection = dial
	}
	if tune.session != nil {
		cfg.Session = tune.session
	}
	if tune.onTakenOver != nil {
		cfg.OnServerDisconnect = func(d *paho.Disconnect) {
			if d.ReasonCode != reasonSessionTakenOver {
				return
			}
			taken.Store(true)
			s.log.Warn(context.Background(), "the broker gave this session to another connection",
				telemetry.Fields{"client_id": clientID, "server": s.server.Redacted()})
			tune.onTakenOver()
		}
	}
	receiveMax := tune.receiveMax
	cfg.ConnectPacketBuilder = func(cp *paho.Connect, _ *url.URL) (*paho.Connect, error) {
		if taken.Load() {
			// Dialing again would take the session back from the connection
			// that took it, and the two would trade it for as long as both
			// run.
			return nil, errSessionTakenOver
		}
		if receiveMax > 0 {
			// The broker holds back what the consumer has not caught up
			// with, in the session's queue, instead of pushing it all down
			// the socket. That is what makes the prefetch mean something: a
			// consumer owes at most that many acknowledgements at once.
			if cp.Properties == nil {
				cp.Properties = &paho.ConnectProperties{}
			}
			cp.Properties.ReceiveMaximum = &receiveMax
		}
		if cp.Properties != nil {
			// Absent, Request Problem Information means yes; paho writes a
			// properties block with it as no, and a broker may then strip
			// user properties (mochi does), which is where headers travel.
			cp.Properties.RequestProblemInfo = true
		}
		return cp, nil
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

// tuning is what differs between the connections open makes beyond their id
// and session: the publishing pool's session wrapper, a durable consumer's
// Receive Maximum, and how its subscription treats retained messages.
type tuning struct {
	// session replaces autopaho's in-memory session state when set; the
	// publishing connections use it to pipeline a batch.
	session session.SessionManager

	// receiveMax, when non-zero, is the MQTT 5 Receive Maximum sent in
	// CONNECT: how many unacknowledged messages the broker may have out to
	// this client at once.
	receiveMax uint16

	// retainHandling is the subscription's MQTT 5 Retain Handling option: 0
	// sends retained messages on every SUBSCRIBE, 1 only when the
	// subscription is new.
	retainHandling byte

	// onTakenOver, when set, is called when the broker disconnects the
	// connection because another took its session, and stops it being dialed
	// again.
	onTakenOver func()
}

// reasonSessionTakenOver is the reason code (0x8E) of the DISCONNECT a broker
// sends a connection whose client id another connection has just claimed.
const reasonSessionTakenOver = 0x8E

// errSessionTakenOver stops a connection whose session was taken from being
// dialed again.
var errSessionTakenOver = errors.New("mqtt: another connection took this session")

// errPacketIDInUse is the reason code (0x91) a broker answers a SUBSCRIBE with
// when its packet identifier is one the broker is still using.
const errPacketIDInUse = 0x91

// restore subscribes a connection that has just come up.
//
// When the session was resumed the broker is redelivering what it kept as the
// SUBSCRIBE goes out, and a broker that shares one packet-identifier space
// across both directions (mochi does) refuses a SUBSCRIBE whose identifier one
// of those redeliveries holds, with reason 0x91. paho allocates identifiers
// round the space, so asking again takes the next one; it is asked again until
// one is free or the dial timeout passes.
func (s *store) restore(cm *autopaho.ConnectionManager, ack *paho.Connack, filter, clientID string, retainHandling byte) error {
	if filter == "" {
		return nil
	}
	if ack.SessionPresent {
		s.log.Debug(context.Background(), "resumed a session; subscribing again",
			telemetry.Fields{"filter": filter, "client_id": clientID})
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.dial)
	defer cancel()
	for {
		suback, err := cm.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: s.cfg.qos, RetainHandling: retainHandling}},
		})
		if err == nil {
			return nil
		}
		if suback != nil && len(suback.Reasons) > 0 && suback.Reasons[0] == errPacketIDInUse && ctx.Err() == nil {
			continue
		}
		// The broker's reason code says why, and paho's error text does not
		// carry it — a refused filter and a quota exceeded look identical
		// without this.
		if suback != nil && len(suback.Reasons) > 0 {
			err = fmt.Errorf("broker reason %d: %w", suback.Reasons[0], err)
		}
		s.log.Error(ctx, "could not subscribe", err, telemetry.Fields{"filter": filter, "client_id": clientID})
		return fmt.Errorf("cannot subscribe to %q: %w", filter, err)
	}
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
