package pubsub

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/internal/batchconn"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// contentType is what Part 14 JSON messages are (7.3.5.2), and the MQTT 5
// content type says so.
const contentType = "application/json"

// maxExpiry is the largest Message Expiry Interval MQTT 5 carries.
const maxExpiry = uint64(^uint32(0))

// linger is how long a closing connection waits for its DISCONNECT to go out.
const linger = time.Second

// schemes are the address schemes autopaho dials, by whether they are secure.
var schemes = map[string]bool{
	"mqtt": false, "tcp": false, "ws": false,
	"tls": true, "ssl": true, "mqtts": true, "mqtt+ssl": true, "tcps": true, "wss": true,
}

// parseServer turns an address into the URL a connection dials, as the mqtt
// package does: host:port is plain MQTT unless TLS is configured, and a port
// left off is 1883 in the clear and 8883 over TLS.
func parseServer(address string, cfg config) (*url.URL, error) {
	if address == "" {
		return nil, errors.New("opcua pubsub: no broker address given")
	}
	if !strings.Contains(address, "://") {
		scheme := "mqtt"
		if cfg.tls != nil {
			scheme = "tls"
		}
		address = scheme + "://" + address
	}
	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("opcua pubsub: %q is not a broker address: %w", address, err)
	}
	secure, ok := schemes[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("opcua pubsub: %q uses the %q scheme; use mqtt://, tls://, mqtts://, ws:// or wss://", address, u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("opcua pubsub: %q names no host", address)
	}
	if cfg.tls != nil && !secure {
		return nil, fmt.Errorf("opcua pubsub: a TLS configuration was given for %q, which would connect in the clear; use tls:// or wss://", address)
	}
	if u.Port() == "" && u.Scheme != "ws" && u.Scheme != "wss" {
		port := "1883"
		if secure {
			port = "8883"
		}
		u.Host = net.JoinHostPort(u.Hostname(), port)
	}
	return u, nil
}

// connection is how one of the provider's connections behaves.
type connection struct {
	clientID string
	// redial keeps the connection up across drops, which the publishing
	// connections do. A subscription's does not: a clean session dialed
	// again has lost what was published meanwhile, and an ended stream says
	// so.
	redial bool
	// onPublish receives every PUBLISH, in order, on paho's reading
	// goroutine.
	onPublish func(paho.PublishReceived) (bool, error)
	// onDown is called once when a connection that does not redial is lost.
	onDown func()
}

// dial opens a clean connection and returns once the broker has accepted
// it. The first attempt is not retried: a broker that is unreachable or
// refuses the credentials is an error now, at the call that asked.
func (p *Provider) dial(ctx context.Context, c connection) (*autopaho.ConnectionManager, error) {
	first := make(chan error, 1)
	report := func(err error) {
		select {
		case first <- err:
		default:
		}
	}
	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{p.server},
		TlsCfg:                        p.cfg.tls,
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		ReconnectBackoff:              autopaho.NewConstantBackoff(reconnectDelay),
		ConnectTimeout:                p.cfg.dial,
		ConnectUsername:               p.cfg.username,
		ConnectPassword:               p.cfg.password,
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			report(nil)
		},
		OnConnectionDown: func() bool {
			if !c.redial && c.onDown != nil {
				go c.onDown()
			}
			return c.redial
		},
		OnConnectError: func(err error) {
			p.log.Warn(context.Background(), "opcua pubsub: could not connect", telemetry.Fields{
				"client_id": c.clientID, "server": p.server.Redacted(), "error": err.Error(),
			})
			report(err)
		},
		ClientConfig: paho.ClientConfig{ClientID: c.clientID},
	}
	// Writes are gathered across packets, as in the mqtt package.
	if dial := batchconn.Dialer(p.server, schemes[p.server.Scheme], p.cfg.tls); dial != nil {
		cfg.AttemptConnection = dial
	}
	if c.onPublish != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){c.onPublish}
	}
	cm, err := autopaho.NewConnection(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("opcua pubsub: cannot configure a connection to %s: %w", p.server.Redacted(), err)
	}
	wait, cancel := context.WithTimeout(ctx, p.cfg.dial)
	defer cancel()
	select {
	case err = <-first:
	case <-wait.Done():
		err = wait.Err()
	}
	if err != nil {
		disconnect(cm)
		return nil, fmt.Errorf("opcua pubsub: cannot connect to %s: %w", p.server.Redacted(), err)
	}
	return cm, nil
}

// disconnect ends a connection and stops it reconnecting.
func disconnect(cm *autopaho.ConnectionManager) {
	ctx, cancel := context.WithTimeout(context.Background(), linger)
	defer cancel()
	_ = cm.Disconnect(ctx)
}

// subscribeAll subscribes a connection to filters, returning once the broker
// has granted every one.
func (p *Provider) subscribeAll(ctx context.Context, cm *autopaho.ConnectionManager, filters []string) error {
	subs := make([]paho.SubscribeOptions, len(filters))
	for i, f := range filters {
		subs[i] = paho.SubscribeOptions{Topic: f, QoS: p.cfg.qos}
	}
	wait, cancel := context.WithTimeout(ctx, p.cfg.dial)
	defer cancel()
	suback, err := cm.Subscribe(wait, &paho.Subscribe{Subscriptions: subs})
	if suback != nil {
		for i, code := range suback.Reasons {
			if code >= 0x80 && i < len(filters) {
				return fmt.Errorf("the broker refused the subscription to %q with reason 0x%02x (%s)", filters[i], code, reasonName(code))
			}
		}
	}
	return err
}

// link is the publishing connection a topic always takes, so one writer's
// messages stay in sequence order on the wire. The hash is unseeded, so
// every publish and every provider makes the same choice.
func (p *Provider) link(topic string) *autopaho.ConnectionManager {
	if len(p.links) == 1 {
		return p.links[0]
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(topic))
	return p.links[h.Sum64()%uint64(len(p.links))]
}

// send publishes once a connection is up, waiting at most the publish
// timeout for the broker's acknowledgement, and again on the next connection
// when the packet never reached the wire.
func (p *Provider) send(ctx context.Context, msg *paho.Publish) error {
	if p.closed.Load() {
		return errors.New("the provider is closed")
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.publishTimeout)
	defer cancel()
	cm := p.link(msg.Topic)
	for {
		if err := cm.AwaitConnection(ctx); err != nil {
			return fmt.Errorf("not connected: %w", err)
		}
		resp, err := cm.Publish(ctx, msg)
		if resp != nil && resp.ReasonCode >= 0x80 {
			// paho's text says "Not authorized"; the reason code's name is
			// what the broker's documentation and the Rust provider use.
			return fmt.Errorf("the broker refused the publish with reason 0x%02x (%s): %w",
				resp.ReasonCode, reasonName(resp.ReasonCode), err)
		}
		if !errors.Is(err, autopaho.ConnectionDownError) && !errors.Is(err, session.ErrNoConnection) {
			return err
		}
		if p.closed.Load() {
			return errors.New("the provider is closed")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("not connected: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// reasonName is an MQTT 5 reason code's name.
func reasonName(code byte) string {
	switch code {
	case 0x10:
		return "NoMatchingSubscribers"
	case 0x80:
		return "UnspecifiedError"
	case 0x83:
		return "ImplementationSpecificError"
	case 0x87:
		return "NotAuthorized"
	case 0x8f:
		return "TopicFilterInvalid"
	case 0x90:
		return "TopicNameInvalid"
	case 0x91:
		return "PacketIdentifierInUse"
	case 0x97:
		return "QuotaExceeded"
	case 0x99:
		return "PayloadFormatInvalid"
	case 0x9e:
		return "SharedSubscriptionsNotSupported"
	}
	return "Unknown"
}

// properties are a Part 14 message's PUBLISH properties: JSON's content type
// and payload format, the headers as MQTT 5 user properties in key order,
// and expires as the Message Expiry Interval, rounded up to whole seconds.
// The Part 14 JSON itself is never touched.
func properties(headers streams.Headers, expires time.Duration) *paho.PublishProperties {
	format := byte(1)
	props := &paho.PublishProperties{ContentType: contentType, PayloadFormat: &format}
	for _, k := range headers.Keys() {
		props.User = append(props.User, paho.UserProperty{Key: k, Value: headers[k]})
	}
	if expires > 0 {
		secs := uint64(expires / time.Second)
		if expires%time.Second != 0 {
			secs++
		}
		secs = max(1, min(secs, maxExpiry))
		interval := uint32(secs)
		props.MessageExpiry = &interval
	}
	return props
}

// headersOf are the headers a PUBLISH arrived with: its MQTT 5 user
// properties. MQTT allows a key more than once, which [streams.Headers] does
// not; the last one wins, as a publish's header wins over its stream's. Nil
// when there are none.
func headersOf(msg *paho.Publish) streams.Headers {
	if msg.Properties == nil || len(msg.Properties.User) == 0 {
		return nil
	}
	h := make(streams.Headers, len(msg.Properties.User))
	for _, u := range msg.Properties.User {
		h[u.Key] = u.Value
	}
	return h
}
