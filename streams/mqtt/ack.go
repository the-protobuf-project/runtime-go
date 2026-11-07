package mqtt

import (
	"context"
	"fmt"

	"github.com/eclipse/paho.golang/paho"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

// received is one delivery and what it has to be acknowledged against: the
// packet, and the client of the connection it arrived on, which after a
// reconnect is no longer the current one.
type received struct {
	streams.Message
	client *paho.Client
	packet *paho.Publish
}

// mqttAck settles one delivery with a PUBACK.
type mqttAck struct {
	received
	metrics           *core.Metrics
	subject, consumer string
}

func (a *mqttAck) Ack(ctx context.Context) error {
	if err := a.client.Ack(a.packet); err != nil {
		return fmt.Errorf("mqtt: cannot acknowledge on %q; the broker will deliver it again: %w", a.subject, err)
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "ack")
	return nil
}

// Nak leaves the message unacknowledged: the broker keeps it against this
// session and delivers it again when the consumer reconnects. MQTT has no
// negative acknowledgement, so there is nothing more truthful to do here.
func (a *mqttAck) Nak(ctx context.Context) error {
	a.metrics.Settled(ctx, a.subject, a.consumer, "nak")
	return nil
}
