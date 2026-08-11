package nats

import "github.com/nats-io/nats.go"

// NatsManager defines the operations exposed by the base NATS connection.
type natsManager interface {
	// Conn returns the underlying *nats.Conn for low-level operations.
	Conn() *nats.Conn
}

// natsHandler is the concrete implementation of NatsManager.
type natsHandler struct {
	client *nats.Conn
}

// Conn returns the underlying *nats.Conn.
func (h *natsHandler) Conn() *nats.Conn {
	return h.client
}
