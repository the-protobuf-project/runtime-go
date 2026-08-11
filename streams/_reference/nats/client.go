// Package nats provides a wrapper around a live NATS connection and optional
// JetStream context. It simplifies connecting, authenticating, and configuring
// NATS and JetStream, and exposes grouped managers for streams, consumers,
// producers, and key-value stores.
package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/machanirobotics/loom/go/nats/options"
	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/machanirobotics/loom/go/nats/template"
	"github.com/machanirobotics/loom/go/nats/types"
)

// NatsService holds a live NATS connection and, if enabled, a JetStream context.
type NatsService struct {
	Nats            natsManager
	Jetstream       jetstreamManager
	StreamRetention options.RegularStreamRetention
}

// NewNatsClient connects to a NATS server using the provided options,
// or environment-backed defaults if none are given.
// NewNatsClient connects to NATS and (optionally) wires JetStream.
func NewNatsClient(in ...options.NatsClientOptions) (*NatsService, error) {
	// Accept zero or one options.
	var raw options.NatsClientOptions
	if len(in) > 0 {
		raw = in[0]
	} else {
		// No options provided → default to enabling JetStream.
		raw.EnableJetStream = true
		shared.Pulse.Logger.Warn("no options provided; enabling JetStream by default")
	}

	cfg, connOpts, err := raw.Validate()
	if err != nil {
		shared.Pulse.Logger.Errorf("options validation failed: %v", err)
		return nil, err
	}

	shared.Pulse.Logger.Debugf("creating client (url=%s name=%s js=%t)", cfg.URL, cfg.Name, cfg.EnableJetStream)

	nc, err := nats.Connect(cfg.URL, connOpts...)
	if err != nil {
		shared.Pulse.Logger.Errorf("connect failed: %v", err)
		return nil, err
	}

	svc := &NatsService{
		Nats:            &natsHandler{client: nc},
		StreamRetention: cfg.RegularStreamRetention,
	}

	if cfg.EnableJetStream {
		js, err := jetstream.New(nc)
		if err != nil {
			_ = nc.Drain()
			nc.Close()
			shared.Pulse.Logger.Errorf("jetstream init failed: %v", err)
			return nil, err
		}

		// (Optional but recommended) Verify JS is actually enabled on the server:
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := js.AccountInfo(ctx); err != nil {
			_ = nc.Drain()
			nc.Close()
			shared.Pulse.Logger.Errorf("jetstream not enabled on server: %v", err)
			return nil, fmt.Errorf("jetstream not enabled on server: %w", err)
		}

		jm, err := newJetstreamManager(js)
		if err != nil {
			_ = nc.Drain()
			nc.Close()
			shared.Pulse.Logger.Errorf("jetstream manager init failed: %v", err)
			return nil, err
		}
		svc.Jetstream = jm
		shared.Pulse.Logger.Debug("jetstream manager initialized")
	}

	shared.Pulse.Logger.Debug("client created successfully")
	return svc, nil
}

// RegularStream builds a JetStream stream config using StreamRetention from validated options (zero = unbounded).
func (s *NatsService) RegularStream(name string, subjects []string) types.StreamConfig {
	if s == nil {
		return types.StreamConfig{}
	}
	return template.RegularStream(name, subjects, s.StreamRetention)
}

// Close gracefully closes the NATS connection (drain then close).
// Safe to call on nil service or already-closed connections.
func (s *NatsService) Close() {
	if s == nil || s.Nats == nil {
		return
	}
	c := s.Nats.Conn()
	if c == nil {
		return
	}

	// Try a graceful drain with a short deadline.
	done := make(chan struct{})
	go func() {
		_ = c.Drain()
		close(done)
	}()

	select {
	case <-done:
		shared.Pulse.Logger.Debug("connection drained")
	case <-time.After(3 * time.Second):
		shared.Pulse.Logger.Warn("drain timed out; closing immediately")
	}

	c.Close()
	shared.Pulse.Logger.Debug("connection closed")
	shared.Close()
}
