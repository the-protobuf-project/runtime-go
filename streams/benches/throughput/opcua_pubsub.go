//go:build live

package main

// OPC UA PubSub over MQTT (opcua-pubsub), on the same brokers as the mqtt
// backend so the two compare like with like: plain is
// docker/compose.mqtt.yaml, tls is docker/compose.mqtt.secure.yaml's password
// listener (mutual TLS, the e2e-app user, its ACLs, so the Part 14 prefix is
// e2e-app/bench/opcua).
//
// Run it with --message opcua --subject-prefix pubsub.: the payload is a typed
// OPC UA value, as on the client/server backend, and every bench subject is a
// DataSetWriter of its own with one field, v. The subjects are mapped here,
// pubsub.bench.s00 to pubsub.bench.s00/v, so each writer has its own topic and
// sequence, as each subject has its own topic on mqtt.
//
// The provider is runtime-go/streams/opcua/pubsub, publishing as "bench" at
// QoS 1 with its defaults otherwise: four publishing connections and up to
// 100 DataSetMessages per NetworkMessage of a batch.

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/opcua/pubsub"
)

// connectOPCUAPubSub connects the PubSub provider and wraps it so every bench
// subject is its own one-field DataSetWriter.
func connectOPCUAPubSub(ctx context.Context, profile string) (*conn, error) {
	addr, _, err := mqttAddress("opcua-pubsub", profile)
	if err != nil {
		return nil, err
	}
	prefix := "bench/opcua"
	if profile == "tls" {
		prefix = mqttUser[0] + "/bench/opcua"
	}
	s, err := dialPubSub(ctx, addr, prefix, profile)
	if err != nil {
		return nil, err
	}
	return one(oneFieldWriters{s}), nil
}

// dialPubSub connects an OPC UA PubSub provider publishing as "bench" under
// prefix on the MQTT broker at addr. The tls profile dials as mqttAddress's
// options do, mutual TLS and the e2e-app user, rebuilt here because an
// mqtt.Option configures only the mqtt package.
func dialPubSub(ctx context.Context, addr, prefix, profile string) (streams.Streams, error) {
	opts := []pubsub.Option{
		pubsub.WithPublisherID("bench"),
		pubsub.WithPrefix(prefix),
		pubsub.WithQoS(1),
		pubsub.WithClientID("bench-pubsub"),
	}
	if profile == "tls" {
		cfg, err := mutualTLS(certDir("STREAMS_E2E_TLS_DIR"))
		if err != nil {
			return nil, err
		}
		opts = append(opts, pubsub.WithTLS(cfg), pubsub.WithCredentials(mqttUser[0], mqttUser[1]))
	}
	p, err := pubsub.Connect(ctx, addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("opcua-pubsub: %w", err)
	}
	return p, nil
}

// oneFieldWriters treats each declared subject as a DataSetWriter, published
// and read as its one field v.
type oneFieldWriters struct {
	streams.Streams
}

// Bind wraps the bound manager so its publishes and subscriptions map to
// field v.
func (w oneFieldWriters) Bind(ctx context.Context, id string) (streams.Manager, error) {
	m, err := w.Streams.Bind(ctx, id)
	if err != nil {
		return nil, err
	}
	return field{m}, nil
}

// Close closes the wrapped provider, which owns its MQTT connections.
func (w oneFieldWriters) Close() error {
	if c, ok := w.Streams.(streams.Closer); ok {
		return c.Close()
	}
	return nil
}

// field is a manager whose subjects are each a writer's field v.
type field struct {
	m streams.Manager
}

var _ streams.Batch = field{}

// fieldOf is the PubSub subject for a bench subject: its writer's single
// field v.
func fieldOf(subject string) string {
	return subject + "/v"
}

// Publish publishes to the subject's field v.
func (f field) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	return f.m.Publish(ctx, fieldOf(subject), value, opts...)
}

// Subscribe subscribes to the subject's field v.
func (f field) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	return f.m.Subscribe(ctx, fieldOf(subject), opts...)
}

// PublishBatch sends the batch to field v as the provider's NetworkMessages;
// it fails if the bound manager has no batch support.
func (f field) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	b, err := streams.AsBatch(f.m)
	if err != nil {
		return nil, err
	}
	return b.PublishBatch(ctx, fieldOf(subject), values, opts...)
}
