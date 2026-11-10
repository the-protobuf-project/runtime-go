//go:build live

package main

// RabbitMQ. plain is docker/compose.rabbitmq.yaml; tls is
// docker/compose.rabbitmq.secure.yaml, reached as the e2e suite reaches it:
// AMQPS with mutual TLS, the client certificate logging in through SASL
// EXTERNAL.
//
// AMQP_URL overrides the plain URL; RABBITMQ_E2E_ADDR (default
// 127.0.0.1:15671) and STREAMS_E2E_TLS_DIR the secure broker and its
// certificates. RABBITMQ_PUBLISH_CHANNELS sets the publishing channel pool
// (default: the driver's).

import (
	"context"
	"fmt"
	"strconv"

	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

// connectRabbitMQ connects over AMQP, or AMQPS with SASL EXTERNAL for tls.
func connectRabbitMQ(ctx context.Context, profile string) (*conn, error) {
	opts := []rabbitmq.Option{rabbitmq.WithPrefix("bench"), rabbitmq.WithCodec(protobuf.Codec)}
	var url string
	switch profile {
	case "plain":
		url = env("AMQP_URL", "amqp://guest:guest@127.0.0.1:15682/%2f")
	case "tls":
		url = "amqps://" + env("RABBITMQ_E2E_ADDR", "127.0.0.1:15671") + "/%2f?auth_mechanism=external"
		cfg, err := mutualTLS(certDir("STREAMS_E2E_TLS_DIR"))
		if err != nil {
			return nil, err
		}
		opts = append(opts, rabbitmq.WithTLS(cfg))
	default:
		return nil, unknownProfile("rabbitmq", profile)
	}
	if n, ok := lookup("RABBITMQ_PUBLISH_CHANNELS"); ok {
		channels, err := strconv.Atoi(n)
		if err != nil {
			return nil, fmt.Errorf("RABBITMQ_PUBLISH_CHANNELS: %q", n)
		}
		opts = append(opts, rabbitmq.WithPublishChannels(channels))
	}
	s, err := rabbitmq.Connect(ctx, url, opts...)
	if err != nil {
		return nil, err
	}
	return one(s), nil
}
