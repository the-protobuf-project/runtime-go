//go:build live

package main

// MQTT. plain is docker/compose.mqtt.yaml; tls is
// docker/compose.mqtt.secure.yaml's password listener, reached as the e2e
// suite reaches it: mutual TLS (the test client certificate, trusting the test
// CA), then the e2e-app user's password, under the broker's ACLs.
//
// Those ACLs give each user only <user>/#, so the tls profile publishes under
// the prefix e2e-app/bench rather than bench: the topic tree is the one the
// user owns, and the broker's authorization stays exactly what the e2e suite
// proves it is.
//
// MQTT_HOST/MQTT_PORT (plain), MQTT_TLS_ADDR (default 127.0.0.1:18884), and
// STREAMS_E2E_TLS_DIR (default docker/tls/out) point it elsewhere.

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

// mqttUser is the e2e suite's password-listener user, from
// docker/compose.mqtt.secure.yaml.
var mqttUser = [2]string{"e2e-app", "e2e-app-secret"}

// mqttAddress is the broker address and the options profile needs to reach
// it, shared with the opcua-pubsub backend, which runs on the same brokers.
func mqttAddress(backend, profile string) (string, []mqtt.Option, error) {
	switch profile {
	case "plain":
		return net.JoinHostPort(env("MQTT_HOST", "127.0.0.1"), env("MQTT_PORT", "11883")), nil, nil
	case "tls":
		cfg, err := mutualTLS(certDir("STREAMS_E2E_TLS_DIR"))
		if err != nil {
			return "", nil, err
		}
		return env("MQTT_TLS_ADDR", "127.0.0.1:18884"), []mqtt.Option{
			mqtt.WithTLS(cfg),
			mqtt.WithCredentials(mqttUser[0], mqttUser[1]),
		}, nil
	default:
		return "", nil, unknownProfile(backend, profile)
	}
}

// connectMQTT connects at QoS 1. tls publishes under e2e-app/bench, the only
// tree the broker's ACLs give that user.
func connectMQTT(ctx context.Context, profile string) (*conn, error) {
	addr, opts, err := mqttAddress("mqtt", profile)
	if err != nil {
		return nil, err
	}
	prefix := "bench"
	if profile == "tls" {
		prefix = mqttUser[0] + "/bench"
	}
	opts = append(opts,
		mqtt.WithPrefix(prefix),
		mqtt.WithQoS(1),
		mqtt.WithClientID("bench"),
		mqtt.WithCodec(protobuf.Codec),
	)
	if n, ok := lookup("MQTT_PUBLISH_CONNECTIONS"); ok {
		v, perr := strconv.Atoi(n)
		if perr != nil {
			return nil, fmt.Errorf("MQTT_PUBLISH_CONNECTIONS: %w", perr)
		}
		opts = append(opts, mqtt.WithPublishConnections(v))
	}
	s, err := mqtt.Connect(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return one(s), nil
}
