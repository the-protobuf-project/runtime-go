//go:build live

package main

// Kafka. plain is docker/compose.kafka.yaml; tls is the mutual-TLS listener of
// docker/compose.kafka.secure.yaml, connecting as the e2e suite does with its
// primary (mTLS) principal: TLS verified against docker/tls/out/ca.crt, client
// certificate presented.
//
// KAFKA_ADDR (plain, default 127.0.0.1:19092), KAFKA_E2E_ADDR (tls, default
// 127.0.0.1:19093) and STREAMS_E2E_TLS_DIR point it elsewhere;
// KAFKA_PRODUCERS sets the producer pool (default 8).

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/kafka"
)

// connectKafka connects plain or mutual-TLS tls with ten partitions per topic,
// so subjects spread across partitions.
func connectKafka(ctx context.Context, profile string) (*conn, error) {
	var seed string
	var client []kgo.Opt
	switch profile {
	case "plain":
		seed = env("KAFKA_ADDR", "127.0.0.1:19092")
	case "tls":
		seed = env("KAFKA_E2E_ADDR", "127.0.0.1:19093")
		cfg, err := mutualTLS(certDir("STREAMS_E2E_TLS_DIR"))
		if err != nil {
			return nil, err
		}
		client = append(client, kgo.DialTLSConfig(cfg))
	default:
		return nil, unknownProfile("kafka", profile)
	}
	producers, err := strconv.Atoi(env("KAFKA_PRODUCERS", "8"))
	if err != nil {
		return nil, fmt.Errorf("KAFKA_PRODUCERS: %w", err)
	}
	s, err := kafka.Connect(ctx, []string{seed},
		kafka.WithPrefix("bench"),
		kafka.WithPartitions(10),
		kafka.WithProducers(producers),
		kafka.WithCodec(protobuf.Codec),
		kafka.WithClientOptions(client...),
	)
	if err != nil {
		return nil, err
	}
	return one(s), nil
}
