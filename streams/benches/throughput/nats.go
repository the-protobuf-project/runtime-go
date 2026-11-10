//go:build live

package main

// NATS core and JetStream.
//
//   - plain is docker/compose.nats.yaml (NATS_URL, default
//     nats://127.0.0.1:14222).
//   - tls is the nats service of docker/compose.nats.secure.yaml
//     (NATS_E2E_URL, default tls://127.0.0.1:14223), reached as the e2e suite
//     reaches it: mutual TLS with docker/tls/out's client certificate
//     (NATS_E2E_CERTS overrides the directory), then the test-only password
//     user, whose permissions include the bench's bench.> subjects. Start the
//     broker with STREAMS_DURABILITY=prod for the server's default fsync
//     interval, or leave it unset for an fsync on every write.

import (
	"context"
	"path/filepath"
	"time"

	gonats "github.com/nats-io/nats.go"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/nats"
)

// connectNATS is core NATS or JetStream, with separate publishing and
// subscribing connections; flush flushes the publisher for publish-only runs.
func connectNATS(_ context.Context, jetstream bool, profile string) (*conn, error) {
	// Publishers and subscribers get a connection each, as nats bench runs
	// them and as a service that both produces and consumes heavily would:
	// sharing one makes PubAcks queue behind deliveries.
	pub, err := dialNATS(profile)
	if err != nil {
		return nil, err
	}
	sub, err := dialNATS(profile)
	if err != nil {
		pub.Close()
		return nil, err
	}
	c := &conn{
		// nats.go flushes only under a deadline, and a minute is far past
		// any flush that is making progress.
		flush: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			return pub.FlushWithContext(ctx)
		},
		release: func() { pub.Close(); sub.Close() },
	}
	if !jetstream {
		c.publisher = nats.Use(pub, nats.WithCodec(protobuf.Codec))
		c.subscriber = nats.Use(sub, nats.WithCodec(protobuf.Codec))
		return c, nil
	}
	var p, s streams.Streams
	if p, err = nats.UseJetStream(pub, nats.WithCodec(protobuf.Codec)); err == nil {
		s, err = nats.UseJetStream(sub, nats.WithCodec(protobuf.Codec))
	}
	if err != nil {
		c.release()
		return nil, err
	}
	c.publisher, c.subscriber = p, s
	return c, nil
}

// dialNATS opens one connection for profile: plain, or mutual TLS plus the
// test password user.
func dialNATS(profile string) (*gonats.Conn, error) {
	switch profile {
	case "plain":
		return gonats.Connect(env("NATS_URL", "nats://127.0.0.1:14222"))
	case "tls":
		certs := certDir("NATS_E2E_CERTS")
		return gonats.Connect(env("NATS_E2E_URL", "tls://127.0.0.1:14223"),
			gonats.RootCAs(filepath.Join(certs, "ca.crt")),
			gonats.ClientCert(filepath.Join(certs, "client.crt"), filepath.Join(certs, "client.key")),
			gonats.UserInfo("e2e-password", "e2e-test-only-password"),
		)
	default:
		return nil, unknownProfile("nats", profile)
	}
}
