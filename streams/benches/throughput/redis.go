//go:build live

package main

// Redis pub/sub and Streams.
//
//   - plain is docker/compose.redis.yaml (REDIS_ADDR, default
//     127.0.0.1:16380).
//   - tls is docker/compose.redis.secure.yaml, reached as the e2e suite
//     reaches it: mutual TLS on REDIS_TLS_ADDR (default 127.0.0.1:16381) with
//     the certificates in REDIS_TLS_CERTS (default docker/tls/out), logged in
//     as the ACL user streams, database 3. That user may touch only keys and
//     channels under e2e* prefixes, so this profile names its keys
//     e2ebench:... rather than widening the ACL for the bench.

import (
	"context"

	goredis "github.com/redis/go-redis/v9"

	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/redis"
)

// connectRedis is pub/sub, or Streams when durable.
func connectRedis(ctx context.Context, durable bool, profile string) (*conn, error) {
	var opts *goredis.Options
	var prefix string
	switch profile {
	case "plain":
		opts, prefix = &goredis.Options{Addr: env("REDIS_ADDR", "127.0.0.1:16380")}, "bench"
	case "tls":
		cfg, err := mutualTLS(env("REDIS_TLS_CERTS", certDir("STREAMS_E2E_TLS_DIR")))
		if err != nil {
			return nil, err
		}
		// The e2e broker's published, test-only credentials.
		opts = &goredis.Options{ //nolint:gosec // see above
			Addr:      env("REDIS_TLS_ADDR", "127.0.0.1:16381"),
			Username:  "streams",
			Password:  "streams-e2e-not-a-secret",
			DB:        3,
			TLSConfig: cfg,
		}
		prefix = "e2ebench"
	default:
		return nil, unknownProfile("redis", profile)
	}
	rdb := goredis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	use := redis.Use
	if durable {
		use = redis.UseDurable
	}
	// Acks are queued rather than awaited, as the Rust harness runs them;
	// REDIS_CONFIRM_ACKS=1 measures the default, an ack that waits for XACK.
	s := use(rdb, redis.WithPrefix(prefix), redis.WithCodec(protobuf.Codec),
		redis.WithConfirmAcks(env("REDIS_CONFIRM_ACKS", "0") == "1"))
	c := one(s)
	c.release = func() { _ = rdb.Close() }
	return c, nil
}
