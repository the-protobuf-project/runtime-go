//go:build live

package main

// How the bench reaches each backend. One file per backend, so each can grow
// its own profiles (plain, tls, ...) without touching the others.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// conn is a connected provider, plus a second one for backends whose
// publishing and subscribing sides are separate objects.
type conn struct {
	publisher  streams.Streams
	subscriber streams.Streams

	// flush pushes out whatever the publishing connection still buffers. A
	// publish-only run awaits it before stopping the clock, since a backend
	// whose publish returns once the message is queued in the client (core
	// NATS) would otherwise be timed before its bytes left.
	flush func(context.Context) error

	// release closes what the target opened itself rather than through a
	// provider that owns it: a client handed to a provider's Use.
	release func()
}

// one is a backend whose one provider both publishes and subscribes, with no
// flush.
func one(s streams.Streams) *conn {
	return &conn{publisher: s}
}

// close closes the providers that own a resource, then whatever the target
// opened itself.
func (c *conn) close() {
	for _, s := range []streams.Streams{c.publisher, c.subscriber} {
		if closer, ok := s.(streams.Closer); ok {
			_ = closer.Close()
		}
	}
	if c.release != nil {
		c.release()
	}
}

// connect connects backend with profile. Backend names: kafka, nats (core),
// jetstream, rabbitmq, redis (pub/sub), redis-durable, mqtt, zeromq, opcua,
// opcua-pubsub.
func connect(ctx context.Context, backend, profile string) (*conn, error) {
	switch backend {
	case "kafka":
		return connectKafka(ctx, profile)
	case "nats":
		return connectNATS(ctx, false, profile)
	case "jetstream":
		return connectNATS(ctx, true, profile)
	case "rabbitmq":
		return connectRabbitMQ(ctx, profile)
	case "redis":
		return connectRedis(ctx, false, profile)
	case "redis-durable":
		return connectRedis(ctx, true, profile)
	case "mqtt":
		return connectMQTT(ctx, profile)
	case "zeromq":
		return connectZeroMQ(ctx, profile)
	case "opcua":
		return connectOPCUA(ctx, profile)
	case "opcua-pubsub":
		return connectOPCUAPubSub(ctx, profile)
	default:
		return nil, fmt.Errorf("backend %q is unknown", backend)
	}
}

// unknownProfile is the refusal every backend gives for a profile it does not
// have yet.
func unknownProfile(backend, profile string) error {
	return fmt.Errorf("%s: no %q profile", backend, profile)
}

// env is an environment variable, or a default.
func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

// lookup is an environment variable and whether it is set.
func lookup(key string) (string, bool) {
	return os.LookupEnv(key)
}

// moduleDir is the streams module's root, found from this file's path so the
// harness reads docker/tls/out wherever it is run from.
func moduleDir() string {
	if _, file, _, ok := runtime.Caller(0); ok && filepath.IsAbs(file) {
		return filepath.Join(filepath.Dir(file), "..", "..")
	}
	return "."
}

// certDir is where the test certificates are: the environment variable key
// (STREAMS_E2E_TLS_DIR for most backends), or docker/tls/out.
func certDir(key string) string {
	if dir := os.Getenv(key); dir != "" {
		return dir
	}
	return filepath.Join(moduleDir(), "docker", "tls", "out")
}

// mutualTLS trusts the test CA in dir and presents its client certificate,
// as every backend's e2e suite does.
func mutualTLS(dir string) (*tls.Config, error) {
	ca, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("cannot read the test CA (run docker/tls/generate.sh): %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("no certificate in " + filepath.Join(dir, "ca.crt"))
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key"))
	if err != nil {
		return nil, fmt.Errorf("cannot load the test client certificate: %w", err)
	}
	return &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
