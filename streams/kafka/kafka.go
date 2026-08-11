package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Option configures a [Connect].
type Option func(*config)

type config struct {
	log        telemetry.Logger
	meter      telemetry.Meter
	prefix     string
	partitions int32
	replicas   int16
	client     []kgo.Opt
}

// WithLogger sets where these streams write their own records. Defaults to
// [telemetry.NoopLogger].
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithMeter sets where these streams report their own measurements. Defaults to
// [telemetry.NoopMeter].
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) { c.meter = m }
}

// WithPrefix namespaces every topic these streams create and read.
//
// Use it to run several independent sets of streams against one cluster, or to
// share a cluster with another concern.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithPartitions sets how many partitions a subject's topic is created with.
// Defaults to 1.
//
// Partitions are the unit of parallelism and of ordering: a consumer group can
// have at most one reader per partition, and Kafka orders within one and
// nowhere else. One partition means total ordering and a single reader; more
// means more readers and ordering only among messages sharing a
// [streams.PartitionKey].
func WithPartitions(n int32) Option {
	return func(c *config) { c.partitions = n }
}

// WithReplicationFactor sets how many brokers hold a copy of each topic.
// Defaults to 1, which is right for a single-broker cluster and wrong for
// anything you would miss.
func WithReplicationFactor(n int16) Option {
	return func(c *config) { c.replicas = n }
}

// WithClientOptions passes franz-go options through to every client this
// provider builds — TLS, SASL, timeouts.
//
// They are applied to the producer and to each consumer group client, so
// credentials set here reach the connections this package makes on your behalf.
func WithClientOptions(opts ...kgo.Opt) Option {
	return func(c *config) { c.client = append(c.client, opts...) }
}

// Connect returns a [streams.Streams] backed by Kafka.
//
// Unlike the other providers this one dials, for the reason given in the
// package documentation, and so must be closed. It returns an error rather than
// a provider when the cluster is unreachable — a misconfigured broker list is
// worth hearing about at startup rather than at the first publish.
func Connect(ctx context.Context, seeds []string, opts ...Option) (streams.Streams, error) {
	cfg := config{log: telemetry.NoopLogger, meter: telemetry.NoopMeter, partitions: 1, replicas: 1}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.log == nil {
		cfg.log = telemetry.NoopLogger
	}
	if cfg.meter == nil {
		cfg.meter = telemetry.NoopMeter
	}
	if len(seeds) == 0 {
		return nil, fmt.Errorf("kafka: no seed brokers given")
	}

	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...)}, cfg.client...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot build the client: %w", err)
	}

	s := &streamStore{
		seeds: seeds,
		cfg:   cfg,
		cl:    client,
		admin: kadm.NewClient(client),
		log:   cfg.log,
	}

	pingCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka: cannot reach %s: %w", strings.Join(seeds, ","), err)
	}
	if err := s.ensureMeta(pingCtx); err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}
