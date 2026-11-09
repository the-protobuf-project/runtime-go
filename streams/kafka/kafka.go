package kafka

import (
	"context"
	"fmt"
	"hash/maphash"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Option configures a [Connect].
type Option func(*config)

type config struct {
	codec      streams.Codec
	log        telemetry.Logger
	meter      telemetry.Meter
	prefix     string
	partitions int32
	replicas   int16
	producers  int
	client     []kgo.Opt
}

// defaultProducers is how many producers publishes are spread over unless
// [WithProducers] says otherwise. See [WithProducers] for why more than one.
const defaultProducers = 8

// defaultConfig is one partition, replication factor one, and eight
// producers: enough for a single-broker development cluster; production wants
// more replicas.
func defaultConfig() config {
	return config{
		log:        telemetry.NoopLogger,
		meter:      telemetry.NoopMeter,
		partitions: 1,
		replicas:   1,
		producers:  defaultProducers,
	}
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

// WithProducers sets how many producers publishes are spread over. Defaults
// to 8; zero or less is taken as 1.
//
// A producer keeps at most five requests in flight per broker while it is
// idempotent, and each request waits its turn on the producer's connection, so
// one producer moves only so many batches per round trip however many records
// are waiting. Each producer is a client of its own, with its own connections
// and its own five, which is what lets publishes keep the network full. Every
// one is built from the same options, durable as described on [Connect].
//
// Spreading changes no ordering a caller could rely on: records sharing a
// [streams.PartitionKey] always go through the same producer, so they reach
// their partition in the order they were published, as with one producer, and
// a batch goes through a single producer whole.
func WithProducers(n int) Option {
	return func(c *config) { c.producers = n }
}

// WithClientOptions passes franz-go options through to every client this
// provider builds — TLS, SASL, timeouts.
//
// They are applied to the producer and to each consumer group client, so
// credentials set here reach the connections this package makes on your behalf.
func WithClientOptions(opts ...kgo.Opt) Option {
	return func(c *config) { c.client = append(c.client, opts...) }
}

// WithCodec sets how payloads are encoded. Defaults to [streams.JSON].
//
// It changes what is published; what is *read* is decided by the message, which
// carries the name of the codec that wrote it. A provider always understands
// JSON as well as whatever is set here, so switching does not orphan a peer
// that has not switched yet.
func WithCodec(c streams.Codec) Option {
	return func(cfg *config) { cfg.codec = c }
}

// producerDefaults are the producer settings applied before the caller's own
// [WithClientOptions], so a caller who sets any of them keeps their value.
//
// A publish that returned an id has to mean the record is in the log, once:
// acks from every in-sync replica rather than the leader alone, and
// idempotence so a retry after a lost acknowledgement does not append the
// record twice. franz-go does both by default; they are stated here so the
// guarantee does not rest on a library default, and so a Rust peer
// (librdkafka, which sets them explicitly) and a Go one are equally durable.
//
// The rest is throughput, none of it weakening the above. A publish waits for
// its acknowledgement, so a publisher's rate is how many publishes it has in
// flight divided by how long one takes, and franz-go's default 10ms linger
// adds that to every one of them. One millisecond still lets concurrent
// publishes share a request. The buffer bounds are raised so many concurrent
// batches wait for room in the network rather than block on a full buffer.
func producerDefaults() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(time.Millisecond),
		kgo.MaxBufferedRecords(1_000_000),
		kgo.MaxBufferedBytes(1 << 30),
	}
}

// Connect returns a [streams.Streams] backed by Kafka.
//
// Unlike the other providers this one dials, for the reason given in the
// package documentation, and so must be closed. It returns an error rather than
// a provider when the cluster is unreachable — a misconfigured broker list is
// worth hearing about at startup rather than at the first publish.
//
// Publishes are spread over [WithProducers] producers, eight by default, each
// waiting for every in-sync replica and idempotent unless [WithClientOptions]
// says otherwise. It also creates the metadata topic if no process has yet.
func Connect(ctx context.Context, seeds []string, opts ...Option) (streams.Streams, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.producers < 1 {
		cfg.producers = 1
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

	producers, err := buildProducers(seeds, cfg)
	if err != nil {
		return nil, err
	}
	client := producers[0]

	codec, registry, metrics := core.ResolveAll(cfg.codec, cfg.meter)

	s := &streamStore{
		seeds:     seeds,
		codec:     codec,
		registry:  registry,
		metrics:   metrics,
		cfg:       cfg,
		cl:        client,
		admin:     kadm.NewClient(client),
		log:       cfg.log,
		producers: producers,
		keys:      maphash.MakeSeed(),
	}

	pingCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		s.closeProducers()
		return nil, fmt.Errorf("kafka: cannot reach %s: %w", strings.Join(seeds, ","), err)
	}
	if err := s.ensureMeta(pingCtx); err != nil {
		s.closeProducers()
		return nil, err
	}
	return s, nil
}

// buildProducers builds cfg.producers clients without touching the network:
// franz-go connects lazily, so nothing here can tell a good broker list from a
// bad one. The first doubles as the admin client.
func buildProducers(seeds []string, cfg config) ([]*kgo.Client, error) {
	opts := append(append([]kgo.Opt{kgo.SeedBrokers(seeds...)}, producerDefaults()...), cfg.client...)
	producers := make([]*kgo.Client, 0, cfg.producers)
	for range cfg.producers {
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			for _, built := range producers {
				built.Close()
			}
			return nil, fmt.Errorf("kafka: cannot build the client: %w", err)
		}
		producers = append(producers, cl)
	}
	return producers, nil
}
