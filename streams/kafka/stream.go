package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// dialTimeout bounds the startup ping and the metadata read, so an
	// unreachable broker fails Connect instead of hanging it.
	dialTimeout = 10 * time.Second

	// metaSuffix names the compacted topic holding stream declarations.
	metaSuffix = "_streams_meta"

	// readMeta bounds how long a metadata scan waits for the log's end. It
	// matches dialTimeout: a scan that has not reached the end by then is an
	// error rather than a partial answer.
	readMeta = dialTimeout
)

// streamStore manages stream lifecycle and hands out managers.
type streamStore struct {
	seeds    []string
	codec    streams.Codec
	registry *streams.Registry
	metrics  *core.Metrics
	cfg      config
	cl       *kgo.Client
	admin    *kadm.Client
	log      telemetry.Logger

	// producers are at least one; see [WithProducers]. cl is producers[0].
	producers []*kgo.Client
	// turn picks the producer for a publish with no partition key.
	turn atomic.Uint64
	// keys seeds the hash that picks the producer for a partition key, the
	// same one every time within this process.
	keys maphash.Seed
}

var (
	_ streams.Streams = (*streamStore)(nil)
	_ streams.Closer  = (*streamStore)(nil)
)

// Close flushes what the producers still hold, then releases them and their
// connections.
//
// Every producer is flushed even when an earlier one fails, so one stuck
// connection does not strand what the others hold; the first failure is the
// one reported. Consumers built by Consume are not owned here — they end with
// the context that started them, so cancel those first or their brokers stay
// connected until they notice.
func (s *streamStore) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	var first error
	for _, p := range s.producers {
		if err := p.Flush(ctx); err != nil && first == nil {
			first = fmt.Errorf("kafka: cannot flush the producer: %w", err)
		}
	}
	s.closeProducers()
	return first
}

// closeProducers closes every producer without flushing.
func (s *streamStore) closeProducers() {
	for _, p := range s.producers {
		p.Close()
	}
}

// producer is the client a publish goes through: always the same one for a
// partition key, so records sharing a key keep their order, and otherwise
// each in turn.
func (s *streamStore) producer(key string) *kgo.Client {
	n := uint64(len(s.producers))
	if key != "" {
		return s.producers[maphash.String(s.keys, key)%n]
	}
	return s.producers[(s.turn.Add(1)-1)%n]
}

// metaTopic is where stream declarations live.
func (s *streamStore) metaTopic() string {
	if s.cfg.prefix != "" {
		return safeName(s.cfg.prefix) + metaSuffix
	}
	return strings.TrimPrefix(metaSuffix, "_")
}

// topic is the Kafka topic carrying one subject's messages.
func (s *streamStore) topic(streamID, subject string) string {
	name := streamID + "." + subject
	if s.cfg.prefix != "" {
		name = s.cfg.prefix + "." + name
	}
	return safeName(name)
}

// ensureMeta creates the metadata topic if it is not there.
//
// It is compacted rather than time-retained: the newest record for a stream id
// is its current declaration, and a declaration must not expire out from under
// the topics it describes.
func (s *streamStore) ensureMeta(ctx context.Context) error {
	resp, err := s.admin.CreateTopics(ctx, 1, s.cfg.replicas,
		map[string]*string{"cleanup.policy": kadm.StringPtr("compact")}, s.metaTopic())
	if err != nil {
		return fmt.Errorf("kafka: cannot create the metadata topic: %w", err)
	}
	for _, t := range resp {
		if t.Err != nil && !isTopicExists(t.Err) {
			return fmt.Errorf("kafka: cannot create the metadata topic: %w", t.Err)
		}
	}
	return nil
}

// isTopicExists reports whether err is Kafka saying the topic is already there,
// which is the answer we want from a create we may run more than once.
func isTopicExists(err error) bool {
	return errors.Is(err, kerr.TopicAlreadyExists)
}

// Create declares a stream, generating an id when one is not supplied.
//
// Default headers are checked first, so a bad one fails here rather than at
// every publish. Kafka reserves no header names.
func (s *streamStore) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	if err := in.Headers.Validate(); err != nil {
		return streams.Stream{}, err
	}
	id := in.ID
	if id == "" {
		id = core.NewStreamID(in.Name)
	}
	id = safeName(id)

	out := streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
		Headers:     in.Headers.Clone(),
	}

	// Topics first: a declaration naming subjects that do not exist would let a
	// publish fail against a stream the caller was just told it created.
	if err := s.createTopics(ctx, out); err != nil {
		return streams.Stream{}, err
	}
	if err := s.writeMeta(ctx, id, &out); err != nil {
		return streams.Stream{}, err
	}

	s.log.Info(ctx, "stream created", telemetry.Fields{"id": id, "name": in.Name, "subjects": out.Subjects})
	return out, nil
}

// createTopics makes one topic per declared subject.
func (s *streamStore) createTopics(ctx context.Context, stream streams.Stream) error {
	if len(stream.Subjects) == 0 {
		return nil
	}

	names := make([]string, 0, len(stream.Subjects))
	for _, subject := range stream.Subjects {
		names = append(names, s.topic(stream.ID, subject))
	}

	resp, err := s.admin.CreateTopics(ctx, s.cfg.partitions, s.cfg.replicas, nil, names...)
	if err != nil {
		return fmt.Errorf("kafka: cannot create topics for stream %s: %w", stream.ID, err)
	}
	for _, t := range resp {
		// Already there is the answer we want from a create we may run twice.
		if t.Err != nil && !isTopicExists(t.Err) {
			return fmt.Errorf("kafka: cannot create topic %s: %w", t.Topic, t.Err)
		}
	}
	return nil
}

// writeMeta records a declaration, or removes it when stream is nil.
func (s *streamStore) writeMeta(ctx context.Context, id string, stream *streams.Stream) error {
	rec := &kgo.Record{Topic: s.metaTopic(), Key: []byte(id)}
	if stream != nil {
		body, err := json.Marshal(stream)
		if err != nil {
			return fmt.Errorf("kafka: cannot encode stream %s: %w", id, err)
		}
		rec.Value = body
	}
	// A nil value is a tombstone, which compaction eventually removes.

	// Keyed by the stream id, so declarations of one stream stay in order
	// whichever producer the pool would otherwise pick.
	if err := s.producer(id).ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("kafka: cannot record stream %s: %w", id, err)
	}
	return nil
}

// readMetaAll folds the compacted metadata topic into the current declarations.
//
// The topic is read whole on every call rather than cached: a declaration made
// by another process has to be visible here, and caching it would make Get
// answer from a snapshot of whenever this process last looked.
func (s *streamStore) readMetaAll(ctx context.Context) (map[string]streams.Stream, error) {
	ctx, cancel := context.WithTimeout(ctx, readMeta)
	defer cancel()

	topic := s.metaTopic()

	// Where each partition currently starts and ends. A partition is read
	// once a record at or past its end has arrived, not after counting
	// records: compaction leaves gaps, so a log ending at offset 100 may hold
	// ten records, and a scan waiting for a hundred would sit out its timeout
	// and then miss declarations.
	starts, err := s.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot read stream metadata: %w", err)
	}
	ends, err := s.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot read stream metadata: %w", err)
	}
	if eerr := ends.Error(); eerr != nil {
		return nil, fmt.Errorf("kafka: cannot read stream metadata from %s: %w", topic, eerr)
	}

	pending := make(map[int32]int64)
	assign := make(map[int32]kgo.Offset)
	ends.Each(func(o kadm.ListedOffset) {
		start, ok := starts.Lookup(topic, o.Partition)
		if ok && start.Err == nil && start.Offset >= o.Offset {
			return // empty, or emptied by retention
		}
		pending[o.Partition] = o.Offset
		assign[o.Partition] = kgo.NewOffset().AtStart()
	})

	out := make(map[string]streams.Stream)
	if len(pending) == 0 {
		return out, nil
	}

	// A plain reader, not a group member: nothing about this read should be
	// remembered, and joining a group would leave server state behind per call.
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(s.seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}),
	}, s.cfg.client...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot read stream metadata: %w", err)
	}
	defer cl.Close()

	for len(pending) > 0 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			// Returning what was read so far would turn a stream that exists
			// into NotFound; an error is more honest.
			return nil, fmt.Errorf("kafka: stream metadata in %s was not read to its end within %s", topic, readMeta)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("kafka: cannot read stream metadata: %w", errs[0].Err)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if end, ok := pending[rec.Partition]; ok && rec.Offset+1 >= end {
				delete(pending, rec.Partition)
			}
			id := string(rec.Key)
			if rec.Value == nil {
				delete(out, id) // a tombstone: the stream was deleted
				return
			}
			var stream streams.Stream
			if err := json.Unmarshal(rec.Value, &stream); err != nil {
				s.log.Warn(ctx, "skipping malformed stream metadata",
					telemetry.Fields{"id": id, "error": err.Error()})
				return
			}
			out[id] = stream
		})
	}
	return out, nil
}

// Get retrieves a stream by id.
func (s *streamStore) Get(ctx context.Context, id string) (streams.Stream, error) {
	all, err := s.readMetaAll(ctx)
	if err != nil {
		return streams.Stream{}, err
	}
	stream, ok := all[safeName(id)]
	if !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	return stream, nil
}

// Bind returns a publisher and subscriber for an existing stream.
func (s *streamStore) Bind(ctx context.Context, id string) (streams.Manager, error) {
	stream, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	s.log.Debug(ctx, "bound to stream", telemetry.Fields{"id": stream.ID, "subjects": stream.Subjects})
	return &manager{store: s, stream: stream}, nil
}

// Update replaces a stream's configuration, preserving its id.
//
// Topics for subjects that were added are created; topics for subjects that
// were dropped are left in place, because deleting them would discard a log the
// declaration never owned exclusively.
func (s *streamStore) Update(ctx context.Context, id string, in streams.Stream) (streams.Stream, error) {
	if err := in.Headers.Validate(); err != nil {
		return streams.Stream{}, err
	}
	name := safeName(id)
	if _, err := s.Get(ctx, name); err != nil {
		return streams.Stream{}, err
	}

	out := streams.Stream{
		ID:          name,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
		Headers:     in.Headers.Clone(),
	}
	if err := s.createTopics(ctx, out); err != nil {
		return streams.Stream{}, err
	}
	if err := s.writeMeta(ctx, name, &out); err != nil {
		return streams.Stream{}, err
	}

	s.log.Info(ctx, "stream updated", telemetry.Fields{"id": name})
	return out, nil
}

// Delete removes a stream and the topics carrying its subjects.
func (s *streamStore) Delete(ctx context.Context, id string) error {
	name := safeName(id)

	stream, err := s.Get(ctx, name)
	if err != nil {
		return err
	}

	topics := make([]string, 0, len(stream.Subjects))
	for _, subject := range stream.Subjects {
		topics = append(topics, s.topic(name, subject))
	}
	if len(topics) > 0 {
		// Per-topic results are not checked: a topic someone already removed
		// is the outcome a delete wanted anyway.
		if _, derr := s.admin.DeleteTopics(ctx, topics...); derr != nil {
			return fmt.Errorf("kafka: cannot delete topics for stream %s: %w", id, derr)
		}
	}
	if err := s.writeMeta(ctx, name, nil); err != nil {
		return err
	}

	s.log.Info(ctx, "stream deleted", telemetry.Fields{"id": name})
	return nil
}

// List returns every declared stream.
func (s *streamStore) List(ctx context.Context) ([]streams.Stream, error) {
	all, err := s.readMetaAll(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]streams.Stream, 0, len(all))
	for _, stream := range all {
		out = append(out, stream)
	}
	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })

	s.log.Debug(ctx, "listed streams", telemetry.Fields{"count": len(out)})
	return out, nil
}

// safeName makes an id or subject usable as part of a Kafka topic name.
//
// Kafka allows letters, digits, dot, dash and underscore; a generated id
// carries the caller's stream name for readability, which may contain anything.
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, s)
}
