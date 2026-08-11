package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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

	// readMeta bounds how long a metadata scan waits for the log's end.
	readMeta = 5 * time.Second
)

// streamStore manages stream lifecycle and hands out managers.
type streamStore struct {
	seeds    []string
	codec    streams.Codec
	registry *streams.Registry
	cfg      config
	cl       *kgo.Client
	admin    *kadm.Client
	log      telemetry.Logger
}

var (
	_ streams.Streams = (*streamStore)(nil)
	_ streams.Closer  = (*streamStore)(nil)
)

// Close releases the producer and its connections.
//
// Consumers built by Consume are not owned here — they end with the context
// that started them, so cancel those first or their brokers stay connected
// until they notice.
func (s *streamStore) Close() error {
	s.cl.Close()
	return nil
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
func (s *streamStore) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
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

	if err := s.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
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

	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(s.seeds...),
		kgo.ConsumeTopics(s.metaTopic()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, s.cfg.client...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot read stream metadata: %w", err)
	}
	defer cl.Close()

	// Where the log currently ends, so the scan stops rather than waiting on a
	// topic nobody is writing to.
	ends, err := s.admin.ListEndOffsets(ctx, s.metaTopic())
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot read stream metadata: %w", err)
	}

	remaining := 0
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining += int(o.Offset)
		}
	})

	out := make(map[string]streams.Stream)
	for remaining > 0 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("kafka: cannot read stream metadata: %w", errs[0].Err)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			remaining--
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
