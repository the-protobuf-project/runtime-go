package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// streamHandler manages stream lifecycle and hands out publishers and
// subscribers.
//
// One type serves both delivery modes. kindStream publishes over pub/sub and
// delivers immediately; kindNotify writes a TTL key whose expiry is the
// delivery. They share every other behavior — subject validation, metadata,
// listing — so they share an implementation and differ only where it matters.
type streamHandler struct {
	rdb      goredis.UniversalClient
	codec    streams.Codec
	registry *streams.Registry
	metrics  *core.Metrics
	keys     keys
	kind     kind
	db       int
	log      telemetry.Logger
	maxLen   int64
	reclaim  time.Duration

	// owned says this package dialed the client and must close it. A client
	// handed in through Use belongs to the caller, and closing it here would
	// take down connections this package never made.
	owned bool
}

// Close releases the connection, if this package made it.
//
// It is a no-op on a provider built by [Use], which does not own its client —
// so a caller may close either kind without having to remember which it has.
func (s *streamHandler) Close() error {
	if !s.owned {
		return nil
	}
	if err := s.rdb.Close(); err != nil {
		return fmt.Errorf("redis: cannot close: %w", err)
	}
	return nil
}

// declares rejects a subject the stream does not declare.
//
// Both managers use it, so an undeclared subject fails the same way whichever
// delivery mode is in play. Publishing to one would write to a key nobody
// reads, and consuming from one would wait forever — both silent.
func (s *streamHandler) declares(ctx context.Context, stream streams.Stream, subject string) error {
	// A Redis key is a literal name, so a declared "orders.*" is a subject
	// called that and not a pattern — matched with Declares, not DeclaresPattern.
	if core.Declares(stream.Subjects, subject) {
		return nil
	}
	s.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": stream.ID, "declared": stream.Subjects,
	})
	return core.ErrSubject(stream.ID, subject, stream.Subjects)
}

var (
	_ streams.Streams = (*streamHandler)(nil)
	_ streams.Closer  = (*streamHandler)(nil)
)

// scheduled reports whether this handler delivers on expiry rather than on
// publish.
func (s *streamHandler) scheduled() bool { return s.kind == kindNotify }

// Create declares a stream, generating an id when one is not supplied.
func (s *streamHandler) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	id := in.ID
	if id == "" {
		id = core.NewStreamID(in.Name)
		s.log.Debug(ctx, "generated a stream id", telemetry.Fields{"id": id})
	}

	out := streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
	}

	body, err := json.Marshal(out)
	if err != nil {
		s.log.Error(ctx, "could not encode the stream metadata", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("redis: cannot encode stream %s: %w", id, err)
	}

	s.log.Debug(ctx, "creating stream", telemetry.Fields{
		"id": id, "name": in.Name, "subjects": out.Subjects, "scheduled": s.scheduled(),
	})

	if err := s.rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: s.keys.stream(id),
		Values: map[string]any{metadataField: body},
	}).Err(); err != nil {
		s.log.Error(ctx, "could not create the stream", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("redis: cannot create stream %s: %w", id, err)
	}

	s.log.Info(ctx, "stream created", telemetry.Fields{"id": id, "name": in.Name})
	return out, nil
}

// Get retrieves a stream by id.
func (s *streamHandler) Get(ctx context.Context, id string) (streams.Stream, error) {
	return s.read(ctx, s.keys.stream(id), id)
}

// read loads and decodes a stream's metadata entry.
func (s *streamHandler) read(ctx context.Context, key, id string) (streams.Stream, error) {
	entries, err := s.rdb.XRangeN(ctx, key, "-", "+", 1).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		s.log.Error(ctx, "could not read the stream", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("redis: cannot read stream %s: %w", id, err)
	}
	if len(entries) == 0 {
		s.log.Debug(ctx, "stream not found", telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}

	raw, ok := entries[0].Values[metadataField].(string)
	if !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s carries no metadata", streams.ErrNotFound, id)
	}

	var out streams.Stream
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		s.log.Error(ctx, "stream metadata is malformed", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("redis: stream %s has invalid metadata: %w", id, err)
	}
	out.ID = id
	return out, nil
}

// Bind returns a publisher and subscriber for an existing stream.
//
// The stream is read here so an unknown id fails at Bind rather than at the
// first publish, and so the subject list is available to validate against
// without a round trip on every call.
func (s *streamHandler) Bind(ctx context.Context, id string) (streams.Manager, error) {
	stream, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	s.log.Debug(ctx, "bound to stream", telemetry.Fields{"id": id, "subjects": stream.Subjects})

	// The two managers are separate types rather than one type with a mode
	// flag, because the difference is visible through the contract:
	// [streams.AsDurable] has to succeed on one and fail on the other, and a
	// single type carrying Consume would make it succeed on both and hand a
	// pub/sub caller a consumer that cannot redeliver.
	if s.kind == kindDurable {
		return &durableManager{handler: s, stream: stream}, nil
	}
	return &streamManager{handler: s, stream: stream}, nil
}

// Update replaces a stream's configuration, preserving its id.
//
// The replacement is written before the old metadata is trimmed, in one
// transaction, so a reader sees either the old configuration or the new one and
// never an empty stream. Deleting first would destroy the stream whenever the
// recreate failed.
func (s *streamHandler) Update(ctx context.Context, id string, in streams.Stream) (streams.Stream, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return streams.Stream{}, err
	}

	out := streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
	}

	body, err := json.Marshal(out)
	if err != nil {
		return streams.Stream{}, fmt.Errorf("redis: cannot encode stream %s: %w", id, err)
	}

	s.log.Debug(ctx, "updating stream", telemetry.Fields{"id": id, "subjects": out.Subjects})

	key := s.keys.stream(id)
	pipe := s.rdb.TxPipeline()
	pipe.XAdd(ctx, &goredis.XAddArgs{Stream: key, Values: map[string]any{metadataField: body}})
	pipe.XTrimMaxLen(ctx, key, 1)
	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Error(ctx, "could not update the stream", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("redis: cannot update stream %s: %w", id, err)
	}

	s.log.Info(ctx, "stream updated", telemetry.Fields{"id": id})
	return out, nil
}

// Delete removes a stream.
//
// A stream that is not there is reported rather than treated as success: a
// silent nil here once made a failed delete look like a completed one.
func (s *streamHandler) Delete(ctx context.Context, id string) error {
	s.log.Debug(ctx, "deleting stream", telemetry.Fields{"id": id})

	removed, err := s.rdb.Del(ctx, s.keys.stream(id)).Result()
	if err != nil {
		s.log.Error(ctx, "could not delete the stream", err, telemetry.Fields{"id": id})
		return fmt.Errorf("redis: cannot delete stream %s: %w", id, err)
	}
	if removed == 0 {
		s.log.Debug(ctx, "stream not found, nothing deleted", telemetry.Fields{"id": id})
		return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}

	s.log.Info(ctx, "stream deleted", telemetry.Fields{"id": id})
	return nil
}

// List returns every stream in this handler's namespace.
//
// Keys are found with SCAN rather than KEYS so a large keyspace does not block
// the server, and each candidate's type is checked before it is read — the
// namespace also holds pub/sub and payload keys that are not streams.
func (s *streamHandler) List(ctx context.Context) ([]streams.Stream, error) {
	var (
		out    []streams.Stream
		cursor uint64
	)

	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, s.keys.streamPattern(), 100).Result()
		if err != nil {
			s.log.Error(ctx, "could not scan for streams", err, nil)
			return nil, fmt.Errorf("redis: cannot list streams: %w", err)
		}

		for _, key := range keys {
			kind, err := s.rdb.Type(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("redis: cannot type %s: %w", key, err)
			}
			if kind != "stream" {
				continue
			}

			id := s.keys.idFromStream(key)
			stream, err := s.read(ctx, key, id)
			if err != nil {
				if errors.Is(err, streams.ErrNotFound) {
					// A key without our metadata belongs to something else in
					// this namespace; skip it rather than fail the listing.
					continue
				}
				return nil, err
			}
			out = append(out, stream)
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })
	s.log.Debug(ctx, "listed streams", telemetry.Fields{"count": len(out)})
	return out, nil
}
