package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/ulid"
)

// Create declares a new stream, generating an ID when one is not supplied.
//
// The metadata is stored as a single entry in a Redis stream key, so the key's
// type identifies it as one of ours during List.
func (p *Provider) Create(ctx context.Context, s streams.Stream) (streams.Stream, error) {
	id := s.ID()
	if id == "" {
		// A time-ordered ID so listings sort chronologically, suffixed with the
		// name to stay readable in redis-cli.
		id = ulid.Generate().GetTimeCode()
		if s.Name != "" {
			id += ":" + s.Name
		}
	}

	out := streams.NewStream(id, streams.Stream{
		Name:        s.Name,
		Description: s.Description,
		Subjects:    slices.Clone(s.Subjects),
		UserID:      s.UserID,
		Active:      true,
	})

	body, err := json.Marshal(out)
	if err != nil {
		return streams.Stream{}, fmt.Errorf("streams/redis: failed to encode stream %s: %w", id, err)
	}

	if err := p.rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.keys.stream(id),
		Values: map[string]any{p.metadataField(): body},
	}).Err(); err != nil {
		return streams.Stream{}, fmt.Errorf("streams/redis: failed to create stream %s: %w", id, err)
	}
	return out, nil
}

// metadataField is the field the stream metadata is stored under. Ordinary
// streams and notification streams use different names so a key written by one
// is never mistaken for the other's.
func (p *Provider) metadataField() string {
	if p.isNotify() {
		return fieldNotifyInfo
	}
	return fieldStreamInfo
}

// Get retrieves a stream by ID.
func (p *Provider) Get(ctx context.Context, id string) (streams.Stream, error) {
	return p.readStream(ctx, p.keys.stream(id), id)
}

// readStream loads and decodes the metadata entry from a stream key.
func (p *Provider) readStream(ctx context.Context, key, id string) (streams.Stream, error) {
	entries, err := p.rdb.XRangeN(ctx, key, "-", "+", 1).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return streams.Stream{}, fmt.Errorf("streams/redis: failed to read stream %s: %w", id, err)
	}
	if len(entries) == 0 {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}

	raw, ok := entries[0].Values[p.metadataField()].(string)
	if !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s carries no metadata", streams.ErrNotFound, id)
	}

	var s streams.Stream
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return streams.Stream{}, fmt.Errorf("streams/redis: stream %s has invalid metadata: %w", id, err)
	}
	s.SetID(id)
	return s, nil
}

// Update replaces a stream's configuration, preserving its ID.
//
// The replacement is written before the old metadata is dropped, so a failure
// partway leaves the original stream intact. Deleting first — as an earlier
// version did — destroyed the stream whenever the recreate failed.
func (p *Provider) Update(ctx context.Context, id string, s streams.Stream) (streams.Stream, error) {
	if _, err := p.Get(ctx, id); err != nil {
		return streams.Stream{}, err
	}

	out := streams.NewStream(id, streams.Stream{
		Name:        s.Name,
		Description: s.Description,
		Subjects:    slices.Clone(s.Subjects),
		UserID:      s.UserID,
		Active:      true,
	})

	body, err := json.Marshal(out)
	if err != nil {
		return streams.Stream{}, fmt.Errorf("streams/redis: failed to encode stream %s: %w", id, err)
	}

	// Append the new metadata, then trim the key to the single newest entry.
	// Both run in one transaction, so a reader either sees the old metadata or
	// the new one and never an empty stream.
	key := p.keys.stream(id)
	pipe := p.rdb.TxPipeline()
	pipe.XAdd(ctx, &goredis.XAddArgs{
		Stream: key,
		Values: map[string]any{p.metadataField(): body},
	})
	pipe.XTrimMaxLen(ctx, key, 1)
	if _, err := pipe.Exec(ctx); err != nil {
		return streams.Stream{}, fmt.Errorf("streams/redis: failed to update stream %s: %w", id, err)
	}
	return out, nil
}

// Delete removes a stream.
//
// A stream that is not there is reported as [streams.ErrNotFound] rather than
// treated as success: an earlier version logged the lookup failure and returned
// nil, so a dropped connection during Update looked like a completed delete.
func (p *Provider) Delete(ctx context.Context, id string) error {
	removed, err := p.rdb.Del(ctx, p.keys.stream(id)).Result()
	if err != nil {
		return fmt.Errorf("streams/redis: failed to delete stream %s: %w", id, err)
	}
	if removed == 0 {
		return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	return nil
}

// List returns every stream in this provider's namespace.
//
// Keys are found with SCAN rather than KEYS so a large keyspace does not block
// the server, and each candidate's type is checked before it is read — the
// namespace may hold pub/sub or payload keys that are not streams.
func (p *Provider) List(ctx context.Context) ([]streams.Stream, error) {
	var (
		out    []streams.Stream
		cursor uint64
	)

	for {
		keys, next, err := p.rdb.Scan(ctx, cursor, p.keys.streamPattern(), 100).Result()
		if err != nil {
			return nil, fmt.Errorf("streams/redis: failed to scan streams: %w", err)
		}

		for _, key := range keys {
			kind, err := p.rdb.Type(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("streams/redis: failed to type %s: %w", key, err)
			}
			if kind != "stream" {
				continue
			}

			id := p.keys.idFromStreamKey(key)
			s, err := p.readStream(ctx, key, id)
			if err != nil {
				// A key without our metadata belongs to something else in this
				// namespace; skip it rather than failing the whole listing.
				if errors.Is(err, streams.ErrNotFound) {
					continue
				}
				return nil, err
			}
			out = append(out, s)
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	slices.SortFunc(out, func(a, b streams.Stream) int {
		return cmpString(a.ID(), b.ID())
	})
	return out, nil
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
