package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/the-protobuf-project/runtime-go/observability"
	"os"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

// durableManager publishes to and consumes from a Redis stream.
//
// Where [streamManager] hands a message to whoever is listening at that moment,
// this appends it to a log. The log is what makes the rest possible: a consumer
// group's position lives on the server, so it outlives the process reading it,
// and a delivered message stays pending until it is acknowledged.
type durableManager struct {
	handler *streamHandler
	stream  streams.Stream
}

var (
	_ streams.Manager    = (*durableManager)(nil)
	_ streams.Durable    = (*durableManager)(nil)
	_ streams.Positioned = (*durableManager)(nil)
)

const (
	// readBatch is how many messages one read asks for. Batching is the
	// difference between one round trip per message and one per batch.
	readBatch = 64

	// blockFor is how long a read waits at the server before coming back
	// empty. It bounds how long cancellation takes to notice, so it is short
	// enough to feel responsive and long enough not to poll in a hot loop.
	blockFor = 2 * time.Second
)

// Publish appends a value to the subject's log.
func (m *durableManager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.handler.declares(ctx, m.stream, subject); err != nil {
		return "", err
	}
	o := streams.NewOptions(opts...)

	if o.TTL > 0 {
		// A Redis stream delivers when it is read, not on a timer. Saying so
		// beats appending now and letting the caller believe it was scheduled.
		m.handler.log.Error(ctx, "a durable stream cannot schedule a delivery", nil,
			observability.Fields{"subject": subject, "ttl": o.TTL.String()})
		return "", fmt.Errorf("%w: a durable stream delivers when it is read, not on a timer; use ConnectScheduled or UseScheduled for a TTL", streams.ErrUnsupported)
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.handler.codec, id, value)
	if err != nil {
		m.handler.log.Error(ctx, "could not encode the value", err,
			observability.Fields{"subject": subject, "id": id})
		return "", err
	}

	key := m.handler.keys.subject(m.stream.ID, subject)
	args := &goredis.XAddArgs{Stream: key, Values: map[string]any{payloadField: body}}
	if m.handler.maxLen > 0 {
		// Approximate: Redis trims on node boundaries, which is what lets it do
		// this without walking the stream.
		args.MaxLen = m.handler.maxLen
		args.Approx = true
	}

	m.handler.log.Debug(ctx, "appending", observability.Fields{
		"subject": subject, "id": id, "key": key, "bytes": len(body),
	})

	if err := m.handler.rdb.XAdd(ctx, args).Err(); err != nil {
		m.handler.log.Error(ctx, "could not append", err,
			observability.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("redis: cannot append to %q: %w", subject, err)
	}
	return id, nil
}

// Subscribe returns a channel of messages for a subject, starting at the end of
// the log.
//
// This is the undurable half of the contract, and on this provider it is a tail
// of the same log the durable half reads: nothing is acknowledged, nothing is
// remembered, and a message read from the channel is gone. Reach for
// [durableManager.Consume] when that matters.
func (m *durableManager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.handler.declares(ctx, m.stream, subject); err != nil {
		return nil, err
	}
	key := m.handler.keys.subject(m.stream.ID, subject)

	// Fix the starting position before returning, so a value published after
	// Subscribe returns is delivered rather than raced against the goroutine
	// getting as far as its first read.
	last, err := m.lastID(ctx, key)
	if err != nil {
		return nil, err
	}

	m.handler.log.Info(ctx, "subscribed", observability.Fields{
		"subject": subject, "key": key, "from": last,
	})

	out := make(chan streams.Message, core.Prefetch(streams.NewOptions(opts...)))
	go func() {
		defer close(out)

		delivered := 0
		defer func() {
			m.handler.log.Info(ctx, "subscription closed",
				observability.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			if ctx.Err() != nil {
				return
			}

			res, rerr := m.handler.rdb.XRead(ctx, &goredis.XReadArgs{
				Streams: []string{key, last},
				Count:   readBatch,
				Block:   blockFor,
			}).Result()
			if rerr != nil {
				// Nil is the block expiring with nothing to report.
				if errors.Is(rerr, goredis.Nil) {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				m.handler.log.Error(ctx, "could not read the stream", rerr,
					observability.Fields{"subject": subject})
				return
			}

			for _, stream := range res {
				for _, entry := range stream.Messages {
					last = entry.ID
					msg, derr := decodeEntry(m.handler.registry, subject, entry)
					if derr != nil {
						m.handler.log.Warn(ctx, "dropping a malformed message",
							observability.Fields{"subject": subject, "entry": entry.ID, "error": derr.Error()})
						continue
					}
					delivered++
					select {
					case out <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

// Consume delivers messages under a named consumer, starting at new ones.
func (m *durableManager) Consume(ctx context.Context, subject, consumer string, opts ...streams.Option) (<-chan streams.Delivery, error) {
	return m.ConsumeFrom(ctx, subject, consumer, streams.FromNew, opts...)
}

// ConsumeFrom delivers messages under a named consumer, starting at a chosen
// position.
//
// The name is a Redis consumer group: two processes consuming under one name
// share its position and split the work, and a process that dies and comes back
// resumes where the name left off. Each process joins that group under an
// identity of its own, which is what lets Redis tell whose message is
// outstanding when one of them stops answering.
//
// The position applies when the group is created and not after. A group that
// already exists keeps the position it has — that is the whole point of a
// durable consumer, and resetting it on every attach would replay the log
// every time a process restarted.
func (m *durableManager) ConsumeFrom(ctx context.Context, subject, consumer string, at streams.Position, opts ...streams.Option) (<-chan streams.Delivery, error) {
	if err := m.handler.declares(ctx, m.stream, subject); err != nil {
		return nil, err
	}
	if consumer == "" {
		return nil, fmt.Errorf("%w: a durable consumer needs a name, because the name is the position that survives a restart", streams.ErrUnsupported)
	}

	o := streams.NewOptions(opts...)
	if o.Group != "" {
		// Group and consumer would be two names for one thing here, and
		// honoring one silently would make the other a no-op.
		return nil, fmt.Errorf("%w: the consumer name %q is already the group on this provider; drop the Group option", streams.ErrUnsupported, consumer)
	}

	key := m.handler.keys.subject(m.stream.ID, subject)
	start := "$"
	if at == streams.FromEarliest {
		start = "0"
	}

	// MkStream so a consumer may attach before anything has been published;
	// without it, consuming a quiet subject fails until the first publish.
	if err := m.handler.rdb.XGroupCreateMkStream(ctx, key, consumer, start).Err(); err != nil && !isBusyGroup(err) {
		m.handler.log.Error(ctx, "could not create the consumer group", err,
			observability.Fields{"subject": subject, "consumer": consumer})
		return nil, fmt.Errorf("redis: cannot create consumer %q on %q: %w", consumer, subject, err)
	}

	name := consumerName()
	m.handler.log.Info(ctx, "consuming", observability.Fields{
		"subject": subject, "consumer": consumer, "identity": name, "from": start,
	})

	out := make(chan streams.Delivery, core.Prefetch(o))
	go m.consume(ctx, key, subject, consumer, name, out)
	return out, nil
}

// consume is the delivery loop: reclaim what a dead consumer abandoned, then
// read what is new.
func (m *durableManager) consume(ctx context.Context, key, subject, group, name string, out chan<- streams.Delivery) {
	defer close(out)

	delivered := 0
	defer func() {
		m.handler.log.Info(ctx, "consumer stopped", observability.Fields{
			"subject": subject, "consumer": group, "delivered": delivered,
		})
		// Best effort, and deliberately not on ctx: the context that ended this
		// loop is already done. A consumer left behind holds no messages once
		// its pending list is empty, but it does stay in the group's roster.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if pending, err := m.handler.rdb.XPendingExt(cleanup, &goredis.XPendingExtArgs{
			Stream: key, Group: group, Start: "-", End: "+", Count: 1, Consumer: name,
		}).Result(); err == nil && len(pending) == 0 {
			_ = m.handler.rdb.XGroupDelConsumer(cleanup, key, group, name).Err()
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		// Messages a consumer took and never acknowledged are invisible to a
		// group read forever. Reclaiming them is what turns "we will redeliver"
		// from a promise into a behavior.
		if sent, ok := m.reclaim(ctx, key, subject, group, name, out); !ok {
			return
		} else if sent > 0 {
			delivered += sent
			continue
		}

		// The group's pending count is what "behind" means on Redis: messages
		// handed out and not yet acknowledged. It is one command, on a loop that
		// already blocks for seconds, so measuring it costs nothing worth
		// counting.
		if n, perr := m.handler.rdb.XPending(ctx, key, group).Result(); perr == nil && n != nil {
			m.handler.metrics.Lag(ctx, subject, group, n.Count)
		}

		res, err := m.handler.rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: name,
			Streams:  []string{key, ">"},
			Count:    readBatch,
			Block:    blockFor,
		}).Result()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			m.handler.log.Error(ctx, "could not read as the consumer group", err,
				observability.Fields{"subject": subject, "consumer": group})
			return
		}

		for _, stream := range res {
			for _, entry := range stream.Messages {
				// Fresh from the log, so this is its first delivery.
				if !m.deliver(ctx, key, subject, group, entry, 1, out) {
					return
				}
				delivered++
			}
		}
	}
}

// reclaim takes over messages that have been outstanding longer than the
// reclaim interval and delivers them. It reports how many it sent, and whether
// the loop should keep going.
func (m *durableManager) reclaim(ctx context.Context, key, subject, group, name string, out chan<- streams.Delivery) (int, bool) {
	if m.handler.reclaim <= 0 {
		return 0, true
	}

	entries, _, err := m.handler.rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   key,
		Group:    group,
		Consumer: name,
		MinIdle:  m.handler.reclaim,
		Start:    "0",
		Count:    readBatch,
	}).Result()
	if err != nil {
		if ctx.Err() != nil {
			return 0, false
		}
		// A reclaim that fails is not a reason to stop consuming new messages.
		m.handler.log.Warn(ctx, "could not reclaim abandoned messages",
			observability.Fields{"subject": subject, "consumer": group, "error": err.Error()})
		return 0, true
	}
	if len(entries) == 0 {
		return 0, true
	}

	attempts := m.attempts(ctx, key, group, name)

	sent := 0
	for _, entry := range entries {
		attempt := attempts[entry.ID]
		if attempt == 0 {
			// Claimed but no longer listed as pending; it has been delivered at
			// least twice by definition, so say so rather than claim it is new.
			attempt = 2
		}
		m.handler.log.Debug(ctx, "reclaimed an abandoned message", observability.Fields{
			"subject": subject, "consumer": group, "entry": entry.ID, "attempt": attempt,
		})
		if !m.deliver(ctx, key, subject, group, entry, attempt, out) {
			return sent, false
		}
		sent++
	}
	return sent, true
}

// attempts maps entry id to how many times it has been delivered, for the
// messages currently outstanding with this consumer.
//
// One call for the batch rather than one per message: the count is only needed
// for reclaimed messages, and a round trip each would undo the batching.
func (m *durableManager) attempts(ctx context.Context, key, group, name string) map[string]int {
	pending, err := m.handler.rdb.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream:   key,
		Group:    group,
		Start:    "-",
		End:      "+",
		Count:    readBatch,
		Consumer: name,
	}).Result()
	if err != nil {
		return nil
	}

	out := make(map[string]int, len(pending))
	for _, p := range pending {
		out[p.ID] = int(p.RetryCount)
	}
	return out
}

// deliver decodes an entry and hands it over, reporting whether the loop should
// keep going.
func (m *durableManager) deliver(ctx context.Context, key, subject, group string, entry goredis.XMessage, attempt int, out chan<- streams.Delivery) bool {
	msg, err := decodeEntry(m.handler.registry, subject, entry)
	if err != nil {
		// A message nobody can decode will never be acknowledged by a handler,
		// so it would be reclaimed forever. Acknowledge it here and say so.
		m.handler.log.Warn(ctx, "acknowledging a malformed message so it does not redeliver forever",
			observability.Fields{"subject": subject, "entry": entry.ID, "error": err.Error()})
		_ = m.handler.rdb.XAck(ctx, key, group, entry.ID).Err()
		return true
	}

	d := streams.NewDelivery(msg, attempt, &redisAck{
		rdb: m.handler.rdb, key: key, group: group, entry: entry.ID, subject: subject,
		metrics: m.handler.metrics,
	})
	m.handler.metrics.Delivered(ctx, subject, group)

	select {
	case out <- d:
		return true
	case <-ctx.Done():
		return false
	}
}

// lastID returns the id of the newest entry in key, or the beginning when there
// is nothing in it yet.
func (m *durableManager) lastID(ctx context.Context, key string) (string, error) {
	entries, err := m.handler.rdb.XRevRangeN(ctx, key, "+", "-", 1).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return "", fmt.Errorf("redis: cannot read the end of %q: %w", key, err)
	}
	if len(entries) == 0 {
		return "0", nil
	}
	return entries[0].ID, nil
}

// decodeEntry turns a Redis stream entry back into a message.
func decodeEntry(reg *streams.Registry, subject string, entry goredis.XMessage) (streams.Message, error) {
	raw, ok := entry.Values[payloadField].(string)
	if !ok {
		return streams.Message{}, fmt.Errorf("redis: entry %s carries no %q field", entry.ID, payloadField)
	}
	return core.Unpack(reg, subject, []byte(raw))
}

// consumerName is this process's identity within a consumer group.
//
// Redis tracks outstanding messages per consumer, so two processes sharing a
// group need different names or each would see the other's work as its own.
// Host and pid make it recognizable in XINFO CONSUMERS; the suffix keeps two
// managers in one process apart.
func consumerName() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid()) + "-" + core.NewID()
}

// isBusyGroup reports whether err is Redis saying the consumer group is already
// there, which is the answer we want from a create we run on every attach.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// PublishBatch sends several values on a subject.
//
// Each value is appended in turn. Redis can pipeline, but an append is already
// a single round trip and the ids have to come back in order.
func (m *durableManager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	if err := core.CheckBatch(streams.NewOptions(opts...)); err != nil {
		return nil, err
	}
	return core.PublishEach(ctx, m, subject, values, opts...)
}

// redisAck settles one delivery against its consumer group.
type redisAck struct {
	rdb                        goredis.UniversalClient
	key, group, entry, subject string
	metrics                    *core.Metrics
}

func (a *redisAck) Ack(ctx context.Context) error {
	if err := a.rdb.XAck(ctx, a.key, a.group, a.entry).Err(); err != nil {
		return fmt.Errorf("redis: cannot acknowledge %s on %q: %w", a.entry, a.subject, err)
	}
	a.metrics.Settled(ctx, a.subject, a.group, "ack")
	return nil
}

// Nak leaves the message pending: it stays outstanding and is reclaimed once it
// has been idle for the reclaim interval, by whichever consumer gets there
// first.
//
// Appending a copy to the tail would redeliver it sooner, but it would arrive
// with a new id and an attempt count of one — and Attempt is the only signal a
// consumer has for noticing it is in a redelivery loop it cannot escape.
func (a *redisAck) Nak(ctx context.Context) error {
	a.metrics.Settled(ctx, a.subject, a.group, "nak")
	return nil
}
