package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
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
	// difference between one round trip per message and one per batch. A
	// durable consumer asks for its prefetch instead, within [maxReadBatch].
	readBatch = 64

	// maxReadBatch is the most entries one group read or reclaim asks for,
	// however large the prefetch: a reply this size is already a few hundred
	// kilobytes, and a bigger one only delays the first message of the batch.
	maxReadBatch = 1024

	// maxAckBatch is the most entry ids one XACK carries when
	// acknowledgements arrive faster than they can be sent one by one.
	maxAckBatch = 1024

	// reclaimEveryUpTo is the longest a consumer goes between looks for
	// abandoned messages while new ones keep arriving. Checking before every
	// read cost a round trip per batch on the reader's connection, the one
	// that bounds a consumer's throughput.
	reclaimEveryUpTo = time.Second

	// measureLagEvery is how often a consumer records its pending count.
	measureLagEvery = time.Second

	// blockFor is how long a read waits at the server before coming back
	// empty. It bounds how long a loop goes without checking for work to
	// reclaim, so it is short enough to feel responsive and long enough not to
	// poll in a hot loop.
	blockFor = 2 * time.Second

	// takeGrace is how much longer than its block a read is waited for once
	// its consumer has stopped, so the entries it took can be handed back.
	takeGrace = time.Second

	// coalesceFor is how long a consumer waits after a read that came back
	// nearly empty, so the next read collects a batch. Without it, every
	// blocked reader is woken for nearly every entry at moderate rates, and
	// Redis spends its single thread answering one-entry reads: on TLS, two
	// consumers per subject capped a paced 50k/s run at about 42k/s, against
	// 50k/s with the pause. It costs at most this much latency, and only when
	// traffic is light enough to afford it.
	coalesceFor = time.Millisecond

	// cleanupFor is how long a stopping consumer may spend handing back what
	// it took and removing itself from its group.
	cleanupFor = 5 * time.Second

	// reconnectFrom and reconnectUpTo are the first and the longest wait
	// between reads after the server went away.
	reconnectFrom = 100 * time.Millisecond
	reconnectUpTo = 2 * time.Second
)

// Why a step of the consume loop could not go on.
var (
	// errStopped: the consumer's context is done.
	errStopped = errors.New("redis: the consumer stopped")
	// errReadFailed: Redis refused a read; it is logged and the channel closes.
	errReadFailed = errors.New("redis: the group read failed")
	// errDisconnected: the connection went away (a restart, a failover, a
	// network blip). The group, its position, and its pending list live on the
	// server, so the loop waits and carries on rather than ending.
	errDisconnected = errors.New("redis: the connection went away")
)

// Publish appends a value to the subject's log.
//
// Headers are fields of the entry beside the payload, header "lang" as the
// field "h:lang", so XRANGE and any other Redis client sees them; the payload
// frame stays version 1. The XADD is never repeated after a dropped
// connection, because a repeat would append a second copy under a new entry id.
func (m *durableManager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.handler.declares(ctx, m.stream, subject); err != nil {
		return "", err
	}
	o := streams.NewOptions(opts...)
	if err := m.refuseTTL(ctx, subject, o); err != nil {
		return "", err
	}
	if err := refuseUnhonored(o); err != nil {
		return "", err
	}
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return "", err
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.handler.codec, id, value)
	if err != nil {
		m.handler.log.Error(ctx, "could not encode the value", err,
			telemetry.Fields{"subject": subject, "id": id})
		return "", err
	}

	key := m.handler.keys.subject(m.stream.ID, subject)
	m.handler.log.Debug(ctx, "appending", telemetry.Fields{
		"subject": subject, "id": id, "key": key, "bytes": len(body),
	})

	if err := sendOnce(ctx, m.handler.rdb, m.xadd(key, body, headers)...); err != nil {
		m.handler.log.Error(ctx, "could not append", err,
			telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("redis: cannot append to %q: %w", subject, err)
	}
	return id, nil
}

// refuseTTL refuses a TTL: a Redis stream delivers when it is read, not on a
// timer. Saying so beats appending now and letting the caller believe it was
// scheduled.
func (m *durableManager) refuseTTL(ctx context.Context, subject string, o streams.Options) error {
	if o.TTL <= 0 {
		return nil
	}
	m.handler.log.Error(ctx, "a durable stream cannot schedule a delivery", nil,
		telemetry.Fields{"subject": subject, "ttl": o.TTL.String()})
	return fmt.Errorf("%w: a durable stream delivers when it is read, not on a timer; use ConnectScheduled or UseScheduled for a TTL", streams.ErrUnsupported)
}

// xadd is the XADD of body and headers to key, trimmed when a cap is set.
func (m *durableManager) xadd(key string, body []byte, headers streams.Headers) []any {
	args := make([]any, 0, 8+2*len(headers))
	args = append(args, "XADD", key)
	if m.handler.maxLen > 0 {
		// Approximate: Redis trims on node boundaries, which is what lets it do
		// this without walking the stream.
		args = append(args, "MAXLEN", "~", m.handler.maxLen)
	}
	args = append(args, "*", payloadField, body)
	for _, name := range headers.Keys() {
		args = append(args, headerFieldPrefix+name, headers[name])
	}
	return args
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
	// Every subscriber here sees every message, so a group would mean nothing.
	if err := refuseGroup(streams.NewOptions(opts...)); err != nil {
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

	m.handler.log.Info(ctx, "subscribed", telemetry.Fields{
		"subject": subject, "key": key, "from": last,
	})

	out := make(chan streams.Message, core.Prefetch(streams.NewOptions(opts...)))
	go func() {
		defer close(out)

		delivered := 0
		defer func() {
			m.handler.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
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
					telemetry.Fields{"subject": subject})
				return
			}

			for _, stream := range res {
				for _, entry := range stream.Messages {
					last = entry.ID
					msg, derr := decodeEntry(m.handler.registry, subject, entry)
					if derr != nil {
						m.handler.log.Warn(ctx, "dropping a malformed message",
							telemetry.Fields{"subject": subject, "entry": entry.ID, "error": derr.Error()})
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
			telemetry.Fields{"subject": subject, "consumer": consumer})
		return nil, fmt.Errorf("redis: cannot create consumer %q on %q: %w", consumer, subject, err)
	}

	name := consumerName()
	m.handler.log.Info(ctx, "consuming", telemetry.Fields{
		"subject": subject, "consumer": consumer, "identity": name, "from": start,
	})

	out := make(chan streams.Delivery, core.Prefetch(o))
	c := &consumerLoop{
		key:     key,
		subject: subject,
		group:   consumer,
		name:    name,
		batch:   min(max(core.Prefetch(o), 1), maxReadBatch),
		out:     out,
	}
	c.acks = &acks{target: &ackTarget{
		handler: m.handler, key: key, group: consumer, name: name, subject: subject,
	}}
	go m.consume(ctx, c)
	return out, nil
}

// consumerLoop is one delivery loop's identity and settings.
type consumerLoop struct {
	key, subject, group string

	// name is this loop's identity within the group. See [consumerName].
	name string

	// batch is how many entries one read or reclaim asks for: the prefetch,
	// within [maxReadBatch].
	batch int

	// acks is where this consumer's deliveries send their acknowledgements.
	acks *acks

	out chan<- streams.Delivery
}

// consume is the delivery loop: reclaim what a dead consumer abandoned, then
// read what is new, until ctx is done or Redis refuses a read, waiting out a
// dropped connection rather than ending.
func (m *durableManager) consume(ctx context.Context, c *consumerLoop) {
	defer close(c.out)

	delivered := 0
	defer func() {
		m.handler.log.Info(ctx, "consumer stopped", telemetry.Fields{
			"subject": c.subject, "consumer": c.group, "delivered": delivered,
		})
		m.leave(ctx, c)
	}()

	// Often enough that an abandoned message waits little past its reclaim
	// interval, rarely enough not to cost a round trip on every read.
	reclaimEvery := min(m.handler.reclaim/2, reclaimEveryUpTo)
	reclaimDue, lagDue := time.Now(), time.Now()
	wait := reconnectFrom
	lost := false

	for {
		if ctx.Err() != nil {
			return
		}

		// Messages a consumer took and never acknowledged are invisible to a
		// group read forever. Reclaiming them is what turns "we will redeliver"
		// from a promise into a behavior.
		if !time.Now().Before(reclaimDue) {
			sent, ok := m.reclaim(ctx, c)
			if !ok {
				return
			}
			if sent > 0 {
				// There may be more: look again straight away.
				delivered += sent
				continue
			}
			reclaimDue = time.Now().Add(reclaimEvery)
		}

		if !time.Now().Before(lagDue) {
			m.measureLag(ctx, c)
			lagDue = time.Now().Add(measureLagEvery)
		}

		sent, err := m.read(ctx, c)
		switch {
		case err == nil:
			if lost {
				m.handler.log.Info(ctx, "consumer reconnected",
					telemetry.Fields{"subject": c.subject, "consumer": c.group})
				lost, wait = false, reconnectFrom
			}
			delivered += sent
			switch {
			case sent == 0:
				// A read that came back empty waited out its block: the
				// consumer is idle, so it may as well look for abandoned work.
				reclaimDue = time.Now()
			case sent < max(c.batch/8, 2):
				select {
				case <-ctx.Done():
					return
				case <-time.After(coalesceFor):
				}
			}
		case errors.Is(err, errDisconnected):
			// Ending here would turn a broker restart into a consumer that
			// silently stops, when nothing it depends on was lost: the group's
			// position and pending list are on the server, and whatever this
			// consumer held unacknowledged is reclaimed once the loop is back.
			// The client opens a fresh connection for the next read.
			lost = true
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			wait = min(wait*2, reconnectUpTo)
		default:
			return
		}
	}
}

// take runs a command that assigns entries to this consumer (XREADGROUP,
// XAUTOCLAIM) and reports whether the consumer stopped while it ran.
//
// It is not abandoned when ctx ends. Redis may already have assigned entries to
// this consumer by then, and a reply nobody reads strands them as pending with a
// consumer that no longer exists until the reclaim interval passes. So the
// command is allowed to finish (a blocking read returns within its block) and
// the caller hands back what it took; see [durableManager.release].
func (m *durableManager) take(ctx context.Context, run func(context.Context)) (stopped bool) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), blockFor+takeGrace)
	defer cancel()
	run(bounded)
	return ctx.Err() != nil
}

// reclaim takes over messages that have been outstanding longer than the
// reclaim interval and delivers them. It reports how many it sent, and whether
// the loop should keep going.
func (m *durableManager) reclaim(ctx context.Context, c *consumerLoop) (int, bool) {
	if m.handler.reclaim <= 0 {
		return 0, true
	}

	var (
		entries []goredis.XMessage
		err     error
	)
	if m.take(ctx, func(ctx context.Context) {
		entries, _, err = m.handler.rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream:   c.key,
			Group:    c.group,
			Consumer: c.name,
			MinIdle:  m.handler.reclaim,
			Start:    "0",
			Count:    int64(c.batch),
		}).Result()
	}) {
		if err == nil {
			m.release(ctx, c, entries)
		}
		return 0, false
	}
	if err != nil {
		// A reclaim that fails is not a reason to stop consuming new messages.
		m.handler.log.Warn(ctx, "could not reclaim abandoned messages",
			telemetry.Fields{"subject": c.subject, "consumer": c.group, "error": err.Error()})
		return 0, true
	}
	if len(entries) == 0 {
		return 0, true
	}

	attempts := m.attempts(ctx, c)
	if ctx.Err() != nil {
		m.release(ctx, c, entries)
		return 0, false
	}

	sent := 0
	for i, entry := range entries {
		attempt := attempts[entry.ID]
		if attempt == 0 {
			// Claimed but no longer listed as pending; it has been delivered at
			// least twice by definition, so say so rather than claim it is new.
			attempt = 2
		}
		m.handler.log.Debug(ctx, "reclaimed an abandoned message", telemetry.Fields{
			"subject": c.subject, "consumer": c.group, "entry": entry.ID, "attempt": attempt,
		})
		if !m.deliver(ctx, c, entry, attempt) {
			m.release(ctx, c, entries[i:])
			return sent, false
		}
		sent++
	}
	return sent, true
}

// read is one group read of new messages, reporting how many it delivered.
func (m *durableManager) read(ctx context.Context, c *consumerLoop) (int, error) {
	var (
		res []goredis.XStream
		err error
	)
	if m.take(ctx, func(ctx context.Context) {
		res, err = m.handler.rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.name,
			Streams:  []string{c.key, ">"},
			Count:    int64(c.batch),
			Block:    blockFor,
		}).Result()
	}) {
		if err == nil {
			for _, stream := range res {
				m.release(ctx, c, stream.Messages)
			}
		}
		return 0, errStopped
	}
	if err != nil {
		switch {
		case errors.Is(err, goredis.Nil):
			// Nil is the block expiring with nothing to report.
			return 0, nil
		case disconnected(err):
			m.handler.log.Warn(ctx, "lost the connection; reconnecting",
				telemetry.Fields{"subject": c.subject, "consumer": c.group, "error": err.Error()})
			return 0, errDisconnected
		default:
			m.handler.log.Error(ctx, "could not read as the consumer group", err,
				telemetry.Fields{"subject": c.subject, "consumer": c.group})
			return 0, errReadFailed
		}
	}

	sent := 0
	for _, stream := range res {
		for i, entry := range stream.Messages {
			// Fresh from the log, so this is its first delivery.
			if !m.deliver(ctx, c, entry, 1) {
				m.release(ctx, c, stream.Messages[i:])
				return sent, errStopped
			}
			sent++
		}
	}
	return sent, nil
}

// release hands back entries this consumer took from the group but could not
// lend, because it had already stopped.
//
// A read in flight when the consumer stops still takes entries, and Redis then
// lists them as pending with this consumer. Left alone they would wait out the
// whole reclaim interval before any consumer saw them, so a consumer that stops
// at the wrong moment would delay messages published just after it. Claiming
// them back with an idle time of the reclaim interval makes them eligible at
// once: the next consumer's first reclaim takes them. JUSTID leaves their
// delivery count as it was, since none of them reached a reader. Best effort:
// if it fails they are still reclaimed, only later.
func (m *durableManager) release(ctx context.Context, c *consumerLoop, entries []goredis.XMessage) {
	if m.handler.reclaim <= 0 || len(entries) == 0 {
		return
	}
	args := make([]any, 0, 8+len(entries))
	args = append(args, "XCLAIM", c.key, c.group, c.name, 0)
	for _, entry := range entries {
		args = append(args, entry.ID)
	}
	args = append(args, "IDLE", m.handler.reclaim.Milliseconds(), "JUSTID")

	// Deliberately not on ctx, which is already done.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupFor)
	defer cancel()
	if err := m.handler.rdb.Do(cleanup, args...).Err(); err != nil {
		m.handler.log.Warn(ctx, "could not hand undelivered entries back; they are reclaimed later",
			telemetry.Fields{"subject": c.subject, "consumer": c.group, "entries": len(entries), "error": err.Error()})
	}
}

// attempts maps entry id to how many times it has been delivered, for the
// messages currently outstanding with this consumer.
//
// One call for the batch rather than one per message: the count is only needed
// for reclaimed messages, and a round trip each would undo the batching.
func (m *durableManager) attempts(ctx context.Context, c *consumerLoop) map[string]int {
	pending, err := m.handler.rdb.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream:   c.key,
		Group:    c.group,
		Start:    "-",
		End:      "+",
		Count:    int64(c.batch),
		Consumer: c.name,
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

// measureLag records the group's pending count, which is what "behind" means
// on Redis: messages handed out and not yet acknowledged.
func (m *durableManager) measureLag(ctx context.Context, c *consumerLoop) {
	if n, err := m.handler.rdb.XPending(ctx, c.key, c.group).Result(); err == nil && n != nil {
		m.handler.metrics.Lag(ctx, c.subject, c.group, n.Count)
	}
}

// deliver decodes an entry and lends it to the reader, reporting whether it
// did; false means ctx is done and the entry was not handed over.
func (m *durableManager) deliver(ctx context.Context, c *consumerLoop, entry goredis.XMessage, attempt int) bool {
	if ctx.Err() != nil {
		return false
	}
	msg, err := decodeEntry(m.handler.registry, c.subject, entry)
	if err != nil {
		// A message nobody can decode will never be acknowledged by a handler,
		// so it would be reclaimed forever. Acknowledge it here and say so.
		m.handler.log.Warn(ctx, "acknowledging a malformed message so it does not redeliver forever",
			telemetry.Fields{"subject": c.subject, "entry": entry.ID, "error": err.Error()})
		_ = m.handler.rdb.XAck(ctx, c.key, c.group, entry.ID).Err()
		return true
	}

	var acker streams.Acker = &redisAck{acks: c.acks, entry: entry.ID}
	if m.handler.reclaim > 0 {
		// Reclaiming is what redelivers, so only with it on can a nak be
		// delayed; without it the contract's own fallback says so.
		acker = &redisDelayedAck{redisAck{acks: c.acks, entry: entry.ID}}
	}
	d := streams.NewDelivery(msg, attempt, acker)
	m.handler.metrics.Delivered(ctx, c.subject, c.group)

	select {
	case c.out <- d:
		return true
	case <-ctx.Done():
		return false
	}
}

// leave removes this identity from the group once it holds nothing.
//
// Best effort and bounded, and deliberately not on ctx: the context that ended
// the loop is already done. A consumer left behind holds no messages once its
// pending list is empty, but it does stay in the group's roster. One still
// holding messages stays, so they can be reclaimed from it.
func (m *durableManager) leave(ctx context.Context, c *consumerLoop) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupFor)
	defer cancel()
	if pending, err := m.handler.rdb.XPendingExt(cleanup, &goredis.XPendingExtArgs{
		Stream: c.key, Group: c.group, Start: "-", End: "+", Count: 1, Consumer: c.name,
	}).Result(); err == nil && len(pending) == 0 {
		_ = m.handler.rdb.XGroupDelConsumer(cleanup, c.key, c.group, c.name).Err()
	}
}

// disconnected reports whether err means the connection is gone rather than
// that Redis refused the command.
func disconnected(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// A restarted server answers LOADING while it replays its file.
	return strings.HasPrefix(err.Error(), "LOADING")
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

// decodeEntry turns a Redis stream entry back into a message, with the headers
// its "h:" fields carry.
func decodeEntry(reg *streams.Registry, subject string, entry goredis.XMessage) (streams.Message, error) {
	raw, ok := entry.Values[payloadField].(string)
	if !ok {
		return streams.Message{}, fmt.Errorf("redis: entry %s carries no %q field", entry.ID, payloadField)
	}
	msg, err := core.Unpack(reg, subject, []byte(raw))
	if err != nil {
		return streams.Message{}, err
	}
	for field, value := range entry.Values {
		name, ok := strings.CutPrefix(field, headerFieldPrefix)
		if !ok {
			continue
		}
		// A field some other writer added that is not text is not a header
		// this contract could have written; it is left out, not fatal.
		text, ok := value.(string)
		if !ok {
			continue
		}
		if msg.Headers == nil {
			msg.Headers = make(streams.Headers)
		}
		msg.Headers[name] = text
	}
	return msg, nil
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
// One pipeline for every XADD, so a thousand values are one round trip, and the
// ids come back in order because they are minted first. Nothing in it is
// repeated after a dropped connection, and a failure is reported against every
// entry the pipeline carried, since Redis does not say how many landed before
// it.
func (m *durableManager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	if err := m.handler.declares(ctx, m.stream, subject); err != nil {
		return nil, err
	}
	if err := m.refuseTTL(ctx, subject, o); err != nil {
		return nil, err
	}
	if err := refuseUnhonored(o); err != nil {
		return nil, err
	}
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(values))
	if len(values) == 0 {
		return ids, nil
	}

	key := m.handler.keys.subject(m.stream.ID, subject)
	pipe := m.handler.rdb.Pipeline()
	var failures []error
	queued := make([]int, 0, len(values))
	for i, value := range values {
		id := core.NewID()
		body, err := core.Pack(m.handler.codec, id, value)
		if err != nil {
			failures = append(failures, fmt.Errorf("entry %d: %w", i, err))
			continue
		}
		ids[i] = id
		queued = append(queued, i)
		_ = pipe.Process(ctx, newOnce(ctx, m.xadd(key, body, headers)...))
	}

	if len(queued) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			m.handler.log.Error(ctx, "could not append a batch", err,
				telemetry.Fields{"subject": subject, "count": len(queued)})
			// One failure for the lot; some entries may still have landed.
			for _, i := range queued {
				failures = append(failures, fmt.Errorf("entry %d: redis: cannot append to %q: %w", i, subject, err))
			}
		}
	}
	return ids, core.BatchError(subject, len(values), failures)
}

// ackTarget is what every delivery of one consumer acknowledges against.
type ackTarget struct {
	handler                   *streamHandler
	key, group, name, subject string
}

// pendingAck is one acknowledgement waiting to be sent, and where to report
// how it went: nowhere, when acks are not confirmed (see [WithConfirmAcks]).
type pendingAck struct {
	entry string
	done  chan error
}

// acks are one consumer's acknowledgements, coalesced into multi-id XACKs.
//
// XACK takes any number of ids, so acknowledgements that arrive while one XACK
// is in flight go out together in the next, rather than as one command each.
// Nothing waits to fill a batch: an ack on an idle consumer is sent at once,
// alone. Each Ack still returns only after the XACK carrying its id has been
// answered, so a successful ack means what it always meant and at-least-once
// is untouched, unless the provider was told not to wait.
//
// The sender is a goroutine started when the first ack queues and ending when
// the queue is empty, so a consumer whose deliveries outlive it, or that is
// never acknowledged at all, leaves nothing running.
type acks struct {
	target *ackTarget

	mu      sync.Mutex
	queue   []pendingAck
	sending bool
}

// send queues one acknowledgement, starting the sender if it is not running.
func (a *acks) send(p pendingAck) {
	a.mu.Lock()
	a.queue = append(a.queue, p)
	if a.sending {
		a.mu.Unlock()
		return
	}
	a.sending = true
	a.mu.Unlock()
	go a.flush()
}

// flush sends what is queued, up to [maxAckBatch] ids per XACK, until nothing
// is.
func (a *acks) flush() {
	t := a.target
	for {
		a.mu.Lock()
		if len(a.queue) == 0 {
			a.sending = false
			a.mu.Unlock()
			return
		}
		n := min(len(a.queue), maxAckBatch)
		batch := a.queue[:n:n]
		a.queue = a.queue[n:]
		a.mu.Unlock()

		ids := make([]string, len(batch))
		for i, p := range batch {
			ids[i] = p.entry
		}
		// Not on any caller's context: the batch belongs to many callers, and
		// acknowledging an acknowledged entry is a no-op, so the client may
		// repeat it freely.
		ctx := context.Background()
		err := t.handler.rdb.XAck(ctx, t.key, t.group, ids...).Err()
		if err != nil {
			t.handler.log.Warn(ctx, "could not acknowledge; the entries stay pending and are redelivered",
				telemetry.Fields{"subject": t.subject, "consumer": t.group, "count": len(ids), "error": err.Error()})
		}
		for _, p := range batch {
			if p.done != nil {
				p.done <- err
			}
		}
	}
}

// redisAck settles one delivery against its consumer group.
type redisAck struct {
	acks  *acks
	entry string
}

// Ack queues an XACK for the consumer's sender, which batches them. With
// [WithConfirmAcks] on, the default, it waits for Redis to answer; off, it
// returns once queued.
func (a *redisAck) Ack(ctx context.Context) error {
	t := a.acks.target
	var done chan error
	if t.handler.confirmAcks {
		done = make(chan error, 1)
	}
	a.acks.send(pendingAck{entry: a.entry, done: done})
	if done != nil {
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("redis: cannot acknowledge %s on %q: %w", a.entry, t.subject, err)
			}
		case <-ctx.Done():
			// The XACK still goes out; this caller just stops waiting for it.
			return fmt.Errorf("redis: cannot acknowledge %s on %q: %w", a.entry, t.subject, ctx.Err())
		}
	}
	t.handler.metrics.Settled(ctx, t.subject, t.group, "ack")
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
	t := a.acks.target
	t.handler.metrics.Settled(ctx, t.subject, t.group, "nak")
	return nil
}

// redisDelayedAck is a [redisAck] that can hold a redelivery back, which it
// can whenever reclaiming is on, since reclaiming is what redelivers.
type redisDelayedAck struct {
	redisAck
}

var _ streams.DelayedAcker = (*redisDelayedAck)(nil)

// NakAfter leaves the message pending, as Nak does, with its idle time set back
// so the group's reclaim reaches it delay from now rather than after the whole
// reclaim interval.
//
// The reclaim takes entries idle for at least the reclaim interval, so an entry
// made (reclaim - delay) idle becomes eligible once delay has passed, and is
// redelivered on the next reclaim after that: never sooner, at most a reclaim
// pass later. XCLAIM with JUSTID sets the idle time without counting a
// delivery, so the attempt number the redelivery carries is still the true one.
// The server holds the timing, so it survives a restart and needs no clock of
// ours.
//
// Redis refuses an idle time in the future, so a delay longer than the reclaim
// interval cannot be honored and is refused with [streams.ErrUnsupported]; the
// entry then redelivers after the reclaim interval, as a plain Nak's does.
func (a *redisDelayedAck) NakAfter(ctx context.Context, delay time.Duration) error {
	t := a.acks.target
	idle, err := idleForDelay(t.handler.reclaim, delay)
	if err != nil {
		return err
	}
	// Safe to repeat: a second XCLAIM sets the same idle time again.
	claimed, err := t.handler.rdb.Do(ctx, "XCLAIM", t.key, t.group, t.name, 0, a.entry,
		"IDLE", idle, "JUSTID").StringSlice()
	if err != nil {
		return fmt.Errorf("redis: cannot delay the redelivery of %s on %q: %w", a.entry, t.subject, err)
	}
	if len(claimed) == 0 {
		// Acknowledged already, or trimmed off the log: nothing is pending to
		// redeliver.
		t.handler.log.Warn(ctx, "a delayed nak found the entry no longer pending",
			telemetry.Fields{"subject": t.subject, "entry": a.entry})
	}
	t.handler.metrics.Settled(ctx, t.subject, t.group, "nak")
	return nil
}

// MaxNakDelay is the reclaim interval: Redis redelivers an entry once it has
// been idle that long and cannot set an idle time in the future, so that is
// the longest a delayed nak can wait.
func (a *redisDelayedAck) MaxNakDelay() time.Duration {
	return a.acks.target.handler.reclaim
}

// idleForDelay is the idle time, in milliseconds, that makes an entry eligible
// for a reclaim of reclaim once delay has passed, or why there is none.
//
// Rounded so the delay is never shortened: the delay up, the result down.
func idleForDelay(reclaim, delay time.Duration) (int64, error) {
	if reclaim <= 0 {
		return 0, fmt.Errorf("%w: redis: reclaiming is off (WithReclaimAfter is zero), so nothing would ever redeliver a delayed nak", streams.ErrUnsupported)
	}
	if delay > reclaim {
		return 0, fmt.Errorf("%w: redis: a delayed nak waits at most the reclaim interval (%v), because Redis redelivers an entry once it has been idle that long and cannot set an idle time in the future; asked for %v. The entry stays pending and redelivers after the reclaim interval", streams.ErrUnsupported, reclaim, delay)
	}
	delay = max(delay, 0)
	delayMillis := int64((delay + time.Millisecond - 1) / time.Millisecond)
	return max(reclaim.Milliseconds()-delayMillis, 0), nil
}
