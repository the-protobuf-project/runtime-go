package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// errorBackoff is the pause after a consumer error before reading again.
// franz-go reconnects on its own; this only keeps a broker outage from
// becoming a log flood.
const errorBackoff = 250 * time.Millisecond

// manager publishes to and consumes from one stream.
type manager struct {
	store  *streamStore
	stream streams.Stream
}

var (
	_ streams.Manager    = (*manager)(nil)
	_ streams.Durable    = (*manager)(nil)
	_ streams.Positioned = (*manager)(nil)
)

// checkSubject rejects a subject the stream does not declare.
//
// A Kafka topic is a literal name, so a declared "orders.*" is a subject called
// that and not a pattern — matched by name, as on Redis.
func (m *manager) checkSubject(ctx context.Context, subject string) error {
	if core.Declares(m.stream.Subjects, subject) {
		return nil
	}
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// checkOptions refuses what Kafka cannot do per message: deliver later,
// expire, or keep a last value.
func checkOptions(o streams.Options) error {
	if o.TTL > 0 {
		return fmt.Errorf("%w: Kafka delivers when it is read, not on a timer", streams.ErrUnsupported)
	}
	if o.Expires > 0 {
		return fmt.Errorf("%w: Kafka has no per-message expiry: a record stays readable until the topic's "+
			"retention removes it, whoever has or has not read it", streams.ErrUnsupported)
	}
	if o.Retain {
		return fmt.Errorf("%w: Kafka has no per-message retain: the nearest thing, a compacted topic, keeps "+
			"only the newest record per key for the whole subject, which would change what every reader "+
			"of the stream sees", streams.ErrUnsupported)
	}
	return nil
}

// recordHeaders turns headers into Kafka record headers, each value its UTF-8
// bytes, in key order; none when there are none, so a header-less record is
// exactly what this package wrote before headers existed.
func recordHeaders(h streams.Headers) []kgo.RecordHeader {
	if len(h) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(h))
	for _, key := range h.Keys() {
		out = append(out, kgo.RecordHeader{Key: key, Value: []byte(h[key])})
	}
	return out
}

// received is the headers a record arrived with.
//
// Kafka headers are bytes and may repeat a key; this contract's are one string
// per key. A repeated key takes its last value, as Kafka's own lastHeader
// does, and a null value reads as empty. A header that is not UTF-8 can only
// come from another producer and cannot be represented, so it is skipped with
// a warning rather than costing the message its payload.
func received(ctx context.Context, log telemetry.Logger, subject string, rec *kgo.Record) streams.Headers {
	if len(rec.Headers) == 0 {
		return nil
	}
	out := make(streams.Headers, len(rec.Headers))
	for _, h := range rec.Headers {
		if !utf8.ValidString(h.Key) || !utf8.Valid(h.Value) {
			log.Warn(ctx, "skipping a header that is not UTF-8",
				telemetry.Fields{"subject": subject, "offset": rec.Offset, "key": h.Key})
			continue
		}
		out[h.Key] = string(h.Value)
	}
	return out
}

// Publish appends a value to the subject's topic.
//
// [streams.PartitionKey] becomes the record key, which is what decides the
// partition and therefore what is ordered relative to what. This is the only
// provider where that option does anything: Redis and NATS order everything in
// one place and have no partition to choose.
//
// Headers, the stream's with the publish's own laid on top, become record
// headers. A non-zero [streams.Expires] and [streams.Retain] are refused:
// Kafka has neither per message.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}

	o := streams.NewOptions(opts...)
	if err := checkOptions(o); err != nil {
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

	body, err := core.Pack(m.store.codec, id, value)
	if err != nil {
		return "", err
	}

	rec := &kgo.Record{Topic: m.store.topic(m.stream.ID, subject), Value: body, Headers: recordHeaders(headers)}
	if o.PartitionKey != "" {
		rec.Key = []byte(o.PartitionKey)
	}

	if err := m.store.producer(o.PartitionKey).ProduceSync(ctx, rec).FirstErr(); err != nil {
		m.store.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("kafka: cannot publish on %q: %w", subject, err)
	}

	m.store.log.Debug(ctx, "published", telemetry.Fields{
		"subject": subject, "id": id, "partition_key": o.PartitionKey, "bytes": len(body),
	})
	return id, nil
}

// PublishBatch appends several values to the subject's topic in one go.
//
// This is where Kafka is worth reaching for. [manager.Publish] waits for the
// broker to acknowledge each message before returning, so a thousand values
// published one at a time are a thousand round trips. Here the records are
// handed to the client together and awaited once, which is what lets franz-go
// accumulate them into the batches the protocol was designed around.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	if err := checkOptions(o); err != nil {
		return nil, err
	}
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return nil, err
	}
	// Every record carries the same headers, and franz-go only reads them, so
	// they are built once.
	recHeaders := recordHeaders(headers)
	// One producer for the whole batch, so its records keep their order.
	producer := m.store.producer(o.PartitionKey)

	ids := make([]string, len(values))
	failures := make([]error, len(values))

	var wg sync.WaitGroup
	for i, value := range values {
		id := core.NewID()
		ids[i] = id

		body, err := core.Pack(m.store.codec, id, value)
		if err != nil {
			failures[i] = fmt.Errorf("entry %d: %w", i, err)
			ids[i] = ""
			continue
		}

		rec := &kgo.Record{Topic: m.store.topic(m.stream.ID, subject), Value: body, Headers: recHeaders}
		if o.PartitionKey != "" {
			rec.Key = []byte(o.PartitionKey)
		}

		wg.Add(1)
		producer.Produce(ctx, rec, func(_ *kgo.Record, perr error) {
			defer wg.Done()
			if perr != nil {
				failures[i] = fmt.Errorf("entry %d: %w", i, perr)
				ids[i] = ""
			}
		})
	}
	wg.Wait()

	m.store.log.Debug(ctx, "published a batch", telemetry.Fields{
		"subject": subject, "entries": len(values), "partition_key": o.PartitionKey,
	})
	return ids, core.BatchError(subject, len(values), compact(failures))
}

// compact drops the nil entries, so BatchError counts only real failures.
func compact(errs []error) []error {
	var out []error
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

// Subscribe returns a channel of messages for a subject, starting at the end of
// the log.
//
// Without [streams.Group] this is a tail: no consumer group and no offsets
// committed, and nothing about what it saw survives the context that started
// it. Every partition is pinned at the end it has when Subscribe is called,
// rather than leaving "the end" to be resolved whenever the first fetch lands,
// so a value published after Subscribe returns is delivered, not raced.
//
// With [streams.Group] it joins that consumer group, so members share the
// subject's partitions, and offsets are committed automatically: a plain
// subscription promises no redelivery. Group membership is settled by the
// broker after this returns; a partition the group has never committed starts
// at the end it had when Subscribe was called, not at whatever the end is once
// the group settles, so nothing published in between is skipped. Reach for
// [manager.Consume] when what was seen must survive.
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	o := streams.NewOptions(opts...)
	var (
		cl  *kgo.Client
		err error
	)
	if o.Group == "" {
		cl, err = m.tailClient(ctx, subject)
	} else {
		cl, err = m.sharedClient(ctx, subject, safeName(o.Group))
	}
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "subscribed", telemetry.Fields{"subject": subject, "group": o.Group})

	out := make(chan streams.Message, core.Prefetch(o))
	go func() {
		defer close(out)
		defer cl.Close()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				if errors.Is(errs[0].Err, kgo.ErrClientClosed) {
					return
				}
				// franz-go reconnects by itself; a broker restart is an
				// outage to sit out, not a reason to end the subscription.
				m.store.log.Warn(ctx, "could not read the topic",
					telemetry.Fields{"subject": subject, "error": errs[0].Err.Error()})
				if !sleep(ctx, errorBackoff) {
					return
				}
				continue
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()
				msg, derr := core.Unpack(m.store.registry, subject, rec.Value)
				if derr != nil {
					m.store.log.Warn(ctx, "dropping a malformed message",
						telemetry.Fields{"subject": subject, "offset": rec.Offset, "error": derr.Error()})
					continue
				}
				msg.Headers = received(ctx, m.store.log, subject, rec)
				delivered++
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// sleep waits d, reporting false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ends returns the current end of every partition of topic.
//
// A topic created a moment ago may not have its leaders elected yet, which the
// broker reports as an error that clears by itself; those are waited out for
// up to dialTimeout rather than failing a subscribe made right after Create.
func (m *manager) ends(ctx context.Context, topic string) (map[int32]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	for {
		ends, err := m.tryEnds(ctx, topic)
		if err == nil || !electing(err) {
			return ends, err
		}
		if !sleep(ctx, 100*time.Millisecond) {
			return nil, err
		}
	}
}

// tryEnds is one attempt at [manager.ends].
func (m *manager) tryEnds(ctx context.Context, topic string) (map[int32]int64, error) {
	listed, err := m.store.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := listed.Error(); err != nil {
		return nil, err
	}
	out := make(map[int32]int64)
	listed.Each(func(o kadm.ListedOffset) { out[o.Partition] = o.Offset })
	// Every topic has at least one partition, so none means the broker has not
	// caught up with it yet: an empty map would pin a tail to nothing, and it
	// would never deliver.
	if len(out) == 0 {
		return nil, kerr.UnknownTopicOrPartition
	}
	return out, nil
}

// electing reports the errors a just-created topic gives while its leaders
// are being elected.
func electing(err error) bool {
	return errors.Is(err, kerr.NotLeaderForPartition) ||
		errors.Is(err, kerr.LeaderNotAvailable) ||
		errors.Is(err, kerr.UnknownTopicOrPartition)
}

// tailClient builds a reader with no group, every partition of the subject's
// topic pinned at the end it has now.
func (m *manager) tailClient(ctx context.Context, subject string) (*kgo.Client, error) {
	topic := m.store.topic(m.stream.ID, subject)
	ends, err := m.ends(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot consume %q: %w", subject, err)
	}
	at := make(map[int32]kgo.Offset, len(ends))
	for partition, end := range ends {
		at[partition] = kgo.NewOffset().At(end)
	}
	return m.newClient(subject,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: at}))
}

// sharedClient builds a member of group whose offsets are committed
// automatically, starting a partition the group has never committed at the end
// it had when this was called.
func (m *manager) sharedClient(ctx context.Context, subject, group string) (*kgo.Client, error) {
	topic := m.store.topic(m.stream.ID, subject)
	ends, err := m.ends(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot consume %q: %w", subject, err)
	}
	return m.newClient(subject,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.AdjustFetchOffsetsFn(startUncommitted(topic, ends)),
	)
}

// startUncommitted points each newly assigned partition that has no committed
// offset at its noted end. Partitions with one are left alone, to resume there.
//
// franz-go hands a partition with no committed offset the reset offset, which
// is AtEnd (-1) here; a committed offset is never negative, which is how the
// two are told apart.
func startUncommitted(topic string, ends map[int32]int64) func(context.Context, map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	return func(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		for partition, at := range offsets[topic] {
			if at.EpochOffset().Offset >= 0 {
				continue
			}
			if end, ok := ends[partition]; ok {
				offsets[topic][partition] = kgo.NewOffset().At(end).WithEpoch(-1)
			}
		}
		return offsets, nil
	}
}

// newClient builds a consumer for subject from the seeds, opts, and the
// caller's own client options last.
func (m *manager) newClient(subject string, opts ...kgo.Opt) (*kgo.Client, error) {
	opts = append(append([]kgo.Opt{kgo.SeedBrokers(m.store.seeds...)}, opts...), m.store.cfg.client...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot consume %q: %w", subject, err)
	}
	return cl, nil
}

// consumerClient builds a client reading one subject's topic as a member of
// a named group whose offsets move only on acknowledgement, tracking which
// partitions it owns in commits.
func (m *manager) consumerClient(subject, group string, at streams.Position, commits *commits) (*kgo.Client, error) {
	start := kgo.NewOffset().AtEnd()
	if at == streams.FromEarliest {
		start = kgo.NewOffset().AtStart()
	}
	topic := m.store.topic(m.stream.ID, subject)

	// Offsets are committed when a delivery is acknowledged and not before,
	// so a message taken by a consumer that dies is read again by whoever
	// takes over the partition. The reset applies only to a group with no
	// committed offset yet.
	return m.newClient(subject,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(start),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			commits.assign(assigned[topic])
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			commits.revoke(revoked[topic])
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			commits.revoke(lost[topic])
		}),
	)
}

// Consume delivers messages under a named consumer, starting at new ones.
func (m *manager) Consume(ctx context.Context, subject, consumer string, opts ...streams.Option) (<-chan streams.Delivery, error) {
	return m.ConsumeFrom(ctx, subject, consumer, streams.FromNew, opts...)
}

// ConsumeFrom delivers messages under a named consumer, starting at a chosen
// position.
//
// The name is a Kafka consumer group: two processes under one name are assigned
// different partitions and split the work, and a process that dies and comes
// back resumes from the last offset the name committed.
//
// The position applies only when the group has no committed offset yet. A group
// that has read before resumes where it was, which is what a durable consumer
// is for.
//
// # Acknowledging
//
// Kafka keeps one offset per partition, not one per message, so the committed
// offset is the lowest one still unsettled in each partition: a delivery
// acknowledged out of order is remembered, but nothing is committed past an
// earlier delivery that has not been acknowledged. A delivery that is naked, or
// dropped unsettled, holds its partition's committed offset where it is, so it
// and everything after it is read again when the partition is next assigned.
//
// An ack is recorded at once and committed in the background, one commit at a
// time per consumer covering every ack since the last, rather than one
// blocking round trip per message. The commit is made before the consumer
// leaves its group, so a clean shutdown loses no acknowledgement; a process
// that dies between an ack and its commit has the message redelivered, which
// is at-least-once doing what it promised.
//
// [streams.Delivery.NakAfter] cannot delay on Kafka: a partition is read in
// order, so holding one record back would hold back everything behind it.
func (m *manager) ConsumeFrom(ctx context.Context, subject, consumer string, at streams.Position, opts ...streams.Option) (<-chan streams.Delivery, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if consumer == "" {
		return nil, fmt.Errorf("%w: a durable consumer needs a name, because the name is the position that survives a restart", streams.ErrUnsupported)
	}

	o := streams.NewOptions(opts...)
	if o.Group != "" {
		return nil, fmt.Errorf("%w: the consumer name %q is already the group on Kafka; drop the Group option", streams.ErrUnsupported, consumer)
	}

	topic := m.store.topic(m.stream.ID, subject)
	ledger := newCommits(topic)
	cl, err := m.consumerClient(subject, safeName(consumer), at, ledger)
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "consuming", telemetry.Fields{
		"subject": subject, "consumer": consumer, "from": at,
	})

	committed := make(chan struct{})
	go func() {
		defer close(committed)
		m.commitAcked(cl, ledger, subject)
	}()

	out := make(chan streams.Delivery, core.Prefetch(o))
	go func() {
		defer close(out)
		defer func() {
			// Commit what was acknowledged before leaving the group, so the
			// next member under this name starts after it. Closing leaves the
			// group.
			ledger.close()
			<-committed
			cl.Close()
		}()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "consumer stopped", telemetry.Fields{
				"subject": subject, "consumer": consumer, "delivered": delivered,
			})
		}()

		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				if errors.Is(errs[0].Err, kgo.ErrClientClosed) {
					return
				}
				m.store.log.Warn(ctx, "could not read as the consumer group", telemetry.Fields{
					"subject": subject, "consumer": consumer, "error": errs[0].Err.Error(),
				})
				if !sleep(ctx, errorBackoff) {
					return
				}
				continue
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()
				ledger.delivered(rec.Partition, rec.Offset)

				msg, derr := core.Unpack(m.store.registry, subject, rec.Value)
				if derr != nil {
					// Nothing will ever decode this, so no handler will ever
					// acknowledge it and the group would stall on it forever.
					// Settle it here and say so.
					m.store.log.Warn(ctx, "committing past a malformed message",
						telemetry.Fields{"subject": subject, "offset": rec.Offset, "error": derr.Error()})
					ledger.ack(rec.Partition, rec.Offset)
					continue
				}
				msg.Headers = received(ctx, m.store.log, subject, rec)

				delivered++
				m.store.metrics.Delivered(ctx, subject, consumer)
				select {
				// Kafka redelivers the same bytes with no record of having done
				// so, so there is nothing honest to count: the contract asks for
				// zero where a provider cannot.
				case out <- streams.NewDelivery(msg, 0, &kafkaAck{
					commits: ledger, partition: rec.Partition, offset: rec.Offset,
					metrics: m.store.metrics, subject: subject, consumer: consumer,
				}):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// commitAcked commits acknowledged offsets in the background until the
// consumer stops, then once more.
//
// One synchronous commit at a time: under load each one carries every ack made
// while the last was in flight, so commits cost a round trip per batch of acks
// rather than per message, and they reach the broker in order. The consumer's
// own context may be done by the last commit, so each commit gets its own
// bounded one.
func (m *manager) commitAcked(cl *kgo.Client, c *commits, subject string) {
	for {
		<-c.wake
		offsets, closed := c.take(c.owned())
		if len(offsets) > 0 {
			if err := commitSync(cl, c.topic, offsets); err != nil {
				m.store.log.Warn(context.Background(), "could not commit acknowledged offsets; retrying",
					telemetry.Fields{"subject": subject, "error": err.Error()})
				c.retry(offsets)
				if !closed {
					time.Sleep(errorBackoff)
					c.notify()
				}
			}
		}
		if closed {
			return
		}
	}
}

// commitSync commits offsets on topic and waits for the answer, returning the
// first error, the request's or a partition's.
func commitSync(cl *kgo.Client, topic string, offsets map[int32]int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	list := make(map[int32]kgo.EpochOffset, len(offsets))
	for partition, offset := range offsets {
		list[partition] = kgo.EpochOffset{Epoch: -1, Offset: offset}
	}
	var failed error
	cl.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{topic: list},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				failed = err
				return
			}
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if perr := kerr.ErrorForCode(p.ErrorCode); perr != nil && failed == nil {
						failed = perr
					}
				}
			}
		})
	return failed
}

// commits is what a durable consumer has delivered and its handlers have
// settled, per partition, and so what it may commit.
type commits struct {
	topic string

	mu         sync.Mutex
	partitions map[int32]*partitionLedger
	// assigned is the partitions this member owns, kept by the group
	// callbacks; nil until the first assignment.
	assigned map[int32]bool
	// closed is set once the consumer stops: an ack after that cannot be
	// committed.
	closed bool

	// wake is signaled by every ack and by closing; a signal sent while a
	// commit is in flight is kept, so the next commit covers every ack made
	// meanwhile.
	wake chan struct{}
}

// partitionLedger is one partition's deliveries.
type partitionLedger struct {
	// unacked is delivered and not yet acknowledged.
	unacked map[int64]struct{}
	// next is one past the highest offset delivered.
	next int64
	// committed is the offset last committed, or being committed.
	committed int64
}

// safe is the offset that may be committed: the oldest unacknowledged
// delivery, or, when everything delivered is acknowledged, the one after the
// last.
func (p *partitionLedger) safe() int64 {
	if len(p.unacked) == 0 {
		return p.next
	}
	return slices.Min(mapKeys(p.unacked))
}

// mapKeys returns m's keys in no particular order.
func mapKeys(m map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// newCommits returns an empty ledger for topic; partitions are added as
// deliveries arrive.
func newCommits(topic string) *commits {
	return &commits{
		topic:      topic,
		partitions: make(map[int32]*partitionLedger),
		wake:       make(chan struct{}, 1),
	}
}

// notify wakes the committer without blocking.
func (c *commits) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// delivered records a delivery as outstanding.
func (c *commits) delivered(partition int32, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	part, ok := c.partitions[partition]
	if !ok {
		part = &partitionLedger{unacked: make(map[int64]struct{})}
		c.partitions[partition] = part
	}
	switch {
	case offset < part.next:
		// The partition was read again from its committed offset, after a
		// rebalance or a reset: what came before it is handled, and what was
		// outstanding from here on is being delivered anew.
		clear(part.unacked)
		part.committed = offset
	case len(part.unacked) == 0 && part.next == 0:
		// First sight of this partition: nothing below this is ours.
		part.committed = offset
	}
	part.unacked[offset] = struct{}{}
	part.next = offset + 1
}

// ack marks a delivery handled, so a commit may pass it. It reports false once
// the consumer has stopped, since nothing will commit it then.
func (c *commits) ack(partition int32, offset int64) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	if part, ok := c.partitions[partition]; ok {
		delete(part.unacked, offset)
	}
	c.mu.Unlock()
	c.notify()
	return true
}

// close stops taking acks and asks for a last commit.
func (c *commits) close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.notify()
}

// assign records partitions newly given to this member.
func (c *commits) assign(partitions []int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.assigned == nil {
		c.assigned = make(map[int32]bool)
	}
	for _, p := range partitions {
		c.assigned[p] = true
	}
}

// revoke records partitions taken from this member.
func (c *commits) revoke(partitions []int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.assigned == nil {
		c.assigned = make(map[int32]bool)
	}
	for _, p := range partitions {
		delete(c.assigned, p)
	}
}

// owned is a copy of the partitions this member owns, or nil before the first
// assignment.
func (c *commits) owned() map[int32]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.assigned == nil {
		return nil
	}
	out := make(map[int32]bool, len(c.assigned))
	for p := range c.assigned {
		out[p] = true
	}
	return out
}

// take returns what has become committable since the last commit, marked as
// committed, and whether the consumer has stopped. owned, when not nil, limits
// it to the partitions this member is still assigned; the rest are forgotten.
func (c *commits) take(owned map[int32]bool) (map[int32]int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if owned != nil {
		for partition := range c.partitions {
			if !owned[partition] {
				delete(c.partitions, partition)
			}
		}
	}
	var out map[int32]int64
	for partition, part := range c.partitions {
		if safe := part.safe(); safe > part.committed {
			if out == nil {
				out = make(map[int32]int64)
			}
			// Kafka's committed offset is the next one to read.
			out[partition] = safe
			part.committed = safe
		}
	}
	return out, c.closed
}

// retry makes the next commit try offsets again after a failed one.
func (c *commits) retry(offsets map[int32]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for partition, offset := range offsets {
		if part, ok := c.partitions[partition]; ok && part.committed == offset {
			part.committed = -1
		}
	}
}

// kafkaAck settles one delivery by recording it in its consumer's ledger, from
// which the committer commits.
//
// Once the consumer's context is done it leaves the group and its partitions
// belong to someone else, so an ack then fails rather than committing on behalf
// of a member that no longer exists, and the message is redelivered as the
// contract promises.
type kafkaAck struct {
	commits           *commits
	partition         int32
	offset            int64
	metrics           *core.Metrics
	subject, consumer string
}

// Ack marks the offset settled for the committer to pick up; it does not wait
// for the commit. It fails once the consumer has stopped, since its partitions
// may already belong to another member.
func (a *kafkaAck) Ack(ctx context.Context) error {
	if !a.commits.ack(a.partition, a.offset) {
		return fmt.Errorf("kafka: cannot acknowledge offset %d on %q: its consumer was stopped, so the message will be redelivered", a.offset, a.subject)
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "ack")
	return nil
}

// Nak leaves the offset uncommitted, so the group reads this record again when
// the partition is next assigned. Kafka has no per-message redelivery, so there
// is nothing more truthful to do here.
func (a *kafkaAck) Nak(ctx context.Context) error {
	a.metrics.Settled(ctx, a.subject, a.consumer, "nak")
	return nil
}
