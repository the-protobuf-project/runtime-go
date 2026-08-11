package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
)

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

// Publish appends a value to the subject's topic.
//
// [streams.PartitionKey] becomes the record key, which is what decides the
// partition and therefore what is ordered relative to what. This is the only
// provider where that option does anything: Redis and NATS order everything in
// one place and have no partition to choose.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}

	o := streams.NewOptions(opts...)
	if o.TTL > 0 {
		return "", fmt.Errorf("%w: Kafka delivers when it is read, not on a timer", streams.ErrUnsupported)
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.store.codec, id, value)
	if err != nil {
		return "", err
	}

	rec := &kgo.Record{Topic: m.store.topic(m.stream.ID, subject), Value: body}
	if o.PartitionKey != "" {
		rec.Key = []byte(o.PartitionKey)
	}

	if err := m.store.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
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
	if o.TTL > 0 {
		return nil, fmt.Errorf("%w: Kafka delivers when it is read, not on a timer", streams.ErrUnsupported)
	}

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

		rec := &kgo.Record{Topic: m.store.topic(m.stream.ID, subject), Value: body}
		if o.PartitionKey != "" {
			rec.Key = []byte(o.PartitionKey)
		}

		wg.Add(1)
		m.store.cl.Produce(ctx, rec, func(_ *kgo.Record, perr error) {
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
// No consumer group and no offsets committed: this is a tail, and nothing about
// what it saw survives the context that started it. Reach for
// [manager.Consume] when that matters.
func (m *manager) Subscribe(ctx context.Context, subject string) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	cl, err := m.consumerClient(subject, "", streams.FromNew)
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "subscribed", telemetry.Fields{"subject": subject})

	out := make(chan streams.Message)
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
				m.store.log.Error(ctx, "could not read the topic", errs[0].Err,
					telemetry.Fields{"subject": subject})
				return
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

// consumerClient builds a client reading one subject's topic, optionally as a
// member of a named group.
func (m *manager) consumerClient(subject, group string, at streams.Position) (*kgo.Client, error) {
	start := kgo.NewOffset().AtEnd()
	if at == streams.FromEarliest {
		start = kgo.NewOffset().AtStart()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(m.store.seeds...),
		kgo.ConsumeTopics(m.store.topic(m.stream.ID, subject)),
		kgo.ConsumeResetOffset(start),
	}
	if group != "" {
		// Offsets are committed when a delivery is acknowledged and not before,
		// so a message taken by a consumer that dies is read again by whoever
		// takes over the partition.
		opts = append(opts, kgo.ConsumerGroup(group), kgo.DisableAutoCommit())
	}
	opts = append(opts, m.store.cfg.client...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot consume %q: %w", subject, err)
	}
	return cl, nil
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
// # Acknowledging is sequential
//
// Kafka tracks one offset per partition, not one per message, so acknowledging
// a delivery marks everything before it in that partition as handled too.
// Handlers that acknowledge out of order will mark messages they never
// finished. Where that matters, process a partition in order — which is what
// [streams.PartitionKey] is for.
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

	cl, err := m.consumerClient(subject, safeName(consumer), at)
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "consuming", telemetry.Fields{
		"subject": subject, "consumer": consumer, "from": at,
	})

	out := make(chan streams.Delivery)
	go func() {
		defer close(out)
		defer cl.Close()

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
				m.store.log.Error(ctx, "could not read as the consumer group", errs[0].Err,
					telemetry.Fields{"subject": subject, "consumer": consumer})
				return
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()

				msg, derr := core.Unpack(m.store.registry, subject, rec.Value)
				if derr != nil {
					// Nothing will ever decode this, so no handler will ever
					// acknowledge it and the group would stall on it forever.
					// Commit past it and say so.
					m.store.log.Warn(ctx, "committing past a malformed message",
						telemetry.Fields{"subject": subject, "offset": rec.Offset, "error": derr.Error()})
					_ = cl.CommitRecords(ctx, rec)
					continue
				}

				delivered++
				select {
				case out <- streams.Delivery{
					Message: msg,
					// Kafka redelivers the same bytes with no record of having
					// done so, so there is nothing honest to count here. The
					// contract asks for zero where a provider cannot.
					Attempt: 0,
					Ack: func(ctx context.Context) error {
						if err := cl.CommitRecords(ctx, rec); err != nil {
							return fmt.Errorf("kafka: cannot acknowledge offset %d on %q: %w", rec.Offset, subject, err)
						}
						return nil
					},
					Nak: func(context.Context) error {
						// Leaving the offset uncommitted is the return: the
						// group reads this record again when the partition is
						// next assigned. Kafka has no per-message redelivery,
						// so there is nothing more truthful to do here.
						return nil
					},
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
