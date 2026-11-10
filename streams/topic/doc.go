// Package topic is typed topics: one subject of a stream, its message type, and
// the defaults it publishes and receives with.
//
// It is [streams.Typed]'s richer sibling, and the runtime protoc-gen-streams
// generates against. Where a Typed view takes the subject and options at every
// call, a [Topic] is fixed to one subject by a [Spec]: the subject, the default
// consumer name, static headers, headers and a partition key read from each
// message, expiry, retain, prefetch and a [BatchPolicy]. It hands out the three
// ways to use the subject:
//
//   - [Publisher]: Publish and PublishBatch, done when the broker accepted it.
//   - [Subscriber]: Subscribe, at most once.
//   - [Consumer]: Consume and ConsumeFrom, at least once, with ack and nak.
//
// plus a [BatchSink] that buffers values and publishes them by the policy.
// [Bind] declares the stream a set of topics lives on, widening an existing
// declaration rather than replacing it, and checks the capabilities the caller
// requires.
//
// Nothing here depends on what a provider can do: a durable consumer on a
// provider without one fails with [streams.ErrUnsupported] at the call, unless
// the stream was bound requiring [streams.CapabilityDurable] and so failed at
// [Bind] instead.
//
// It is the Go twin of runtime-rs's streams::topic, with the same names where
// Go allows: TopicSpec is [Spec] here, since topic.Spec does not stutter.
//
// # The codec
//
// A Topic encodes with whatever codec its provider was given, as every
// [streams.Manager] does: hand the provider runtime-go/streams/codec/protobuf
// and a Topic[*pb.Order] carries protobuf. A Rust topic takes its codec as a
// type parameter instead; the frame on the wire is the same either way.
//
//	var placed = &topic.Spec[*pb.Order]{
//	    Subject:      "order.placed",
//	    Consumer:     "billing",
//	    Headers:      streams.Headers{"source": "checkout"},
//	    PartitionKey: func(o *pb.Order) (string, bool) { return o.GetOrderId(), true },
//	    Batch:        topic.BatchPolicy{MaxSize: 100, MaxDelay: 10 * time.Millisecond},
//	}
//
//	orders := topic.New(mgr, placed)
//	id, err := orders.Publisher().Publish(ctx, order)
package topic
