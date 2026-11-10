// Package stream publishes [events] to a broker through runtime-go's streams
// contract: Kafka, NATS, RabbitMQ, Redis, MQTT, ZeroMQ, whichever provider the
// stream was created on.
//
// A [Sink] is the dispatcher's [events.Sink]. Every event goes out as one
// message:
//
//   - subject: the event type, or what [Options.Subject] maps it to;
//   - message id: the event id, the same id the CloudEvent, an outbox row and
//     a webhook's webhook-id header carry, so a consumer dedups on one key;
//   - partition key: the aggregate (or record) the event is about;
//   - payload: the CloudEvent, structured-mode JSON, the bytes a webhook is
//     POSTed;
//   - headers: the CloudEvent attributes as ce-* keys, and content-type.
//
// Events of one partition key are published one after another, each
// acknowledged before the next; different keys publish at once on
// [Options.Lanes] lanes. An event the broker can never take (an undeclared
// subject, a header it refuses) is reported as [events.ErrUndeliverable], and
// stalls its key unless [Options.DeadLetter] names a subject to send it to.
//
// [NewSink] checks, before anything is published, that the stream declares a
// subject for every event type it will be given.
//
//	sink, err := stream.NewSink(ctx, provider, streamID, []string{
//	    orderv1svc.OrderCreatedEvent, orderv1svc.OrderPaidEvent,
//	}, stream.Options{Meter: meter})
//	d := events.NewDispatcher(store, feeds, events.Options{Sink: sink, Outboxes: outboxes})
//
// The stream's provider must encode with JSON (the default codec): the
// payload is a CloudEvent, already JSON, and travels unchanged.
package stream
