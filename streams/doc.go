// Package streams defines the backend-agnostic contract for runtime-go's
// messaging layer.
//
// Providers live in their own modules — [github.com/the-protobuf-project/runtime-go/redis]
// today, NATS and others alongside it — and reach this contract through a
// manager they hand back:
//
//	c, _ := redis.New(ctx, redis.Config{Address: "localhost", Port: "6379"})
//	mgr, _ := c.SetDatabase(ctx, "events")
//
//	s, _ := mgr.Channel.Stream.Create(ctx, streams.Stream{
//	    Name: "orders", Subjects: []string{"order.placed"},
//	})
//	pub, _ := mgr.Channel.Stream.Bind(ctx, s.ID)
//	pub.Publish(ctx, "order.placed", order)
//
// # Your model, not ours
//
// There is no message type on the publish path. A value goes out as it is and
// arrives decoded into a destination you own, so adding a field to your model
// is not a change to this package. [For] puts a typed view over any Manager
// when you want the compiler to check that:
//
//	orders := streams.For[Order](pub)
//	orders.Publish(ctx, "order.placed", o)
//	for msg := range orders.Subscribe(ctx, "order.placed") { … }
//
// # Lifetime is the context's
//
// [Subscriber.Subscribe] returns a channel closed when its context ends. That
// is the only way to stop a subscription, and it is deliberate: a consumer that
// walks away without canceling would otherwise leak the delivery goroutine and
// its server-side subscription for the life of the process.
//
// For storage rather than messaging see the sibling
// [github.com/the-protobuf-project/runtime-go/cache] and
// [github.com/the-protobuf-project/runtime-go/database] modules.
package streams
