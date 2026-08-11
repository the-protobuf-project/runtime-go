package main

import (
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/nats"
	"github.com/machanirobotics/loom/go/nats/options"
	"github.com/machanirobotics/loom/go/nats/types"
)

func main() {
	// Dev: to clear a huge backlog once, run:
	//   nats stream purge <stream-name>
	// JetStream retention: leave RegularStreamRetention zero for unbounded, or set MaxAge/MaxBytes on options.

	clientOpts := options.NatsClientOptions{
		URL:             "nats://localhost:4222",
		EnableJetStream: true,
		RegularStreamRetention: options.RegularStreamRetention{
			MaxAge:   5 * time.Minute,
			MaxBytes: 128 << 20,
		},
	}
	client, err := nats.NewNatsClient(clientOpts)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// 1) Create/update stream with retention (use client.RegularStream, not template.RegularStream,
	// so MaxAge/MaxBytes come from NatsClientOptions.RegularStreamRetention above).
	stream, err := client.Jetstream.Stream.CreateStream(
		client.RegularStream("animation-stream", []string{"animation.*"}),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Stream ready:", stream.Stream.Config.Name)

	// 2) Durable pull consumer: DeliverNew avoids draining an existing backlog;
	// UpdateConsumer applies policy if the consumer already existed.
	consCfg := types.ConsumerConfig{
		Name:          "animation-consumer-test",
		FilterSubject: "animation.*",
		AckPolicy:     types.AckExplicit,
		DeliverPolicy: types.DeliverNew,
	}
	if _, err := stream.Consumer.EnsureConsumer(consCfg); err != nil {
		panic(err)
	}
	cons, err := stream.Consumer.UpdateConsumer(consCfg)
	if err != nil {
		panic(err)
	}

	// 3) Start a pull subscription (read in background)
	msgCh, err := cons.SubscribePull()
	if err != nil {
		panic(err)
	}

	// 4a) Publish a synchronous message
	ack, err := stream.Producer.Publish(types.JetStreamPublishRequest{
		Subject: "example.test",
		Data:    []byte("hello, world (sync)"),
		Header:  map[string][]string{"X-Reply-To": {"response.subject-example"}},
		Async:   false, // sync
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Sync publish ack: stream=%s seq=%d dup=%t\n", ack.Stream, ack.Sequence, ack.Duplicate)

	// 4b) Kick off a few async messages so PublishState is meaningful
	for i := 0; i < 3; i++ {
		if _, err := stream.Producer.Publish(types.JetStreamPublishRequest{
			Subject: "example.test",
			Data:    []byte(fmt.Sprintf("hello, world (async %d)", i)),
			Header:  map[string][]string{"X-Reply-To": {"response.subject-example"}},
			Async:   true, // async
		}); err != nil {
			panic(err)
		}
	}

	// 5) Watch async publish state until complete
	state, err := stream.Producer.PublishState()
	if err != nil {
		panic(err)
	}
	for st := range state {
		fmt.Printf("Publish state: pending=%d complete=%t\n", st.Pending, st.Complete)
		if st.Complete {
			break
		}
	}

	// 6) KV example (before the long-running pull loop; that loop never returns)
	kv, err := client.Jetstream.Store.New(types.KeyValueConfig{
		Bucket: "example-bucket",
	})
	if err != nil {
		panic(err)
	}

	if _, err := kv.Create("example-key", []byte("example-value")); err != nil {
		panic(err)
	}
	val, _, err := kv.Get("example-key")
	if err != nil {
		panic(err)
	}
	fmt.Println("KV value:", string(val))

	// 7) Read from SubscribePull. Rates use a wall-clock ticker so sparse traffic still prints
	//    once per second (the old logic only advanced time on message arrival, which skewed msg/s).
	var recvCount int64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				fmt.Println("[NATS] SubscribePull channel closed, exiting")
				return
			}
			atomic.AddInt64(&recvCount, 1)
			// SubscribePull Ack()s after enqueueing each message (see loom go/nats consumer.go).
			_ = msg
		case <-ticker.C:
			n := atomic.SwapInt64(&recvCount, 0)
			fmt.Printf("[NATS] msgs in last wall 1s: %d\n", n)
		}
	}
}
