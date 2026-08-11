package main

import (
	"fmt"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/nats"
	"github.com/machanirobotics/loom/go/nats/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	client, err := nats.NewNatsClient()
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// 1) Create a stream for protobuf example
	stream, err := client.Jetstream.Stream.CreateStream(
		client.RegularStream("protobuf-stream", []string{"proto.*"}),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Stream created:", stream.Stream.Config.Name)

	// 2) Create a durable pull consumer
	cons, err := stream.Consumer.CreateConsumer(types.ConsumerConfig{
		Name:          "proto-consumer",
		FilterSubject: "proto.*",
		AckPolicy:     types.AckExplicit,
	})
	if err != nil {
		panic(err)
	}

	// 3) Start pull subscription
	msgCh, err := cons.SubscribePull()
	if err != nil {
		panic(err)
	}

	// 4) Protobuf -> bytes: build a timestamp and marshal
	ts := timestamppb.Now()
	data, err := proto.Marshal(ts)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Marshalled protobuf (%d bytes)\n", len(data))

	// 5) Publish bytes to NATS
	ack, err := stream.Producer.Publish(types.JetStreamPublishRequest{
		Subject: "proto.event",
		Data:    data,
		Async:   false,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Published: stream=%s seq=%d\n", ack.Stream, ack.Sequence)

	// 6) Read from pull subscription: bytes -> protobuf
	select {
	case msg, ok := <-msgCh:
		if !ok {
			panic("channel closed")
		}
		var got timestamppb.Timestamp
		if err := proto.Unmarshal(msg.Data(), &got); err != nil {
			panic(err)
		}
		t := got.AsTime()
		fmt.Printf("Unmarshalled back: %s (RFC3339: %s)\n", t.Format(time.RFC3339Nano), t.UTC().Format(time.RFC3339))
	case <-time.After(5 * time.Second):
		fmt.Println("Timeout waiting for message")
	}
}
