package main

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/nats"
	"github.com/machanirobotics/loom/go/nats/types"
)

func userFromSubject(subj string) string {
	parts := strings.Split(subj, ".")
	if len(parts) >= 2 {
		return parts[1]
	}
	return "unknown"
}

func main() {
	client, err := nats.NewNatsClient()
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// 0) Stream: user-partitioned subjects for all stages
	stream, err := client.Jetstream.Stream.CreateStream(
		client.RegularStream("pipeline-users", []string{
			"transcribe.*",
			"chat.*",
			"speech.*",
		}),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Stream ready:", stream.Stream.Config.Name)

	// 1) Shared workers that handle ALL users (wildcard FilterSubject)
	// C1: transcribe.* -> chat.<user>
	c1, err := stream.Consumer.CreateConsumer(types.ConsumerConfig{
		Name:          "svc-transcribe",
		FilterSubject: "transcribe.*",
		AckPolicy:     types.AckExplicit,
	})
	if err != nil {
		panic(err)
	}
	c1Ch, err := c1.SubscribePull()
	if err != nil {
		panic(err)
	}
	go func() {
		for msg := range c1Ch {
			u := userFromSubject(msg.Subject())
			in := string(msg.Data())
			out := fmt.Sprintf(`{"stage":"transcribe","user":"%s","text":"%s"}`, u, strings.ToUpper(in))
			_, _ = stream.Producer.Publish(types.JetStreamPublishRequest{
				Subject: "chat." + u,
				Data:    []byte(out),
				Async:   false,
				Header:  map[string][]string{"X-Stage": {"transcribe->chat"}},
			})
			_ = msg.Ack()
			fmt.Printf("[C1] %s: transcribe -> chat\n", u)
		}
	}()

	// C2: chat.* -> speech.<user>
	c2, err := stream.Consumer.CreateConsumer(types.ConsumerConfig{
		Name:          "svc-chat",
		FilterSubject: "chat.*",
		AckPolicy:     types.AckExplicit,
	})
	if err != nil {
		panic(err)
	}
	c2Ch, err := c2.SubscribePull()
	if err != nil {
		panic(err)
	}
	go func() {
		for msg := range c2Ch {
			u := userFromSubject(msg.Subject())
			in := string(msg.Data())
			out := fmt.Sprintf(`{"stage":"chat","user":"%s","response":"Replying to %s"}`, u, in)
			_, _ = stream.Producer.Publish(types.JetStreamPublishRequest{
				Subject: "speech." + u,
				Data:    []byte(out),
				Async:   false,
				Header:  map[string][]string{"X-Stage": {"chat->speech"}},
			})
			_ = msg.Ack()
			fmt.Printf("[C2] %s: chat -> speech\n", u)
		}
	}()

	// 2) “User apps”: each user subscribes to their speech.<user> and can publish to transcribe.<user>
	// User A: Oscar Piastri
	oscarCons, err := stream.Consumer.CreateConsumer(types.ConsumerConfig{
		Name:          "user-oscar-sink",
		FilterSubject: "speech.oscar",
		AckPolicy:     types.AckExplicit,
	})
	if err != nil {
		panic(err)
	}
	oscarCh, err := oscarCons.SubscribePull()
	if err != nil {
		panic(err)
	}
	go func() {
		for msg := range oscarCh {
			fmt.Println("[User A - Oscar] got speech:", string(msg.Data()))
			_ = msg.Ack()
		}
	}()

	// User B: Charles Leclerc
	charlesCons, err := stream.Consumer.CreateConsumer(types.ConsumerConfig{
		Name:          "user-charles-sink",
		FilterSubject: "speech.charles",
		AckPolicy:     types.AckExplicit,
	})
	if err != nil {
		panic(err)
	}
	charlesCh, err := charlesCons.SubscribePull()
	if err != nil {
		panic(err)
	}
	go func() {
		for msg := range charlesCh {
			fmt.Println("[User B - Charles] got speech:", string(msg.Data()))
			_ = msg.Ack()
		}
	}()

	// 3) Both users PUBLISH to their own “transcribe.<user>”
	seed := func(user, text string) {
		_, _ = stream.Producer.Publish(types.JetStreamPublishRequest{
			Subject: "transcribe." + user,
			Data:    []byte(text),
			Async:   false,
			Header:  map[string][]string{"X-Seed": {"true"}, "X-User": {user}},
		})
	}

	seed("oscar", "box box?")
	seed("charles", "plan b confirmed.")

	// 4) Let the demo run briefly
	time.Sleep(4 * time.Second)
	fmt.Println("done.")
}
