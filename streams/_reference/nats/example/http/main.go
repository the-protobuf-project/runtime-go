package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/joho/godotenv/autoload"

	"github.com/machanirobotics/loom/go/nats"
	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/machanirobotics/loom/go/nats/types"
)

const (
	streamName  = "chat-stream"
	chatSubject = "cutlery.data.chat.v1"
	durableName = "chat-durable"
)

type ChatMessage struct {
	Room   string    `json:"room"`
	Author string    `json:"author"`
	Text   string    `json:"text"`
	At     time.Time `json:"at"`
}

type hub struct {
	mu    sync.RWMutex
	rooms map[string]map[chan []byte]struct{}
}

func newHub() *hub {
	return &hub{rooms: make(map[string]map[chan []byte]struct{})}
}

func (h *hub) subscribe(room string) chan []byte {
	ch := make(chan []byte, 128)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[room] == nil {
		h.rooms[room] = make(map[chan []byte]struct{})
	}
	h.rooms[room][ch] = struct{}{}
	return ch
}

func (h *hub) unsubscribe(room string, ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m, ok := h.rooms[room]; ok {
		delete(m, ch)
		if len(m) == 0 {
			delete(h.rooms, room)
		}
	}
	close(ch)
}

func (h *hub) broadcast(room string, payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.rooms[room] {
		select {
		case ch <- payload:
		case <-time.After(250 * time.Millisecond):
			// slow client — drop it
			go h.unsubscribe(room, ch)
		}
	}
}

func main() {
	// 1) Connect (reads env via your options defaults)
	client, err := nats.NewNatsClient()
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// 2) Ensure a stream that captures our subject
	stream, err := client.Jetstream.Stream.CreateStream(
		client.RegularStream(streamName, []string{chatSubject}),
	)
	if err != nil {
		panic(fmt.Errorf("ensure stream: %w", err))
	}
	shared.Pulse.Logger.Debugf("stream.ready name=%q subject=%q", stream.Stream.Config.Name, chatSubject)

	// 3) Ensure a durable pull consumer on that stream
	consOps, err := stream.Consumer.EnsureConsumer(types.ConsumerConfig{
		Name:          durableName, // durable name
		FilterSubject: chatSubject, // filter to our chat subject
		AckPolicy:     types.AckExplicit,
		DeliverPolicy: types.DeliverAll, // get all (or make it DeliverNew)
	})
	if err != nil {
		panic(fmt.Errorf("ensure consumer: %w", err))
	}
	shared.Pulse.Logger.Debugf("consumer.ready stream=%q durable=%q", streamName, durableName)

	// 4) Supervise a pull subscription; broadcast each message to its room
	hb := newHub()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go supervisePull(ctx, consOps, hb)

	// 5) HTTP: send messages
	mux := http.NewServeMux()
	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var msg ChatMessage
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if msg.Room == "" || msg.Text == "" {
			http.Error(w, "room and text required", http.StatusBadRequest)
			return
		}
		if msg.Author == "" {
			msg.Author = "anon"
		}
		msg.At = time.Now()

		ack, err := stream.Producer.Publish(types.JetStreamPublishRequest{
			Subject: chatSubject,
			Data:    []byte(fmt.Sprintf("%+v", msg)), // will JSON-encode via ConvertAnyDataToBytes
			Async:   false,                           // wait for ack
		})
		if err != nil {
			shared.Pulse.Logger.Errorf("chat.publish.failed room=%q err=%v", msg.Room, err)
			http.Error(w, "publish failed", http.StatusInternalServerError)
			return
		}
		shared.Pulse.Logger.Debugf("chat.publish.ok stream=%q seq=%d room=%q", ack.Stream, ack.Sequence, msg.Room)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	})

	// 6) HTTP: SSE stream per room
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		if room == "" {
			http.Error(w, "missing room", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		ch := hb.subscribe(room)
		defer hb.unsubscribe(room, ch)

		fmt.Fprintf(w, ": connected to room %s\n\n", room)
		flusher.Flush()

		notify := r.Context().Done()
		for {
			select {
			case <-notify:
				return
			case payload, ok := <-ch:
				if !ok {
					return
				}
				fmt.Fprintf(w, "data: %s\n\n", payload)
				flusher.Flush()
			}
		}
	})

	// 7) HTTP server & graceful shutdown
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		shared.Pulse.Logger.Debugf("http.listen addr=%s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			shared.Pulse.Logger.Errorf("http.failed err=%v", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	shared.Pulse.Logger.Debug("shutdown.ok")
}

func supervisePull(ctx context.Context, cons nats.ConsumerOperations, hb *hub) {
	backoff := 250 * time.Millisecond
	const backoffMax = 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// start/restore the pull subscription with a cancelable context
		pctx, pcancel := context.WithCancel(ctx)
		msgCh, err := cons.SubscribePull(helpers.NatsContext{Ctx: pctx})
		if err != nil {
			pcancel()
			shared.Pulse.Logger.Errorf("pull.subscribe.failed err=%v", err)
			time.Sleep(backoff)
			if backoff < backoffMax {
				backoff *= 2
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			continue
		}
		shared.Pulse.Logger.Debug("pull.subscribe.ok")
		backoff = 250 * time.Millisecond

		// read until the channel closes or parent ctx ends
		for {
			select {
			case <-ctx.Done():
				pcancel()
				return

			case raw, ok := <-msgCh:
				if !ok {
					shared.Pulse.Logger.Warn("pull.channel.closed; will retry")
					pcancel()
					time.Sleep(backoff)
					if backoff < backoffMax {
						backoff *= 2
						if backoff > backoffMax {
							backoff = backoffMax
						}
					}
					break
				}

				var msg ChatMessage
				if err := json.Unmarshal(raw.Data(), &msg); err != nil {
					shared.Pulse.Logger.Errorf("pull.unmarshal.failed err=%v", err)
					continue
				}
				payload, _ := json.Marshal(msg)
				hb.broadcast(msg.Room, payload)
			}
		}
	}
}
