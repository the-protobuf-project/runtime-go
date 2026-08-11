package nats

import (
	"fmt"
	"strings"
	"time"

	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/machanirobotics/loom/go/nats/types"
)

// ConsumerManager provides CRUD and consume operations for JetStream consumers.
type ConsumerManager interface {
	// CreateConsumer creates a pull consumer on the given stream using cfg.
	// It blocks up to timeout waiting for the creation to complete.
	CreateConsumer(cfg types.ConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error)

	// UpdateConsumer updates an existing pull consumer on the given stream using cfg.
	// It blocks up to timeout waiting for the update to complete.
	UpdateConsumer(cfg types.ConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error)

	// DeleteConsumer removes the named consumer from the given stream.
	// It blocks up to timeout waiting for the deletion to complete.
	DeleteConsumer(name string, context ...helpers.NatsContext) error

	// GetConsumer retrieves a handle to the named consumer on the given stream.
	// It blocks up to timeout waiting for the info to be fetched.
	GetConsumer(name string, context ...helpers.NatsContext) (ConsumerOperations, error)

	// SetConsumer sets the current consumer to be used by default
	// for consume operations.
	// Returns an error if the consumer does not exist or setting fails.
	// It blocks up to timeout waiting for the operation to complete.
	SetConsumer(name string, context ...helpers.NatsContext) (ConsumerOperations, error)

	// OrderedConsumer creates an ordered (ephemeral pull) consumer on the given stream using cfg.
	// It blocks up to timeout waiting for creation to complete.
	OrderedConsumer(cfg types.OrderedConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error)

	// ListConsumers lists the names of all consumers on the given stream.
	// It blocks up to timeout waiting for the list to be fetched.
	ListConsumers(context ...helpers.NatsContext) ([]string, error)

	// EnsureConsumer creates or gets a pull consumer on the given stream using cfg.
	// If the consumer already exists, it is returned as-is; otherwise, it is created.
	// It blocks up to timeout waiting for the operation to complete.
	EnsureConsumer(cfg types.ConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error)
}

type ConsumerOperations interface {
	// PauseConsumer pauses delivery on the given consumer until until.
	// It blocks up to timeout waiting for the pause to take effect.
	PauseConsumer(until time.Time, context ...helpers.NatsContext) (*types.ConsumerPauseResponse, error)

	// ResumeConsumer resumes a previously paused consumer.
	// It blocks up to timeout waiting for the resume to take effect.
	ResumeConsumer(context ...helpers.NatsContext) (*types.ConsumerPauseResponse, error)

	// SubscribePull pulls messages from a pull consumer on the given stream.
	// It blocks up to timeout waiting for the consumer handle, then starts
	// fetching messages. Messages are ACKed and their payloads sent on the returned channel.
	SubscribePull(ctx ...helpers.NatsContext) (<-chan types.JetstreamMessage, error)
}

type consumerHandler struct {
	stream types.Stream
}

type consumerOperationsHandler struct {
	stream   types.Stream
	consumer types.Consumer
}

// CreateConsumer creates or gets a pull consumer on the given stream using cfg.
func (ch *consumerHandler) CreateConsumer(cfg types.ConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error) {
	streamName := ch.stream.CachedInfo().Config.Name
	shared.Pulse.Logger.Debugf("creating consumer stream=%q name=%q", streamName, cfg.Name)

	ctx := helpers.HandleContext(context...)
	cons, err := ch.stream.CreateConsumer(ctx.Ctx, cfg)
	if err != nil {
		// If it already exists (duplicate), just ensure/get it.
		if strings.Contains(strings.ToLower(err.Error()), "exists") {
			shared.Pulse.Logger.Warnf("consumer already exists stream=%q name=%q, returning existing", streamName, cfg.Name)
			return ch.EnsureConsumer(cfg, ctx)
		}
		shared.Pulse.Logger.Errorf("create consumer failed stream=%q name=%q err=%v", streamName, cfg.Name, err)
		return nil, fmt.Errorf("create consumer %q on stream %q: %w", cfg.Name, streamName, err)
	}
	shared.Pulse.Logger.Debugf("consumer created stream=%q name=%q", streamName, cfg.Name)
	return &consumerOperationsHandler{stream: ch.stream, consumer: cons}, nil
}

// UpdateConsumer updates an existing pull consumer on the given stream using cfg.
func (ch *consumerHandler) UpdateConsumer(cfg types.ConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error) {
	streamName := ch.stream.CachedInfo().Config.Name
	shared.Pulse.Logger.Debugf("updating consumer stream=%q name=%q", streamName, cfg.Name)

	ctx := helpers.HandleContext(context...)
	cons, err := ch.stream.UpdateConsumer(ctx.Ctx, cfg)
	if err != nil {
		shared.Pulse.Logger.Errorf("update consumer failed stream=%q name=%q err=%v", streamName, cfg.Name, err)
		return nil, fmt.Errorf("update consumer %q on stream %q: %w", cfg.Name, streamName, err)
	}

	shared.Pulse.Logger.Debugf("consumer updated stream=%q name=%q", streamName, cfg.Name)
	return &consumerOperationsHandler{
		stream:   ch.stream,
		consumer: cons,
	}, nil
}

// DeleteConsumer removes the named consumer from the given stream.
func (ch *consumerHandler) DeleteConsumer(name string, context ...helpers.NatsContext) error {
	streamName := ch.stream.CachedInfo().Config.Name
	shared.Pulse.Logger.Debugf("deleting consumer stream=%q name=%q", streamName, name)

	ctx := helpers.HandleContext(context...)
	if err := ch.stream.DeleteConsumer(ctx.Ctx, name); err != nil {
		shared.Pulse.Logger.Errorf("delete consumer failed stream=%q name=%q err=%v", streamName, name, err)
		return fmt.Errorf("delete consumer %q on stream %q: %w", name, streamName, err)
	}

	shared.Pulse.Logger.Debugf("consumer deleted stream=%q name=%q", streamName, name)
	return nil
}

// GetConsumer retrieves a handle to the named consumer on the given stream.
func (ch *consumerHandler) GetConsumer(name string, context ...helpers.NatsContext) (ConsumerOperations, error) {
	streamName := ch.stream.CachedInfo().Config.Name
	shared.Pulse.Logger.Debugf("fetching consumer stream=%q name=%q", streamName, name)

	ctx := helpers.HandleContext(context...)
	cons, err := ch.stream.Consumer(ctx.Ctx, name)
	if err != nil {
		shared.Pulse.Logger.Errorf("get consumer failed stream=%q name=%q err=%v", streamName, name, err)
		return nil, fmt.Errorf("get consumer %q on stream %q: %w", name, streamName, err)
	}
	if cons == nil {
		shared.Pulse.Logger.Errorf("consumer not found stream=%q name=%q", streamName, name)
		return nil, fmt.Errorf("consumer %q on stream %q not found", name, streamName)
	}

	shared.Pulse.Logger.Debugf("consumer fetched stream=%q name=%q", streamName, name)
	return &consumerOperationsHandler{
		stream:   ch.stream,
		consumer: cons,
	}, nil
}

// SetConsumer sets the current consumer to be used by default
// for consume operations.
// Returns an error if the consumer does not exist or setting fails.
// It blocks up to timeout waiting for the operation to complete.
func (ch *consumerHandler) SetConsumer(name string, context ...helpers.NatsContext) (ConsumerOperations, error) {
	shared.Pulse.Logger.Debugf("setting current consumer name=%q", name)
	return ch.GetConsumer(name, context...) // Check if consumer exists
}

// OrderedConsumer creates an ordered (ephemeral pull) consumer on the given stream using cfg.
func (ch *consumerHandler) OrderedConsumer(cfg types.OrderedConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error) {
	streamName := ch.stream.CachedInfo().Config.Name
	shared.Pulse.Logger.Debugf("creating ordered consumer stream=%q", streamName)

	ctx := helpers.HandleContext(context...)
	cons, err := ch.stream.OrderedConsumer(ctx.Ctx, cfg)
	if err != nil {
		shared.Pulse.Logger.Errorf("create ordered consumer failed stream=%q err=%v", streamName, err)
		return nil, fmt.Errorf("create ordered consumer on stream %q: %w", streamName, err)
	}

	shared.Pulse.Logger.Debugf("ordered consumer created stream=%q", streamName)
	return &consumerOperationsHandler{
		stream:   ch.stream,
		consumer: cons,
	}, nil
}

// ListConsumers lists the names of all consumers on the given stream.
func (ch *consumerHandler) ListConsumers(context ...helpers.NatsContext) ([]string, error) {
	streamName := ch.stream.CachedInfo().Config.Name
	shared.Pulse.Logger.Debugf("listing consumers stream=%q", streamName)

	ctx := helpers.HandleContext(context...)
	iter := ch.stream.ListConsumers(ctx.Ctx)

	var names []string
	for info := range iter.Info() {
		names = append(names, info.Name)
	}
	if err := iter.Err(); err != nil {
		shared.Pulse.Logger.Errorf("list consumers failed stream=%q err=%v", streamName, err)
		return nil, fmt.Errorf("list consumers on stream %q: %w", streamName, err)
	}
	if len(names) == 0 {
		shared.Pulse.Logger.Warnf("no consumers found stream=%q", streamName)
		return nil, nil
	}

	shared.Pulse.Logger.Debugf("consumers listed stream=%q count=%d", streamName, len(names))
	return names, nil
}

// EnsureConsumer creates or gets a pull consumer on the given stream using cfg.
// If the consumer already exists, it is returned; otherwise, it is created.
func (ch *consumerHandler) EnsureConsumer(cfg types.ConsumerConfig, context ...helpers.NatsContext) (ConsumerOperations, error) {
	streamName := ch.stream.CachedInfo().Config.Name
	shared.Pulse.Logger.Debugf("ensuring consumer stream=%q name=%q", streamName, cfg.Name)

	ctx := helpers.HandleContext(context...)

	// 1) Try to fetch
	cons, err := ch.stream.Consumer(ctx.Ctx, cfg.Name)
	if err != nil {
		// If it's truly "not found", fall through to create.
		if !isConsumerNotFound(err) {
			shared.Pulse.Logger.Errorf("ensure consumer lookup failed stream=%q name=%q err=%v", streamName, cfg.Name, err)
			return nil, fmt.Errorf("ensure consumer %q on stream %q: %w", cfg.Name, streamName, err)
		}
		// else: proceed to create
	} else if cons != nil {
		shared.Pulse.Logger.Debugf("consumer exists stream=%q name=%q", streamName, cfg.Name)
		return &consumerOperationsHandler{stream: ch.stream, consumer: cons}, nil
	}

	// 2) Not found → create
	cons, err = ch.stream.CreateConsumer(ctx.Ctx, cfg)
	if err != nil {
		shared.Pulse.Logger.Errorf("ensure consumer create failed stream=%q name=%q err=%v", streamName, cfg.Name, err)
		return nil, fmt.Errorf("create consumer %q on stream %q: %w", cfg.Name, streamName, err)
	}

	shared.Pulse.Logger.Debugf("consumer ensured (created) stream=%q name=%q", streamName, cfg.Name)
	return &consumerOperationsHandler{stream: ch.stream, consumer: cons}, nil
}

// PauseConsumer pauses delivery on the given consumer until until.
// It blocks up to timeout waiting for the pause to take effect.
func (coh *consumerOperationsHandler) PauseConsumer(until time.Time, context ...helpers.NatsContext) (*types.ConsumerPauseResponse, error) {
	stream := coh.stream.CachedInfo().Config.Name
	consumer := coh.consumer.CachedInfo().Name
	shared.Pulse.Logger.Debugf("pausing consumer stream=%q name=%q until=%s", stream, consumer, until.Format(time.RFC3339))

	ctx := helpers.HandleContext(context...)
	resp, err := coh.stream.PauseConsumer(ctx.Ctx, consumer, until)
	if err != nil {
		shared.Pulse.Logger.Errorf("pause consumer failed stream=%q name=%q err=%v", stream, consumer, err)
		return nil, fmt.Errorf("pause consumer %q on stream %q: %w", consumer, stream, err)
	}

	shared.Pulse.Logger.Debugf("consumer paused stream=%q name=%q", stream, consumer)
	return resp, nil
}

// ResumeConsumer resumes a previously paused consumer.
// It blocks up to timeout waiting for the resume to take effect.
func (coh *consumerOperationsHandler) ResumeConsumer(context ...helpers.NatsContext) (*types.ConsumerPauseResponse, error) {
	stream := coh.stream.CachedInfo().Config.Name
	consumer := coh.consumer.CachedInfo().Name
	shared.Pulse.Logger.Debugf("resuming consumer stream=%q name=%q", stream, consumer)

	ctx := helpers.HandleContext(context...)
	resp, err := coh.stream.ResumeConsumer(ctx.Ctx, consumer)
	if err != nil {
		shared.Pulse.Logger.Errorf("resume consumer failed stream=%q name=%q err=%v", stream, consumer, err)
		return nil, fmt.Errorf("resume consumer %q on stream %q: %w", consumer, stream, err)
	}

	shared.Pulse.Logger.Debugf("consumer resumed stream=%q name=%q", stream, consumer)
	return resp, nil
}

// SubscribePull pulls messages until ctx is canceled or the iterator/connection closes.
// It treats iterator/connection close as a graceful stop.
func (coh *consumerOperationsHandler) SubscribePull(ctx ...helpers.NatsContext) (<-chan types.JetstreamMessage, error) {
	stream := coh.stream.CachedInfo().Config.Name
	consumerName := coh.consumer.CachedInfo().Name
	shared.Pulse.Logger.Debugf("starting pull subscription stream=%q name=%q", stream, consumerName)

	hctx := helpers.HandleContext(ctx...) // provides ctx.Ctx (caller may pass a deadline/cancel)

	out := make(chan types.JetstreamMessage)
	go func() {
		defer close(out)

		for {
			// Respect cancellation from caller
			select {
			case <-hctx.Ctx.Done():
				shared.Pulse.Logger.Debugf("pull subscription context done stream=%q name=%q", stream, consumerName)
				return
			default:
			}

			msgs, err := coh.consumer.Messages(
				types.PullMaxMessages(1),
				types.PullExpiry(2*time.Second),
			)
			if err != nil {
				// If connection is closing/closed, exit quietly.
				if isConnClosedErr(err) {
					shared.Pulse.Logger.Debugf("pull subscription iterator closed (conn closed) stream=%q name=%q", stream, consumerName)
					return
				}
				shared.Pulse.Logger.Errorf("pull subscription open failed stream=%q name=%q err=%v", stream, consumerName, err)
				time.Sleep(250 * time.Millisecond)
				continue
			}

			// Ensure iterator gets stopped in all paths.
			func() {
				defer func() {
					// Stop closes internal resources; safe to call multiple times.
					msgs.Stop()
				}()

				// Next blocks until a message, expiry, or iterator/conn close.
				m, err := msgs.Next()
				if err != nil {
					if isIdlePullError(err) {
						// Expired with no messages – loop and try again.
						return
					}
					if isIteratorClosedErr(err) || isConnClosedErr(err) {
						shared.Pulse.Logger.Debugf("pull subscription ended (iterator/conn closed) stream=%q name=%q", stream, consumerName)
						return
					}
					shared.Pulse.Logger.Errorf("pull subscription next failed stream=%q name=%q err=%v", stream, consumerName, err)
					return
				}

				out <- m
				_ = m.Ack()
			}()
		}
	}()
	return out, nil
}
