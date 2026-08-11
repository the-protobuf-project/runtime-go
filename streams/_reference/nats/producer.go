package nats

import (
	"fmt"
	"time"

	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/machanirobotics/loom/go/nats/types"
)

// ProducerManager handles publishing messages to NATS/JetStream.
//
// Notes on behavior:
//   - Publish / PublishRaw support synchronous and asynchronous modes.
//   - Non-[]byte, non-string bodies are JSON-encoded before send.
//   - PublishState exposes a periodic snapshot of async publish progress.
type ProducerManager interface {
	// Publish sends req.Data to JetStream subject req.Subject.
	// If async is true, it returns after the async publish is queued and
	// then waits on the returned future for either Ok or Err.
	// If async is false, it blocks up to 5s for the server ack.
	Publish(req types.JetStreamPublishRequest, context ...helpers.NatsContext) (*types.PubAck, error)

	// PublishRaw publishes a raw NATS message (no stream expectations) to req.Subject.
	// Semantics for async vs sync mirror Publish.
	PublishRaw(req types.JetStreamPublishRequest, context ...helpers.NatsContext) (*types.PubAck, error)

	// PublishState returns a channel emitting async publish state snapshots.
	// It polls PublishAsyncPending() every 500ms and also listens for
	// PublishAsyncComplete(). When all acks arrive, it emits a final
	// state with Pending=0, Complete=true and then closes the channel.
	PublishState() (<-chan types.JetStreamPublishState, error)

	// Clean cleans up any pending async publish state.
	// No-op if there is nothing pending.
	Clean() error
}

// producerHandler implements ProducerManager against a JetStream-like client.
type producerHandler struct {
	stream types.Jetstream // underlying JetStream client
}

// Publish sends data to a JetStream subject in either synchronous or asynchronous mode.
// Non-byte/string payloads are JSON-encoded. Returns a PubAck or an error.
func (p *producerHandler) Publish(req types.JetStreamPublishRequest, context ...helpers.NatsContext) (*types.PubAck, error) {
	shared.Pulse.Logger.Debugf("publishing message to subject=%q (async=%t)", req.Subject, req.Async)
	b, err := ConvertAnyDataToBytes(req.Data)
	if err != nil {
		shared.Pulse.Logger.Errorf("encode payload failed for subject=%q: %v", req.Subject, err)
		return nil, fmt.Errorf("publish %q: %w", req.Subject, err)
	}

	// Async path
	if req.Async {
		future, err := p.stream.PublishAsync(req.Subject, b)
		if err != nil {
			shared.Pulse.Logger.Errorf("failed to start async publish for subject=%q: %v", req.Subject, err)
			return nil, fmt.Errorf("publish async %q: %w", req.Subject, err)
		}

		select {
		case ack := <-future.Ok():
			shared.Pulse.Logger.Debugf("async publish acked subject=%q stream=%q seq=%d", req.Subject, ack.Stream, ack.Sequence)
			return ack, nil
		case err := <-future.Err():
			shared.Pulse.Logger.Errorf("async publish failed for subject=%q: %v", req.Subject, err)
			return nil, fmt.Errorf("publish async %q: %w", req.Subject, err)
		}
	}

	// Handle synchronous publish
	ctx := helpers.HandleContext(context...)

	ack, err := p.stream.Publish(ctx.Ctx, req.Subject, b)
	if err != nil {
		shared.Pulse.Logger.Errorf("sync publish failed for subject=%q: %v", req.Subject, err)
		return nil, fmt.Errorf("publish %q: %w", req.Subject, err)
	}
	if ack == nil {
		shared.Pulse.Logger.Errorf("sync publish returned empty ack for subject=%q", req.Subject)
		return nil, fmt.Errorf("publish %q: empty ack", req.Subject)
	}

	shared.Pulse.Logger.Debugf("sync publish acked subject=%q stream=%q seq=%d", req.Subject, ack.Stream, ack.Sequence)
	return ack, nil
}

// PublishRaw sends a raw NATS message to the given subject (no stream expectations).
// Asynchronous and synchronous modes match Publish.
func (p *producerHandler) PublishRaw(req types.JetStreamPublishRequest, context ...helpers.NatsContext) (*types.PubAck, error) {
	shared.Pulse.Logger.Debugf("publishing raw message to subject=%q (async=%t)", req.Subject, req.Async)

	b, err := ConvertAnyDataToBytes(req.Data)
	if err != nil {
		shared.Pulse.Logger.Errorf("encode payload failed for raw publish subject=%q: %v", req.Subject, err)
		return nil, fmt.Errorf("publish raw %q: %w", req.Subject, err)
	}

	// Build a raw NATS message
	msg := types.BuildRawMessage(req, b)

	// Async path
	if req.Async {
		future, err := p.stream.PublishMsgAsync(msg)
		if err != nil {
			shared.Pulse.Logger.Errorf("failed to start async raw publish subject=%q: %v", req.Subject, err)
			return nil, fmt.Errorf("publish raw async %q: %w", req.Subject, err)
		}
		select {
		case ack := <-future.Ok():
			shared.Pulse.Logger.Debugf("async raw publish acked subject=%q stream=%q seq=%d", req.Subject, ack.Stream, ack.Sequence)
			return ack, nil
		case err := <-future.Err():
			shared.Pulse.Logger.Errorf("async raw publish failed subject=%q: %v", req.Subject, err)
			return nil, fmt.Errorf("publish raw async %q: %w", req.Subject, err)
		}
	}

	// Handle synchronous publish
	ctx := helpers.HandleContext(context...)

	ack, err := p.stream.PublishMsg(ctx.Ctx, msg)
	if err != nil {
		shared.Pulse.Logger.Errorf("sync raw publish failed subject=%q: %v", req.Subject, err)
		return nil, fmt.Errorf("publish raw %q: %w", req.Subject, err)
	}

	shared.Pulse.Logger.Debugf("sync raw publish acked subject=%q stream=%q seq=%d", req.Subject, ack.Stream, ack.Sequence)
	return ack, nil
}

// PublishState returns a channel with async publish progress snapshots.
//
// The returned channel will emit every ~500ms a types.JetStreamPublishState
// {Pending: <n>, Complete: false} until all acks are received, at which point
// it emits a final {Pending: 0, Complete: true} and closes the channel.
func (p *producerHandler) PublishState() (<-chan types.JetStreamPublishState, error) {
	shared.Pulse.Logger.Debug("starting publish state monitor")

	completeCh := p.stream.PublishAsyncComplete()
	if completeCh == nil {
		shared.Pulse.Logger.Errorf("cannot start publish state monitor: nil completion channel")
		return nil, fmt.Errorf("publish state: nil complete channel")
	}

	stateCh := make(chan types.JetStreamPublishState)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer func() {
			ticker.Stop()
			close(stateCh)
			shared.Pulse.Logger.Debug("publish state monitor stopped")
		}()

		for {
			select {
			case <-completeCh:
				shared.Pulse.Logger.Debug("async publishes complete")
				stateCh <- types.JetStreamPublishState{Pending: 0, Complete: true}
				return

			case <-ticker.C:
				pending := p.stream.PublishAsyncPending()
				shared.Pulse.Logger.Debugf("async publish pending=%d", pending)
				stateCh <- types.JetStreamPublishState{Pending: pending, Complete: false}
			}
		}
	}()
	return stateCh, nil
}

// PublishClean cleans up any pending async publish resources/state.
func (p *producerHandler) Clean() error {
	shared.Pulse.Logger.Debug("cleaning async publish state")
	p.stream.CleanupPublisher()
	shared.Pulse.Logger.Debug("async publish state cleaned")
	return nil
}
