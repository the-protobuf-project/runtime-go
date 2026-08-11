package nats

import (
	"fmt"
	"strings"

	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/machanirobotics/loom/go/nats/types"
)

// StreamManager defines operations for managing JetStream streams.
//
// All methods accept an optional helpers.NatsContext (variadic). If omitted,
// a cancellable background context is used. Provide a NatsContext with a
// deadline when you want operations to time out deterministically.
type StreamManager interface {
	// CreateStream creates a new stream using the provided configuration.
	// If a stream with the same name exists, it is updated to match opts.
	// The call blocks until completion or until the provided/implicit context ends.
	CreateStream(opts types.StreamConfig, context ...helpers.NatsContext) (*StreamHandler, error)

	// UpdateStream updates an existing stream to match the provided configuration.
	// Returns an error if the stream does not exist or the update fails.
	// The call blocks until completion or until the provided/implicit context ends.
	UpdateStream(opts types.StreamConfig, context ...helpers.NatsContext) (*StreamHandler, error)

	// GetStream retrieves the stream identified by name.
	// Returns an error if the stream does not exist or retrieval fails.
	// The call blocks until completion or until the provided/implicit context ends.
	GetStream(name string, context ...helpers.NatsContext) (*StreamHandler, error)

	// ListStreams returns the names of all existing streams.
	// The call blocks until completion or until the provided/implicit context ends.
	ListStreams(context ...helpers.NatsContext) ([]string, error)

	// DeleteStream deletes the stream identified by name.
	// Returns an error if the stream does not exist or deletion fails.
	// The call blocks until completion or until the provided/implicit context ends.
	DeleteStream(name string, context ...helpers.NatsContext) error

	// SetStream sets the current stream to be used by default
	// for Producer and Consumer operations.
	// Returns an error if the stream does not exist or setting fails.
	SetStream(name string, context ...helpers.NatsContext) (*StreamHandler, error)
}

// StreamHandler provides a unified interface for interacting with a single stream.
//
// A StreamHandler encapsulates both the ProducerManager and ConsumerManager
// associated with a stream, allowing clients to publish (produce) and subscribe
// (consume) messages through a single entry point.
//
// Typical usage involves obtaining a StreamHandler for a given stream from
// a higher-level StreamManager, then using the Producer to publish messages
// and the Consumer to subscribe to or process messages.
type StreamHandler struct {
	// Stream Info provides metadata and configuration details about the
	// associated stream.
	Stream types.StreamInfo

	// Producer manages publishing messages to the stream.
	Producer ProducerManager

	// Consumer manages subscribing to and processing messages from the stream.
	Consumer ConsumerManager
}

// CreateStream creates or updates a stream with the given options.
// If the stream already exists, UpdateStream is called; otherwise, CreateStream is used.
func (j *jetstreamHandler) CreateStream(opts types.StreamConfig, context ...helpers.NatsContext) (*StreamHandler, error) {
	shared.Pulse.Logger.Debugf("creating stream %q", opts.Name)
	ctx := helpers.HandleContext(context...)

	s, err := j.client.CreateStream(ctx.Ctx, opts)
	if err != nil {
		if isAlreadyExists(err) {
			shared.Pulse.Logger.Debugf("stream %q exists; attempting update", opts.Name)
			return j.UpdateStream(opts, context...)
		}
		shared.Pulse.Logger.Errorf("failed to create stream %q: %v", opts.Name, err)
		return nil, fmt.Errorf("create stream %q: %w", opts.Name, err)
	}

	// Verify info to avoid nil CachedInfo deref.
	info, ierr := s.Info(ctx.Ctx)
	if ierr != nil {
		shared.Pulse.Logger.Warnf("created stream %q but failed to fetch info: %v", opts.Name, ierr)
	}
	return &StreamHandler{
		Stream:   *info, // safe if ierr==nil; otherwise consider zero value or fetch later
		Producer: &producerHandler{stream: j.client},
		Consumer: &consumerHandler{stream: s},
	}, nil
}

// UpdateStream updates the configuration of an existing stream.
func (j *jetstreamHandler) UpdateStream(opts types.StreamConfig, context ...helpers.NatsContext) (*StreamHandler, error) {
	shared.Pulse.Logger.Debugf("updating stream %q", opts.Name)
	ctx := helpers.HandleContext(context...)

	// Get handle, then Info() to truly verify
	s, _ := j.client.Stream(ctx.Ctx, opts.Name)
	if s == nil {
		return nil, fmt.Errorf("update stream %q: handle is nil", opts.Name)
	}
	if _, err := s.Info(ctx.Ctx); err != nil {
		if isNotFound(err) {
			shared.Pulse.Logger.Errorf("cannot update non-existent stream %q: %v", opts.Name, err)
			return nil, fmt.Errorf("update stream %q: %w", opts.Name, err)
		}
		// non-not-found error
		return nil, fmt.Errorf("update stream %q (info): %w", opts.Name, err)
	}

	us, err := j.client.UpdateStream(ctx.Ctx, opts)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to update stream %q: %v", opts.Name, err)
		return nil, fmt.Errorf("update stream %q: %w", opts.Name, err)
	}
	info, ierr := us.Info(ctx.Ctx)
	if ierr != nil {
		shared.Pulse.Logger.Warnf("updated stream %q but failed to fetch info: %v", opts.Name, ierr)
	}
	return &StreamHandler{
		Stream:   *info,
		Producer: &producerHandler{stream: j.client},
		Consumer: &consumerHandler{stream: us},
	}, nil
}

// GetStream retrieves a stream by name, returning a clear “not found” error if missing.
func (j *jetstreamHandler) GetStream(name string, context ...helpers.NatsContext) (*StreamHandler, error) {
	shared.Pulse.Logger.Debugf("fetching stream %q", name)
	ctx := helpers.HandleContext(context...)

	s, err := j.client.Stream(ctx.Ctx, name)
	if err != nil {
		// usually nil, Stream() just gives a handle
		return nil, fmt.Errorf("fetch stream %q: %w", name, err)
	}
	if s == nil {
		return nil, fmt.Errorf("stream %q not found", name)
	}
	info, ierr := s.Info(ctx.Ctx)
	if ierr != nil {
		if isNotFound(ierr) {
			shared.Pulse.Logger.Errorf("stream %q not found", name)
			return nil, fmt.Errorf("stream %q not found", name)
		}
		return nil, fmt.Errorf("fetch stream %q info: %w", name, ierr)
	}

	return &StreamHandler{
		Stream:   *info,
		Producer: &producerHandler{stream: j.client},
		Consumer: &consumerHandler{stream: s},
	}, nil
}

// ListStreams returns the names of all existing streams.
func (j *jetstreamHandler) ListStreams(context ...helpers.NatsContext) ([]string, error) {
	shared.Pulse.Logger.Debug("listing streams")

	ctx := helpers.HandleContext(context...)
	iter := j.client.ListStreams(ctx.Ctx)

	var names []string
	for info := range iter.Info() {
		names = append(names, info.Config.Name)
	}

	if err := iter.Err(); err != nil {
		shared.Pulse.Logger.Errorf("failed to list streams: %v", err)
		return nil, fmt.Errorf("list streams: %w", err)
	}

	shared.Pulse.Logger.Debugf("listed streams successfully (count=%d)", len(names))
	return names, nil
}

// DeleteStream removes a stream by name.
func (j *jetstreamHandler) DeleteStream(name string, context ...helpers.NatsContext) error {
	shared.Pulse.Logger.Debugf("deleting stream %q", name)

	if _, err := j.GetStream(name, context...); err != nil {
		shared.Pulse.Logger.Errorf("cannot delete non-existent stream %q: %v", name, err)
		return fmt.Errorf("delete stream %q: %w", name, err)
	}

	ctx := helpers.HandleContext(context...)
	if err := j.client.DeleteStream(ctx.Ctx, name); err != nil {
		shared.Pulse.Logger.Errorf("failed to delete stream %q: %v", name, err)
		return fmt.Errorf("delete stream %q: %w", name, err)
	}

	shared.Pulse.Logger.Debugf("deleted stream %q successfully", name)
	return nil
}

// SetStream sets the current stream to be used by default
// for Producer and Consumer operations.
func (j *jetstreamHandler) SetStream(name string, context ...helpers.NatsContext) (*StreamHandler, error) {
	shared.Pulse.Logger.Debugf("setting current stream to %q", name)

	stream, err := j.GetStream(name, context...)
	if err != nil {
		shared.Pulse.Logger.Errorf("cannot set non-existent stream %q: %v", name, err)
		return nil, fmt.Errorf("set current stream to %q: %w", name, err)
	}
	shared.Pulse.Logger.Debugf("set current stream to %q successfully", name)
	return stream, nil
}

// isNotFound reports whether err indicates that a stream was not found.
func isNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

// isAlreadyExists reports whether err indicates the stream already exists.
// (NATS returns various texts; match loosely.)
func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "already exists") ||
		strings.Contains(s, "in use") ||
		strings.Contains(s, "stream name already in use")
}
