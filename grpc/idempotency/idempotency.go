package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// The fields an RPC declares to opt in, and to make a dry run.
const (
	RequestIDField    = "request_id"
	ValidateOnlyField = "validate_only"
)

// The ErrorInfo reasons of the two retryable refusals, so a client can tell
// them from a stale etag -- the other ABORTED an API returns -- without
// reading the message.
const (
	ReasonInFlight  = "REQUEST_IN_FLIGHT"
	ReasonAbandoned = "REQUEST_ABANDONED"
)

// ErrNotFound is what Store.Get reports for a key that does not exist.
var ErrNotFound = errors.New("idempotency: key not found")

// State is where a key stands.
type State int

const (
	// InFlight: the first call is running.
	InFlight State = iota + 1
	// Done: the first call succeeded; Response holds what it returned.
	Done
)

// Key records one (method, request_id).
type Key struct {
	// Name is KeyName(Method, RequestID).
	Name      string
	Method    string
	RequestID string
	State     State
	// Response is the first call's response: a protojson google.protobuf.Any.
	Response string
	// CreateTime is set by the store when the key is claimed.
	CreateTime time.Time
}

// Store keeps keys. Build it over the live connection, whatever the backend.
type Store interface {
	// Create claims a key. It fails when one of that name exists -- with any
	// error: the interceptor reads the key to tell a lost race from a fault.
	Create(ctx context.Context, k Key) error
	// Get reads a key: ErrNotFound when there is none.
	Get(ctx context.Context, name string) (Key, error)
	// Settle writes a claimed key's State and Response.
	Settle(ctx context.Context, k Key) error
	// Delete releases a key.
	Delete(ctx context.Context, name string) error
}

// Options tune the interceptor. The zero value of each field is its default.
type Options struct {
	// Domain is the ErrorInfo domain of the refusals.
	Domain string
	// StaleAfter is how long a claim may sit IN_FLIGHT before it is treated
	// as abandoned: far longer than any handler runs, so reclaiming a key this
	// old cannot race a handler still working. Default 10 minutes.
	StaleAfter time.Duration
	// CreateError is returned when a claim failed and no key explains it --
	// the store is unwell -- given the claim's error. Default: Internal.
	CreateError func(error) error
	// Logger reports the failures the interceptor cannot return. Default
	// slog.Default().
	Logger telemetry.Logger
	// Now is the clock. Default time.Now.
	Now func() time.Time
}

// Interceptor returns the unary interceptor that enforces request_id
// semantics against store.
func Interceptor(store Store, opts Options) grpc.UnaryServerInterceptor {
	if opts.StaleAfter <= 0 {
		opts.StaleAfter = 10 * time.Minute
	}
	if opts.CreateError == nil {
		opts.CreateError = func(err error) error { return status.Error(codes.Internal, "idempotency: claim key: "+err.Error()) }
	}
	if opts.Logger == nil {
		opts.Logger = telemetry.NewSlogLogger(slog.Default())
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	i := &interceptor{store: store, opts: opts}
	return i.intercept
}

type interceptor struct {
	store Store
	opts  Options
}

func (i *interceptor) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	msg, ok := req.(proto.Message)
	if !ok {
		return handler(ctx, req)
	}
	requestID := stringField(msg, RequestIDField)
	if requestID == "" || boolField(msg, ValidateOnlyField) {
		// The RPC does not offer idempotency, the caller declined it, or it
		// is a dry run that writes nothing. Nothing to dedupe.
		return handler(ctx, req)
	}

	key := Key{Name: KeyName(info.FullMethod, requestID), Method: info.FullMethod, RequestID: requestID, State: InFlight}
	if err := i.store.Create(ctx, key); err != nil {
		// Lost the race to claim this id, or the store is unwell. Only the
		// key itself can tell which, so read it.
		return i.replay(ctx, key.Name, err)
	}

	resp, err := handler(ctx, req)
	if err != nil {
		// The call failed and wrote nothing worth remembering. Release the id
		// so the caller may retry it for real.
		if derr := i.store.Delete(ctx, key.Name); derr != nil {
			i.opts.Logger.Error(ctx, "idempotency: release key after handler error", derr,
				telemetry.Fields{"method": info.FullMethod, "request_id": requestID})
		}
		return nil, err
	}
	i.settle(ctx, key, resp)
	return resp, nil
}

// replay answers a caller whose claim lost the race. createErr is the failure
// that sent it here, returned when the key turns out not to explain it.
func (i *interceptor) replay(ctx context.Context, name string, createErr error) (any, error) {
	existing, err := i.store.Get(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// The create failed for some reason other than a duplicate key,
			// so idempotency has nothing to say about it.
			return nil, i.opts.CreateError(createErr)
		}
		return nil, status.Error(codes.Internal, "idempotency: read key: "+err.Error())
	}

	if existing.State == Done {
		resp, err := decode(existing.Response)
		if err != nil {
			return nil, status.Error(codes.Internal, "idempotency: decode recorded response: "+err.Error())
		}
		return resp, nil
	}

	// IN_FLIGHT. A claim older than any handler could run for was made by a
	// process that died before settling; drop it so the next attempt claims
	// cleanly rather than wedging this id forever.
	if !existing.CreateTime.IsZero() && i.opts.Now().Sub(existing.CreateTime) > i.opts.StaleAfter {
		if err := i.store.Delete(ctx, name); err != nil {
			return nil, status.Error(codes.Internal, "idempotency: reclaim abandoned key: "+err.Error())
		}
		return nil, i.aborted(ReasonAbandoned, "An earlier request with this request_id stopped before it finished, and its claim has now been cleared. "+
			"Retry the same request with the same request_id: it will run once")
	}
	return nil, i.aborted(ReasonInFlight, "A request with this request_id is still running. "+
		"Wait a moment and retry with the same request_id: once the first finishes, the retry returns its result without running it again")
}

func (i *interceptor) aborted(reason, msg string) error {
	st := status.New(codes.Aborted, msg+".")
	if detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: i.opts.Domain}); err == nil {
		return detailed.Err()
	}
	return st.Err()
}

// settle records what the handler returned, so a later retry replays it. A
// failure here is logged and swallowed: the write the caller asked for has
// happened, and an error would tell them otherwise. The cost is that a retry
// re-runs the handler and meets the underlying constraint instead of
// replaying.
func (i *interceptor) settle(ctx context.Context, key Key, resp any) {
	msg, ok := resp.(proto.Message)
	if !ok {
		return
	}
	fields := telemetry.Fields{"method": key.Method, "request_id": key.RequestID}
	encoded, err := encode(msg)
	if err != nil {
		i.opts.Logger.Error(ctx, "idempotency: encode response", err, fields)
		return
	}
	key.State, key.Response = Done, encoded
	if err := i.store.Settle(ctx, key); err != nil {
		i.opts.Logger.Error(ctx, "idempotency: settle key", err, fields)
	}
}

// encode packs msg into an Any and renders it as protojson. The Any carries
// the message's type, so a replay rebuilds the response without knowing which
// RPC recorded it.
func encode(msg proto.Message) (string, error) {
	a, err := anypb.New(msg)
	if err != nil {
		return "", err
	}
	out, err := protojson.Marshal(a)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// decode rebuilds the response encode recorded.
func decode(s string) (proto.Message, error) {
	var a anypb.Any
	if err := protojson.Unmarshal([]byte(s), &a); err != nil {
		return nil, err
	}
	return a.UnmarshalNew()
}

// KeyName addresses the key for (method, request_id): "idempotencyKeys/"
// and a SHA-256. The digest keeps the name a fixed, safe length whatever the
// caller sent, and makes the name itself the uniqueness constraint that
// decides who wins a concurrent claim.
func KeyName(method, requestID string) string {
	sum := sha256.Sum256([]byte(method + "\x00" + requestID))
	return "idempotencyKeys/" + hex.EncodeToString(sum[:])
}

func stringField(msg proto.Message, name string) string {
	fd := field(msg, name)
	if fd == nil || fd.Kind() != protoreflect.StringKind {
		return ""
	}
	return msg.ProtoReflect().Get(fd).String()
}

func boolField(msg proto.Message, name string) bool {
	fd := field(msg, name)
	if fd == nil || fd.Kind() != protoreflect.BoolKind {
		return false
	}
	return msg.ProtoReflect().Get(fd).Bool()
}

func field(msg proto.Message, name string) protoreflect.FieldDescriptor {
	fd := msg.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil || fd.IsList() || fd.IsMap() {
		return nil
	}
	return fd
}
