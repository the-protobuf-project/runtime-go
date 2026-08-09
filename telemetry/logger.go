package telemetry

import "context"

// Level is a log severity. The values match log/slog's so the slog adapter is a
// straight conversion and a caller who already has a slog level can pass it
// through without a lookup table.
type Level int

const (
	// LevelDebug is for detail only useful when diagnosing a problem: which key
	// was read, which branch was taken. Off in production.
	LevelDebug Level = -4

	// LevelInfo is for events an operator would want in a normal log: a
	// component starting, a connection established.
	LevelInfo Level = 0

	// LevelWarn is for something unexpected that did not fail the operation —
	// a stale index entry swept, a retry that succeeded.
	LevelWarn Level = 4

	// LevelError is for an operation that failed.
	LevelError Level = 8
)

// String returns the conventional lowercase name of the level.
func (l Level) String() string {
	switch {
	case l < LevelInfo:
		return "debug"
	case l < LevelWarn:
		return "info"
	case l < LevelError:
		return "warn"
	default:
		return "error"
	}
}

// Fields are the structured key/value pairs attached to a log record.
//
// Unlike [Labels], values are unconstrained: a log line is written once and
// read by a human, so it can carry an ID, a duration, or a nested value without
// the cardinality worry that applies to a metric dimension.
type Fields map[string]any

// Logger is the backend-agnostic logging contract.
//
// Every method takes a context so an implementation can pull the trace and span
// IDs out of it and correlate a log line with the request that produced it.
// Passing context.Background() is fine when there is nothing to correlate.
//
// Implementations must tolerate a nil Fields — it is the ordinary way to log a
// message that needs no structure.
type Logger interface {
	// Debug records detail useful only when diagnosing a problem.
	Debug(ctx context.Context, msg string, fields Fields)

	// Info records a normal, noteworthy event.
	Info(ctx context.Context, msg string, fields Fields)

	// Warn records something unexpected that did not fail the operation.
	Warn(ctx context.Context, msg string, fields Fields)

	// Error records a failed operation. The error is a distinct argument rather
	// than a field so that it cannot be forgotten and so implementations can
	// give it consistent treatment — an "error" key, a stack trace, a span
	// status. A nil err is allowed for a failure with no error value.
	Error(ctx context.Context, msg string, err error, fields Fields)

	// Enabled reports whether a record at this level would be kept.
	//
	// Use it to skip work that only exists to build a log line:
	//
	//	if log.Enabled(ctx, telemetry.LevelDebug) {
	//	    log.Debug(ctx, "swept index", telemetry.Fields{"ids": expensive()})
	//	}
	Enabled(ctx context.Context, level Level) bool

	// With returns a Logger that adds fields to every record it writes. Use it
	// to bind context once — a component name, a stream ID — instead of
	// repeating it at each call.
	With(fields Fields) Logger
}

// NoopLogger is a Logger that discards every record. It is the safe default
// when no backend has been wired in: libraries can hold a Logger
// unconditionally, with no nil checks, and pay nothing.
//
// Its Enabled always reports false, so guarded call sites skip building their
// fields entirely.
var NoopLogger Logger = noopLogger{}

type noopLogger struct{}

func (noopLogger) Debug(context.Context, string, Fields)        {}
func (noopLogger) Info(context.Context, string, Fields)         {}
func (noopLogger) Warn(context.Context, string, Fields)         {}
func (noopLogger) Error(context.Context, string, error, Fields) {}
func (noopLogger) Enabled(context.Context, Level) bool          { return false }
func (noopLogger) With(Fields) Logger                           { return noopLogger{} }
