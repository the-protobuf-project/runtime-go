package observability

import (
	"context"
	"maps"

	"github.com/the-protobuf-project/opentelemetry/opentelemetry-go"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// logger adapts opentelemetry's logger to [telemetry.Logger].
//
// Two shape differences are reconciled here. The SDK's logger takes no context
// per call — it carries one, set with WithContext — while the contract passes a
// context to every call; and its Error returns an error for `return l.Error(…)`
// convenience, while the contract records rather than returns. The adapter
// binds the context per call and discards the convenience return.
//
// It holds the parent rather than the logger itself because the SDK's logger
// type lives in an internal package: its methods are callable, but the type
// cannot be named from out here.
type logger struct {
	otel  *opentelemetry.Opentelemetry
	bound telemetry.Fields
}

var _ telemetry.Logger = logger{}

func newLogger(o *opentelemetry.Opentelemetry) telemetry.Logger {
	return logger{otel: o}
}

// merge combines the bound fields with a call's. The call's win on a collision,
// which is what With({"id": x}).Info(…, {"id": y}) means.
//
// It returns nil when there is nothing to record, so the SDK takes its no-data
// path rather than emitting an empty attribute set.
func (l logger) merge(fields telemetry.Fields) map[string]any {
	if len(l.bound) == 0 && len(fields) == 0 {
		return nil
	}
	out := make(map[string]any, len(l.bound)+len(fields))
	maps.Copy(out, l.bound)
	maps.Copy(out, fields)
	return out
}

// data adapts merged fields to the variadic the SDK takes. It passes a single
// map, which is the shape the SDK formats and converts to OTLP attributes.
func (l logger) data(fields telemetry.Fields) []any {
	m := l.merge(fields)
	if m == nil {
		return nil
	}
	return []any{m}
}

// bind attaches the call's context so exported records carry the right trace
// and span IDs. The SDK holds its context rather than taking one per call, so
// this makes a shallow copy per record — cheap, and the only way to correlate
// without changing the SDK's signature.
func (l logger) bind(ctx context.Context) interface {
	Debug(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any) error
} {
	if ctx == nil {
		return l.otel.Logger
	}
	return l.otel.Logger.WithContext(ctx)
}

func (l logger) Debug(ctx context.Context, msg string, fields telemetry.Fields) {
	l.bind(ctx).Debug(msg, l.data(fields)...)
}

func (l logger) Info(ctx context.Context, msg string, fields telemetry.Fields) {
	l.bind(ctx).Info(msg, l.data(fields)...)
}

func (l logger) Warn(ctx context.Context, msg string, fields telemetry.Fields) {
	l.bind(ctx).Warn(msg, l.data(fields)...)
}

// Error records the error under the conventional "error" key, so a handler can
// find it without knowing which field the caller chose.
func (l logger) Error(ctx context.Context, msg string, err error, fields telemetry.Fields) {
	if err != nil {
		withErr := make(telemetry.Fields, len(fields)+1)
		maps.Copy(withErr, fields)
		withErr["error"] = err.Error()
		fields = withErr
	}
	// The SDK's Error returns an error for `return l.Error(…)` convenience; the
	// contract records rather than returns, so it is discarded here.
	_ = l.bind(ctx).Error(msg, l.data(fields)...)
}

// Enabled always reports true.
//
// The SDK filters by level internally but does not expose the threshold — its
// logger field is an internal type whose level accessor is unexported — so this
// cannot answer accurately. Reporting true is the safe direction: a guarded
// call site does its work and the SDK drops the record, which costs some
// formatting but never loses a log line. Reporting false would silently
// suppress records the SDK would have kept.
//
// If opentelemetry later exposes its level, this should return the real
// answer so debug-guarded paths can be skipped outright.
func (l logger) Enabled(context.Context, telemetry.Level) bool {
	return true
}

func (l logger) With(fields telemetry.Fields) telemetry.Logger {
	if len(fields) == 0 {
		return l
	}
	return logger{otel: l.otel, bound: l.merge(fields)}
}
