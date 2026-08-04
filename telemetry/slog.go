package telemetry

import (
	"context"
	"log/slog"
)

// slogLogger adapts a *slog.Logger to [Logger].
type slogLogger struct {
	l *slog.Logger
}

// NewSlogLogger wraps a [log/slog.Logger] as a [Logger].
//
// slog is the default because it is in the standard library: a caller gets
// leveled, structured logging without this module taking on a dependency, and
// anything that already emits slog records — a JSON handler, an OTel bridge,
// a test handler — plugs in unchanged.
//
//	log := telemetry.NewSlogLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
//	c = cache.WithLogging(c, log)
//
// A nil logger yields [NoopLogger] rather than a value that panics on first
// use.
func NewSlogLogger(l *slog.Logger) Logger {
	if l == nil {
		return NoopLogger
	}
	return slogLogger{l: l}
}

// attrs converts fields to slog attributes. Order is not guaranteed — slog
// handlers are responsible for their own key ordering.
func attrs(fields Fields) []any {
	if len(fields) == 0 {
		return nil
	}
	out := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		out = append(out, k, v)
	}
	return out
}

func (s slogLogger) Debug(ctx context.Context, msg string, fields Fields) {
	s.l.DebugContext(ctx, msg, attrs(fields)...)
}

func (s slogLogger) Info(ctx context.Context, msg string, fields Fields) {
	s.l.InfoContext(ctx, msg, attrs(fields)...)
}

func (s slogLogger) Warn(ctx context.Context, msg string, fields Fields) {
	s.l.WarnContext(ctx, msg, attrs(fields)...)
}

// Error records the error under the conventional "error" key, so a handler can
// find it without knowing which field the caller chose.
func (s slogLogger) Error(ctx context.Context, msg string, err error, fields Fields) {
	a := attrs(fields)
	if err != nil {
		a = append(a, "error", err)
	}
	s.l.ErrorContext(ctx, msg, a...)
}

func (s slogLogger) Enabled(ctx context.Context, level Level) bool {
	return s.l.Enabled(ctx, slog.Level(level))
}

func (s slogLogger) With(fields Fields) Logger {
	if len(fields) == 0 {
		return s
	}
	return slogLogger{l: s.l.With(attrs(fields)...)}
}
