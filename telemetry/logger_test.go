package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// newTestLogger returns a JSON logger writing into buf, at the given level.
func newTestLogger(buf *bytes.Buffer, level telemetry.Level) telemetry.Logger {
	return telemetry.NewSlogLogger(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.Level(level),
	})))
}

// decode returns the one record written to buf.
func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("nothing was logged")
	}
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		t.Fatalf("expected one record, got several:\n%s", line)
	}

	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, line)
	}
	return rec
}

func TestLevelsMapToSlog(t *testing.T) {
	for _, tc := range []struct {
		level telemetry.Level
		want  string
	}{
		{telemetry.LevelDebug, "DEBUG"},
		{telemetry.LevelInfo, "INFO"},
		{telemetry.LevelWarn, "WARN"},
		{telemetry.LevelError, "ERROR"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			var buf bytes.Buffer
			log := newTestLogger(&buf, telemetry.LevelDebug)

			switch tc.level {
			case telemetry.LevelDebug:
				log.Debug(t.Context(), "m", nil)
			case telemetry.LevelInfo:
				log.Info(t.Context(), "m", nil)
			case telemetry.LevelWarn:
				log.Warn(t.Context(), "m", nil)
			case telemetry.LevelError:
				log.Error(t.Context(), "m", nil, nil)
			}

			if got := decode(t, &buf)["level"]; got != tc.want {
				t.Errorf("level = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFieldsBecomeAttributes(t *testing.T) {
	var buf bytes.Buffer
	log := newTestLogger(&buf, telemetry.LevelDebug)

	log.Info(t.Context(), "stored", telemetry.Fields{"id": "abc", "count": 3})

	rec := decode(t, &buf)
	if rec["id"] != "abc" {
		t.Errorf("id = %v, want abc", rec["id"])
	}
	if rec["count"] != float64(3) {
		t.Errorf("count = %v, want 3", rec["count"])
	}
	if rec["msg"] != "stored" {
		t.Errorf("msg = %v, want stored", rec["msg"])
	}
}

// The error is a distinct argument so it cannot be forgotten, and it must land
// under a predictable key.
func TestErrorIsRecordedUnderErrorKey(t *testing.T) {
	var buf bytes.Buffer
	log := newTestLogger(&buf, telemetry.LevelDebug)

	log.Error(t.Context(), "write failed", errors.New("boom"), telemetry.Fields{"id": "abc"})

	rec := decode(t, &buf)
	if rec["error"] != "boom" {
		t.Errorf("error = %v, want boom", rec["error"])
	}
	if rec["id"] != "abc" {
		t.Errorf("fields were dropped alongside the error: %v", rec)
	}
}

// A failure with no error value is legitimate and must not emit a null field.
func TestErrorToleratesNilError(t *testing.T) {
	var buf bytes.Buffer
	log := newTestLogger(&buf, telemetry.LevelDebug)

	log.Error(t.Context(), "failed", nil, nil)

	if _, ok := decode(t, &buf)["error"]; ok {
		t.Error("a nil error still wrote an error key")
	}
}

func TestWithBindsFieldsToEveryRecord(t *testing.T) {
	var buf bytes.Buffer
	log := newTestLogger(&buf, telemetry.LevelDebug).With(telemetry.Fields{"component": "cache"})

	log.Info(t.Context(), "hello", telemetry.Fields{"id": "abc"})

	rec := decode(t, &buf)
	if rec["component"] != "cache" {
		t.Errorf("bound field missing: %v", rec)
	}
	if rec["id"] != "abc" {
		t.Errorf("per-call field missing: %v", rec)
	}
}

// Enabled is what lets a hot path skip building fields it would only discard.
func TestEnabledReflectsTheHandlerLevel(t *testing.T) {
	var buf bytes.Buffer
	log := newTestLogger(&buf, telemetry.LevelInfo)

	if log.Enabled(t.Context(), telemetry.LevelDebug) {
		t.Error("Enabled(debug) is true on an info-level logger")
	}
	if !log.Enabled(t.Context(), telemetry.LevelError) {
		t.Error("Enabled(error) is false on an info-level logger")
	}
}

func TestBelowLevelRecordsAreDropped(t *testing.T) {
	var buf bytes.Buffer
	log := newTestLogger(&buf, telemetry.LevelInfo)

	log.Debug(t.Context(), "invisible", nil)

	if buf.Len() != 0 {
		t.Errorf("a debug record was written to an info-level logger: %s", buf.String())
	}
}

// A nil *slog.Logger is an easy mistake; it must degrade rather than panic.
func TestNewSlogLoggerToleratesNil(t *testing.T) {
	log := telemetry.NewSlogLogger(nil)

	log.Info(context.Background(), "no panic", nil)
	if log.Enabled(context.Background(), telemetry.LevelError) {
		t.Error("the nil fallback reports Enabled(error) = true")
	}
}

// The no-op must be usable unconditionally, including its With chain.
func TestNoopLoggerIsInert(t *testing.T) {
	log := telemetry.NoopLogger.With(telemetry.Fields{"a": 1})

	log.Debug(context.Background(), "m", nil)
	log.Info(context.Background(), "m", nil)
	log.Warn(context.Background(), "m", nil)
	log.Error(context.Background(), "m", errors.New("x"), nil)

	if log.Enabled(context.Background(), telemetry.LevelError) {
		t.Error("NoopLogger reports Enabled = true; guarded call sites would do wasted work")
	}
}

func TestLevelString(t *testing.T) {
	for level, want := range map[telemetry.Level]string{
		telemetry.LevelDebug: "debug",
		telemetry.LevelInfo:  "info",
		telemetry.LevelWarn:  "warn",
		telemetry.LevelError: "error",
	} {
		if got := level.String(); got != want {
			t.Errorf("Level(%d).String() = %q, want %q", level, got, want)
		}
	}
}
