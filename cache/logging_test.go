package cache

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// capturedRecord is one log line the fake logger kept.
type capturedRecord struct {
	level  telemetry.Level
	msg    string
	err    error
	fields telemetry.Fields
}

// captureLogger records everything written to it.
type captureLogger struct {
	records *[]capturedRecord
	bound   telemetry.Fields
}

func newCaptureLogger() (*captureLogger, *[]capturedRecord) {
	recs := &[]capturedRecord{}
	return &captureLogger{records: recs}, recs
}

func (c *captureLogger) add(level telemetry.Level, msg string, err error, fields telemetry.Fields) {
	merged := telemetry.Fields{}
	maps.Copy(merged, c.bound)
	maps.Copy(merged, fields)
	*c.records = append(*c.records, capturedRecord{level: level, msg: msg, err: err, fields: merged})
}

func (c *captureLogger) Debug(_ context.Context, msg string, f telemetry.Fields) {
	c.add(telemetry.LevelDebug, msg, nil, f)
}
func (c *captureLogger) Info(_ context.Context, msg string, f telemetry.Fields) {
	c.add(telemetry.LevelInfo, msg, nil, f)
}
func (c *captureLogger) Warn(_ context.Context, msg string, f telemetry.Fields) {
	c.add(telemetry.LevelWarn, msg, nil, f)
}
func (c *captureLogger) Error(_ context.Context, msg string, err error, f telemetry.Fields) {
	c.add(telemetry.LevelError, msg, err, f)
}
func (c *captureLogger) Enabled(context.Context, telemetry.Level) bool { return true }
func (c *captureLogger) With(f telemetry.Fields) telemetry.Logger {
	merged := telemetry.Fields{}
	maps.Copy(merged, c.bound)
	maps.Copy(merged, f)
	return &captureLogger{records: c.records, bound: merged}
}

// only returns the single record at the given level, failing otherwise.
func only(t *testing.T, recs []capturedRecord, level telemetry.Level) capturedRecord {
	t.Helper()

	var found []capturedRecord
	for _, r := range recs {
		if r.level == level {
			found = append(found, r)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one %s record, got %d (all: %+v)", level, len(found), recs)
	}
	return found[0]
}

func TestLoggingRecordsSuccessAtDebug(t *testing.T) {
	log, recs := newCaptureLogger()
	c := WithLogging(&fakeCache{}, log)

	if err := c.Get(t.Context(), "abc", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	rec := only(t, *recs, telemetry.LevelDebug)
	if rec.fields["operation"] != "get" {
		t.Errorf("operation = %v, want get", rec.fields["operation"])
	}
	if rec.fields["id"] != "abc" {
		t.Errorf("id = %v, want abc", rec.fields["id"])
	}
	if _, ok := rec.fields["duration"]; !ok {
		t.Error("no duration was recorded")
	}
}

// A miss is a normal cache outcome. Logging it at error would bury real
// failures in noise.
func TestLoggingRecordsMissAtWarnNotError(t *testing.T) {
	log, recs := newCaptureLogger()
	c := WithLogging(&fakeCache{errs: []error{ErrNotFound}}, log)

	if err := c.Get(t.Context(), "abc", &struct{}{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get error = %v", err)
	}

	rec := only(t, *recs, telemetry.LevelWarn)
	if rec.msg != "cache miss" {
		t.Errorf("msg = %q, want %q", rec.msg, "cache miss")
	}
	for _, r := range *recs {
		if r.level == telemetry.LevelError {
			t.Error("a miss was also logged at error level")
		}
	}
}

func TestLoggingRecordsFailureAtError(t *testing.T) {
	boom := errors.New("connection refused")
	log, recs := newCaptureLogger()
	c := WithLogging(&fakeCache{errs: []error{boom}}, log)

	if err := c.Get(t.Context(), "abc", &struct{}{}); !errors.Is(err, boom) {
		t.Fatalf("Get error = %v", err)
	}

	rec := only(t, *recs, telemetry.LevelError)
	if !errors.Is(rec.err, boom) {
		t.Errorf("logged error = %v, want %v", rec.err, boom)
	}
}

// Create assigns an ID when the caller supplies none; the log has to show the
// ID that was actually stored, not the empty one that went in.
func TestLoggingRecordsTheAssignedID(t *testing.T) {
	log, recs := newCaptureLogger()
	c := WithLogging(&idAssigningCache{id: "generated"}, log)

	if _, err := c.Create(t.Context(), "", nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if rec := only(t, *recs, telemetry.LevelDebug); rec.fields["id"] != "generated" {
		t.Errorf("id = %v, want generated", rec.fields["id"])
	}
}

// Logging must be transparent: it observes, it does not alter results.
func TestLoggingPassesResultsThrough(t *testing.T) {
	boom := errors.New("backend down")
	log, _ := newCaptureLogger()
	c := WithLogging(&fakeCache{errs: []error{boom}}, log)

	if err := c.Get(t.Context(), "abc", &struct{}{}); !errors.Is(err, boom) {
		t.Errorf("Get error = %v, want %v", err, boom)
	}
}

// A nil logger is the natural thing to pass when logging is not configured.
func TestWithLoggingToleratesNilLogger(t *testing.T) {
	c := WithLogging(&fakeCache{}, nil)

	if err := c.Get(t.Context(), "abc", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}
}

// Fields bound with With must survive onto the decorator's records.
func TestLoggingKeepsBoundFields(t *testing.T) {
	log, recs := newCaptureLogger()
	c := WithLogging(&fakeCache{}, log.With(telemetry.Fields{"component": "cache"}))

	if err := c.Get(t.Context(), "abc", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if rec := only(t, *recs, telemetry.LevelDebug); rec.fields["component"] != "cache" {
		t.Errorf("bound field missing: %+v", rec.fields)
	}
}

// idAssigningCache mimics a provider that generates an id during Create.
type idAssigningCache struct {
	id string
}

func (c *idAssigningCache) Create(context.Context, string, any, ...Option) (string, error) {
	return c.id, nil
}
func (c *idAssigningCache) Get(context.Context, string, any) error               { return nil }
func (c *idAssigningCache) Update(context.Context, string, any, ...Option) error { return nil }
func (c *idAssigningCache) Delete(context.Context, string) error                 { return nil }
func (c *idAssigningCache) Keys(context.Context) ([]string, error)               { return nil, nil }
func (c *idAssigningCache) List(context.Context, any) error                      { return nil }
func (c *idAssigningCache) TTL(context.Context, string) (time.Duration, error)   { return 0, nil }
