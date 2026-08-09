package database

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

type capturedRecord struct {
	level  telemetry.Level
	msg    string
	err    error
	fields telemetry.Fields
}

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

func levelOf(t *testing.T, recs []capturedRecord, wantLevel telemetry.Level) capturedRecord {
	t.Helper()
	for _, r := range recs {
		if r.level == wantLevel {
			return r
		}
	}
	t.Fatalf("no %s record among %+v", wantLevel, recs)
	return capturedRecord{}
}

func TestLoggingRecordsSuccessAtDebug(t *testing.T) {
	log, recs := newCaptureLogger()
	s := WithLogging(&fakeStore{}, log)

	if err := s.Get(t.Context(), "abc", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	rec := levelOf(t, *recs, telemetry.LevelDebug)
	if rec.fields["operation"] != "get" {
		t.Errorf("operation = %v, want get", rec.fields["operation"])
	}
	if rec.fields["id"] != "abc" {
		t.Errorf("id = %v, want abc", rec.fields["id"])
	}
}

// Unlike a cache, a missing record here is a genuine failure — documents do not
// expire on their own.
func TestLoggingRecordsNotFoundAtError(t *testing.T) {
	log, recs := newCaptureLogger()
	s := WithLogging(&fakeStore{errs: []error{ErrNotFound}}, log)

	if err := s.Get(t.Context(), "abc", &struct{}{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get error = %v", err)
	}

	levelOf(t, *recs, telemetry.LevelError)
	for _, r := range *recs {
		if r.level == telemetry.LevelWarn {
			t.Error("a missing document was logged at warn; it is a real failure here")
		}
	}
}

// A refused duplicate leaves the store in the state the caller wanted, with the
// content stored exactly once — a warning, not an error.
func TestLoggingRecordsDuplicateAtWarn(t *testing.T) {
	log, recs := newCaptureLogger()
	s := WithLogging(&fakeStore{errs: []error{ErrDuplicate}}, log)

	if err := s.Update(t.Context(), "abc", nil); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Update error = %v", err)
	}

	levelOf(t, *recs, telemetry.LevelWarn)
	for _, r := range *recs {
		if r.level == telemetry.LevelError {
			t.Error("a duplicate was logged at error level")
		}
	}
}

// A Create that comes back under a different ID means the content was already
// stored — the caller's write produced no new document, which is worth saying.
func TestLoggingReportsDeduplication(t *testing.T) {
	log, recs := newCaptureLogger()
	s := WithLogging(&idAssigningStore{id: "existing"}, log)

	if _, err := s.Create(t.Context(), "requested", nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	rec := levelOf(t, *recs, telemetry.LevelInfo)
	if rec.fields["existing"] != "existing" || rec.fields["requested"] != "requested" {
		t.Errorf("deduplication record is missing its IDs: %+v", rec.fields)
	}
}

func TestWithLoggingToleratesNilLogger(t *testing.T) {
	s := WithLogging(&fakeStore{}, nil)

	if err := s.Get(t.Context(), "abc", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}
}

// idAssigningStore mimics a provider that deduplicates to an existing document.
type idAssigningStore struct {
	id string
}

func (s *idAssigningStore) Create(context.Context, string, any, ...Option) (string, error) {
	return s.id, nil
}
func (s *idAssigningStore) Get(context.Context, string, any) error               { return nil }
func (s *idAssigningStore) Update(context.Context, string, any, ...Option) error { return nil }
func (s *idAssigningStore) Delete(context.Context, string) error                 { return nil }
func (s *idAssigningStore) Keys(context.Context, ...Option) ([]string, error)    { return nil, nil }
func (s *idAssigningStore) List(context.Context, any, ...Option) error           { return nil }
