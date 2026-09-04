// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"google.golang.org/genproto/googleapis/type/datetime"
)

func TestRoundTrip(t *testing.T) {
	paths, err := filepath.Glob("testdata/*.ics")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no fixtures: %v", err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			first, err := Decode(string(src))
			if err != nil {
				t.Fatalf("first decode: %v", err)
			}
			out, err := Encode(first)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			second, err := Decode(out)
			if err != nil {
				t.Fatalf("second decode of\n%s: %v", out, err)
			}
			if diff := cmp.Diff(first, second, protocmp.Transform()); diff != "" {
				t.Errorf("round trip changed the model (-first +second):\n%s", diff)
			}
		})
	}
}

// TestTimeForms is the test the CalendarTime change was made for.
//
// RFC 5545 section 3.3.5 gives DATE-TIME three forms that mean different
// things, and section 3.3.4 adds the all-day DATE. A google.protobuf.Timestamp
// can express only the UTC form, so before CalendarTime three of these four
// cases were unrepresentable and this test could not have been written.
func TestTimeForms(t *testing.T) {
	read := func(name string) *eventv1.Event {
		t.Helper()
		src, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		e, err := Decode(string(src))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}

	t.Run("floating has no offset", func(t *testing.T) {
		dt := read("floating.ics").GetStart().GetDateTime()
		if dt == nil {
			t.Fatal("start is not a date-time")
		}
		if dt.GetTimeOffset() != nil {
			t.Errorf("floating time carries an offset %v; it must have none, "+
				"or 09:00 local becomes an absolute instant and shifts on DST",
				dt.GetTimeOffset())
		}
		if dt.GetHours() != 9 {
			t.Errorf("hours = %d, want 9", dt.GetHours())
		}
	})

	t.Run("UTC has a zero offset", func(t *testing.T) {
		dt := read("rfc5545_example.ics").GetStart().GetDateTime()
		off, ok := dt.GetTimeOffset().(*datetime.DateTime_UtcOffset)
		if !ok {
			t.Fatalf("UTC time has offset %T, want UtcOffset", dt.GetTimeOffset())
		}
		if off.UtcOffset.GetSeconds() != 0 {
			t.Errorf("UTC offset = %ds, want 0", off.UtcOffset.GetSeconds())
		}
	})

	t.Run("TZID keeps the zone", func(t *testing.T) {
		dt := read("zoned.ics").GetStart().GetDateTime()
		tz, ok := dt.GetTimeOffset().(*datetime.DateTime_TimeZone)
		if !ok {
			t.Fatalf("zoned time has offset %T, want TimeZone", dt.GetTimeOffset())
		}
		if got := tz.TimeZone.GetId(); got != "Europe/London" {
			t.Errorf("TZID = %q, want Europe/London", got)
		}
	})

	t.Run("all-day is a DATE", func(t *testing.T) {
		e := read("allday.ics")
		d := e.GetStart().GetDate()
		if d == nil {
			t.Fatal("all-day start is not a Date; a DATE is not a midnight instant")
		}
		if d.GetYear() != 2026 || d.GetMonth() != 7 || d.GetDay() != 1 {
			t.Errorf("date = %v, want 2026-07-01", d)
		}
	})
}
