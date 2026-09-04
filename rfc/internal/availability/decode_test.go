// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package availability

import (
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/testing/protocmp"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"
	"github.com/google/go-cmp/cmp"
)

// TestDecodeValues asserts what a VAVAILABILITY decodes to, by value.
//
// Round-trip stability is asserted separately, in TestRoundTrip. Both are
// needed: this repository has three recorded cases where a decoder and an
// encoder were wrong in the same direction and agreed with each other while
// the data was gone. See docs/codec-findings.md.
func TestDecodeValues(t *testing.T) {
	src, err := os.ReadFile("testdata/office_hours.ics")
	if err != nil {
		t.Fatal(err)
	}
	a, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}

	if got := a.GetIcalUid(); got != "avail-1@example.com" {
		t.Errorf("UID = %q", got)
	}
	if got := a.GetBusyType(); got != availabilityv1.BusyType_BUSY_TYPE_BUSY_UNAVAILABLE {
		t.Errorf("BUSYTYPE = %v", got)
	}
	if got := a.GetSummary(); got != "Working hours" {
		t.Errorf("SUMMARY = %q", got)
	}
	if got := a.GetPriority(); got != 1 {
		t.Errorf("PRIORITY = %d, want 1", got)
	}
	if got := a.GetOrganizer(); got != "mailto:jane@example.com" {
		t.Errorf("ORGANIZER = %q", got)
	}

	// The zoned DTSTART must keep its TZID. A working-hours pattern that
	// silently became floating or UTC would shift by the offset, which is
	// the whole reason CalendarTime exists rather than a Timestamp.
	start := a.GetStart().GetDateTime()
	if start == nil {
		t.Fatal("DTSTART did not decode as a DATE-TIME")
	}
	if got := start.GetTimeZone().GetId(); got != "America/New_York" {
		t.Errorf("DTSTART TZID = %q, want America/New_York", got)
	}

	if len(a.GetAvailablePeriods()) != 2 {
		t.Fatalf("got %d available periods, want 2", len(a.GetAvailablePeriods()))
	}
	weekday, saturday := a.GetAvailablePeriods()[0], a.GetAvailablePeriods()[1]

	// The sub-component's own properties must land on it, not on the parent.
	// Reading an AVAILABLE's SUMMARY into the enclosing Availability is the
	// exact bug that VALARM inside VEVENT already produced once here.
	if got := weekday.GetSummary(); got != "Office hours" {
		t.Errorf("AVAILABLE summary = %q", got)
	}
	if got := a.GetSummary(); got != "Working hours" {
		t.Errorf("parent SUMMARY was overwritten by a sub-component: %q", got)
	}
	if got := weekday.GetLocation(); got != "Room 4" {
		t.Errorf("AVAILABLE location = %q", got)
	}
	if got := weekday.GetRecurrenceRule(); got != "FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR" {
		t.Errorf("AVAILABLE RRULE = %q", got)
	}

	// DTEND and DURATION are a oneof: each period must carry exactly the form
	// it was written with, not a normalized one.
	if weekday.GetEnd() == nil {
		t.Error("first period lost its DTEND")
	}
	if d := saturday.GetDuration(); d == nil || d.GetSeconds() != 2*3600 {
		t.Errorf("second period DURATION = %v, want 2h", d)
	}
	if saturday.GetEnd() != nil {
		t.Error("DURATION period also carries a DTEND")
	}
}

// TestRoundTrip asserts decode -> encode -> decode is stable.
func TestRoundTrip(t *testing.T) {
	src, err := os.ReadFile("testdata/office_hours.ics")
	if err != nil {
		t.Fatal(err)
	}
	first, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	out, err := Encode(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Decode(out)
	if err != nil {
		t.Fatalf("re-decode:\n%s\n%v", out, err)
	}
	if diff := cmp.Diff(first, second, protocmp.Transform()); diff != "" {
		t.Errorf("round trip lost data (-first +second):\n%s\nencoded:\n%s", diff, out)
	}
}

// TestBusyTypeDefaultIsNotZero pins RFC 7953 section 3.2's inverted default.
//
// An absent BUSYTYPE means BUSY-UNAVAILABLE, not "no opinion" -- the opposite
// of how a proto zero value usually reads. The codec must not invent a value
// on decode, and must not emit one it did not receive.
func TestBusyTypeDefaultIsNotZero(t *testing.T) {
	const src = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VAVAILABILITY\r\n" +
		"UID:avail-2@example.com\r\n" +
		"DTSTAMP:20260101T090000Z\r\n" +
		"END:VAVAILABILITY\r\n" +
		"END:VCALENDAR\r\n"
	a, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.GetBusyType(); got != availabilityv1.BusyType_BUSY_TYPE_UNSPECIFIED {
		t.Errorf("absent BUSYTYPE decoded as %v; the codec must not apply the default", got)
	}
	out, err := Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "BUSYTYPE") {
		t.Errorf("encoder invented a BUSYTYPE that was never present:\n%s", out)
	}

	// An Availability with no AVAILABLE periods is valid and means "never
	// schedulable in this range", RFC 7953 section 3.1.
	if len(a.GetAvailablePeriods()) != 0 {
		t.Error("periods appeared from nowhere")
	}
}

// TestSubComponentIsolation checks the component stack directly: an unknown
// property inside AVAILABLE must be preserved on the period, not the parent.
func TestSubComponentIsolation(t *testing.T) {
	const src = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VAVAILABILITY\r\n" +
		"UID:avail-3@example.com\r\n" +
		"X-PARENT-PROP:parent\r\n" +
		"BEGIN:AVAILABLE\r\n" +
		"UID:period-1@example.com\r\n" +
		"DTSTART:20260105T090000Z\r\n" +
		"X-CHILD-PROP:child\r\n" +
		"END:AVAILABLE\r\n" +
		"END:VAVAILABILITY\r\n" +
		"END:VCALENDAR\r\n"
	a, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.GetExtensions()) != 1 || a.GetExtensions()[0].GetKey() != "X-PARENT-PROP" {
		t.Errorf("parent extensions = %v", a.GetExtensions())
	}
	p := a.GetAvailablePeriods()[0]
	if len(p.GetExtensions()) != 1 || p.GetExtensions()[0].GetKey() != "X-CHILD-PROP" {
		t.Errorf("period extensions = %v", p.GetExtensions())
	}
}

// TestUnclosedComponent checks the stack rejects malformed nesting rather
// than silently producing a half-built value.
func TestUnclosedComponent(t *testing.T) {
	for name, src := range map[string]string{
		"unclosed AVAILABLE": "BEGIN:VCALENDAR\r\nBEGIN:VAVAILABILITY\r\nUID:x\r\nBEGIN:AVAILABLE\r\nDTSTART:20260105T090000Z\r\nEND:VAVAILABILITY\r\nEND:VCALENDAR\r\n",
		"no VAVAILABILITY":   "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n",
	} {
		if _, err := Decode(src); err == nil {
			t.Errorf("%s: expected an error, got none", name)
		}
	}
}

// TestEncodingIsDeterministic pins that identical input produces identical
// bytes, which extension parameters broke on first implementation.
//
// ExtensionProperty.parameters was briefly a repeated "NAME=value" string
// built by ranging over the parser's parameter map. Go randomizes map
// iteration, so one input produced four distinct encodings across twenty
// runs. Neither the round-trip test nor the value assertions caught it: every
// individual encoding decoded back correctly, and the fixtures had no
// extension carrying more than one parameter.
//
// The field is now a map, matching every other ExtensionProperty in the
// schema, and the encoder sorts its keys -- the same guard ical and vcard
// already carried.
func TestEncodingIsDeterministic(t *testing.T) {
	const src = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VAVAILABILITY\r\n" +
		"UID:avail-4@example.com\r\n" +
		"X-CUSTOM;AAA=1;BBB=2;CCC=3;DDD=4:value\r\n" +
		"BEGIN:AVAILABLE\r\n" +
		"UID:period-4@example.com\r\n" +
		"DTSTART:20260105T090000Z\r\n" +
		"X-CHILD;EEE=5;FFF=6;GGG=7:value\r\n" +
		"END:AVAILABLE\r\n" +
		"END:VAVAILABILITY\r\n" +
		"END:VCALENDAR\r\n"

	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		a, err := Decode(src)
		if err != nil {
			t.Fatal(err)
		}
		out, err := Encode(a)
		if err != nil {
			t.Fatal(err)
		}
		seen[out] = true
	}
	if len(seen) != 1 {
		t.Errorf("identical input produced %d distinct encodings, want 1", len(seen))
	}

	// And the parameters must actually have survived, not merely been dropped
	// into a stable emptiness.
	a, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.GetExtensions()[0].GetParameters(); len(got) != 4 {
		t.Errorf("parent extension parameters = %v, want 4", got)
	}
	if got := a.GetAvailablePeriods()[0].GetExtensions()[0].GetParameters(); len(got) != 3 {
		t.Errorf("period extension parameters = %v, want 3", got)
	}
}
