// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
)

// The RANGE parameter on RECURRENCE-ID, RFC 5545 section 3.2.13.

// thisAndFuture is the series from override_test.go with its first override
// widened to every later instance, the shape a client sends for "this and
// following events".
var thisAndFuture = strings.Replace(series,
	"RECURRENCE-ID;TZID", "RECURRENCE-ID;RANGE=THISANDFUTURE;TZID", 1)

func TestThisAndFutureDecodes(t *testing.T) {
	events, err := DecodeAll(thisAndFuture)
	if err != nil {
		t.Fatal(err)
	}
	master, split, cancelled := events[0], events[1], events[2]
	if r := master.GetOriginalStartRange(); r != eventv1.RecurrenceRange_RECURRENCE_RANGE_UNSPECIFIED {
		t.Fatalf("series carries range %v", r)
	}
	if r := split.GetOriginalStartRange(); r != eventv1.RecurrenceRange_RECURRENCE_RANGE_THIS_AND_FUTURE {
		t.Fatalf("split override range %v, want THIS_AND_FUTURE", r)
	}
	// RANGE is a parameter beside TZID, not in place of it.
	if id := split.GetOriginalStart().GetDateTime(); id.GetDay() != 29 || id.GetTimeZone().GetId() != "Europe/Madrid" {
		t.Fatalf("split RECURRENCE-ID %v", id)
	}
	if r := cancelled.GetOriginalStartRange(); r != eventv1.RecurrenceRange_RECURRENCE_RANGE_UNSPECIFIED {
		t.Fatalf("plain override carries range %v", r)
	}
}

// The examples in sections 3.2.13 and 3.8.4.4 verbatim, plus a lower-case
// value: section 3.2 makes parameter values case-insensitive.
func TestThisAndFutureRFCExamples(t *testing.T) {
	for _, line := range []string{
		"RECURRENCE-ID;RANGE=THISANDFUTURE:19980401T133000Z",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:19960120T120000Z",
		"RECURRENCE-ID;RANGE=thisandfuture:19960120T120000Z",
	} {
		src := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\nBEGIN:VEVENT\r\nUID:x\r\n" +
			"DTSTART:19960120T130000Z\r\n" + line + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		e, err := Decode(src)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if e.GetOriginalStartRange() != eventv1.RecurrenceRange_RECURRENCE_RANGE_THIS_AND_FUTURE {
			t.Fatalf("%s: range %v", line, e.GetOriginalStartRange())
		}
		if e.GetOriginalStart().GetDateTime().GetUtcOffset() == nil {
			t.Fatalf("%s: UTC form lost: %v", line, e.GetOriginalStart())
		}
		out, err := Encode(e)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out, "RECURRENCE-ID;RANGE=THISANDFUTURE:") {
			t.Fatalf("%s: encoded as\n%s", line, out)
		}
	}
}

func TestThisAndFutureRoundTrip(t *testing.T) {
	first, err := DecodeAll(thisAndFuture)
	if err != nil {
		t.Fatal(err)
	}
	out, err := EncodeAll(first)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=Europe/Madrid:20270329T100000") {
		t.Fatalf("RANGE not emitted:\n%s", out)
	}
	if n := strings.Count(out, "RANGE="); n != 1 {
		t.Fatalf("%d RANGE parameters, want only the split override's", n)
	}
	second, err := DecodeAll(out)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(first, second, protocmp.Transform()); diff != "" {
		t.Errorf("round trip changed the model (-first +second):\n%s", diff)
	}
}

func TestThisAndFutureJCalRoundTrip(t *testing.T) {
	events, err := DecodeAll(thisAndFuture)
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeJCal(events[1])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"range":"THISANDFUTURE"`) {
		t.Fatalf("jCal lost RANGE: %s", data)
	}
	back, err := DecodeJCal(data)
	if err != nil {
		t.Fatalf("decode of %s: %v", data, err)
	}
	if diff := cmp.Diff(events[1], back, protocmp.Transform()); diff != "" {
		t.Errorf("jCal round trip changed the override (-want +got):\n%s", diff)
	}
}

// THISANDPRIOR is deprecated by section 3.2.13, and any other value is not
// in its grammar. Reading either as a single-instance override would apply
// the edit to the wrong instances.
func TestRangeRefused(t *testing.T) {
	for _, v := range []string{"THISANDPRIOR", "ALL"} {
		src := strings.Replace(series, "RECURRENCE-ID;TZID", "RECURRENCE-ID;RANGE="+v+";TZID", 1)
		_, err := DecodeAll(src)
		if err == nil {
			t.Fatalf("RANGE=%s accepted", v)
		}
		if !strings.Contains(err.Error(), v) {
			t.Fatalf("RANGE=%s: error does not name the value: %v", v, err)
		}
	}
}

func TestRangeWithoutOriginalStartRefused(t *testing.T) {
	e := &eventv1.Event{
		IcalUid:            "x",
		Start:              &eventv1.CalendarTime{Value: &eventv1.CalendarTime_Date{}},
		OriginalStartRange: eventv1.RecurrenceRange_RECURRENCE_RANGE_THIS_AND_FUTURE,
	}
	if _, err := Encode(e); err == nil {
		t.Fatal("RANGE encoded with no RECURRENCE-ID to carry it")
	}
}
