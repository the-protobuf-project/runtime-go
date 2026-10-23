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

// series is a weekly meeting with two overridden instances, RFC 5545 section
// 3.8.4.4: the second week moved an hour later, the third cancelled.
const series = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:standup\r\nDTSTART;TZID=Europe/Madrid:20270322T100000\r\n" +
	"DTEND;TZID=Europe/Madrid:20270322T103000\r\nRRULE:FREQ=WEEKLY\r\nSUMMARY:Standup\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:standup\r\nRECURRENCE-ID;TZID=Europe/Madrid:20270329T100000\r\n" +
	"DTSTART;TZID=Europe/Madrid:20270329T110000\r\nDTEND;TZID=Europe/Madrid:20270329T113000\r\n" +
	"SUMMARY:Standup (moved)\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:standup\r\nRECURRENCE-ID;TZID=Europe/Madrid:20270405T100000\r\n" +
	"DTSTART;TZID=Europe/Madrid:20270405T100000\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

func TestDecodeAllReadsOverrides(t *testing.T) {
	events, err := DecodeAll(series)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want the series and two overrides", len(events))
	}
	master, moved, cancelled := events[0], events[1], events[2]
	if master.GetOriginalStart() != nil || master.GetRecurrence() == nil {
		t.Fatalf("series: recurrence_id %v, rule %v", master.GetOriginalStart(), master.GetRecurrence())
	}
	if master.GetSummary() != "Standup" {
		t.Fatalf("an override's properties leaked into the series: %q", master.GetSummary())
	}
	if id := moved.GetOriginalStart().GetDateTime(); id.GetDay() != 29 || id.GetHours() != 10 || id.GetTimeZone().GetId() != "Europe/Madrid" {
		t.Fatalf("moved RECURRENCE-ID %v", id)
	}
	if moved.GetStart().GetDateTime().GetHours() != 11 || moved.GetIcalUid() != "standup" {
		t.Fatalf("moved override %v", moved)
	}
	if cancelled.GetConfirmation() != eventv1.Confirmation_CONFIRMATION_CANCELLED {
		t.Fatalf("cancelled override confirmation %v", cancelled.GetConfirmation())
	}
}

func TestEncodeAllRoundTrip(t *testing.T) {
	first, err := DecodeAll(series)
	if err != nil {
		t.Fatal(err)
	}
	out, err := EncodeAll(first)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(out, "BEGIN:VCALENDAR"); n != 1 {
		t.Fatalf("%d VCALENDARs, want one wrapping every VEVENT", n)
	}
	second, err := DecodeAll(out)
	if err != nil {
		t.Fatalf("decode of\n%s: %v", out, err)
	}
	if diff := cmp.Diff(first, second, protocmp.Transform()); diff != "" {
		t.Errorf("round trip changed the model (-first +second):\n%s", diff)
	}
}

func TestDecodeRefusesSeveralEvents(t *testing.T) {
	if _, err := Decode(series); err == nil {
		t.Fatal("Decode merged three VEVENTs into one event")
	}
}

func TestOverrideJCalRoundTrip(t *testing.T) {
	events, err := DecodeAll(series)
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeJCal(events[1])
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeJCal(data)
	if err != nil {
		t.Fatalf("decode of %s: %v", data, err)
	}
	if diff := cmp.Diff(events[1], back, protocmp.Transform()); diff != "" {
		t.Errorf("jCal round trip changed the override (-want +got):\n%s", diff)
	}
}
