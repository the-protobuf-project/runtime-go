// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"os"
	"testing"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
)

func participantsFixture(t *testing.T) *eventv1.Event {
	t.Helper()
	src, err := os.ReadFile("testdata/participants.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// TestSubcomponentsDoNotLeak is the regression test for a real bug.
//
// The decoder tracked "am I inside a VEVENT" as a boolean, so a VALARM's
// DESCRIPTION overwrote the event's own. RFC 5545 section 3.6 nests
// components, and a nested component's properties belong to it alone.
func TestSubcomponentsDoNotLeak(t *testing.T) {
	e := participantsFixture(t)
	if got := e.GetDescription(); got != "The event description, not the alarm one." {
		t.Errorf("event description = %q; an alarm's DESCRIPTION overwrote it", got)
	}
	if len(e.GetAlarms()) != 2 {
		t.Fatalf("want 2 alarms, got %d", len(e.GetAlarms()))
	}
	if got := e.GetAlarms()[0].GetDescription(); got != "Starts in 15 minutes" {
		t.Errorf("alarm description = %q", got)
	}
}

// TestDecodeOrganizer covers section 3.8.4.3, including SENT-BY.
func TestDecodeOrganizer(t *testing.T) {
	o := participantsFixture(t).GetOrganizer()
	if o == nil {
		t.Fatal("no organizer decoded")
	}
	if got := o.GetAddress(); got != "mailto:jane@example.com" {
		t.Errorf("address = %q", got)
	}
	if got := o.GetDisplayName(); got != "Jane Doe" {
		t.Errorf("CN = %q", got)
	}
	// SENT-BY is quoted in the source because it contains a colon; the
	// quotes are transport and must not survive into the value.
	if got := o.GetSender(); got != "mailto:assistant@example.com" {
		t.Errorf("SENT-BY = %q, want the unquoted address", got)
	}
}

// TestDecodeAttendees covers section 3.8.4.1 and its parameters.
func TestDecodeAttendees(t *testing.T) {
	as := participantsFixture(t).GetAttendees()
	if len(as) != 3 {
		t.Fatalf("want 3 attendees, got %d", len(as))
	}

	if got := as[0].GetRole(); got != eventv1.ParticipationRole_PARTICIPATION_ROLE_REQUIRED {
		t.Errorf("ROLE = %v, want REQUIRED", got)
	}
	if got := as[0].GetParticipation(); got != eventv1.Participation_PARTICIPATION_ACCEPTED {
		t.Errorf("PARTSTAT = %v, want ACCEPTED", got)
	}
	if !as[0].GetRsvp() {
		t.Error("RSVP=TRUE did not decode")
	}
	if got := as[0].GetUserType(); got != eventv1.CalendarUserType_CALENDAR_USER_TYPE_INDIVIDUAL {
		t.Errorf("CUTYPE = %v, want INDIVIDUAL", got)
	}

	// A room is an attendee too, which is why CUTYPE exists.
	if got := as[1].GetUserType(); got != eventv1.CalendarUserType_CALENDAR_USER_TYPE_ROOM {
		t.Errorf("room CUTYPE = %v, want ROOM", got)
	}

	// DELEGATED-TO is section 3.2.5, titled "Delegatees" -- which is where
	// the field name came from, since AIP-140 bans the preposition.
	if got := as[2].GetDelegatees(); len(got) != 1 || got[0] != "mailto:carol@example.com" {
		t.Errorf("DELEGATED-TO = %v", got)
	}
	if got := as[2].GetParticipation(); got != eventv1.Participation_PARTICIPATION_DELEGATED {
		t.Errorf("PARTSTAT = %v, want DELEGATED", got)
	}
}

// TestDecodeAlarms covers section 3.6.6, both trigger forms.
func TestDecodeAlarms(t *testing.T) {
	as := participantsFixture(t).GetAlarms()

	// A relative trigger: a signed duration, plus RELATED from section 3.2.14.
	if got := as[0].GetAction(); got != eventv1.AlarmAction_ALARM_ACTION_DISPLAY {
		t.Errorf("ACTION = %v, want DISPLAY", got)
	}
	if got := as[0].GetTriggerOffset().GetSeconds(); got != -900 {
		t.Errorf("TRIGGER = %ds, want -900 (fifteen minutes before)", got)
	}
	if got := as[0].GetTriggerRelation(); got != eventv1.TriggerRelation_TRIGGER_RELATION_START {
		t.Errorf("RELATED = %v, want START", got)
	}
	if as[0].GetRepeatCount() != 2 || as[0].GetRepeatInterval().GetSeconds() != 300 {
		t.Errorf("REPEAT/DURATION = %d/%v", as[0].GetRepeatCount(), as[0].GetRepeatInterval())
	}

	// An absolute trigger. Section 3.8.6.3 requires this form to be UTC,
	// which is why the schema types it as a Timestamp and not a CalendarTime.
	if got := as[1].GetAction(); got != eventv1.AlarmAction_ALARM_ACTION_EMAIL {
		t.Errorf("ACTION = %v, want EMAIL", got)
	}
	ts := as[1].GetTriggerTime()
	if ts == nil {
		t.Fatal("absolute TRIGGER did not decode as a timestamp")
	}
	if got := ts.AsTime().UTC().Format("20060102T150405Z"); got != "20260420T120000Z" {
		t.Errorf("TRIGGER time = %s", got)
	}
	// An EMAIL alarm carries its recipients, section 3.6.6.
	if len(as[1].GetAttendees()) != 1 {
		t.Errorf("EMAIL alarm has %d attendees, want 1", len(as[1].GetAttendees()))
	}
}
