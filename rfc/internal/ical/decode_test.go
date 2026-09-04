// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"os"
	"strings"
	"testing"

	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/genproto/googleapis/type/month"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/icaltime"
)

// TestDecodeRecurrence asserts specific RRULE values. Round-trip stability
// cannot prove these: a rule part dropped on both sides compares equal.
func TestDecodeRecurrence(t *testing.T) {
	src, err := os.ReadFile("testdata/zoned.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	r := e.GetRecurrence()
	if r == nil {
		t.Fatal("no recurrence decoded")
	}

	// RRULE:FREQ=MONTHLY;COUNT=12;BYDAY=-1FR;BYSETPOS=-1
	if r.GetFrequency() != eventv1.Frequency_FREQUENCY_MONTHLY {
		t.Errorf("FREQ = %v, want MONTHLY", r.GetFrequency())
	}
	if r.GetCount() != 12 {
		t.Errorf("COUNT = %d, want 12", r.GetCount())
	}
	if got := r.GetWeekdays(); len(got) != 1 {
		t.Fatalf("BYDAY has %d entries, want 1", len(got))
	}
	wd := r.GetWeekdays()[0]
	if wd.GetDay() != dayofweek.DayOfWeek_FRIDAY {
		t.Errorf("BYDAY day = %v, want FRIDAY", wd.GetDay())
	}
	// The ordinal is what makes "-1FR" the *last* Friday rather than every
	// Friday, and losing it silently changes the schedule.
	if wd.GetOrdinal() != -1 {
		t.Errorf("BYDAY ordinal = %d, want -1", wd.GetOrdinal())
	}
	if got := r.GetSetPositions(); len(got) != 1 || got[0] != -1 {
		t.Errorf("BYSETPOS = %v, want [-1]", got)
	}
}

// TestDecodeMonths checks BYMONTH maps onto google.type.Month, whose enum
// numbers are 1-12, so the range is carried by the type.
func TestDecodeMonths(t *testing.T) {
	src, err := os.ReadFile("testdata/allday.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	got := e.GetRecurrence().GetMonths()
	if len(got) != 1 || got[0] != month.Month_JULY {
		t.Errorf("BYMONTH = %v, want [JULY]", got)
	}
}

// TestDurationForms covers section 3.3.6, which Go's time.ParseDuration
// cannot read: it has days and weeks.
func TestDurationForms(t *testing.T) {
	cases := map[string]int64{
		"PT30M":     30 * 60,
		"P1DT2H30M": 24*3600 + 2*3600 + 30*60,
		"P2W":       14 * 24 * 3600,
		"PT1H":      3600,
		"-PT15M":    -15 * 60,
	}
	for in, want := range cases {
		d, err := icaltime.ParseDuration(in)
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if d.GetSeconds() != want {
			t.Errorf("%s = %ds, want %ds", in, d.GetSeconds(), want)
		}
	}
}

// TestRejectsBothEndAndDuration is the hazard plan/02 named: section 3.6.1
// permits DTEND or DURATION, never both, and a file carrying both must be
// rejected rather than silently resolved one way.
func TestRejectsBothEndAndDuration(t *testing.T) {
	src := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\n" +
		"UID:x@example.com\r\nDTSTART:20260101T120000Z\r\n" +
		"DTEND:20260101T130000Z\r\nDURATION:PT1H\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"
	_, err := Decode(src)
	if err == nil {
		t.Fatal("accepted a VEVENT with both DTEND and DURATION")
	}
	if !strings.Contains(err.Error(), "3.6.1") {
		t.Errorf("error should cite section 3.6.1, got: %v", err)
	}
}

// TestUnmodelledPropertiesSurvive is the ExtensionProperty contract for
// iCalendar, sections 3.8.8.1 and 3.8.8.2.
func TestUnmodelledPropertiesSurvive(t *testing.T) {
	src, err := os.ReadFile("testdata/allday.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, x := range e.GetExtensions() {
		if x.GetKey() == "X-APPLE-CALENDAR-COLOR" {
			found = true
			if len(x.GetValues()) != 1 || x.GetValues()[0] != "#FF2968" {
				t.Errorf("extension values = %v", x.GetValues())
			}
		}
	}
	if !found {
		t.Error("X-APPLE-CALENDAR-COLOR was dropped instead of preserved")
	}
}

// TestRejectsMalformed covers what a conforming parser owes its caller.
func TestRejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"no VEVENT":  "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n",
		"no DTSTART": "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:x\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		"bad RRULE":  "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nDTSTART:20260101T120000Z\r\nRRULE:INTERVAL=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(src); err == nil {
				t.Error("expected an error, got none")
			}
		})
	}
}

// TestRfc9074Alarm asserts the properties RFC 9074 adds to VALARM.
//
// These are what make an alarm addressable: before RFC 9074 a VALARM had no
// UID, so acknowledging or snoozing one could not be expressed at all and
// implementations invented proprietary properties instead. Asserted by value
// rather than by round trip, because this file already had a bug where the
// decoder and encoder were wrong in the same direction and agreed with each
// other -- see docs/codec-findings.md.
func TestRfc9074Alarm(t *testing.T) {
	src, err := os.ReadFile("testdata/rfc9074_alarm.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.GetAlarms()) != 2 {
		t.Fatalf("got %d alarms, want 2", len(e.GetAlarms()))
	}
	first, second := e.GetAlarms()[0], e.GetAlarms()[1]

	// UID, section 4.
	if got := first.GetIcalUid(); got != "alarm-1@example.com" {
		t.Errorf("alarm UID = %q", got)
	}
	// ACKNOWLEDGED, section 6.1, a UTC DATE-TIME.
	ack := first.GetAcknowledgedTime()
	if ack == nil {
		t.Fatal("ACKNOWLEDGED did not decode")
	}
	if got := ack.AsTime().UTC().Format("20060102T150405Z"); got != "20260301T085500Z" {
		t.Errorf("ACKNOWLEDGED = %s", got)
	}
	// PROXIMITY, section 8.1.
	if got := first.GetProximity(); got != eventv1.Proximity_PROXIMITY_ARRIVE {
		t.Errorf("PROXIMITY = %v, want ARRIVE", got)
	}
	// The alarm's own DESCRIPTION must not have leaked onto the event: that
	// is the bug codec-findings.md records, and this pins icaltime.
	if e.GetSummary() != "Standup" {
		t.Errorf("event summary = %q, alarm properties leaked", e.GetSummary())
	}

	// RELATED-TO;RELTYPE=SNOOZE, sections 5 and 7.1.
	if got := second.GetSnoozedAlarmUid(); got != "alarm-1@example.com" {
		t.Errorf("SNOOZE relation = %q", got)
	}
	if got := first.GetSnoozedAlarmUid(); got != "" {
		t.Errorf("first alarm gained a snooze relation it does not have: %q", got)
	}

	// And the whole thing must survive re-encoding.
	out, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"UID:alarm-1@example.com",
		"ACKNOWLEDGED:20260301T085500Z",
		"PROXIMITY:ARRIVE",
		"RELATED-TO;RELTYPE=SNOOZE:alarm-1@example.com",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("encoded output is missing %q", want)
		}
	}
}

// TestRfc7529Rscale asserts the non-Gregorian recurrence parts.
//
// The leap-month suffix is the interesting one: BYMONTH=5L is not an integer,
// so before RFC 7529 support it did not merely get dropped -- it failed the
// whole RRULE parse. google.type.Month is a plain 1-12 enum and cannot carry
// the suffix, which is why leap months are a second repeated field rather
// than a flag on the first.
func TestRfc7529Rscale(t *testing.T) {
	const src = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:hebrew-1@example.com\r\n" +
		"DTSTAMP:20260101T090000Z\r\n" +
		"DTSTART:20260301T090000Z\r\n" +
		"SUMMARY:Anniversary\r\n" +
		"RRULE:FREQ=YEARLY;RSCALE=HEBREW;SKIP=FORWARD;BYMONTH=5,5L\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	e, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}
	r := e.GetRecurrence()
	if r == nil {
		t.Fatal("RRULE did not decode")
	}
	if got := r.GetRscale(); got != "HEBREW" {
		t.Errorf("RSCALE = %q, want HEBREW", got)
	}
	if got := r.GetSkip(); got != eventv1.RecurrenceSkip_RECURRENCE_SKIP_FORWARD {
		t.Errorf("SKIP = %v, want FORWARD", got)
	}
	if got := r.GetMonths(); len(got) != 1 || got[0] != month.Month_MAY {
		t.Errorf("BYMONTH ordinary = %v, want [MAY]", got)
	}
	if got := r.GetLeapMonths(); len(got) != 1 || got[0] != month.Month_MAY {
		t.Errorf("BYMONTH leap = %v, want [MAY]", got)
	}

	// Re-encoding must keep the two apart, and must not emit SKIP without
	// RSCALE -- section 4 forbids that pairing.
	out, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "BYMONTH=5,5L") {
		t.Errorf("leap month lost on encode:\n%s", out)
	}
	if !strings.Contains(out, "RSCALE=HEBREW") || !strings.Contains(out, "SKIP=FORWARD") {
		t.Errorf("RSCALE/SKIP lost on encode:\n%s", out)
	}
}

// TestRfc7529SkipNeedsRscale pins section 4's rule that SKIP must not appear
// without RSCALE: a Recurrence carrying only SKIP encodes neither.
func TestRfc7529SkipNeedsRscale(t *testing.T) {
	const src = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:plain-1@example.com\r\n" +
		"DTSTAMP:20260101T090000Z\r\n" +
		"DTSTART:20260301T090000Z\r\n" +
		"SUMMARY:Plain\r\n" +
		"RRULE:FREQ=YEARLY;BYMONTH=5\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	e, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}
	e.GetRecurrence().Skip = eventv1.RecurrenceSkip_RECURRENCE_SKIP_BACKWARD
	out, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "SKIP=") {
		t.Errorf("SKIP emitted without RSCALE, which section 4 forbids:\n%s", out)
	}
	if strings.Contains(out, "5L") {
		t.Errorf("plain BYMONTH gained a leap suffix:\n%s", out)
	}
}

// TestRfc9253Relationships asserts CONCEPT, LINK and REFID.
//
// The LINK value type is the part worth pinning: RFC 9253 section 8.2 gives
// it three forms and decides between them with VALUE, not with the value's
// shape. A UID may itself look like a URI -- "event-2@example.com" does --
// so inferring the form from the string would silently mistype icaltime.
func TestRfc9253Relationships(t *testing.T) {
	const src = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1@example.com\r\n" +
		"DTSTAMP:20260101T090000Z\r\n" +
		"DTSTART:20260301T090000Z\r\n" +
		"SUMMARY:Conference talk\r\n" +
		"CONCEPT:http://example.com/vocab/conference\r\n" +
		"REFID:track-alpha\r\n" +
		"LINK;LINKREL=about;FMTTYPE=text/html:http://example.com/talk\r\n" +
		"LINK;VALUE=UID;LINKREL=alternate:event-2@example.com\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	e, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}

	if got := e.GetConcepts(); len(got) != 1 || got[0] != "http://example.com/vocab/conference" {
		t.Errorf("CONCEPT = %q", got)
	}
	if got := e.GetRefids(); len(got) != 1 || got[0] != "track-alpha" {
		t.Errorf("REFID = %q", got)
	}
	if len(e.GetLinks()) != 2 {
		t.Fatalf("got %d links, want 2", len(e.GetLinks()))
	}

	// Default form is URI, section 8.2, with no VALUE parameter present.
	uriLink := e.GetLinks()[0]
	if got := uriLink.GetUri(); got != "http://example.com/talk" {
		t.Errorf("LINK uri = %q", got)
	}
	if got := uriLink.GetRelation(); got != "about" {
		t.Errorf("LINKREL = %q", got)
	}
	if got := uriLink.GetFormatType(); got != "text/html" {
		t.Errorf("FMTTYPE = %q", got)
	}

	// VALUE=UID must land in the uid arm even though the value parses as a
	// perfectly good URI-ish string.
	uidLink := e.GetLinks()[1]
	if got := uidLink.GetIcalUid(); got != "event-2@example.com" {
		t.Errorf("LINK uid = %q", got)
	}
	if uidLink.GetUri() != "" {
		t.Error("VALUE=UID was decoded into the uri arm")
	}

	// Re-encoding must keep the forms apart and keep LINKREL, which section
	// 6.1 requires on every LINK.
	out, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"CONCEPT:http://example.com/vocab/conference",
		"REFID:track-alpha",
		"LINKREL=about",
		"VALUE=UID",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("encoded output is missing %q:\n%s", want, out)
		}
	}
}

// TestQuotedParamComma pins RFC 5545 section 3.1's rule that a comma inside a
// quoted-string belongs to the value: "Property parameter values that contain
// the COLON, SEMICOLON, or COMMA character separators MUST be specified as
// quoted-string text values."
//
// The parser used to unquote before splitting, which turned CN="Doe, John"
// into the single value "Doe" and silently lost the given name. Every
// free-text parameter had the same flaw -- LABEL, ALTREP, DIR -- and none of
// it was visible through a round trip, because the encoder re-quoted whatever
// truncated value came back.
func TestQuotedParamComma(t *testing.T) {
	const src = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1@example.com\r\n" +
		"DTSTAMP:20260101T090000Z\r\n" +
		"DTSTART:20260301T090000Z\r\n" +
		"SUMMARY:Review\r\n" +
		"ATTENDEE;CN=\"Doe, John\":mailto:john@example.com\r\n" +
		"ATTENDEE;CN=Plain:mailto:plain@example.com\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	e, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.GetAttendees()) != 2 {
		t.Fatalf("got %d attendees, want 2", len(e.GetAttendees()))
	}
	if got := e.GetAttendees()[0].GetDisplayName(); got != "Doe, John" {
		t.Errorf("quoted CN = %q, want %q", got, "Doe, John")
	}
	if got := e.GetAttendees()[1].GetDisplayName(); got != "Plain" {
		t.Errorf("unquoted CN = %q, want Plain", got)
	}
}

// TestRfc9073Publishing asserts the components and properties RFC 9073 adds.
//
// The nesting is the part that matters. Section 7.1's grammar is
// `partprop *locationc *resourcec`, so a VLOCATION inside a PARTICIPANT
// belongs to that participant, not to the event -- and reading it onto the
// event is precisely the bug VALARM-inside-VEVENT already produced here once.
// See docs/codec-findings-calendar.md.
func TestRfc9073Publishing(t *testing.T) {
	src, err := os.ReadFile("testdata/rfc9073_publishing.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}

	// Event-level content properties.
	if got := e.GetStyledDescriptions(); len(got) != 1 ||
		got[0].GetText() != "<p>Annual <b>conference</b></p>" ||
		got[0].GetFormatType() != "text/html" {
		t.Errorf("STYLED-DESCRIPTION = %v", got)
	}
	// Section 6.5 requires the plain DESCRIPTION to survive alongside icaltime.
	if got := e.GetDescription(); got != "Plain text description" {
		t.Errorf("plain DESCRIPTION was displaced: %q", got)
	}
	if got := e.GetStructuredData(); len(got) != 1 ||
		got[0].GetSchema() != "https://schema.org/Event" {
		t.Errorf("STRUCTURED-DATA = %v", got)
	}

	if len(e.GetParticipants()) != 2 {
		t.Fatalf("got %d participants, want 2", len(e.GetParticipants()))
	}
	speaker, sponsor := e.GetParticipants()[0], e.GetParticipants()[1]

	if got := speaker.GetParticipantTypes(); len(got) != 1 ||
		got[0] != eventv1.ParticipantType_PARTICIPANT_TYPE_SPEAKER {
		t.Errorf("speaker PARTICIPANT-TYPE = %v", got)
	}
	// A comma-separated list must decode as several roles, not one token.
	if got := sponsor.GetParticipantTypes(); len(got) != 2 {
		t.Errorf("sponsor PARTICIPANT-TYPE = %v, want two roles", got)
	}
	// CALENDAR-ADDRESS is optional here, unlike Attendee.address.
	if got := sponsor.GetCalendarAddress(); got != "" {
		t.Errorf("sponsor gained a calendar address: %q", got)
	}

	// The nested VLOCATION belongs to the speaker.
	if got := speaker.GetStructuredLocations(); len(got) != 1 ||
		got[0].GetDisplayName() != "Main stage" {
		t.Errorf("speaker's nested VLOCATION = %v", got)
	}
	// And the event keeps only its own, not the participant's.
	if got := e.GetStructuredLocations(); len(got) != 1 ||
		got[0].GetDisplayName() != "Conference center" {
		t.Errorf("event VLOCATION = %v; a nested one leaked up", got)
	}
	if got := e.GetResources(); len(got) != 1 || got[0].GetDisplayName() != "Projector" {
		t.Errorf("event VRESOURCE = %v", got)
	}

	// Participants are not attendees: the event has none of the latter.
	if len(e.GetAttendees()) != 0 {
		t.Errorf("participants leaked into attendees: %v", e.GetAttendees())
	}

	// Round trip, with the nesting preserved.
	out, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Decode(out)
	if err != nil {
		t.Fatalf("re-decode:\n%s\n%v", out, err)
	}
	if len(again.GetParticipants()) != 2 ||
		len(again.GetParticipants()[0].GetStructuredLocations()) != 1 {
		t.Errorf("nesting lost on round trip:\n%s", out)
	}
	if len(again.GetStructuredLocations()) != 1 {
		t.Errorf("event location count changed on round trip:\n%s", out)
	}

	// SCHEMA is a URI and so always contains a colon. RFC 5545 section 3.1
	// requires such a parameter value to be quoted; emitting it bare made the
	// parser read the colon as the value separator, so "https" became the
	// schema and the rest of the URI became the payload. Caught by this test
	// on its first run.
	// Unfold first: section 3.1 wraps long lines at 75 octets, so the
	// parameter is split across a CRLF and a leading space in the raw output.
	unfolded := strings.ReplaceAll(out, "\r\n ", "")
	if !strings.Contains(unfolded, `SCHEMA="https://schema.org/Event"`) {
		t.Errorf("SCHEMA was not quoted on encode:\n%s", out)
	}
	if got := again.GetStructuredData(); len(got) != 1 ||
		got[0].GetSchema() != "https://schema.org/Event" {
		t.Errorf("SCHEMA did not survive the round trip: %v", got)
	}
}
