package caldav

import (
	"errors"
	"testing"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
)

const series = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:s@example.com\r\nDTSTAMP:20270101T000000Z\r\n" +
	"DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\nEND:VEVENT\r\n"

func rangeOverride(week string) string {
	return "BEGIN:VEVENT\r\nUID:s@example.com\r\nDTSTAMP:20270101T000000Z\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:202701" + week + "T100000Z\r\nDTSTART:202701" + week + "T120000Z\r\nDTEND:202701" + week + "T130000Z\r\nEND:VEVENT\r\n"
}

// An override reaching every later instance is set apart for the split, not
// stored as a single-instance override.
func TestDecodeRange(t *testing.T) {
	d, err := decode(series + rangeOverride("25") + "END:VCALENDAR\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if d.event == nil || len(d.overrides) != 0 || len(d.splits) != 1 {
		t.Fatalf("decoded %d overrides and %d splits", len(d.overrides), len(d.splits))
	}
	for name, text := range map[string]string{
		"two splits": series + rangeOverride("18") + rangeOverride("25") + "END:VCALENDAR\r\n",
		"no series":  "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\n" + rangeOverride("25") + "END:VCALENDAR\r\n",
	} {
		_, err := decode(text)
		var f *Error
		if !errors.As(err, &f) || f.Status != 403 || f.Precondition != ValidCalendarObjectResource {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// Bodies that are not one calendar object resource are refused with the
// precondition naming why, RFC 4791 section 5.3.2.1.
func TestDecodeRefuses(t *testing.T) {
	cal := func(s string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\n" + s + "END:VCALENDAR\r\n"
	}
	vevent := func(uid, extra string) string {
		return "BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20270101T000000Z\r\nDTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\n" + extra + "END:VEVENT\r\n"
	}
	for name, tc := range map[string]struct {
		body, precondition string
	}{
		"vtodo":        {cal("BEGIN:VTODO\r\nUID:x\r\nDTSTAMP:20270101T000000Z\r\nEND:VTODO\r\n"), SupportedCalendarComponent},
		"garbage":      {cal("BEGIN:VEVENT\r\nDTSTART:nope\r\nEND:VEVENT\r\n"), ValidCalendarData},
		"two uids":     {cal(vevent("a", "") + vevent("b", "RECURRENCE-ID:20270111T100000Z\r\n")), ValidCalendarObjectResource},
		"two series":   {cal(vevent("a", "") + vevent("a", "")), ValidCalendarObjectResource},
		"single ovr":   {cal(vevent("a", "") + vevent("a", "RECURRENCE-ID:20270104T100000Z\r\n")), ValidCalendarObjectResource},
		"rrule on ovr": {cal(series[len("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\n"):] + vevent("s@example.com", "RECURRENCE-ID:20270111T100000Z\r\nRRULE:FREQ=DAILY\r\n")), ValidCalendarObjectResource},
	} {
		_, err := decode(tc.body)
		var f *Error
		if !errors.As(err, &f) || f.Status != 403 || f.Precondition != tc.precondition {
			t.Errorf("%s: %v, want 403 %s", name, err, tc.precondition)
		}
	}
	d, err := decode(series + rangeOverride("11") + "END:VCALENDAR\r\n")
	if err != nil || d.uid() != "s@example.com" {
		t.Fatalf("uid of a series: %q, %v", d.uid(), err)
	}
}

func TestMatchesIf(t *testing.T) {
	for _, tc := range []struct {
		header, stored string
		exists, want   bool
	}{
		{"*", "a", true, true},
		{"*", "", false, false},
		{`"a"`, "a", true, true},
		{`W/"a"`, "a", true, true},
		{`"b", "a"`, "a", true, true},
		{`"b"`, "a", true, false},
		{`"a"`, "a", false, false},
	} {
		if got := matchesIf(tc.header, tc.stored, tc.exists); got != tc.want {
			t.Errorf("matchesIf(%q, %q, %v) = %v", tc.header, tc.stored, tc.exists, got)
		}
	}
}

// An object's tag covers its overrides, in any order.
func TestObjectETag(t *testing.T) {
	series := &eventv1.Event{Name: "s", Etag: "1"}
	a := &eventv1.Event{Name: "a", Etag: "2"}
	b := &eventv1.Event{Name: "b", Etag: "3"}
	plain := Object{Event: series}
	if plain.ETag() != "1" {
		t.Fatalf("a single event's tag is its own: %q", plain.ETag())
	}
	ab := Object{Event: series, Overrides: []*eventv1.Event{a, b}}.ETag()
	ba := Object{Event: series, Overrides: []*eventv1.Event{b, a}}.ETag()
	if ab != ba || ab == "1" {
		t.Fatalf("override order changed the tag: %q vs %q", ab, ba)
	}
	b2 := &eventv1.Event{Name: "b", Etag: "4"}
	if (Object{Event: series, Overrides: []*eventv1.Event{a, b2}}).ETag() == ab {
		t.Fatal("editing one override left the object's tag unchanged")
	}
}

func TestParsePath(t *testing.T) {
	h := New(nil)
	for path, want := range map[string]target{
		"/dav":                           {kind: kindRoot},
		"/dav/":                          {kind: kindRoot},
		"/dav/principals/default/":       {kind: kindPrincipal},
		"/dav/calendars/":                {kind: kindHome},
		"/dav/calendars/c/":              {kind: kindCalendar, calendar: "c"},
		"/dav/calendars/c/a%2Fb.ics":     {kind: kindObject, calendar: "c", object: "a/b.ics"},
		"/dav/calendars/c/Weekly%20.ics": {kind: kindObject, calendar: "c", object: "Weekly .ics"},
	} {
		got, ok := h.parsePath(path)
		if !ok || got != want {
			t.Errorf("parsePath(%q) = %+v, %v", path, got, ok)
		}
	}
	for _, path := range []string{"/davx", "/other", "/dav/principals/x/", "/dav/calendars/c/o/extra", "/dav/x"} {
		if _, ok := h.parsePath(path); ok {
			t.Errorf("parsePath(%q) accepted", path)
		}
	}
	if got := h.objectHref("c", "a/b.ics"); got != "/dav/calendars/c/a%2Fb.ics" {
		t.Errorf("objectHref = %q", got)
	}
}
