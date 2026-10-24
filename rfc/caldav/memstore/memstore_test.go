package memstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
)

func event(t *testing.T, lines string) *eventv1.Event {
	t.Helper()
	e, err := rfc.ICalendar("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//t//EN\r\nBEGIN:VEVENT\r\nUID:u\r\nDTSTAMP:20270101T000000Z\r\n" +
		lines + "END:VEVENT\r\nEND:VCALENDAR\r\n").Event()
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func utc(s string) time.Time {
	t, err := time.Parse("20060102T150405Z", s)
	if err != nil {
		panic(err)
	}
	return t
}

func starts(t *testing.T, e *eventv1.Event, from, to string) []string {
	t.Helper()
	at, _, err := occurrences(e, nil, time.UTC, caldav.Period{Start: utc(from), End: utc(to)})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(at))
	for _, a := range at {
		out = append(out, a.UTC().Format("20060102T150405Z"))
	}
	return out
}

func TestOccurrences(t *testing.T) {
	for name, tc := range map[string]struct {
		lines    string
		from, to string
		want     string
	}{
		"single":   {"DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\n", "20270104T103000Z", "20270105T000000Z", "[20270104T100000Z]"},
		"touching": {"DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\n", "20270104T110000Z", "20270105T000000Z", "[]"},
		"instant":  {"DTSTART:20270104T100000Z\r\n", "20270104T100000Z", "20270104T100001Z", "[20270104T100000Z]"},
		"weekly byday": {"DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\nRRULE:FREQ=WEEKLY;BYDAY=MO,WE;COUNT=4\r\n",
			"20270101T000000Z", "20270201T000000Z", "[20270104T100000Z 20270106T100000Z 20270111T100000Z 20270113T100000Z]"},
		"interval until": {"DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\nRRULE:FREQ=DAILY;INTERVAL=2;UNTIL=20270110T100000Z\r\n",
			"20270101T000000Z", "20270201T000000Z", "[20270104T100000Z 20270106T100000Z 20270108T100000Z 20270110T100000Z]"},
		"monthly 31st": {"DTSTART:20270131T100000Z\r\nDTEND:20270131T110000Z\r\nRRULE:FREQ=MONTHLY;COUNT=3\r\n",
			"20270101T000000Z", "20280101T000000Z", "[20270131T100000Z 20270331T100000Z 20270531T100000Z]"},
		"yearly bymonth": {"DTSTART:20270115T100000Z\r\nDTEND:20270115T110000Z\r\nRRULE:FREQ=YEARLY;BYMONTH=1,7;COUNT=3\r\n",
			"20270101T000000Z", "20290101T000000Z", "[20270115T100000Z 20270715T100000Z 20280115T100000Z]"},
		"exdate rdate": {"DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEXDATE:20270105T100000Z\r\nRDATE:20270110T100000Z\r\n",
			"20270101T000000Z", "20270201T000000Z", "[20270104T100000Z 20270106T100000Z 20270110T100000Z]"},
		"window mid series": {"DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\nRRULE:FREQ=DAILY;COUNT=10\r\n",
			"20270108T103000Z", "20270109T103000Z", "[20270108T100000Z 20270109T100000Z]"},
	} {
		if got := fmt.Sprint(starts(t, event(t, tc.lines), tc.from, tc.to)); got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
}

// A zoned series keeps its wall clock across a DST change.
func TestOccurrencesKeepWallClock(t *testing.T) {
	e := event(t, "DTSTART;TZID=Europe/Madrid:20270322T100000\r\nDTEND;TZID=Europe/Madrid:20270322T110000\r\nRRULE:FREQ=WEEKLY;COUNT=2\r\n")
	got := fmt.Sprint(starts(t, e, "20270301T000000Z", "20270501T000000Z"))
	if want := "[20270322T090000Z 20270329T080000Z]"; got != want {
		t.Fatalf("%s, want %s", got, want)
	}
}

func TestUnsupportedRule(t *testing.T) {
	e := event(t, "DTSTART:20270104T100000Z\r\nRRULE:FREQ=MONTHLY;BYDAY=1MO\r\n")
	if _, _, err := occurrences(e, nil, time.UTC, caldav.Period{Start: utc("20270101T000000Z"), End: utc("20270201T000000Z")}); !errors.Is(err, ErrUnsupportedRule) {
		t.Fatalf("BYDAY=1MO: %v", err)
	}
}

func TestPeriods(t *testing.T) {
	p := func(a, b string) caldav.Period {
		return caldav.Period{Start: utc("20270101T" + a + "0000Z"), End: utc("20270101T" + b + "0000Z")}
	}
	merged := merge([]caldav.Period{p("12", "13"), p("08", "10"), p("10", "11")})
	if fmt.Sprint(merged) != fmt.Sprint([]caldav.Period{p("08", "11"), p("12", "13")}) {
		t.Fatalf("merge: %v", merged)
	}
	left := subtract([]caldav.Period{p("06", "22")}, []caldav.Period{p("08", "12"), p("13", "20")})
	if fmt.Sprint(left) != fmt.Sprint([]caldav.Period{p("06", "08"), p("12", "13"), p("20", "22")}) {
		t.Fatalf("subtract: %v", left)
	}
	if c := clip([]caldav.Period{p("06", "09"), p("21", "23")}, p("08", "22")); fmt.Sprint(c) != fmt.Sprint([]caldav.Period{p("08", "09"), p("21", "22")}) {
		t.Fatalf("clip: %v", c)
	}
}

func TestTailNames(t *testing.T) {
	at := time.Date(2027, 1, 25, 9, 0, 0, 0, time.UTC)
	for in, want := range map[string]string{
		"calendars/c/events/weekly":     "calendars/c/events/weekly-r20270125t090000z",
		"calendars/c/events/Weekly.ics": "calendars/c/events/Weekly_R20270125T090000Z.ics",
	} {
		if got := tailName(in, at); got != want {
			t.Errorf("tailName(%q) = %q", in, got)
		}
	}
	if got := tailUID("maint@example.com", at); got != "maint_R20270125T090000Z@example.com" {
		t.Errorf("tailUID = %q", got)
	}
}

// A weekly rule with BYDAY moves its days with a shift across days.
func TestShiftRule(t *testing.T) {
	r := event(t, "DTSTART:20270104T100000Z\r\nRRULE:FREQ=WEEKLY;BYDAY=MO,WE\r\n").GetRecurrence()
	shifted, err := shiftRule(r, utc("20270111T100000Z"), utc("20270112T100000Z"))
	if err != nil {
		t.Fatal(err)
	}
	if d := shifted.GetWeekdays(); d[0].GetDay().String() != "TUESDAY" || d[1].GetDay().String() != "THURSDAY" {
		t.Fatalf("shifted BYDAY: %v", d)
	}
	if r.GetWeekdays()[0].GetDay().String() != "MONDAY" {
		t.Fatal("shiftRule changed its input")
	}
	m := event(t, "DTSTART:20270130T100000Z\r\nRRULE:FREQ=MONTHLY;BYMONTHDAY=30\r\n").GetRecurrence()
	if _, err := shiftRule(m, utc("20270130T100000Z"), utc("20270202T100000Z")); err == nil {
		t.Fatal("BYMONTHDAY pushed off the month was accepted")
	}
}

func newCalendar(t *testing.T, s *Store) *calendarv1.Calendar {
	t.Helper()
	ctx := context.Background()
	if err := s.CreateCalendar(ctx, &calendarv1.Calendar{Name: "calendars/c"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateCalendar(ctx, &calendarv1.Calendar{Name: "calendars/c"}); !errors.Is(err, caldav.ErrAlreadyExists) {
		t.Fatalf("second create: %v", err)
	}
	c, err := s.Calendar(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGuards(t *testing.T) {
	ctx := context.Background()
	s := New()
	c := newCalendar(t, s)
	e := event(t, "DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\n")
	if _, err := s.PutObject(ctx, caldav.PutRequest{Calendar: c, Name: "a.ics", Event: e}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutObject(ctx, caldav.PutRequest{Calendar: c, Name: "a.ics", Event: e}); !errors.Is(err, caldav.ErrPreconditionFailed) {
		t.Fatalf("create over an existing object: %v", err)
	}
	stale, err := s.Object(ctx, "c", "a.ics")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutObject(ctx, caldav.PutRequest{Calendar: c, Name: "a.ics", Existing: &stale, Event: e}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutObject(ctx, caldav.PutRequest{Calendar: c, Name: "a.ics", Existing: &stale, Event: e}); !errors.Is(err, caldav.ErrPreconditionFailed) {
		t.Fatalf("replace with a stale etag: %v", err)
	}
	if err := s.DeleteObject(ctx, "c", stale); !errors.Is(err, caldav.ErrPreconditionFailed) {
		t.Fatalf("delete with a stale etag: %v", err)
	}
	if err := s.UpdateCalendar(ctx, &calendarv1.Calendar{Name: c.GetName(), Etag: "stale", DisplayName: "x"}, []string{"display_name"}); !errors.Is(err, caldav.ErrPreconditionFailed) {
		t.Fatalf("update with a stale etag: %v", err)
	}
	// Handed-out messages are copies.
	got, _ := s.Object(ctx, "c", "a.ics")
	got.Event.Summary = "mutated"
	if again, _ := s.Object(ctx, "c", "a.ics"); again.Event.GetSummary() == "mutated" {
		t.Fatal("the store shares its messages with callers")
	}
	if _, err := s.Object(ctx, "c", "missing.ics"); !errors.Is(err, caldav.ErrNotFound) {
		t.Fatalf("missing object: %v", err)
	}
	if _, err := s.Objects(ctx, "missing"); !errors.Is(err, caldav.ErrNotFound) {
		t.Fatalf("missing calendar: %v", err)
	}
}

// Concurrent conditional writes: exactly one of a set racing on one etag
// lands; run under -race.
func TestConcurrentPuts(t *testing.T) {
	ctx := context.Background()
	s := New()
	c := newCalendar(t, s)
	e := event(t, "DTSTART:20270104T100000Z\r\nDTEND:20270104T110000Z\r\n")
	if _, err := s.PutObject(ctx, caldav.PutRequest{Calendar: c, Name: "a.ics", Event: e}); err != nil {
		t.Fatal(err)
	}
	base, _ := s.Object(ctx, "c", "a.ics")
	var wg sync.WaitGroup
	var mu sync.Mutex
	won := 0
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			existing := base
			if _, err := s.PutObject(ctx, caldav.PutRequest{Calendar: c, Name: "a.ics", Existing: &existing, Event: e}); err == nil {
				mu.Lock()
				won++
				mu.Unlock()
			}
			_, _ = s.Objects(ctx, "c")
		}()
	}
	wg.Wait()
	if won != 1 {
		t.Fatalf("%d racing writes on one etag landed, want 1", won)
	}
}

func TestFreeBusyAvailability(t *testing.T) {
	ctx := context.Background()
	s := New()
	c := newCalendar(t, s)
	a, err := rfc.ICalendar("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//t//EN\r\nBEGIN:VAVAILABILITY\r\nUID:h\r\nDTSTAMP:20270101T000000Z\r\nDTSTART:20270104T000000Z\r\nDTEND:20270106T000000Z\r\n" +
		"BEGIN:AVAILABLE\r\nUID:h-1\r\nDTSTAMP:20270101T000000Z\r\nDTSTART:20270104T080000Z\r\nDTEND:20270104T200000Z\r\nRRULE:FREQ=DAILY\r\nEND:AVAILABLE\r\n" +
		"END:VAVAILABILITY\r\nEND:VCALENDAR\r\n").Availability()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutObject(ctx, caldav.PutRequest{Calendar: c, Name: "h.ics", Availability: a}); err != nil {
		t.Fatal(err)
	}
	fb, err := s.FreeBusy(ctx, c, caldav.Period{Start: utc("20270103T000000Z"), End: utc("20270107T000000Z")})
	if err != nil {
		t.Fatal(err)
	}
	p := func(a, b string) caldav.Period { return caldav.Period{Start: utc(a), End: utc(b)} }
	want := []caldav.Period{p("20270104T000000Z", "20270104T080000Z"), p("20270104T200000Z", "20270105T080000Z"), p("20270105T200000Z", "20270106T000000Z")}
	if !samePeriods(fb.Unavailable, want) {
		t.Fatalf("unavailable %v, want %v", fb.Unavailable, want)
	}
}

func samePeriods(a, b []caldav.Period) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Start.Equal(b[i].Start) || !a[i].End.Equal(b[i].End) {
			return false
		}
	}
	return true
}
