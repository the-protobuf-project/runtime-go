package memstore

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/internal/caltime"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/protobuf/proto"
)

// A RECURRENCE-ID;RANGE=THISANDFUTURE override (RFC 5545 section 3.2.13)
// changes the instance it names and every later one. It is stored the way
// CalDAV servers do: the series is split at that instance. The original ends
// just before it (COUNT or UNTIL), and a new series -- the edit, with the
// original's rule -- carries on from it under a UID of its own. Section
// 3.8.4.4: the edit's start minus the instance it names is applied to every
// later instance too.

// plan is a split worked out, before anything is written.
type plan struct {
	// truncated is the series ending before the split instance S -- or,
	// when S is its first instance, the whole series edited in place.
	truncated *eventv1.Event
	// kept are the overrides before S, which stay with truncated.
	kept []*eventv1.Event
	// tail is the new series from S on; nil when the split cancels the
	// rest of the series or edits it in place.
	tail *eventv1.Event
	// rehomed are the overrides after S, moved to tail (or kept on the
	// series edited in place).
	rehomed []*eventv1.Event
}

func refuse(format string, args ...any) error {
	return caldav.Errorf(http.StatusForbidden, caldav.ValidCalendarObjectResource, format, args...)
}

// planSplit works out a THISANDFUTURE split of series at the instance edit's
// original_start names, S:
//
//   - truncated is series ending before S -- COUNT cut to the rule instances
//     before S, else UNTIL set to the last of them, in the form section
//     3.3.10 asks -- with its EXDATEs and RDATEs from S on dropped and its
//     SEQUENCE raised.
//   - tail is edit as a series of its own from S on: the original's rule
//     (COUNT reduced by the instances before S, UNTIL moved by the shift),
//     the EXDATEs and RDATEs from S on moved by edit's shift, a UID derived
//     from the original's, and the original UID in its REFIDs.
//   - rehomed are the overrides after S moved to tail (their original_start
//     shifted, and their start and end when they had not moved). The
//     override at S is replaced by edit; when edit is CANCELLED every
//     override after S goes too, since the series ends there.
//
// S must be an instance of series; a rule part the shift would change the
// meaning of (BYSETPOS, a BYMONTHDAY pushed off the month) is refused rather
// than guessed at.
func planSplit(series *eventv1.Event, overrides []*eventv1.Event, edit *eventv1.Event, loc *time.Location) (plan, error) {
	if series.GetRecurrence() == nil {
		return plan{}, refuse("%s has no recurrence rule, so there is no series to split", series.GetName())
	}
	sAt, _, err := caltime.Read(edit.GetOriginalStart(), loc)
	if err != nil {
		return plan{}, err
	}
	first, startForm, err := eventSpan(series, loc)
	if err != nil {
		return plan{}, err
	}
	at, _, err := occurrences(series, nil, loc, caldav.Period{Start: sAt, End: sAt.Add(time.Nanosecond)})
	if err != nil {
		return plan{}, err
	}
	if !slices.ContainsFunc(at, sAt.Equal) {
		return plan{}, refuse("%s is not an instance of the series", sAt.UTC().Format(time.RFC3339))
	}
	r, err := compile(series.GetRecurrence(), first.Start, startForm)
	if err != nil {
		return plan{}, err
	}
	before := r.starts(first.Start, sAt)
	isRule := len(r.starts(sAt, sAt.Add(time.Nanosecond))) > 0
	if !isRule && len(r.starts(sAt, sAt.AddDate(100, 0, 0))) > 0 {
		return plan{}, refuse("that instance is an added date (RDATE) with rule instances after it")
	}
	if len(before) == 0 && slices.ContainsFunc(series.GetAdditionalTimes(), func(ct *eventv1.CalendarTime) bool {
		t, _, rerr := caltime.Read(ct, loc)
		return rerr == nil && t.Before(sAt)
	}) {
		return plan{}, refuse("only added dates (RDATE) come before it")
	}

	eAt, eForm := sAt, startForm
	if edit.GetStart() != nil {
		if eAt, eForm, err = caltime.Read(edit.GetStart(), loc); err != nil {
			return plan{}, err
		}
	}
	delta := eAt.Sub(sAt)
	shiftedRule, err := shiftRule(series.GetRecurrence(), sAt.In(first.Start.Location()), eAt)
	if err != nil {
		return plan{}, refuse("%v", err)
	}
	cancel := edit.GetConfirmation() == eventv1.Confirmation_CONFIRMATION_CANCELLED

	var out plan
	var later []*eventv1.Event
	for _, o := range overrides {
		oAt, _, err := caltime.Read(o.GetOriginalStart(), loc)
		switch {
		case err != nil || oAt.Before(sAt):
			out.kept = append(out.kept, o)
		case oAt.Equal(sAt) || cancel:
		default:
			later = append(later, o)
		}
	}
	shift := func(ct *eventv1.CalendarTime) *eventv1.CalendarTime {
		t, f, err := caltime.Read(ct, loc)
		if err != nil {
			return ct
		}
		return f.Render(t.Add(delta))
	}
	rehome := func(uid string) []*eventv1.Event {
		moved := make([]*eventv1.Event, 0, len(later))
		for _, o := range later {
			c := proto.Clone(o).(*eventv1.Event)
			c.IcalUid = uid
			if start, _, err := caltime.Read(o.GetStart(), loc); err == nil {
				if orig, _, _ := caltime.Read(o.GetOriginalStart(), loc); start.Equal(orig) {
					c.Start = shift(o.GetStart())
					if o.GetEnd() != nil {
						c.Ends = &eventv1.Event_End{End: shift(o.GetEnd())}
					}
				}
			}
			c.OriginalStart = shift(o.GetOriginalStart())
			moved = append(moved, c)
		}
		return moved
	}
	fromEdit := func() *eventv1.Event {
		c := proto.Clone(edit).(*eventv1.Event)
		c.OriginalStart, c.OriginalStartRange = nil, eventv1.RecurrenceRange_RECURRENCE_RANGE_UNSPECIFIED
		c.Start = eForm.Render(eAt)
		if c.GetEnd() == nil && c.GetDuration() == nil {
			switch {
			case series.GetDuration() != nil:
				c.Ends = &eventv1.Event_Duration{Duration: series.GetDuration()}
			case series.GetEnd() != nil:
				if _, endForm, err := caltime.Read(series.GetEnd(), loc); err == nil {
					c.Ends = &eventv1.Event_End{End: endForm.Render(eAt.Add(first.End.Sub(first.Start)))}
				}
			}
		}
		return c
	}

	if len(before) == 0 {
		// S is the first instance: the whole series changes, in place.
		if cancel {
			whole := proto.Clone(series).(*eventv1.Event)
			whole.Confirmation = eventv1.Confirmation_CONFIRMATION_CANCELLED
			whole.Sequence = series.GetSequence() + 1
			out.truncated = whole
			return out, nil
		}
		whole := fromEdit()
		whole.Name, whole.Etag, whole.Uid, whole.IcalUid = series.GetName(), series.GetEtag(), series.GetUid(), series.GetIcalUid()
		whole.Recurrence = shiftedRule
		if u := series.GetRecurrence().GetUntil(); u != nil {
			whole.Recurrence.Bound = &eventv1.Recurrence_Until{Until: shift(u)}
		}
		whole.ExcludedTimes, whole.AdditionalTimes = nil, nil
		for _, x := range series.GetExcludedTimes() {
			whole.ExcludedTimes = append(whole.ExcludedTimes, shift(x))
		}
		for _, x := range series.GetAdditionalTimes() {
			whole.AdditionalTimes = append(whole.AdditionalTimes, shift(x))
		}
		whole.Sequence = max(series.GetSequence(), edit.GetSequence()) + 1
		out.truncated, out.rehomed = whole, rehome(series.GetIcalUid())
		return out, nil
	}

	truncated := proto.Clone(series).(*eventv1.Event)
	n := series.GetRecurrence().GetCount()
	if isRule {
		rr := proto.Clone(series.GetRecurrence()).(*eventv1.Recurrence)
		if n > 0 {
			rr.Bound = &eventv1.Recurrence_Count{Count: int32(len(before))}
		} else {
			rr.Bound = &eventv1.Recurrence_Until{Until: untilOf(before[len(before)-1], startForm, loc)}
		}
		truncated.Recurrence = rr
	}
	truncated.ExcludedTimes, truncated.AdditionalTimes = nil, nil
	var movedEx, movedAdd []*eventv1.CalendarTime
	for _, x := range series.GetExcludedTimes() {
		if t, _, err := caltime.Read(x, loc); err == nil && !t.Before(sAt) {
			movedEx = append(movedEx, shift(x))
			continue
		}
		truncated.ExcludedTimes = append(truncated.ExcludedTimes, x)
	}
	for _, x := range series.GetAdditionalTimes() {
		t, _, err := caltime.Read(x, loc)
		if err == nil && !t.Before(sAt) {
			if !t.Equal(sAt) {
				movedAdd = append(movedAdd, shift(x))
			}
			continue
		}
		truncated.AdditionalTimes = append(truncated.AdditionalTimes, x)
	}
	truncated.Sequence = series.GetSequence() + 1
	out.truncated = truncated
	if cancel {
		return out, nil
	}

	base := series.GetIcalUid()
	if base == "" {
		base = series.GetUid()
	}
	tail := fromEdit()
	tail.Name, tail.Etag, tail.Uid = tailName(series.GetName(), sAt), "", ""
	tail.CreateTime, tail.UpdateTime, tail.DeleteTime, tail.ExpireTime = nil, nil, nil, nil
	tail.IcalUid = tailUID(base, sAt)
	tail.Recurrence = nil
	if isRule {
		tail.Recurrence = shiftedRule
		switch b := series.GetRecurrence().GetBound().(type) {
		case *eventv1.Recurrence_Count:
			tail.Recurrence.Bound = &eventv1.Recurrence_Count{Count: n - int32(len(before))}
		case *eventv1.Recurrence_Until:
			tail.Recurrence.Bound = &eventv1.Recurrence_Until{Until: shift(b.Until)}
		}
	} else if len(movedAdd) > 0 {
		return plan{}, refuse("that instance is an added date (RDATE) with more added dates after it")
	}
	tail.ExcludedTimes, tail.AdditionalTimes = movedEx, movedAdd
	if base != "" && !slices.Contains(tail.GetRefids(), base) {
		tail.Refids = append(tail.Refids, base)
	}
	out.tail, out.rehomed = tail, rehome(tail.GetIcalUid())
	return out, nil
}

// tailName is the resource name of the series split off name at instance at:
// "calendars/c/events/weekly" -> "calendars/c/events/weekly-r20270125t090000z",
// and a CalDAV object "Weekly.ics" -> "Weekly_R20270125T090000Z.ics". It is
// the same on every attempt, so a retried split finds the tail it made.
func tailName(name string, at time.Time) string {
	stamp := at.UTC().Format(caltime.UTCStamp)
	dir, id := "", name
	if i := strings.LastIndex(name, "/"); i >= 0 {
		dir, id = name[:i+1], name[i+1:]
	}
	if i := strings.LastIndex(id, "."); i > 0 {
		return dir + id[:i] + "_R" + stamp + id[i:]
	}
	return dir + id + "-r" + strings.ToLower(stamp)
}

// tailUID is the UID of the series split off uid at instance at, the
// instance's UTC time inserted before any @domain:
// "maint@example.com" -> "maint_R20270125T090000Z@example.com".
func tailUID(uid string, at time.Time) string {
	suffix := "_R" + at.UTC().Format(caltime.UTCStamp)
	if i := strings.LastIndex(uid, "@"); i >= 0 {
		return uid[:i] + suffix + uid[i:]
	}
	return uid + suffix
}

// untilOf is UNTIL for a rule whose last instance is last, in the form
// section 3.3.10 requires: a DATE when DTSTART is a DATE, a floating
// DATE-TIME when DTSTART floats, and otherwise UTC.
func untilOf(last time.Time, start caltime.Form, loc *time.Location) *eventv1.CalendarTime {
	switch {
	case start.Date:
		l := last.In(loc)
		return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_Date{Date: &date.Date{
			Year: int32(l.Year()), Month: int32(l.Month()), Day: int32(l.Day()),
		}}}
	case start.Zone == nil && start.Offset == nil:
		return caltime.Form{Loc: loc}.Render(last)
	}
	return caltime.UTC().Render(last)
}

// shiftRule returns r with the parts that pin a day moved from the instance
// at s to e: the shift an edit applies to every later instance. A rule that
// takes its days and times from DTSTART needs nothing; one whose parts would
// change meaning under the shift is an error.
func shiftRule(r *eventv1.Recurrence, s, e time.Time) (*eventv1.Recurrence, error) {
	out := proto.Clone(r).(*eventv1.Recurrence)
	days := civilDays(e) - civilDays(s)
	if days == 0 {
		return out, nil
	}
	if len(r.GetSetPositions()) > 0 {
		return nil, errors.New("its rule has BYSETPOS, which a shift of the day would change the meaning of")
	}
	if len(r.GetMonths()) > 0 && e.Month() != s.Month() {
		return nil, errors.New("the shift moves it into another month and its rule has BYMONTH")
	}
	for i, d := range r.GetWeekdays() {
		out.Weekdays[i].Day = dayofweek.DayOfWeek((int(d.GetDay())-1+days%7+7)%7 + 1)
	}
	for i, md := range r.GetMonthDays() {
		v := md + int32(days)
		if md < 0 || v < 1 || v > 31 {
			return nil, fmt.Errorf("its rule has BYMONTHDAY=%d, which a shift by %d days moves off the month", md, days)
		}
		out.MonthDays[i] = v
	}
	return out, nil
}

// civilDays numbers t's calendar date in its own zone.
func civilDays(t time.Time) int {
	return int(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
}
