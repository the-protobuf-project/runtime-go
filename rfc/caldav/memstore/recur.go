package memstore

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/internal/caltime"
	"google.golang.org/protobuf/proto"
)

// ErrUnsupportedRule is a recurrence rule this store does not expand: a
// sub-daily frequency, BYSETPOS, BYYEARDAY, BYWEEKNO, BYHOUR, BYMINUTE,
// BYSECOND, or a BYDAY with an ordinal.
var ErrUnsupportedRule = errors.New("memstore: unsupported recurrence rule")

// maxPeriods bounds the expansion of a rule whose parts rarely match, so a
// rule that never produces another instance cannot loop forever.
const maxPeriods = 100_000

// eventSpan is e's first occurrence, in the calendar's zone loc: a DATE event
// without an end lasts a day, any other event without one lasts no time.
func eventSpan(e *eventv1.Event, loc *time.Location) (caldav.Period, caltime.Form, error) {
	start, form, err := caltime.Read(e.GetStart(), loc)
	if err != nil {
		return caldav.Period{}, form, fmt.Errorf("memstore: event %s start: %w", e.GetName(), err)
	}
	end := start
	switch {
	case e.GetDuration() != nil:
		end = start.Add(e.GetDuration().AsDuration())
	case e.GetEnd() != nil:
		if end, _, err = caltime.Read(e.GetEnd(), loc); err != nil {
			return caldav.Period{}, form, fmt.Errorf("memstore: event %s end: %w", e.GetName(), err)
		}
	case form.Date:
		end = start.AddDate(0, 0, 1)
	}
	if end.Before(start) {
		return caldav.Period{}, form, fmt.Errorf("%w: event %s ends before it starts", caldav.ErrInvalid, e.GetName())
	}
	return caldav.Period{Start: start, End: end}, form, nil
}

// rule is a compiled RRULE, without its EXDATEs or RDATEs: COUNT counts these
// instances.
type rule struct {
	r        *eventv1.Recurrence
	start    time.Time
	until    time.Time
	hasUntil bool
}

func compile(r *eventv1.Recurrence, start time.Time, startForm caltime.Form) (rule, error) {
	switch {
	case r.GetFrequency() < eventv1.Frequency_FREQUENCY_DAILY:
		return rule{}, fmt.Errorf("%w: FREQ=%s", ErrUnsupportedRule, r.GetFrequency())
	case len(r.GetSetPositions()) > 0, len(r.GetYearDays()) > 0, len(r.GetWeekNumbers()) > 0,
		len(r.GetHourNumbers()) > 0, len(r.GetMinuteNumbers()) > 0, len(r.GetSecondNumbers()) > 0:
		return rule{}, fmt.Errorf("%w: BYSETPOS, BYYEARDAY, BYWEEKNO, BYHOUR, BYMINUTE and BYSECOND are not expanded", ErrUnsupportedRule)
	}
	for _, d := range r.GetWeekdays() {
		if d.GetOrdinal() != 0 {
			return rule{}, fmt.Errorf("%w: BYDAY with an ordinal", ErrUnsupportedRule)
		}
	}
	out := rule{r: r, start: start}
	if u := r.GetUntil(); u != nil {
		t, f, err := caltime.Read(u, start.Location())
		if err != nil {
			return rule{}, err
		}
		if f.Date && !startForm.Date {
			t = t.AddDate(0, 0, 1).Add(-time.Nanosecond)
		}
		out.until, out.hasUntil = t, true
	}
	return out, nil
}

// starts returns the rule's instance starts in [from, to), in order.
func (r rule) starts(from, to time.Time) []time.Time {
	var out []time.Time
	count := int32(0)
	limit := r.r.GetCount()
	for p := 0; p < maxPeriods; p++ {
		for _, c := range r.candidates(p) {
			if c.Before(r.start) {
				continue
			}
			if r.hasUntil && c.After(r.until) {
				return out
			}
			count++
			if limit > 0 && count > limit {
				return out
			}
			if !c.Before(to) {
				return out
			}
			if !c.Before(from) {
				out = append(out, c)
			}
		}
	}
	return out
}

// candidates are the instances period p of the rule's frequency produces,
// before COUNT and UNTIL.
func (r rule) candidates(p int) []time.Time {
	s := r.start
	loc := s.Location()
	interval := max(int(r.r.GetInterval()), 1)
	at := func(y int, m time.Month, d int) time.Time {
		return time.Date(y, m, d, s.Hour(), s.Minute(), s.Second(), s.Nanosecond(), loc)
	}
	var days []time.Time
	switch r.r.GetFrequency() {
	case eventv1.Frequency_FREQUENCY_DAILY:
		days = []time.Time{at(s.Year(), s.Month(), s.Day()+p*interval)}
	case eventv1.Frequency_FREQUENCY_WEEKLY:
		// WKST=MO: weeks run Monday to Sunday.
		monday := s.Day() - (int(s.Weekday())+6)%7 + 7*p*interval
		wanted := r.weekdays(s.Weekday())
		for i := range 7 {
			d := at(s.Year(), s.Month(), monday+i)
			if wanted[d.Weekday()] {
				days = append(days, d)
			}
		}
		return r.filter(days, false)
	case eventv1.Frequency_FREQUENCY_MONTHLY:
		first := time.Date(s.Year(), s.Month()+time.Month(p*interval), 1, 0, 0, 0, 0, loc)
		days = r.monthDays(first, at)
	case eventv1.Frequency_FREQUENCY_YEARLY:
		y := s.Year() + p*interval
		months := []time.Month{s.Month()}
		if len(r.r.GetMonths()) > 0 {
			months = months[:0]
			for _, m := range r.r.GetMonths() {
				months = append(months, time.Month(m))
			}
			slices.Sort(months)
		}
		for _, m := range months {
			days = append(days, r.monthDays(time.Date(y, m, 1, 0, 0, 0, 0, loc), at)...)
		}
		return r.filter(days, true)
	}
	return r.filter(days, false)
}

// monthDays are the days of the month starting at first that BYMONTHDAY,
// BYDAY or DTSTART's day select. A day the month does not have is skipped,
// RFC 5545 section 3.3.10.
func (r rule) monthDays(first time.Time, at func(int, time.Month, int) time.Time) []time.Time {
	n := time.Date(first.Year(), first.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
	var nums []int
	switch {
	case len(r.r.GetMonthDays()) > 0:
		for _, d := range r.r.GetMonthDays() {
			v := int(d)
			if v < 0 {
				v = n + 1 + v
			}
			nums = append(nums, v)
		}
	case len(r.r.GetWeekdays()) > 0:
		wanted := r.weekdays(r.start.Weekday())
		for d := 1; d <= n; d++ {
			if wanted[time.Date(first.Year(), first.Month(), d, 0, 0, 0, 0, time.UTC).Weekday()] {
				nums = append(nums, d)
			}
		}
	default:
		nums = []int{r.start.Day()}
	}
	slices.Sort(nums)
	var out []time.Time
	for _, d := range slices.Compact(nums) {
		if d >= 1 && d <= n {
			out = append(out, at(first.Year(), first.Month(), d))
		}
	}
	return out
}

// weekdays is BYDAY as a set, or def when the rule has none.
func (r rule) weekdays(def time.Weekday) map[time.Weekday]bool {
	out := map[time.Weekday]bool{}
	for _, d := range r.r.GetWeekdays() {
		out[time.Weekday(int(d.GetDay())%7)] = true
	}
	if len(out) == 0 {
		out[def] = true
	}
	return out
}

// filter applies the BY parts that narrow rather than expand at the rule's
// frequency: BYMONTH (unless the frequency is yearly, where it expanded),
// BYDAY and BYMONTHDAY at a daily frequency.
func (r rule) filter(days []time.Time, monthsExpanded bool) []time.Time {
	months := map[time.Month]bool{}
	for _, m := range r.r.GetMonths() {
		months[time.Month(m)] = true
	}
	daily := r.r.GetFrequency() == eventv1.Frequency_FREQUENCY_DAILY
	out := days[:0]
	for _, d := range days {
		if len(months) > 0 && !monthsExpanded && !months[d.Month()] {
			continue
		}
		if daily && len(r.r.GetWeekdays()) > 0 && !r.weekdays(d.Weekday())[d.Weekday()] {
			continue
		}
		if daily && len(r.r.GetMonthDays()) > 0 && !slices.ContainsFunc(r.r.GetMonthDays(), func(md int32) bool {
			n := time.Date(d.Year(), d.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
			v := int(md)
			if v < 0 {
				v = n + 1 + v
			}
			return v == d.Day()
		}) {
			continue
		}
		out = append(out, d)
	}
	return out
}

// occurrences returns the start of every occurrence of e overlapping window,
// and how long each lasts: the RRULE's instances less EXDATE, plus RDATE, RFC
// 5545 section 3.8.5; a single event is its one occurrence. The instances
// overrides replace are left out. A zero-length occurrence overlaps a window
// containing its instant, RFC 4791 section 9.9.
func occurrences(e *eventv1.Event, overrides []*eventv1.Event, loc *time.Location, window caldav.Period) ([]time.Time, time.Duration, error) {
	first, form, err := eventSpan(e, loc)
	if err != nil {
		return nil, 0, err
	}
	dur := first.End.Sub(first.Start)
	probe := max(dur, time.Nanosecond)
	occurs := func(t time.Time) bool { return t.Before(window.End) && t.Add(probe).After(window.Start) }
	if e.GetRecurrence() == nil {
		if occurs(first.Start) {
			return []time.Time{first.Start}, dur, nil
		}
		return nil, dur, nil
	}
	r, err := compile(e.GetRecurrence(), first.Start, form)
	if err != nil {
		return nil, dur, err
	}
	excluded := map[int64]bool{}
	for _, x := range e.GetExcludedTimes() {
		excluded[caltime.Key(x, loc)] = true
	}
	for _, o := range overrides {
		excluded[caltime.Key(o.GetOriginalStart(), loc)] = true
	}
	var starts []time.Time
	seen := map[int64]bool{}
	keep := func(t time.Time) {
		if k := t.UnixNano(); !excluded[k] && !seen[k] && occurs(t) {
			seen[k] = true
			starts = append(starts, t)
		}
	}
	for _, t := range r.starts(window.Start.Add(-probe), window.End) {
		keep(t)
	}
	for _, x := range e.GetAdditionalTimes() {
		if t, _, err := caltime.Read(x, loc); err == nil {
			keep(t)
		}
	}
	slices.SortFunc(starts, func(a, b time.Time) int { return a.Compare(b) })
	return starts, dur, nil
}

// instances answers [caldav.Store.Instances]: for a recurring series, its
// instances overlapping window with overrides applied -- a moved one at its
// new time, a cancelled one not at all -- and for any other event, the event
// itself when it overlaps. Instances keep the event's own time form, carry the
// original_start that identifies them and no name; an override keeps its own.
func instances(e *eventv1.Event, overrides []*eventv1.Event, loc *time.Location, window caldav.Period) ([]*eventv1.Event, error) {
	starts, dur, err := occurrences(e, overrides, loc, window)
	if err != nil {
		return nil, err
	}
	if e.GetRecurrence() == nil {
		if len(starts) == 0 {
			return nil, nil
		}
		return []*eventv1.Event{e}, nil
	}
	_, startForm, err := caltime.Read(e.GetStart(), loc)
	if err != nil {
		return nil, err
	}
	var endForm caltime.Form
	if e.GetEnd() != nil {
		if _, endForm, err = caltime.Read(e.GetEnd(), loc); err != nil {
			return nil, err
		}
	}
	type instance struct {
		start time.Time
		event *eventv1.Event
	}
	out := make([]instance, 0, len(starts)+len(overrides))
	for _, t := range starts {
		inst := proto.Clone(e).(*eventv1.Event)
		inst.Name, inst.Etag = "", ""
		inst.Recurrence, inst.ExcludedTimes, inst.AdditionalTimes = nil, nil, nil
		inst.Start = startForm.Render(t)
		inst.OriginalStart = startForm.Render(t)
		if e.GetEnd() != nil {
			inst.Ends = &eventv1.Event_End{End: endForm.Render(t.Add(dur))}
		}
		out = append(out, instance{t, inst})
	}
	for _, o := range overrides {
		if o.GetConfirmation() == eventv1.Confirmation_CONFIRMATION_CANCELLED {
			continue
		}
		at, _, err := occurrences(o, nil, loc, window)
		if err != nil {
			return nil, err
		}
		if len(at) > 0 {
			out = append(out, instance{at[0], o})
		}
	}
	slices.SortStableFunc(out, func(a, b instance) int { return a.start.Compare(b.start) })
	events := make([]*eventv1.Event, len(out))
	for i, in := range out {
		events[i] = in.event
	}
	return events, nil
}
