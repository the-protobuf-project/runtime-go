package memstore

import (
	"context"
	"slices"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/internal/caltime"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/ical"
)

// FreeBusy implements [caldav.Store]:
//
//   - Busy is every opaque, confirmed (or unmarked) occurrence of an event.
//   - Tentative is every opaque occurrence of a TENTATIVE event.
//   - Unavailable is the time inside an RFC 7953 availability's range that
//     none of its AVAILABLE periods offers. A calendar with no availability
//     has none: RFC 7953 section 4 reads it as available throughout.
//
// Transparent and cancelled occurrences block nothing. Priorities between
// overlapping availabilities are not weighed: offered by any is offered.
func (s *Store) FreeBusy(ctx context.Context, c *calendarv1.Calendar, window caldav.Period) (caldav.FreeBusy, error) {
	objs, err := s.Objects(ctx, lastSegment(c.GetName()))
	if err != nil {
		return caldav.FreeBusy{}, err
	}
	loc := caltime.Zone(c.GetTimeZone().GetId())
	var fb caldav.FreeBusy
	var ranges, offered []caldav.Period
	for _, o := range objs {
		if o.Availability != nil {
			r, free, err := availabilityPeriods(o.Availability, loc, window)
			if err != nil {
				return caldav.FreeBusy{}, err
			}
			ranges, offered = append(ranges, r...), append(offered, free...)
			continue
		}
		insts, err := instances(o.Event, o.Overrides, loc, window)
		if err != nil {
			return caldav.FreeBusy{}, err
		}
		for _, in := range insts {
			if in.GetTransparency() == eventv1.Transparency_TRANSPARENCY_TRANSPARENT ||
				in.GetConfirmation() == eventv1.Confirmation_CONFIRMATION_CANCELLED {
				continue
			}
			span, _, err := eventSpan(in, loc)
			if err != nil {
				return caldav.FreeBusy{}, err
			}
			if in.GetConfirmation() == eventv1.Confirmation_CONFIRMATION_TENTATIVE {
				fb.Tentative = append(fb.Tentative, span)
			} else {
				fb.Busy = append(fb.Busy, span)
			}
		}
	}
	fb.Busy = clip(merge(fb.Busy), window)
	fb.Tentative = clip(merge(fb.Tentative), window)
	fb.Unavailable = clip(subtract(merge(ranges), merge(offered)), window)
	return fb, nil
}

// availabilityPeriods returns a's range within window and the AVAILABLE
// occurrences inside it.
func availabilityPeriods(a *availabilityv1.Availability, loc *time.Location, window caldav.Period) (ranges, free []caldav.Period, err error) {
	r := window
	if a.GetStart() != nil {
		start, _, serr := caltime.Read(a.GetStart(), loc)
		if serr != nil {
			return nil, nil, serr
		}
		r.Start = later(r.Start, start)
		switch {
		case a.GetEnd() != nil:
			end, _, eerr := caltime.Read(a.GetEnd(), loc)
			if eerr != nil {
				return nil, nil, eerr
			}
			r.End = earlier(r.End, end)
		case a.GetDuration() != nil:
			r.End = earlier(r.End, start.Add(a.GetDuration().AsDuration()))
		}
	}
	if !r.End.After(r.Start) {
		return nil, nil, nil
	}
	for _, p := range a.GetAvailablePeriods() {
		e := &eventv1.Event{Name: a.GetName(), Start: eventTime(p.GetStart())}
		switch {
		case p.GetEnd() != nil:
			e.Ends = &eventv1.Event_End{End: eventTime(p.GetEnd())}
		case p.GetDuration() != nil:
			e.Ends = &eventv1.Event_Duration{Duration: p.GetDuration()}
		}
		if p.GetRecurrenceRule() != "" {
			if e.Recurrence, err = ical.DecodeRecurrence(p.GetRecurrenceRule()); err != nil {
				return nil, nil, err
			}
		}
		starts, dur, err := occurrences(e, nil, loc, r)
		if err != nil {
			return nil, nil, err
		}
		for _, t := range starts {
			free = append(free, caldav.Period{Start: t, End: t.Add(dur)})
		}
	}
	return []caldav.Period{r}, clip(free, r), nil
}

// eventTime carries an RFC 7953 time across to RFC 5545's message of the
// same shape.
func eventTime(t *availabilityv1.CalendarTime) *eventv1.CalendarTime {
	switch {
	case t.GetDate() != nil:
		return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_Date{Date: t.GetDate()}}
	case t.GetDateTime() != nil:
		return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_DateTime{DateTime: t.GetDateTime()}}
	}
	return nil
}

// merge sorts periods and joins those that overlap or touch.
func merge(ps []caldav.Period) []caldav.Period {
	ps = slices.Clone(ps)
	slices.SortFunc(ps, func(a, b caldav.Period) int { return a.Start.Compare(b.Start) })
	var out []caldav.Period
	for _, p := range ps {
		if n := len(out); n > 0 && !p.Start.After(out[n-1].End) {
			out[n-1].End = later(out[n-1].End, p.End)
			continue
		}
		out = append(out, p)
	}
	return out
}

// subtract returns the parts of merged periods a that no period of merged b
// covers.
func subtract(a, b []caldav.Period) []caldav.Period {
	var out []caldav.Period
	for _, p := range a {
		cur := p.Start
		for _, q := range b {
			if !q.End.After(cur) || !q.Start.Before(p.End) {
				continue
			}
			if q.Start.After(cur) {
				out = append(out, caldav.Period{Start: cur, End: q.Start})
			}
			cur = later(cur, q.End)
		}
		if cur.Before(p.End) {
			out = append(out, caldav.Period{Start: cur, End: p.End})
		}
	}
	return out
}

// clip cuts periods to window, dropping those left empty -- but keeping a
// zero-length one inside it.
func clip(ps []caldav.Period, window caldav.Period) []caldav.Period {
	var out []caldav.Period
	for _, p := range ps {
		c := caldav.Period{Start: later(p.Start, window.Start), End: earlier(p.End, window.End)}
		if c.End.After(c.Start) || (c.End.Equal(c.Start) && !c.Start.Before(window.Start) && c.Start.Before(window.End)) {
			out = append(out, c)
		}
	}
	return out
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
