package caltime

import (
	"errors"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/protobuf/types/known/durationpb"
)

// UTCStamp is the RFC 5545 section 3.3.5 form of a UTC DATE-TIME.
const UTCStamp = "20060102T150405Z"

// Value is what every schema's CalendarTime has in common: RFC 5545's
// eventv1.CalendarTime and RFC 7953's availabilityv1.CalendarTime are two
// messages of one shape.
type Value interface {
	GetDate() *date.Date
	GetDateTime() *datetime.DateTime
}

// ErrEmpty is a calendar time with neither a date nor a date_time.
var ErrEmpty = errors.New("caltime: a calendar time with neither date nor date_time")

// Form is how a stored time was written, so a time derived from it can be
// written the same way.
type Form struct {
	// Date is set for a DATE.
	Date bool
	// Loc is the zone the wall clock is read in.
	Loc *time.Location
	// Offset is set for a DATE-TIME given as a fixed UTC offset; Zone for one
	// given in an IANA zone. Neither: a floating time, read in the
	// calendar's zone.
	Offset *durationpb.Duration
	Zone   *datetime.TimeZone
}

// Read reads a stored time as an instant. A DATE is local midnight in the
// calendar's zone loc, and so is a floating DATE-TIME's wall clock.
func Read(ct Value, loc *time.Location) (time.Time, Form, error) {
	if d := ct.GetDate(); d != nil {
		return time.Date(int(d.GetYear()), time.Month(d.GetMonth()), int(d.GetDay()), 0, 0, 0, 0, loc),
			Form{Date: true, Loc: loc}, nil
	}
	dt := ct.GetDateTime()
	if dt == nil {
		return time.Time{}, Form{}, ErrEmpty
	}
	f := Form{Loc: loc}
	switch {
	case dt.GetTimeZone() != nil:
		f.Zone = dt.GetTimeZone()
		f.Loc = Zone(f.Zone.GetId())
	case dt.GetUtcOffset() != nil:
		f.Offset = dt.GetUtcOffset()
		f.Loc = time.FixedZone("", int(f.Offset.AsDuration()/time.Second))
	}
	return time.Date(int(dt.GetYear()), time.Month(dt.GetMonth()), int(dt.GetDay()),
		int(dt.GetHours()), int(dt.GetMinutes()), int(dt.GetSeconds()), int(dt.GetNanos()), f.Loc), f, nil
}

// Render writes instant t in form f.
func (f Form) Render(t time.Time) *eventv1.CalendarTime {
	l := t.In(f.Loc)
	if f.Date {
		return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_Date{Date: &date.Date{
			Year: int32(l.Year()), Month: int32(l.Month()), Day: int32(l.Day()),
		}}}
	}
	dt := &datetime.DateTime{
		Year: int32(l.Year()), Month: int32(l.Month()), Day: int32(l.Day()),
		Hours: int32(l.Hour()), Minutes: int32(l.Minute()), Seconds: int32(l.Second()), Nanos: int32(l.Nanosecond()),
	}
	switch {
	case f.Zone != nil:
		dt.TimeOffset = &datetime.DateTime_TimeZone{TimeZone: f.Zone}
	case f.Offset != nil:
		dt.TimeOffset = &datetime.DateTime_UtcOffset{UtcOffset: f.Offset}
	}
	return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_DateTime{DateTime: dt}}
}

// UTC is the form of a DATE-TIME written in UTC, "...Z".
func UTC() Form { return Form{Loc: time.UTC, Offset: &durationpb.Duration{}} }

// Key identifies the instant a time names, for matching an override to the
// instance it replaces. An unreadable time keys as zero.
func Key(ct Value, loc *time.Location) int64 {
	t, _, err := Read(ct, loc)
	if err != nil {
		return 0
	}
	return t.UnixNano()
}

// Zone loads an IANA zone, falling back to UTC: a bad zone shifts wall clocks
// by hours, and refusing to answer would be worse.
func Zone(name string) *time.Location {
	if name == "" {
		return time.UTC
	}
	if loc, err := time.LoadLocation(name); err == nil {
		return loc
	}
	return time.UTC
}
