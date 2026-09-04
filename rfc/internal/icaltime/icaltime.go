// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package icaltime

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"

	"google.golang.org/protobuf/types/known/durationpb"
)

// digits parses a fixed-width all-digit field. strconv.ParseInt alone is not
// enough: it accepts a leading sign, so "2024-101" would read as month -1.
func digits(v, field string) (int64, error) {
	for i := 0; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return 0, fmt.Errorf("%s %q is not numeric", field, v)
		}
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not numeric", field, v)
	}
	return n, nil
}

// daysIn is the length of a month, Gregorian and leap-year aware. RFC 5545
// section 3.3.4 defines DATE as a Gregorian calendar date, so 20240230 is not
// a date and must not decode to one.
func daysIn(year, month int64) int64 {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	}
	return 0
}

// ParseDate reads a DATE, RFC 5545 section 3.3.4
// <https://www.rfc-editor.org/rfc/rfc5545.html#section-3.3.4>.
//
// The components are range-checked against the calendar rather than only for
// width: "20241340" is eight numeric characters and is not a date, and letting
// it through produced a date.Date with month 13 that every consumer downstream
// would have to re-validate.
func ParseDate(v string) (*date.Date, error) {
	if len(v) != 8 {
		return nil, fmt.Errorf("DATE %q is not 8 characters", v)
	}
	y, err1 := digits(v[0:4], "DATE year")
	m, err2 := digits(v[4:6], "DATE month")
	d, err3 := digits(v[6:8], "DATE day")
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, fmt.Errorf("DATE %q is not numeric", v)
	}
	if y < 1 {
		return nil, fmt.Errorf("DATE %q has year %d, which is not a Gregorian year", v, y)
	}
	if m < 1 || m > 12 {
		return nil, fmt.Errorf("DATE %q has month %d, outside 1-12", v, m)
	}
	if max := daysIn(y, m); d < 1 || d > max {
		return nil, fmt.Errorf("DATE %q has day %d, outside 1-%d for month %d of %d", v, d, max, m, y)
	}
	return &date.Date{Year: int32(y), Month: int32(m), Day: int32(d)}, nil
}

func ParseDateTime(v string) (*datetime.DateTime, error) {
	utc := strings.HasSuffix(v, "Z")
	v = strings.TrimSuffix(v, "Z")

	parts := strings.SplitN(v, "T", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("DATE-TIME %q has no time part", v)
	}
	d, err := ParseDate(parts[0])
	if err != nil {
		return nil, err
	}
	t := parts[1]
	if len(t) != 6 {
		return nil, fmt.Errorf("DATE-TIME time %q is not 6 characters", t)
	}
	h, err1 := digits(t[0:2], "DATE-TIME hour")
	mi, err2 := digits(t[2:4], "DATE-TIME minute")
	sec, err3 := digits(t[4:6], "DATE-TIME second")
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, fmt.Errorf("DATE-TIME time %q is not numeric", t)
	}
	// Section 3.3.5's time is 00-23 / 00-59 / 00-60, the 60 being the leap
	// second the grammar allows.
	if h > 23 {
		return nil, fmt.Errorf("DATE-TIME %q has hour %d, outside 0-23", v, h)
	}
	if mi > 59 {
		return nil, fmt.Errorf("DATE-TIME %q has minute %d, outside 0-59", v, mi)
	}
	if sec > 60 {
		return nil, fmt.Errorf("DATE-TIME %q has second %d, outside 0-60", v, sec)
	}

	dt := &datetime.DateTime{
		Year: d.Year, Month: d.Month, Day: d.Day,
		Hours: int32(h), Minutes: int32(mi), Seconds: int32(sec),
	}
	if utc {
		// Form #2. An explicit zero offset is what makes this absolute;
		// leaving TimeOffset unset would mean floating, a different value.
		dt.TimeOffset = &datetime.DateTime_UtcOffset{
			UtcOffset: &durationpb.Duration{},
		}
	}
	return dt, nil
}

// parseDuration reads RFC 5545 section 3.3.6, "PnW" / "PnDTnHnMnS".
//
// Not Go's time.ParseDuration: the iCalendar grammar is ISO 8601-derived and
// has weeks and days, which Go's parser rejects outright.
func ParseDuration(v string) (*durationpb.Duration, error) {
	s := strings.TrimSpace(v)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	if !strings.HasPrefix(s, "P") {
		return nil, fmt.Errorf("DURATION %q does not start with P", v)
	}
	s = s[1:]

	// Section 3.3.6's grammar is a sequence, not a bag:
	//
	//	dur-value = ["+" / "-"] "P" (dur-date / dur-time / dur-week)
	//	dur-date  = dur-day [dur-time]
	//	dur-time  = "T" (dur-hour / dur-minute / dur-second)
	//	dur-hour  = 1*DIGIT "H" [dur-minute]
	//	...
	//
	// so each unit may appear at most once, they are ordered W|D then T then
	// H then M then S, weeks combine with nothing, and at least one component
	// must be present. Tracking the last unit seen enforces all of that;
	// summing into secs without it accepted "P1W1D", "P1D1D" and a bare "P".
	const (
		unitNone = iota
		unitWeek
		unitDay
		unitTime // the "T" separator itself
		unitHour
		unitMinute
		unitSecond
	)
	names := map[rune]int{'W': unitWeek, 'D': unitDay, 'H': unitHour, 'M': unitMinute, 'S': unitSecond}

	var secs int64
	last := unitNone
	inTime := false
	components := 0
	num := ""
	for _, r := range s {
		if r == 'T' {
			if inTime {
				return nil, fmt.Errorf("DURATION %q has more than one T", v)
			}
			if num != "" {
				return nil, fmt.Errorf("DURATION %q has a number with no unit before T", v)
			}
			if last == unitWeek {
				return nil, fmt.Errorf("DURATION %q combines weeks with a time part; section 3.3.6 makes dur-week exclusive", v)
			}
			inTime = true
			last = unitTime
			continue
		}
		if r >= '0' && r <= '9' {
			num += string(r)
			continue
		}
		unit, ok := names[r]
		if !ok {
			return nil, fmt.Errorf("DURATION %q has unknown unit %q", v, r)
		}
		if num == "" {
			return nil, fmt.Errorf("DURATION %q has a unit with no number", v)
		}
		n, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("DURATION %q has a unit with no number", v)
		}
		num = ""

		if unit <= last {
			return nil, fmt.Errorf("DURATION %q repeats or misorders unit %q; section 3.3.6 fixes the order as W|D T H M S", v, r)
		}
		switch unit {
		case unitWeek:
			if components > 0 {
				return nil, fmt.Errorf("DURATION %q combines weeks with other components; section 3.3.6 makes dur-week exclusive", v)
			}
			secs += n * 7 * 24 * 3600
		case unitDay:
			secs += n * 24 * 3600
		case unitHour, unitMinute, unitSecond:
			if !inTime {
				return nil, fmt.Errorf("DURATION %q has %q outside the time part; section 3.3.6 requires T first", v, r)
			}
			switch unit {
			case unitHour:
				secs += n * 3600
			case unitMinute:
				secs += n * 60
			default:
				secs += n
			}
		}
		last = unit
		components++
	}
	if num != "" {
		return nil, fmt.Errorf("DURATION %q ends with a number and no unit", v)
	}
	if components == 0 {
		return nil, fmt.Errorf("DURATION %q has no components; section 3.3.6 requires at least one", v)
	}
	if inTime && last == unitTime {
		return nil, fmt.Errorf("DURATION %q has a T with no time component", v)
	}
	if neg {
		secs = -secs
	}
	return &durationpb.Duration{Seconds: secs}, nil
}

// EncodeDuration writes a DURATION, RFC 5545 section 3.3.6
// <https://www.rfc-editor.org/rfc/rfc5545.html#section-3.3.6>.
//
// The grammar's smallest unit is the second, so a sub-second value has no
// representation. Rejecting it is the only honest option: rounding or
// truncating would silently move an alarm trigger, and emitting the whole
// seconds alone loses data the caller supplied.
func EncodeDuration(d *durationpb.Duration) (string, error) {
	if n := d.GetNanos(); n != 0 {
		return "", fmt.Errorf("DURATION has %dns; section 3.3.6 represents whole seconds only", n)
	}
	s := d.GetSeconds()
	sign := ""
	if s < 0 {
		sign, s = "-", -s
	}
	days, rem := s/86400, s%86400
	h, m, sec := rem/3600, (rem%3600)/60, rem%60

	var b strings.Builder
	b.WriteString(sign + "P")
	if days > 0 {
		fmt.Fprintf(&b, "%dD", days)
	}
	if h > 0 || m > 0 || sec > 0 || days == 0 {
		b.WriteString("T")
		if h > 0 {
			fmt.Fprintf(&b, "%dH", h)
		}
		if m > 0 {
			fmt.Fprintf(&b, "%dM", m)
		}
		if sec > 0 || (h == 0 && m == 0) {
			fmt.Fprintf(&b, "%dS", sec)
		}
	}
	return b.String(), nil
}
