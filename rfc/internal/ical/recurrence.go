// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/type/dayofweek"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// RRULE, RFC 5545 section 3.3.10. This is what verifies that Recurrence
// models a real recurrence rule rather than a plausible-looking one.

var frequencies = map[string]eventv1.Frequency{
	"SECONDLY": eventv1.Frequency_FREQUENCY_SECONDLY,
	"MINUTELY": eventv1.Frequency_FREQUENCY_MINUTELY,
	"HOURLY":   eventv1.Frequency_FREQUENCY_HOURLY,
	"DAILY":    eventv1.Frequency_FREQUENCY_DAILY,
	"WEEKLY":   eventv1.Frequency_FREQUENCY_WEEKLY,
	"MONTHLY":  eventv1.Frequency_FREQUENCY_MONTHLY,
	"YEARLY":   eventv1.Frequency_FREQUENCY_YEARLY,
}

// The two-letter day abbreviations of section 3.3.10, mapped to
// google.type.DayOfWeek -- which is why the schema needed no Weekday enum.
var weekdays = map[string]dayofweek.DayOfWeek{
	"SU": dayofweek.DayOfWeek_SUNDAY,
	"MO": dayofweek.DayOfWeek_MONDAY,
	"TU": dayofweek.DayOfWeek_TUESDAY,
	"WE": dayofweek.DayOfWeek_WEDNESDAY,
	"TH": dayofweek.DayOfWeek_THURSDAY,
	"FR": dayofweek.DayOfWeek_FRIDAY,
	"SA": dayofweek.DayOfWeek_SATURDAY,
}

func decodeRecurrence(v string) (*eventv1.Recurrence, error) {
	r := &eventv1.Recurrence{}
	for _, part := range strings.Split(v, ";") {
		if part == "" {
			continue
		}
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("RRULE part %q has no value", part)
		}
		key = strings.ToUpper(strings.TrimSpace(key))

		switch key {
		case "FREQ":
			f, ok := frequencies[strings.ToUpper(val)]
			if !ok {
				return nil, fmt.Errorf("RRULE FREQ %q is not one of section 3.3.10's values", val)
			}
			r.Frequency = f
		case "UNTIL":
			// Section 3.3.10: UNTIL takes DTSTART's value type, and a
			// DATE-TIME here MUST be UTC.
			t, err := decodeTime(contentline.Line{Value: val, Params: map[string][]string{}})
			if err != nil {
				return nil, fmt.Errorf("RRULE UNTIL: %w", err)
			}
			r.Bound = &eventv1.Recurrence_Until{Until: t}
		case "COUNT":
			n, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("RRULE COUNT %q is not a number", val)
			}
			r.Bound = &eventv1.Recurrence_Count{Count: int32(n)}
		case "INTERVAL":
			n, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("RRULE INTERVAL %q is not a number", val)
			}
			r.Interval = int32(n)
		case "WKST":
			d, ok := weekdays[strings.ToUpper(val)]
			if !ok {
				return nil, fmt.Errorf("RRULE WKST %q is not a weekday", val)
			}
			r.WeekStart = d
		case "BYDAY":
			for _, one := range strings.Split(val, ",") {
				wd, err := parseWeekdayNum(one)
				if err != nil {
					return nil, err
				}
				r.Weekdays = append(r.Weekdays, wd)
			}
		// RFC 7529 section 4.
		case "RSCALE":
			// Section 4: names are case insensitive, uppercase preferred.
			r.Rscale = strings.ToUpper(strings.TrimSpace(val))
		case "SKIP":
			switch strings.ToUpper(strings.TrimSpace(val)) {
			case "OMIT":
				r.Skip = eventv1.RecurrenceSkip_RECURRENCE_SKIP_OMIT
			case "BACKWARD":
				r.Skip = eventv1.RecurrenceSkip_RECURRENCE_SKIP_BACKWARD
			case "FORWARD":
				r.Skip = eventv1.RecurrenceSkip_RECURRENCE_SKIP_FORWARD
			default:
				return nil, fmt.Errorf("RRULE SKIP %q is not one of section 4's values", val)
			}
		case "BYMONTH":
			// Handled here rather than in the numeric default below because
			// RFC 7529 section 4 lets a month number carry an "L" suffix for
			// a leap month, which is not an integer and made intList fail.
			for _, one := range strings.Split(val, ",") {
				one = strings.TrimSpace(one)
				leap := false
				if u := strings.ToUpper(one); strings.HasSuffix(u, "L") {
					leap, one = true, one[:len(one)-1]
				}
				n, err := strconv.ParseInt(one, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("RRULE BYMONTH %q is not a month number", one)
				}
				if leap {
					r.LeapMonths = append(r.LeapMonths, monthOf(int32(n)))
				} else {
					r.Months = append(r.Months, monthOf(int32(n)))
				}
			}
		default:
			ns, err := intList(val)
			if err != nil {
				return nil, fmt.Errorf("RRULE %s: %w", key, err)
			}
			switch key {
			case "BYSECOND":
				r.SecondNumbers = ns
			case "BYMINUTE":
				r.MinuteNumbers = ns
			case "BYHOUR":
				r.HourNumbers = ns
			case "BYMONTHDAY":
				r.MonthDays = ns
			case "BYYEARDAY":
				r.YearDays = ns
			case "BYWEEKNO":
				r.WeekNumbers = ns
			case "BYSETPOS":
				r.SetPositions = ns
			default:
				return nil, fmt.Errorf("RRULE has unknown part %q", key)
			}
		}
	}
	if r.GetFrequency() == eventv1.Frequency_FREQUENCY_UNSPECIFIED {
		return nil, fmt.Errorf("RRULE has no FREQ, which section 3.3.10 requires")
	}
	return r, nil
}

// parseWeekdayNum reads one BYDAY entry: an optional signed ordinal followed
// by a two-letter day, e.g. "-1FR" for the last Friday.
func parseWeekdayNum(s string) (*eventv1.WeekdayNum, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if len(s) < 2 {
		return nil, fmt.Errorf("BYDAY entry %q is too short", s)
	}
	day, ok := weekdays[s[len(s)-2:]]
	if !ok {
		return nil, fmt.Errorf("BYDAY entry %q does not end in a weekday", s)
	}
	wd := &eventv1.WeekdayNum{Day: day}
	if ord := s[:len(s)-2]; ord != "" {
		n, err := strconv.ParseInt(ord, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("BYDAY ordinal %q is not a number", ord)
		}
		wd.Ordinal = int32(n)
	}
	return wd, nil
}

func intList(v string) ([]int32, error) {
	var out []int32
	for _, s := range strings.Split(v, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", s)
		}
		out = append(out, int32(n))
	}
	return out, nil
}
