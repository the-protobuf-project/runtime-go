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

func ParseDate(v string) (*date.Date, error) {
	if len(v) != 8 {
		return nil, fmt.Errorf("DATE %q is not 8 characters", v)
	}
	y, err1 := strconv.ParseInt(v[0:4], 10, 32)
	m, err2 := strconv.ParseInt(v[4:6], 10, 32)
	d, err3 := strconv.ParseInt(v[6:8], 10, 32)
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, fmt.Errorf("DATE %q is not numeric", v)
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
	h, err1 := strconv.ParseInt(t[0:2], 10, 32)
	mi, err2 := strconv.ParseInt(t[2:4], 10, 32)
	sec, err3 := strconv.ParseInt(t[4:6], 10, 32)
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, fmt.Errorf("DATE-TIME time %q is not numeric", t)
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

	var secs int64
	inTime := false
	num := ""
	for _, r := range s {
		switch {
		case r == 'T':
			inTime = true
		case r >= '0' && r <= '9':
			num += string(r)
		default:
			n, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("DURATION %q has a unit with no number", v)
			}
			num = ""
			switch r {
			case 'W':
				secs += n * 7 * 24 * 3600
			case 'D':
				secs += n * 24 * 3600
			case 'H':
				if !inTime {
					return nil, fmt.Errorf("DURATION %q has H before T", v)
				}
				secs += n * 3600
			case 'M':
				if !inTime {
					return nil, fmt.Errorf("DURATION %q has M before T; months are not a duration here", v)
				}
				secs += n * 60
			case 'S':
				secs += n
			default:
				return nil, fmt.Errorf("DURATION %q has unknown unit %q", v, r)
			}
		}
	}
	if num != "" {
		return nil, fmt.Errorf("DURATION %q ends with a number and no unit", v)
	}
	if neg {
		secs = -secs
	}
	return &durationpb.Duration{Seconds: secs}, nil
}

func EncodeDuration(d *durationpb.Duration) string {
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
	return b.String()
}
