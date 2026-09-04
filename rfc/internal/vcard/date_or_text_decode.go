// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/protobuf/types/known/durationpb"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// decodeDateOrText parses a BDAY or ANNIVERSARY value. See date_or_text.go
// for the scope this covers.
// The error is returned rather than swallowed: a malformed BDAY used to yield
// a nil DateOrText that decodeProperty assigned straight onto the Contact, so
// the birthday simply vanished and the vCard decoded "successfully".
func decodeDateOrText(l contentline.Line) (*vcardv1.DateOrText, error) {
	if vals := l.Params["VALUE"]; len(vals) > 0 && strings.EqualFold(vals[0], "text") {
		return &vcardv1.DateOrText{Value: &vcardv1.DateOrText_Text{Text: contentline.Unescape(l.Value)}}, nil
	}
	v := l.Value
	if strings.Contains(v, "T") {
		dt, err := parseDateOrTextDateTime(v)
		if err != nil {
			return nil, err
		}
		return &vcardv1.DateOrText{Value: &vcardv1.DateOrText_DateTime{DateTime: dt}}, nil
	}
	d, err := parsePartialDate(v)
	if err != nil {
		return nil, err
	}
	return &vcardv1.DateOrText{Value: &vcardv1.DateOrText_Date{Date: d}}, nil
}

// validPartial range-checks a reduced-accuracy date. A zero component means
// "not recorded" -- that is what makes "--0415", a birthday with no year,
// representable -- so only non-zero components are checked.
//
// Without this, "20241340" was eight numeric characters and decoded to a
// date.Date with month 13 and day 40.
func validPartial(d *date.Date) error {
	y, m, day := d.GetYear(), d.GetMonth(), d.GetDay()
	if m != 0 && (m < 1 || m > 12) {
		return fmt.Errorf("month %d is outside 1-12", m)
	}
	if day == 0 {
		return nil
	}
	// The day bound needs the month, and a leap day needs the year. With
	// neither recorded, 29 February is a legitimate date to hold, so the
	// limit falls back to the longest month.
	max := int32(31)
	switch {
	case m != 0 && y != 0:
		max = int32(daysInMonth(y, m))
	case m != 0:
		max = int32(daysInMonth(2000, m)) // a leap year, so 29 February passes
	}
	if day < 1 || day > max {
		return fmt.Errorf("day %d is outside 1-%d", day, max)
	}
	return nil
}

func daysInMonth(year, month int32) int {
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

// parsePartialDate is encodePartialDate's inverse: year [month day] /
// year "-" month / "--" month [day] / "--" "-" day.
func parsePartialDate(v string) (*date.Date, error) {
	d, err := parsePartialDateFields(v)
	if err != nil {
		return nil, err
	}
	if err := validPartial(d); err != nil {
		return nil, fmt.Errorf("date %q: %w", v, err)
	}
	return d, nil
}

func parsePartialDateFields(v string) (*date.Date, error) {
	switch {
	case strings.HasPrefix(v, "---") && len(v) == 5:
		day, err := strconv.ParseInt(v[3:5], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("date %q day is not numeric", v)
		}
		return &date.Date{Day: int32(day)}, nil
	case strings.HasPrefix(v, "--") && len(v) == 4:
		m, err := strconv.ParseInt(v[2:4], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("date %q month is not numeric", v)
		}
		return &date.Date{Month: int32(m)}, nil
	case strings.HasPrefix(v, "--") && len(v) == 6:
		m, err1 := strconv.ParseInt(v[2:4], 10, 32)
		d, err2 := strconv.ParseInt(v[4:6], 10, 32)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("date %q is not numeric", v)
		}
		return &date.Date{Month: int32(m), Day: int32(d)}, nil
	case len(v) == 4:
		y, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("date %q year is not numeric", v)
		}
		return &date.Date{Year: int32(y)}, nil
	case len(v) == 7 && v[4] == '-':
		y, err1 := strconv.ParseInt(v[0:4], 10, 32)
		m, err2 := strconv.ParseInt(v[5:7], 10, 32)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("date %q is not numeric", v)
		}
		return &date.Date{Year: int32(y), Month: int32(m)}, nil
	case len(v) == 8:
		y, err1 := strconv.ParseInt(v[0:4], 10, 32)
		m, err2 := strconv.ParseInt(v[4:6], 10, 32)
		d, err3 := strconv.ParseInt(v[6:8], 10, 32)
		if err1 != nil || err2 != nil || err3 != nil {
			return nil, fmt.Errorf("date %q is not numeric", v)
		}
		return &date.Date{Year: int32(y), Month: int32(m), Day: int32(d)}, nil
	}
	return nil, fmt.Errorf("date %q matches no section 4.3.1 form this schema parses", v)
}

// parseDateOrTextDateTime is encodeDateOrTextDateTime's inverse.
func parseDateOrTextDateTime(v string) (*datetime.DateTime, error) {
	parts := strings.SplitN(v, "T", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("date-time %q has no time part", v)
	}
	d, err := parsePartialDate(parts[0])
	if err != nil {
		return nil, err
	}
	t := parts[1]

	var offset time.Duration
	haveOffset := false
	switch {
	case strings.HasSuffix(t, "Z"):
		t = strings.TrimSuffix(t, "Z")
		haveOffset = true
	case len(t) > 6 && (t[6] == '+' || t[6] == '-'):
		o, err := parseUTCOffset(t[6:])
		if err != nil {
			return nil, err
		}
		offset, haveOffset = o, true
		t = t[:6]
	}
	if len(t) != 6 {
		return nil, fmt.Errorf("date-time time %q is not 6 characters", t)
	}
	h, err1 := strconv.ParseInt(t[0:2], 10, 32)
	mi, err2 := strconv.ParseInt(t[2:4], 10, 32)
	sec, err3 := strconv.ParseInt(t[4:6], 10, 32)
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, fmt.Errorf("date-time time %q is not numeric", t)
	}
	// Section 4.3.2's time is 00-23 / 00-59 / 00-60, the 60 being a leap
	// second.
	if h < 0 || h > 23 {
		return nil, fmt.Errorf("date-time %q has hour %d, outside 0-23", v, h)
	}
	if mi < 0 || mi > 59 {
		return nil, fmt.Errorf("date-time %q has minute %d, outside 0-59", v, mi)
	}
	if sec < 0 || sec > 60 {
		return nil, fmt.Errorf("date-time %q has second %d, outside 0-60", v, sec)
	}
	dt := &datetime.DateTime{
		Year: d.Year, Month: d.Month, Day: d.Day,
		Hours: int32(h), Minutes: int32(mi), Seconds: int32(sec),
	}
	if haveOffset {
		dt.TimeOffset = &datetime.DateTime_UtcOffset{UtcOffset: durationpb.New(offset)}
	}
	return dt, nil
}

// parseUTCOffset is formatUTCOffset's inverse.
//
// The sign is applied to the assembled duration rather than carried as a
// multiplier: multiplying two time.Duration values is a unit error -- the
// result would be duration-squared -- and it happens to work only because one
// side is 1 or -1.
func parseUTCOffset(s string) (time.Duration, error) {
	if len(s) != 3 && len(s) != 5 {
		return 0, fmt.Errorf("zone offset %q is not 3 or 5 characters", s)
	}
	negative := false
	switch s[0] {
	case '-':
		negative = true
	case '+':
	default:
		return 0, fmt.Errorf("zone offset %q has no sign", s)
	}
	h, err := strconv.ParseInt(s[1:3], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("zone offset %q hour is not numeric", s)
	}
	var m int64
	if len(s) == 5 {
		m, err = strconv.ParseInt(s[3:5], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("zone offset %q minute is not numeric", s)
		}
	}
	offset := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute
	if negative {
		offset = -offset
	}
	return offset, nil
}
