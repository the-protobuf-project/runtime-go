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
func decodeDateOrText(l contentline.Line) *vcardv1.DateOrText {
	if vals := l.Params["VALUE"]; len(vals) > 0 && strings.EqualFold(vals[0], "text") {
		return &vcardv1.DateOrText{Value: &vcardv1.DateOrText_Text{Text: contentline.Unescape(l.Value)}}
	}
	v := l.Value
	if strings.Contains(v, "T") {
		if dt, err := parseDateOrTextDateTime(v); err == nil {
			return &vcardv1.DateOrText{Value: &vcardv1.DateOrText_DateTime{DateTime: dt}}
		}
		return nil
	}
	if d, err := parsePartialDate(v); err == nil {
		return &vcardv1.DateOrText{Value: &vcardv1.DateOrText_Date{Date: d}}
	}
	return nil
}

// parsePartialDate is encodePartialDate's inverse: year [month day] /
// year "-" month / "--" month [day] / "--" "-" day.
func parsePartialDate(v string) (*date.Date, error) {
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
