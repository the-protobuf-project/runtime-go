// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"strings"

	"time"

	"google.golang.org/genproto/googleapis/type/datetime"

	"google.golang.org/protobuf/types/known/timestamppb"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/icaltime"
)

// decodeTime parses a DATE or DATE-TIME value into a CalendarTime.
//
// This is the function that justifies CalendarTime existing. RFC 5545
// section 3.3.5 gives DATE-TIME three forms and they are semantically
// different, not merely formatted differently:
//
//	19980118T230000        floating -- local time, no zone
//	19980119T070000Z       UTC
//	TZID=..:19980119T0700  local time in a named zone
//
// A google.protobuf.Timestamp can only hold the second. Parsing the other two
// into one would silently move an event by the zone offset, which is exactly
// the DST bug the schema change was made to prevent.
func decodeTime(l contentline.Line) (*eventv1.CalendarTime, error) {
	v := l.Value

	// VALUE=DATE, section 3.3.4: an all-day value.
	if vals := l.Params["VALUE"]; len(vals) > 0 && strings.EqualFold(vals[0], "DATE") {
		d, err := icaltime.ParseDate(v)
		if err != nil {
			return nil, err
		}
		return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_Date{Date: d}}, nil
	}
	// A bare 8-digit value is a DATE even without the parameter.
	if len(v) == 8 && !strings.ContainsAny(v, "T") {
		d, err := icaltime.ParseDate(v)
		if err != nil {
			return nil, err
		}
		return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_Date{Date: d}}, nil
	}

	dt, err := icaltime.ParseDateTime(v)
	if err != nil {
		return nil, err
	}
	// Form #3: TZID names a zone, section 3.2.19.
	if tz := l.Params["TZID"]; len(tz) > 0 {
		dt.TimeOffset = &datetime.DateTime_TimeZone{
			TimeZone: &datetime.TimeZone{Id: tz[0]},
		}
	}
	return &eventv1.CalendarTime{Value: &eventv1.CalendarTime_DateTime{DateTime: dt}}, nil
}

// encodeTime is the inverse, and preserves which of the three forms was used.
func encodeTime(t *eventv1.CalendarTime) (value string, params string) {
	switch v := t.GetValue().(type) {
	case *eventv1.CalendarTime_Date:
		d := v.Date
		return fmt.Sprintf("%04d%02d%02d", d.GetYear(), d.GetMonth(), d.GetDay()), ";VALUE=DATE"
	case *eventv1.CalendarTime_DateTime:
		dt := v.DateTime
		s := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d",
			dt.GetYear(), dt.GetMonth(), dt.GetDay(),
			dt.GetHours(), dt.GetMinutes(), dt.GetSeconds())
		switch off := dt.GetTimeOffset().(type) {
		case *datetime.DateTime_UtcOffset:
			return s + "Z", ""
		case *datetime.DateTime_TimeZone:
			return s, ";TZID=" + off.TimeZone.GetId()
		default:
			return s, "" // floating
		}
	}
	return "", ""
}

// timestampOf converts a parsed DATE-TIME to a Timestamp.
//
// Used only for an absolute alarm TRIGGER, which section 3.8.6.3 requires to
// be UTC. Everywhere else a calendar time is a CalendarTime, because
// everywhere else the other two forms are legal.
func timestampOf(dt *datetime.DateTime) *timestamppb.Timestamp {
	t := time.Date(
		int(dt.GetYear()), time.Month(dt.GetMonth()), int(dt.GetDay()),
		int(dt.GetHours()), int(dt.GetMinutes()), int(dt.GetSeconds()),
		int(dt.GetNanos()), time.UTC)
	return timestamppb.New(t)
}

// utcStamp renders a Timestamp in the basic UTC form section 3.3.5 requires.
func utcStamp(ts *timestamppb.Timestamp) string {
	return ts.AsTime().UTC().Format("20060102T150405Z")
}
