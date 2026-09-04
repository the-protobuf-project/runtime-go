// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"fmt"
	"time"

	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// Codec for DateOrText, BDAY and ANNIVERSARY's DATE-AND-OR-TIME value,
// section 4.3.4. Scope is the date forms DateOrText.date actually produces
// (full date, year-month, month-day, month-only, day-only, year-only) and a
// full date-time with a Z or numeric zone offset. Section 4.3.2's time-only
// forms ("T1022") are not produced or parsed: a birthday is never a bare
// time of day. This file encodes; date_or_text_decode.go is the inverse.

// encodeDateOrText renders the value and any VALUE parameter it needs.
func encodeDateOrText(d *vcardv1.DateOrText) (value, params string) {
	switch v := d.GetValue().(type) {
	case *vcardv1.DateOrText_Date:
		return encodePartialDate(v.Date), ""
	case *vcardv1.DateOrText_DateTime:
		return encodeDateOrTextDateTime(v.DateTime), ""
	case *vcardv1.DateOrText_Text:
		// Section 6.2.5 permits resetting the value to text; the parameter
		// says so, since the value alone is not distinguishable from a
		// malformed date.
		return contentline.Escape(v.Text), ";VALUE=text"
	}
	return "", ""
}

// encodePartialDate covers date's six reduced-accuracy forms, section 4.3.1.
// A zero component means "not recorded", which is what makes "--0415" (a
// birthday with no year) representable at all.
func encodePartialDate(d *date.Date) string {
	y, m, day := d.GetYear(), d.GetMonth(), d.GetDay()
	switch {
	case y > 0 && m > 0 && day > 0:
		return fmt.Sprintf("%04d%02d%02d", y, m, day)
	case y > 0 && m > 0:
		return fmt.Sprintf("%04d-%02d", y, m)
	case y > 0:
		return fmt.Sprintf("%04d", y)
	case m > 0 && day > 0:
		return fmt.Sprintf("--%02d%02d", m, day)
	case m > 0:
		return fmt.Sprintf("--%02d", m)
	case day > 0:
		return fmt.Sprintf("---%02d", day)
	}
	return ""
}

func encodeDateOrTextDateTime(dt *datetime.DateTime) string {
	s := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d",
		dt.GetYear(), dt.GetMonth(), dt.GetDay(),
		dt.GetHours(), dt.GetMinutes(), dt.GetSeconds())
	if off, ok := dt.GetTimeOffset().(*datetime.DateTime_UtcOffset); ok {
		d := off.UtcOffset.AsDuration()
		if d == 0 {
			return s + "Z"
		}
		return s + formatUTCOffset(d)
	}
	// A TimeZone-id offset and a floating value both lack section 4.3.4's
	// zone production, so both round-trip as a bare local date-time.
	return s
}

// formatUTCOffset renders a fixed offset in section 4.3.2's basic ISO 8601
// form, "+HHMM" / "-HHMM": no colon, no seconds.
func formatUTCOffset(d time.Duration) string {
	sign := "+"
	if d < 0 {
		sign = "-"
		d = -d
	}
	h := int(d / time.Hour)
	m := int((d % time.Hour) / time.Minute)
	return fmt.Sprintf("%s%02d%02d", sign, h, m)
}
