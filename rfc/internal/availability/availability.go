// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package availability

import (
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/type/datetime"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/icaltime"
)

// Decode parses a VCALENDAR containing one VAVAILABILITY, RFC 7953
// section 3.1 <https://www.rfc-editor.org/rfc/rfc7953.html#section-3.1>.
//
// The nesting is VCALENDAR > VAVAILABILITY > AVAILABLE, and the same component
// stack the event decoder uses applies here for the same reason: an AVAILABLE
// sub-component has its own DTSTART, SUMMARY and LOCATION, and reading them
// into the enclosing Availability would overwrite its. That bug has already
// been made once in this repository, with VALARM inside VEVENT.
func Decode(src string) (*availabilityv1.Availability, error) {
	raws := contentline.Unfold(src)
	if len(raws) == 0 {
		return nil, fmt.Errorf("empty calendar")
	}

	a := &availabilityv1.Availability{}
	var stack []string
	var period *availabilityv1.AvailablePeriod
	seen := false

	for _, raw := range raws {
		l, err := contentline.Parse(raw)
		if err != nil {
			return nil, err
		}
		switch l.Name {
		case "BEGIN":
			name := strings.ToUpper(l.Value)
			stack = append(stack, name)
			switch name {
			case "VAVAILABILITY":
				seen = true
			case "AVAILABLE":
				period = &availabilityv1.AvailablePeriod{}
			}
			continue
		case "END":
			if len(stack) == 0 {
				return nil, fmt.Errorf("END:%s without a matching BEGIN", l.Value)
			}
			name := strings.ToUpper(l.Value)
			if top := stack[len(stack)-1]; top != name {
				return nil, fmt.Errorf("END:%s closes BEGIN:%s", name, top)
			}
			stack = stack[:len(stack)-1]
			if name == "AVAILABLE" && period != nil {
				a.AvailablePeriods = append(a.AvailablePeriods, period)
				period = nil
			}
			continue
		}

		switch current(stack) {
		case "AVAILABLE":
			if period == nil {
				return nil, fmt.Errorf("AVAILABLE property outside an AVAILABLE component")
			}
			if err := decodePeriodProperty(period, l); err != nil {
				return nil, err
			}
		case "VAVAILABILITY":
			if err := decodeProperty(a, l); err != nil {
				return nil, err
			}
		default:
			// VCALENDAR-level properties describe the stream, not the
			// availability -- the same split the event codec makes.
		}
	}

	if !seen {
		return nil, fmt.Errorf("no VAVAILABILITY component found")
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("unclosed component %s", current(stack))
	}
	return a, nil
}

// current is the innermost open component, or "" at the top level.
func current(stack []string) string {
	if len(stack) == 0 {
		return ""
	}
	return stack[len(stack)-1]
}

// decodeTime boxes a parsed DATE or DATE-TIME into this package's own
// CalendarTime. The primitives are shared; only this wrapper is duplicated,
// because AIP-215 makes the type package-local.
func decodeTime(l contentline.Line) (*availabilityv1.CalendarTime, error) {
	v := l.Value

	if vals := l.Params["VALUE"]; len(vals) > 0 && strings.EqualFold(vals[0], "DATE") {
		d, err := icaltime.ParseDate(v)
		if err != nil {
			return nil, err
		}
		return &availabilityv1.CalendarTime{Value: &availabilityv1.CalendarTime_Date{Date: d}}, nil
	}
	if len(v) == 8 && !strings.Contains(v, "T") {
		d, err := icaltime.ParseDate(v)
		if err != nil {
			return nil, err
		}
		return &availabilityv1.CalendarTime{Value: &availabilityv1.CalendarTime_Date{Date: d}}, nil
	}

	dt, err := icaltime.ParseDateTime(v)
	if err != nil {
		return nil, err
	}
	if tz := l.Params["TZID"]; len(tz) > 0 {
		dt.TimeOffset = &datetime.DateTime_TimeZone{
			TimeZone: &datetime.TimeZone{Id: tz[0]},
		}
	}
	return &availabilityv1.CalendarTime{Value: &availabilityv1.CalendarTime_DateTime{DateTime: dt}}, nil
}

// encodeTime is the inverse, preserving which of the four forms was used.
func encodeTime(t *availabilityv1.CalendarTime) (value string, params string) {
	switch v := t.GetValue().(type) {
	case *availabilityv1.CalendarTime_Date:
		d := v.Date
		return fmt.Sprintf("%04d%02d%02d", d.GetYear(), d.GetMonth(), d.GetDay()), ";VALUE=DATE"
	case *availabilityv1.CalendarTime_DateTime:
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

// extensionOf preserves a property this schema does not model, RFC 5545
// sections 3.8.8.1 and 3.8.8.2, which RFC 7953 section 3.1 permits on both
// components.
func extensionOf(l contentline.Line) *availabilityv1.ExtensionProperty {
	e := &availabilityv1.ExtensionProperty{Key: l.RawName, Values: contentline.SplitList(l.Value)}
	if len(e.Values) == 0 && l.Value != "" {
		e.Values = []string{contentline.Unescape(l.Value)}
	}
	if len(l.Params) > 0 {
		e.Parameters = map[string]string{}
		for k, v := range l.Params {
			e.Parameters[k] = strings.Join(v, ",")
		}
	}
	return e
}
