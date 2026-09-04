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

	// Section 3.1 marks UID and DTSTAMP REQUIRED on VAVAILABILITY, and UID,
	// DTSTAMP and DTSTART REQUIRED on AVAILABLE, each "MUST NOT occur more
	// than once". Counting them per open component enforces both halves; a
	// missing one used to decode to a zero value and a repeated one used to
	// overwrite silently.
	//
	// DTSTAMP on AVAILABLE is the exception, and it is deliberate. The
	// grammar lists it REQUIRED, but not one of the AVAILABLE blocks in the
	// RFC's own section 8 examples carries it, while every VAVAILABILITY in
	// those examples does. Enforcing the grammar there would reject the
	// specification's sample data and the exporters written against it, so
	// AVAILABLE's DTSTAMP is held to "at most once" only.
	var availSeen, periodSeen map[string]int

	for _, raw := range raws {
		l, err := contentline.Parse(raw)
		if err != nil {
			return nil, err
		}
		switch l.Name {
		case "BEGIN":
			name := strings.ToUpper(l.Value)
			switch name {
			case "VAVAILABILITY":
				// Section 3.1 permits several per VCALENDAR, but this
				// function returns one Availability. Merging a second into
				// the first would silently blend two calendars' properties,
				// so it is refused rather than guessed at.
				if seen {
					return nil, fmt.Errorf("a second VAVAILABILITY: Decode returns a single availability, so a stream carrying more than one must be split first")
				}
				seen = true
				availSeen = map[string]int{}
			case "AVAILABLE":
				if current(stack) != "VAVAILABILITY" {
					return nil, fmt.Errorf("BEGIN:AVAILABLE inside %q; RFC 7953 section 3.1 nests AVAILABLE directly in VAVAILABILITY", current(stack))
				}
				period = &availabilityv1.AvailablePeriod{}
				periodSeen = map[string]int{}
			}
			stack = append(stack, name)
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
			switch {
			case name == "AVAILABLE" && period != nil:
				if err := requireOnce("AVAILABLE", periodSeen, "UID", "DTSTART"); err != nil {
					return nil, err
				}
				if err := atMostOnce("AVAILABLE", periodSeen, "DTSTAMP"); err != nil {
					return nil, err
				}
				a.AvailablePeriods = append(a.AvailablePeriods, period)
				period = nil
			case name == "VAVAILABILITY":
				if err := requireOnce("VAVAILABILITY", availSeen, "UID", "DTSTAMP"); err != nil {
					return nil, err
				}
			}
			continue
		}

		switch current(stack) {
		case "AVAILABLE":
			if period == nil {
				return nil, fmt.Errorf("AVAILABLE property outside an AVAILABLE component")
			}
			periodSeen[l.Name]++
			if err := decodePeriodProperty(period, l); err != nil {
				return nil, err
			}
		case "VAVAILABILITY":
			availSeen[l.Name]++
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

// requireOnce checks section 3.1's "REQUIRED but MUST NOT occur more than
// once" properties for one component.
func requireOnce(component string, seen map[string]int, names ...string) error {
	for _, n := range names {
		switch seen[n] {
		case 1:
		case 0:
			return fmt.Errorf("%s has no %s; RFC 7953 section 3.1 requires it", component, n)
		default:
			return fmt.Errorf("%s has %d %s properties; RFC 7953 section 3.1 allows at most one", component, seen[n], n)
		}
	}
	return nil
}

// atMostOnce checks only the uniqueness half, for a property this decoder does
// not insist on. See the note in Decode about AVAILABLE's DTSTAMP.
func atMostOnce(component string, seen map[string]int, names ...string) error {
	for _, n := range names {
		if seen[n] > 1 {
			return fmt.Errorf("%s has %d %s properties; RFC 7953 section 3.1 allows at most one", component, seen[n], n)
		}
	}
	return nil
}

// current is the innermost open component, or "" at the top level.
func current(stack []string) string {
	if len(stack) == 0 {
		return ""
	}
	return stack[len(stack)-1]
}

// decodeTime boxes a parsed DATE-TIME into this package's own CalendarTime.
// The primitives are shared; only this wrapper is duplicated, because AIP-215
// makes the type package-local.
//
// RFC 7953 section 3.1 <https://www.rfc-editor.org/rfc/rfc7953.html#section-3.1>
// is narrower than RFC 5545 here: "the 'DTSTART' and 'DTEND' properties in
// 'VAVAILABILITY' components and 'AVAILABLE' subcomponents MUST be 'DATE-TIME'
// values specified as either the date with UTC time or the date with local time
// and a time zone reference." So of RFC 5545's four forms only two are legal,
// and DATE and floating DATE-TIME are refused rather than decoded. A floating
// bound is not merely irregular: an availability window with no zone cannot be
// compared against a request, so accepting one produced a record that could not
// be evaluated.
func decodeTime(l contentline.Line) (*availabilityv1.CalendarTime, error) {
	v := l.Value

	if vals := l.Params["VALUE"]; len(vals) > 0 && !strings.EqualFold(vals[0], "DATE-TIME") {
		return nil, fmt.Errorf("VALUE=%s: RFC 7953 section 3.1 requires a DATE-TIME bound", vals[0])
	}
	if len(v) == 8 && !strings.Contains(v, "T") {
		return nil, fmt.Errorf("%q is a DATE; RFC 7953 section 3.1 requires a DATE-TIME bound", v)
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
	if dt.GetTimeOffset() == nil {
		return nil, fmt.Errorf("%q is a floating DATE-TIME; RFC 7953 section 3.1 requires UTC time or a TZID", v)
	}
	return &availabilityv1.CalendarTime{Value: &availabilityv1.CalendarTime_DateTime{DateTime: dt}}, nil
}

// encodeBound is encodeTime for a DTSTART or DTEND, refusing the two forms
// section 3.1 excludes rather than emitting a bound no conforming consumer
// could evaluate.
func encodeBound(prop string, t *availabilityv1.CalendarTime) (string, error) {
	dt, ok := t.GetValue().(*availabilityv1.CalendarTime_DateTime)
	if !ok {
		return "", fmt.Errorf("%s is a DATE; RFC 7953 section 3.1 requires a DATE-TIME bound", prop)
	}
	if dt.DateTime.GetTimeOffset() == nil {
		return "", fmt.Errorf("%s is a floating DATE-TIME; RFC 7953 section 3.1 requires UTC time or a TZID", prop)
	}
	v, p := encodeTime(t)
	return prop + p + ":" + v, nil
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
