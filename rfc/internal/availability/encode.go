// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package availability

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/type/datetime"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/icaltime"
)

// Encode serializes an Availability as a VCALENDAR containing one
// VAVAILABILITY, RFC 7953 section 3.1.
func Encode(a *availabilityv1.Availability) (string, error) {
	lines, err := contentLines(a)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, l := range lines {
		b.WriteString(contentline.Fold(l))
		b.WriteString("\r\n")
	}
	return b.String(), nil
}

// reserved names the encoder emits from modeled fields. An extension
// carrying one of them would produce a second copy of a property section 3.1
// allows at most once, so it is refused rather than written out.
var reserved = map[string]bool{
	"UID": true, "DTSTAMP": true, "DTSTART": true, "DTEND": true, "DURATION": true,
}

// dtstampOf pulls DTSTAMP out of the preserved extensions.
//
// The schema has no field for it -- it stamps the iCalendar object, not the
// availability -- so the decoder leaves it among the extensions and this reads
// it back. RFC 5545 section 3.8.7.2
// <https://www.rfc-editor.org/rfc/rfc5545.html#section-3.8.7.2> fixes its value
// as a UTC DATE-TIME, so a preserved one is validated before being written back
// out rather than trusted.
//
// required says whether absence is an error. It is on VAVAILABILITY and is not
// on AVAILABLE; see the note in Decode.
func dtstampOf(component string, required bool, exts []*availabilityv1.ExtensionProperty) (string, error) {
	var found string
	n := 0
	for _, e := range exts {
		if !strings.EqualFold(e.GetKey(), "DTSTAMP") {
			continue
		}
		n++
		if vs := e.GetValues(); len(vs) > 0 {
			found = vs[0]
		}
	}
	switch {
	case n == 0:
		if required {
			return "", fmt.Errorf("%s has no DTSTAMP; RFC 7953 section 3.1 requires it", component)
		}
		return "", nil
	case n > 1:
		return "", fmt.Errorf("%s has %d DTSTAMP properties; RFC 7953 section 3.1 allows at most one", component, n)
	}
	dt, err := icaltime.ParseDateTime(found)
	if err != nil {
		return "", fmt.Errorf("%s DTSTAMP %q: %w", component, found, err)
	}
	if _, utc := dt.GetTimeOffset().(*datetime.DateTime_UtcOffset); !utc {
		return "", fmt.Errorf("%s DTSTAMP %q is not UTC; RFC 5545 section 3.8.7.2 requires a UTC DATE-TIME", component, found)
	}
	return found, nil
}

func contentLines(a *availabilityv1.Availability) ([]string, error) {
	if a.GetIcalUid() == "" {
		return nil, fmt.Errorf("availability has no ical_uid; RFC 7953 section 3.1 requires UID")
	}
	stamp, err := dtstampOf("VAVAILABILITY", true, a.GetExtensions())
	if err != nil {
		return nil, err
	}

	var out []string
	w := func(s string) { out = append(out, s) }

	w("BEGIN:VCALENDAR")
	w("VERSION:2.0")
	w("PRODID:-//The Protobuf Project//runtime-go//EN")
	w("BEGIN:VAVAILABILITY")
	w("UID:" + contentline.Escape(a.GetIcalUid()))
	w("DTSTAMP:" + stamp)

	if s := encodeBusyType(a.GetBusyType()); s != "" {
		w("BUSYTYPE:" + s)
	}
	if t := a.GetStart(); t != nil {
		l, err := encodeBound("DTSTART", t)
		if err != nil {
			return nil, err
		}
		w(l)
	}
	switch e := a.GetEndForm().(type) {
	case *availabilityv1.Availability_End:
		l, err := encodeBound("DTEND", e.End)
		if err != nil {
			return nil, err
		}
		w(l)
	case *availabilityv1.Availability_Duration:
		dur, err := icaltime.EncodeDuration(e.Duration)
		if err != nil {
			return nil, err
		}
		w("DURATION:" + dur)
	}
	if v := a.GetSummary(); v != "" {
		w("SUMMARY:" + contentline.Escape(v))
	}
	if v := a.GetDescription(); v != "" {
		w("DESCRIPTION:" + contentline.Escape(v))
	}
	if v := a.GetOrganizer(); v != "" {
		w("ORGANIZER:" + v)
	}
	// Section 3.1 makes 1 the highest priority and 9 the lowest; 0 means
	// undefined, so it is omitted rather than written as a value.
	if n := a.GetPriority(); n > 0 {
		w("PRIORITY:" + strconv.Itoa(int(n)))
	}
	if v := a.GetCategories(); len(v) > 0 {
		w("CATEGORIES:" + contentline.JoinList(v))
	}
	for _, e := range a.GetExtensions() {
		enc, err := encodeExtension(e)
		if err != nil {
			return nil, err
		}
		if enc == "" {
			continue // DTSTAMP, already written above from the modeled path
		}
		w(enc)
	}

	for _, p := range a.GetAvailablePeriods() {
		lines, err := periodLines(p)
		if err != nil {
			return nil, err
		}
		out = append(out, lines...)
	}

	w("END:VAVAILABILITY")
	w("END:VCALENDAR")
	return out, nil
}

func periodLines(p *availabilityv1.AvailablePeriod) ([]string, error) {
	if p.GetStart() == nil {
		return nil, fmt.Errorf("available period has no start; RFC 7953 section 3.1 requires DTSTART")
	}
	if p.GetIcalUid() == "" {
		return nil, fmt.Errorf("available period has no ical_uid; RFC 7953 section 3.1 requires UID")
	}
	stamp, err := dtstampOf("AVAILABLE", false, p.GetExtensions())
	if err != nil {
		return nil, err
	}

	var out []string
	w := func(s string) { out = append(out, s) }

	w("BEGIN:AVAILABLE")
	w("UID:" + contentline.Escape(p.GetIcalUid()))
	if stamp != "" {
		w("DTSTAMP:" + stamp)
	}
	start, err := encodeBound("DTSTART", p.GetStart())
	if err != nil {
		return nil, err
	}
	w(start)

	switch e := p.GetEndForm().(type) {
	case *availabilityv1.AvailablePeriod_End:
		l, err := encodeBound("DTEND", e.End)
		if err != nil {
			return nil, err
		}
		w(l)
	case *availabilityv1.AvailablePeriod_Duration:
		dur, err := icaltime.EncodeDuration(e.Duration)
		if err != nil {
			return nil, err
		}
		w("DURATION:" + dur)
	}
	if v := p.GetSummary(); v != "" {
		w("SUMMARY:" + contentline.Escape(v))
	}
	if v := p.GetDescription(); v != "" {
		w("DESCRIPTION:" + contentline.Escape(v))
	}
	if v := p.GetLocation(); v != "" {
		w("LOCATION:" + contentline.Escape(v))
	}
	if v := p.GetRecurrenceRule(); v != "" {
		w("RRULE:" + v)
	}
	if v := p.GetCategories(); len(v) > 0 {
		w("CATEGORIES:" + contentline.JoinList(v))
	}
	for _, c := range p.GetComments() {
		w("COMMENT:" + contentline.Escape(c))
	}
	for _, e := range p.GetExtensions() {
		enc, err := encodeExtension(e)
		if err != nil {
			return nil, err
		}
		if enc == "" {
			continue // DTSTAMP, already written above from the modeled path
		}
		w(enc)
	}
	w("END:AVAILABLE")
	return out, nil
}

func encodeBusyType(b availabilityv1.BusyType) string {
	switch b {
	case availabilityv1.BusyType_BUSY_TYPE_BUSY:
		return "BUSY"
	case availabilityv1.BusyType_BUSY_TYPE_BUSY_UNAVAILABLE:
		return "BUSY-UNAVAILABLE"
	case availabilityv1.BusyType_BUSY_TYPE_BUSY_TENTATIVE:
		return "BUSY-TENTATIVE"
	}
	return ""
}

// encodeExtension writes one preserved property, or "" for the DTSTAMP the
// caller has already emitted from the modeled path.
func encodeExtension(e *availabilityv1.ExtensionProperty) (string, error) {
	key := strings.ToUpper(e.GetKey())
	if key == "DTSTAMP" {
		return "", nil
	}
	if reserved[key] {
		return "", fmt.Errorf("extension %s duplicates a property the encoder writes from a modeled field; RFC 7953 section 3.1 allows it at most once", e.GetKey())
	}

	var b strings.Builder
	b.WriteString(e.GetKey())

	// Sorted so output is deterministic; Go map order is not. The same guard
	// the ical and vcard encoders carry, for the same reason.
	keys := make([]string, 0, len(e.GetParameters()))
	for k := range e.GetParameters() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		esc, err := contentline.EscapeParam(e.GetParameters()[k])
		if err != nil {
			return "", fmt.Errorf("%s parameter %s: %w", e.GetKey(), k, err)
		}
		b.WriteString(";" + k + "=" + esc)
	}
	b.WriteByte(':')
	b.WriteString(contentline.JoinList(e.GetValues()))
	return b.String(), nil
}
