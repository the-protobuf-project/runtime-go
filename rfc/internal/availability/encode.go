// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package availability

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

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

func contentLines(a *availabilityv1.Availability) ([]string, error) {
	if a.GetIcalUid() == "" {
		return nil, fmt.Errorf("availability has no ical_uid; RFC 7953 section 3.1 requires UID")
	}

	var out []string
	w := func(s string) { out = append(out, s) }

	w("BEGIN:VCALENDAR")
	w("VERSION:2.0")
	w("PRODID:-//The Protobuf Project//runtime-go//EN")
	w("BEGIN:VAVAILABILITY")
	w("UID:" + contentline.Escape(a.GetIcalUid()))

	if s := encodeBusyType(a.GetBusyType()); s != "" {
		w("BUSYTYPE:" + s)
	}
	if t := a.GetStart(); t != nil {
		v, p := encodeTime(t)
		w("DTSTART" + p + ":" + v)
	}
	switch e := a.GetEndForm().(type) {
	case *availabilityv1.Availability_End:
		v, p := encodeTime(e.End)
		w("DTEND" + p + ":" + v)
	case *availabilityv1.Availability_Duration:
		w("DURATION:" + icaltime.EncodeDuration(e.Duration))
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
		w(encodeExtension(e))
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

	var out []string
	w := func(s string) { out = append(out, s) }

	w("BEGIN:AVAILABLE")
	if v := p.GetIcalUid(); v != "" {
		w("UID:" + contentline.Escape(v))
	}
	v, prm := encodeTime(p.GetStart())
	w("DTSTART" + prm + ":" + v)

	switch e := p.GetEndForm().(type) {
	case *availabilityv1.AvailablePeriod_End:
		v, prm := encodeTime(e.End)
		w("DTEND" + prm + ":" + v)
	case *availabilityv1.AvailablePeriod_Duration:
		w("DURATION:" + icaltime.EncodeDuration(e.Duration))
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
		w(encodeExtension(e))
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

func encodeExtension(e *availabilityv1.ExtensionProperty) string {
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
		b.WriteString(";" + k + "=" + e.GetParameters()[k])
	}
	b.WriteByte(':')
	b.WriteString(contentline.JoinList(e.GetValues()))
	return b.String()
}
