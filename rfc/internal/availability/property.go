// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package availability

import (
	"fmt"
	"strconv"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/icaltime"
)

// decodeProperty reads one VAVAILABILITY property, RFC 7953 section 3.1.
func decodeProperty(a *availabilityv1.Availability, l contentline.Line) error {
	switch l.Name {
	case "UID":
		a.IcalUid = contentline.Unescape(l.Value)
	case "BUSYTYPE":
		// Section 3.2. An unrecognized token leaves the field unset rather
		// than guessing; the default is BUSY-UNAVAILABLE, which is what an
		// absent BUSYTYPE means and what the enum's zero value documents.
		switch strings.ToUpper(strings.TrimSpace(l.Value)) {
		case "BUSY":
			a.BusyType = availabilityv1.BusyType_BUSY_TYPE_BUSY
		case "BUSY-UNAVAILABLE":
			a.BusyType = availabilityv1.BusyType_BUSY_TYPE_BUSY_UNAVAILABLE
		case "BUSY-TENTATIVE":
			a.BusyType = availabilityv1.BusyType_BUSY_TYPE_BUSY_TENTATIVE
		}
	case "DTSTART":
		t, err := decodeTime(l)
		if err != nil {
			return fmt.Errorf("DTSTART: %w", err)
		}
		a.Start = t
	case "DTEND":
		// Section 3.1: "'dtend' and 'duration' MUST NOT occur in the same
		// 'availabilityprop'". Overwriting whichever came first would take the
		// last one silently and make an invalid stream decode clean.
		if a.GetEndForm() != nil {
			return fmt.Errorf("DTEND after an end form is already set; RFC 7953 section 3.1 permits one of DTEND or DURATION")
		}
		t, err := decodeTime(l)
		if err != nil {
			return fmt.Errorf("DTEND: %w", err)
		}
		a.EndForm = &availabilityv1.Availability_End{End: t}
	case "DURATION":
		if a.GetEndForm() != nil {
			return fmt.Errorf("DURATION after an end form is already set; RFC 7953 section 3.1 permits one of DTEND or DURATION")
		}
		d, err := icaltime.ParseDuration(l.Value)
		if err != nil {
			return err
		}
		a.EndForm = &availabilityv1.Availability_Duration{Duration: d}
	case "SUMMARY":
		a.Summary = contentline.Unescape(l.Value)
	case "DESCRIPTION":
		a.Description = contentline.Unescape(l.Value)
	case "ORGANIZER":
		a.Organizer = contentline.Unescape(l.Value)
	case "PRIORITY":
		n, err := strconv.ParseInt(strings.TrimSpace(l.Value), 10, 32)
		if err != nil {
			return fmt.Errorf("PRIORITY %q is not a number", l.Value)
		}
		a.Priority = int32(n)
	case "CATEGORIES":
		a.Categories = append(a.Categories, contentline.SplitList(l.Value)...)
	default:
		a.Extensions = append(a.Extensions, extensionOf(l))
	}
	return nil
}

// decodePeriodProperty reads one AVAILABLE property, RFC 7953 section 3.1.
//
// Deliberately a separate function from decodeProperty even though the two
// overlap: the components accept different property sets, and sharing one
// switch is how an AVAILABLE's SUMMARY ends up on its parent.
func decodePeriodProperty(p *availabilityv1.AvailablePeriod, l contentline.Line) error {
	switch l.Name {
	case "UID":
		p.IcalUid = contentline.Unescape(l.Value)
	case "DTSTART":
		t, err := decodeTime(l)
		if err != nil {
			return fmt.Errorf("DTSTART: %w", err)
		}
		p.Start = t
	case "DTEND":
		if p.GetEndForm() != nil {
			return fmt.Errorf("DTEND after an end form is already set; RFC 7953 section 3.1 permits one of DTEND or DURATION")
		}
		t, err := decodeTime(l)
		if err != nil {
			return fmt.Errorf("DTEND: %w", err)
		}
		p.EndForm = &availabilityv1.AvailablePeriod_End{End: t}
	case "DURATION":
		if p.GetEndForm() != nil {
			return fmt.Errorf("DURATION after an end form is already set; RFC 7953 section 3.1 permits one of DTEND or DURATION")
		}
		d, err := icaltime.ParseDuration(l.Value)
		if err != nil {
			return err
		}
		p.EndForm = &availabilityv1.AvailablePeriod_Duration{Duration: d}
	case "SUMMARY":
		p.Summary = contentline.Unescape(l.Value)
	case "DESCRIPTION":
		p.Description = contentline.Unescape(l.Value)
	case "LOCATION":
		p.Location = contentline.Unescape(l.Value)
	case "RRULE":
		// Kept as the raw rule text: the schema types this as a string
		// rather than copying the 160-line Recurrence message across the
		// AIP-215 boundary. See AvailablePeriod.recurrence_rule.
		p.RecurrenceRule = l.Value
	case "CATEGORIES":
		p.Categories = append(p.Categories, contentline.SplitList(l.Value)...)
	case "COMMENT":
		p.Comments = append(p.Comments, contentline.Unescape(l.Value))
	default:
		p.Extensions = append(p.Extensions, extensionOf(l))
	}
	return nil
}
