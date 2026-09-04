// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"strconv"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/icaltime"
)

// The VEVENT property mapping. Every encoding reaches this: text/calendar
// through Decode, jCal through DecodeJCal.

func decodeProperty(e *eventv1.Event, l contentline.Line) error {
	switch l.Name {
	case "UID":
		e.IcalUid = contentline.Unescape(l.Value)
	case "SUMMARY":
		e.Summary = contentline.Unescape(l.Value)
	case "DESCRIPTION":
		e.Description = contentline.Unescape(l.Value)
	case "LOCATION":
		e.Location = contentline.Unescape(l.Value)
	case "DTSTART":
		t, err := decodeTime(l)
		if err != nil {
			return err
		}
		e.Start = t
	case "DTEND":
		if e.GetEnds() != nil {
			// Section 3.6.1 forbids both. A file carrying both is malformed
			// and must be rejected, not silently resolved one way.
			return fmt.Errorf("VEVENT has both DTEND and DURATION, which section 3.6.1 forbids")
		}
		t, err := decodeTime(l)
		if err != nil {
			return err
		}
		e.Ends = &eventv1.Event_End{End: t}
	case "DURATION":
		if e.GetEnds() != nil {
			return fmt.Errorf("VEVENT has both DTEND and DURATION, which section 3.6.1 forbids")
		}
		d, err := icaltime.ParseDuration(l.Value)
		if err != nil {
			return err
		}
		e.Ends = &eventv1.Event_Duration{Duration: d}
	case "STATUS":
		e.Confirmation = decodeConfirmation(l.Value)
	case "CLASS":
		e.Classification = decodeClassification(l.Value)
	case "TRANSP":
		e.Transparency = decodeTransparency(l.Value)
	case "SEQUENCE":
		n, err := strconv.ParseInt(strings.TrimSpace(l.Value), 10, 32)
		if err != nil {
			return fmt.Errorf("SEQUENCE %q is not a number", l.Value)
		}
		e.Sequence = int32(n)
	case "GEO":
		p, err := decodeGeo(l.Value)
		if err != nil {
			return err
		}
		e.Position = p
	case "RRULE":
		r, err := decodeRecurrence(l.Value)
		if err != nil {
			return err
		}
		e.Recurrence = r
	case "EXDATE":
		ts, err := decodeTimeList(l)
		if err != nil {
			return err
		}
		e.ExcludedTimes = append(e.ExcludedTimes, ts...)
	case "ORGANIZER":
		e.Organizer = decodeOrganizer(l)
	case "ATTENDEE":
		e.Attendees = append(e.Attendees, decodeAttendee(l))
	case "RDATE":
		ts, err := decodeTimeList(l)
		if err != nil {
			return err
		}
		e.AdditionalTimes = append(e.AdditionalTimes, ts...)
	// RFC 9253.
	case "CONCEPT":
		e.Concepts = append(e.Concepts, contentline.Unescape(l.Value))
	case "REFID":
		e.Refids = append(e.Refids, contentline.Unescape(l.Value))
	case "LINK":
		e.Links = append(e.Links, decodeLink(l))
	// RFC 9073's two content properties. Its three components are handled by
	// the component stack in Decode, not here.
	case "STYLED-DESCRIPTION":
		e.StyledDescriptions = append(e.StyledDescriptions, decodeStyledDescription(l))
	case "STRUCTURED-DATA":
		e.StructuredData = append(e.StructuredData, decodeStructuredData(l))
	default:
		e.Extensions = append(e.Extensions, extensionOf(l))
	}
	return nil
}

// decodeLink reads a LINK property, RFC 9253 section 8.2.
//
// The value type comes from VALUE, which section 8.2 requires. It defaults to
// URI, so an absent VALUE is not an error -- guessing from the string's shape
// would be, since a UID may itself look like a URI.
func decodeLink(l contentline.Line) *eventv1.Link {
	link := &eventv1.Link{
		FormatType:   firstParam(l, "FMTTYPE"),
		Label:        firstParam(l, "LABEL"),
		LanguageCode: firstParam(l, "LANGUAGE"),
		Relation:     firstParam(l, "LINKREL"),
	}
	v := contentline.Unescape(l.Value)
	switch strings.ToUpper(firstParam(l, "VALUE")) {
	case "UID":
		link.Value = &eventv1.Link_IcalUid{IcalUid: v}
	case "XML-REFERENCE":
		link.Value = &eventv1.Link_XmlReference{XmlReference: v}
	default:
		link.Value = &eventv1.Link_Uri{Uri: v}
	}
	return link
}

// firstParam returns a parameter's first value, or "" when it is absent.
func firstParam(l contentline.Line, name string) string {
	if v := l.Params[name]; len(v) > 0 {
		return v[0]
	}
	return ""
}
