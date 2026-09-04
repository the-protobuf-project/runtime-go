// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// Decode parses a VCALENDAR containing one VEVENT, RFC 5545 section 3.4.
//
// Only the VEVENT is mapped: the enclosing VCALENDAR carries PRODID and
// VERSION, which describe the stream rather than the event and belong to the
// codec, not to the schema. That is the same reasoning that kept them off
// Calendar.
func Decode(src string) (*eventv1.Event, error) {
	raws := contentline.Unfold(src)
	if len(raws) == 0 {
		return nil, fmt.Errorf("empty calendar")
	}

	e := &eventv1.Event{}
	// A component stack, not a boolean. RFC 5545 section 3.6 nests
	// components: a VALARM lives inside a VEVENT, and its DESCRIPTION is the
	// alarm's, not the event's. Tracking only "am I inside a VEVENT" silently
	// overwrites the event's own properties with the alarm's -- which is
	// exactly what this decoder used to do.
	var stack []string
	var alarm *eventv1.Alarm
	// RFC 9073's components. `participant` doubles as the attachment target:
	// section 7.1 nests VLOCATION and VRESOURCE inside PARTICIPANT, so a
	// location closing while a participant is open belongs to that
	// participant, not to the event.
	var participant *eventv1.Participant
	var location *eventv1.Location
	var resource *eventv1.Resource
	seenEvent := false

	for _, raw := range raws {
		l, err := contentline.Parse(raw)
		if err != nil {
			return nil, err
		}
		switch l.Name {
		case "BEGIN":
			name := strings.ToUpper(l.Value)
			stack = append(stack, name)
			if name == "VEVENT" {
				seenEvent = true
			}
			switch name {
			case "VALARM":
				alarm = &eventv1.Alarm{}
			case "PARTICIPANT":
				participant = &eventv1.Participant{}
			case "VLOCATION":
				location = &eventv1.Location{}
			case "VRESOURCE":
				resource = &eventv1.Resource{}
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
			switch {
			case name == "VALARM" && alarm != nil:
				e.Alarms = append(e.Alarms, alarm)
				alarm = nil
			case name == "PARTICIPANT" && participant != nil:
				e.Participants = append(e.Participants, participant)
				participant = nil
			case name == "VLOCATION" && location != nil:
				if participant != nil {
					participant.StructuredLocations = append(participant.StructuredLocations, location)
				} else {
					e.StructuredLocations = append(e.StructuredLocations, location)
				}
				location = nil
			case name == "VRESOURCE" && resource != nil:
				if participant != nil {
					participant.Resources = append(participant.Resources, resource)
				} else {
					e.Resources = append(e.Resources, resource)
				}
				resource = nil
			}
			continue
		}

		switch current(stack) {
		case "VALARM":
			if alarm == nil {
				return nil, fmt.Errorf("alarm property outside a VALARM")
			}
			if err := decodeAlarmProperty(alarm, l); err != nil {
				return nil, err
			}
		case "PARTICIPANT":
			if participant == nil {
				return nil, fmt.Errorf("participant property outside a PARTICIPANT")
			}
			if err := decodeParticipantProperty(participant, l); err != nil {
				return nil, err
			}
		case "VLOCATION":
			if location == nil {
				return nil, fmt.Errorf("location property outside a VLOCATION")
			}
			if err := decodeLocationProperty(location, l); err != nil {
				return nil, err
			}
		case "VRESOURCE":
			if resource == nil {
				return nil, fmt.Errorf("resource property outside a VRESOURCE")
			}
			if err := decodeResourceProperty(resource, l); err != nil {
				return nil, err
			}
		case "VEVENT":
			if err := decodeProperty(e, l); err != nil {
				return nil, err
			}
		default:
			// VCALENDAR-level properties describe the stream, not the event.
		}
	}

	if !seenEvent {
		return nil, fmt.Errorf("no VEVENT component found")
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("unclosed component %s", current(stack))
	}
	if e.GetStart() == nil {
		return nil, fmt.Errorf("VEVENT has no DTSTART")
	}
	return e, nil
}

// decodeTimeList handles EXDATE and RDATE, which are comma-separated lists of
// DATE or DATE-TIME values sharing one set of parameters.
func decodeTimeList(l contentline.Line) ([]*eventv1.CalendarTime, error) {
	var out []*eventv1.CalendarTime
	for _, v := range contentline.SplitUnescaped(l.Value, ',') {
		one := l
		one.Value = strings.TrimSpace(v)
		if one.Value == "" {
			continue
		}
		t, err := decodeTime(one)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// extensionOf preserves a property this schema does not model, RFC 5545
// sections 3.8.8.1 and 3.8.8.2. Section 3.8.8.2 requires applications to
// ignore what they do not recognize, and ignoring is not the same as dropping.
func extensionOf(l contentline.Line) *eventv1.ExtensionProperty {
	e := &eventv1.ExtensionProperty{Key: l.RawName, Values: contentline.SplitList(l.Value)}
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

// current is the innermost open component, or "" at the top level.
func current(stack []string) string {
	if len(stack) == 0 {
		return ""
	}
	return stack[len(stack)-1]
}
