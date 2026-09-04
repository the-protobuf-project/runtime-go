// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"encoding/json"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
)

// DecodeJCal parses application/calendar+json into an Event.
func DecodeJCal(data []byte) (*eventv1.Event, error) {
	var doc []any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("jcal is not JSON: %w", err)
	}
	if len(doc) != 3 {
		return nil, fmt.Errorf("jcal component must be a 3-element array, got %d", len(doc))
	}
	if s, _ := doc[0].(string); s != "vcalendar" {
		return nil, fmt.Errorf("jcal root is %v, want \"vcalendar\"", doc[0])
	}
	subs, ok := doc[2].([]any)
	if !ok {
		return nil, fmt.Errorf("jcal subcomponents is not an array")
	}

	e := &eventv1.Event{}
	found := false
	for _, sub := range subs {
		comp, ok := sub.([]any)
		if !ok || len(comp) != 3 {
			return nil, fmt.Errorf("jcal subcomponent is not a 3-element array")
		}
		if name, _ := comp[0].(string); name != "vevent" {
			continue // VTIMEZONE and friends are not modeled here.
		}
		found = true
		props, ok := comp[1].([]any)
		if !ok {
			return nil, fmt.Errorf("vevent properties is not an array")
		}
		for _, rp := range props {
			l, err := jcalToLine(rp)
			if err != nil {
				return nil, err
			}
			if err := decodeProperty(e, l); err != nil {
				return nil, err
			}
		}
		// Sub-components live in the third element, section 3.3. Dispatching
		// on the name rather than looking only for VALARM is what lets RFC
		// 9073's components through; not reading one drops it while the
		// document itself stays correct, which is the failure mode the
		// cross-format test exists to catch.
		inner, _ := comp[2].([]any)
		if err := decodeJCalSubcomponents(e, nil, inner); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, fmt.Errorf("no vevent component found")
	}
	if e.GetStart() == nil {
		return nil, fmt.Errorf("vevent has no dtstart")
	}
	return e, nil
}

// decodeJCalSubcomponents reads a component's third element.
//
// `into` is the enclosing PARTICIPANT when there is one: RFC 9073 section 7.1
// nests VLOCATION and VRESOURCE inside PARTICIPANT, so a location found there
// belongs to the participant and not to the event. Passing nil means the
// event is the parent.
func decodeJCalSubcomponents(e *eventv1.Event, into *eventv1.Participant, subs []any) error {
	for _, sc := range subs {
		c, ok := sc.([]any)
		if !ok || len(c) != 3 {
			return fmt.Errorf("jcal subcomponent is not a 3-element array")
		}
		name, _ := c[0].(string)
		props, _ := c[1].([]any)
		nested, _ := c[2].([]any)

		switch name {
		case "valarm":
			alarm := &eventv1.Alarm{}
			if err := eachJCalProperty(props, func(l contentline.Line) error {
				return decodeAlarmProperty(alarm, l)
			}); err != nil {
				return err
			}
			e.Alarms = append(e.Alarms, alarm)

		case "participant":
			p := &eventv1.Participant{}
			if err := eachJCalProperty(props, func(l contentline.Line) error {
				return decodeParticipantProperty(p, l)
			}); err != nil {
				return err
			}
			// Recurse so the participant's own locations and resources land
			// on it rather than on the event.
			if err := decodeJCalSubcomponents(e, p, nested); err != nil {
				return err
			}
			e.Participants = append(e.Participants, p)

		case "vlocation":
			loc := &eventv1.Location{}
			if err := eachJCalProperty(props, func(l contentline.Line) error {
				return decodeLocationProperty(loc, l)
			}); err != nil {
				return err
			}
			if into != nil {
				into.StructuredLocations = append(into.StructuredLocations, loc)
			} else {
				e.StructuredLocations = append(e.StructuredLocations, loc)
			}

		case "vresource":
			r := &eventv1.Resource{}
			if err := eachJCalProperty(props, func(l contentline.Line) error {
				return decodeResourceProperty(r, l)
			}); err != nil {
				return err
			}
			if into != nil {
				into.Resources = append(into.Resources, r)
			} else {
				e.Resources = append(e.Resources, r)
			}
		}
		// An unmodelled component is skipped, as VTIMEZONE already is.
	}
	return nil
}

// eachJCalProperty converts each jCal property array to a content line and
// hands it to fn.
func eachJCalProperty(props []any, fn func(contentline.Line) error) error {
	for _, rp := range props {
		l, err := jcalToLine(rp)
		if err != nil {
			return err
		}
		if err := fn(l); err != nil {
			return err
		}
	}
	return nil
}
