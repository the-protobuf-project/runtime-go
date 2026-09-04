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

// VALARM, RFC 5545 section 3.6.6.
//
// A sub-component of VEVENT, not a component of its own, which is why the
// schema makes it a value object with no identity. Its properties are read
// into the enclosing Alarm rather than the Event -- decoding them into the
// Event would overwrite the event's own DESCRIPTION with the reminder text.

func decodeAlarmProperty(a *eventv1.Alarm, l contentline.Line) error {
	switch l.Name {
	case "ACTION":
		switch strings.ToUpper(strings.TrimSpace(l.Value)) {
		case "AUDIO":
			a.Action = eventv1.AlarmAction_ALARM_ACTION_AUDIO
		case "DISPLAY":
			a.Action = eventv1.AlarmAction_ALARM_ACTION_DISPLAY
		case "EMAIL":
			a.Action = eventv1.AlarmAction_ALARM_ACTION_EMAIL
		default:
			// RFC 2445 also had PROCEDURE; RFC 5545 dropped it and the schema
			// does not model icaltime.
			return fmt.Errorf("ACTION %q is not one of section 3.8.6.1's values", l.Value)
		}
	case "TRIGGER":
		return decodeTrigger(a, l)
	case "DURATION":
		d, err := icaltime.ParseDuration(l.Value)
		if err != nil {
			return err
		}
		a.RepeatInterval = d
	case "REPEAT":
		n, err := strconv.ParseInt(strings.TrimSpace(l.Value), 10, 32)
		if err != nil {
			return fmt.Errorf("REPEAT %q is not a number", l.Value)
		}
		a.RepeatCount = int32(n)
	case "DESCRIPTION":
		a.Description = contentline.Unescape(l.Value)
	case "SUMMARY":
		a.Summary = contentline.Unescape(l.Value)
	case "ATTENDEE":
		a.Attendees = append(a.Attendees, decodeAttendee(l))
	// RFC 9074. Before these, an alarm had no identity, so nothing could be
	// acknowledged or snoozed across devices.
	case "UID":
		a.IcalUid = contentline.Unescape(l.Value)
	case "ACKNOWLEDGED":
		// Section 6.1 fixes the value as a UTC DATE-TIME, so the trailing Z
		// is required rather than optional -- the same shape an absolute
		// TRIGGER takes, and parsed the same way.
		dt, err := icaltime.ParseDateTime(strings.TrimSuffix(l.Value, "Z"))
		if err != nil {
			return fmt.Errorf("ACKNOWLEDGED: %w", err)
		}
		a.AcknowledgedTime = timestampOf(dt)
	case "PROXIMITY":
		switch strings.ToUpper(strings.TrimSpace(l.Value)) {
		case "ARRIVE":
			a.Proximity = eventv1.Proximity_PROXIMITY_ARRIVE
		case "DEPART":
			a.Proximity = eventv1.Proximity_PROXIMITY_DEPART
		case "CONNECT":
			a.Proximity = eventv1.Proximity_PROXIMITY_CONNECT
		case "DISCONNECT":
			a.Proximity = eventv1.Proximity_PROXIMITY_DISCONNECT
		}
		// Section 8.1 permits iana-token and x-name. An unrecognized value
		// leaves proximity unset rather than guessing a trigger the alarm
		// did not ask for.
	case "RELATED-TO":
		// Only RELTYPE=SNOOZE is modeled, section 7.1. A general
		// relationship list is RFC 9253's job and belongs on the component.
		for _, rt := range l.Params["RELTYPE"] {
			if strings.EqualFold(rt, "SNOOZE") {
				a.SnoozedAlarmUid = contentline.Unescape(l.Value)
			}
		}
	}
	// Unmodelled alarm properties are dropped rather than preserved: Alarm is
	// a value object with no extensions field, because section 3.6.6 defines
	// a closed set and an alarm has no identity to hang extras on.
	return nil
}

// decodeTrigger reads TRIGGER, section 3.8.6.3.
//
// The default value type is DURATION, relative to the enclosing component.
// VALUE=DATE-TIME makes it absolute, and section 3.8.6.3 requires that form
// to be UTC -- which is why the schema types it as a Timestamp while every
// other calendar field is a CalendarTime.
func decodeTrigger(a *eventv1.Alarm, l contentline.Line) error {
	// The form is decided by the value, not only by VALUE. text/calendar says
	// VALUE=DATE-TIME, but jCal carries the same fact in its type identifier
	// and has no VALUE parameter at all -- so relying on the parameter alone
	// makes every absolute trigger fail to parse when it arrives as JSON.
	// A duration always begins with P, optionally signed; a DATE-TIME never
	// does, so the two are unambiguous.
	absolute := false
	if v := l.Params["VALUE"]; len(v) > 0 && strings.EqualFold(v[0], "DATE-TIME") {
		absolute = true
	}
	if t := strings.TrimLeft(l.Value, "+-"); t != "" && !strings.HasPrefix(t, "P") {
		absolute = true
	}
	if absolute {
		t, err := icaltime.ParseDateTime(strings.TrimSuffix(l.Value, "Z"))
		if err != nil {
			return err
		}
		a.Trigger = &eventv1.Alarm_TriggerTime{TriggerTime: timestampOf(t)}
		return nil
	}
	d, err := icaltime.ParseDuration(l.Value)
	if err != nil {
		return err
	}
	a.Trigger = &eventv1.Alarm_TriggerOffset{TriggerOffset: d}
	// RELATED says whether the offset counts from the start or the end,
	// section 3.2.14. Absent means start.
	if v := l.Params["RELATED"]; len(v) > 0 && strings.EqualFold(v[0], "END") {
		a.TriggerRelation = eventv1.TriggerRelation_TRIGGER_RELATION_END
	} else if len(v) > 0 {
		a.TriggerRelation = eventv1.TriggerRelation_TRIGGER_RELATION_START
	}
	return nil
}

func encodeAlarm(a *eventv1.Alarm) ([]string, error) {
	out := []string{"BEGIN:VALARM"}
	switch a.GetAction() {
	case eventv1.AlarmAction_ALARM_ACTION_AUDIO:
		out = append(out, "ACTION:AUDIO")
	case eventv1.AlarmAction_ALARM_ACTION_DISPLAY:
		out = append(out, "ACTION:DISPLAY")
	case eventv1.AlarmAction_ALARM_ACTION_EMAIL:
		out = append(out, "ACTION:EMAIL")
	}

	switch t := a.GetTrigger().(type) {
	case *eventv1.Alarm_TriggerOffset:
		// RELATED is emitted whenever it was set, not only for END. Section
		// 3.2.14 defaults to START, but decoding an explicit RELATED=START
		// records START, so dropping it here would make the value disappear
		// on the next decode.
		params := ""
		switch a.GetTriggerRelation() {
		case eventv1.TriggerRelation_TRIGGER_RELATION_END:
			params = ";RELATED=END"
		case eventv1.TriggerRelation_TRIGGER_RELATION_START:
			params = ";RELATED=START"
		}
		dur, err := icaltime.EncodeDuration(t.TriggerOffset)
		if err != nil {
			return nil, fmt.Errorf("TRIGGER: %w", err)
		}
		out = append(out, "TRIGGER"+params+":"+dur)
	case *eventv1.Alarm_TriggerTime:
		out = append(out, "TRIGGER;VALUE=DATE-TIME:"+utcStamp(t.TriggerTime))
	}

	if d := a.GetRepeatInterval(); d != nil {
		dur, err := icaltime.EncodeDuration(d)
		if err != nil {
			return nil, fmt.Errorf("VALARM DURATION: %w", err)
		}
		out = append(out, "DURATION:"+dur)
	}
	if n := a.GetRepeatCount(); n > 0 {
		out = append(out, "REPEAT:"+strconv.Itoa(int(n)))
	}
	if v := a.GetDescription(); v != "" {
		out = append(out, "DESCRIPTION:"+contentline.Escape(v))
	}
	if v := a.GetSummary(); v != "" {
		out = append(out, "SUMMARY:"+contentline.Escape(v))
	}
	// RFC 9074.
	if v := a.GetIcalUid(); v != "" {
		out = append(out, "UID:"+contentline.Escape(v))
	}
	if ts := a.GetAcknowledgedTime(); ts != nil {
		out = append(out, "ACKNOWLEDGED:"+utcStamp(ts))
	}
	switch a.GetProximity() {
	case eventv1.Proximity_PROXIMITY_ARRIVE:
		out = append(out, "PROXIMITY:ARRIVE")
	case eventv1.Proximity_PROXIMITY_DEPART:
		out = append(out, "PROXIMITY:DEPART")
	case eventv1.Proximity_PROXIMITY_CONNECT:
		out = append(out, "PROXIMITY:CONNECT")
	case eventv1.Proximity_PROXIMITY_DISCONNECT:
		out = append(out, "PROXIMITY:DISCONNECT")
	}
	if v := a.GetSnoozedAlarmUid(); v != "" {
		out = append(out, "RELATED-TO;RELTYPE=SNOOZE:"+contentline.Escape(v))
	}
	for _, at := range a.GetAttendees() {
		enc, err := encodeAttendee(at)
		if err != nil {
			return nil, err
		}
		out = append(out, enc)
	}
	return append(out, "END:VALARM"), nil
}
