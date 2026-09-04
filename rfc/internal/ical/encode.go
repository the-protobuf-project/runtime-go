// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/icaltime"
)

// prodID identifies the software that produced the stream, section 3.7.3.
// It is a property of this codec, not of the event, which is why the schema
// has no field for icaltime.
const prodID = "-//The Protobuf Project//runtime-go//EN"

// Encode serializes an Event as a VCALENDAR containing one VEVENT.
func Encode(e *eventv1.Event) (string, error) {
	lines, err := contentLines(e)
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

// contentLines is the semantic half: an Event to unfolded content lines.
func contentLines(e *eventv1.Event) ([]string, error) {
	if e.GetStart() == nil {
		return nil, fmt.Errorf("event has no start; section 3.6.1 requires DTSTART")
	}

	var out []string
	w := func(s string) { out = append(out, s) }

	w("BEGIN:VCALENDAR")
	w("VERSION:2.0") // section 3.7.4: MUST be 2.0
	w("PRODID:" + prodID)
	w("BEGIN:VEVENT")

	if v := e.GetIcalUid(); v != "" {
		w("UID:" + contentline.Escape(v))
	}
	v, params := encodeTime(e.GetStart())
	w("DTSTART" + params + ":" + v)

	switch t := e.GetEnds().(type) {
	case *eventv1.Event_End:
		v, params := encodeTime(t.End)
		w("DTEND" + params + ":" + v)
	case *eventv1.Event_Duration:
		dur, err := icaltime.EncodeDuration(t.Duration)
		if err != nil {
			return nil, err
		}
		w("DURATION:" + dur)
	}

	if v := e.GetSummary(); v != "" {
		w("SUMMARY:" + contentline.Escape(v))
	}
	if v := e.GetDescription(); v != "" {
		w("DESCRIPTION:" + contentline.Escape(v))
	}
	if v := e.GetLocation(); v != "" {
		w("LOCATION:" + contentline.Escape(v))
	}
	if p := e.GetPosition(); p != nil {
		w("GEO:" + encodeGeo(p))
	}
	// RFC 9253.
	for _, c := range e.GetConcepts() {
		w("CONCEPT:" + contentline.Escape(c))
	}
	for _, r := range e.GetRefids() {
		w("REFID:" + contentline.Escape(r))
	}
	for _, l := range e.GetLinks() {
		enc, err := encodeLink(l)
		if err != nil {
			return nil, err
		}
		w(enc)
	}
	// RFC 9073's content properties. Its *components* are sub-components and
	// so are written further down, with the alarms.
	for _, s := range e.GetStyledDescriptions() {
		enc, err := encodeStyledDescription(s)
		if err != nil {
			return nil, err
		}
		w(enc)
	}
	for _, d := range e.GetStructuredData() {
		enc, err := encodeStructuredData(d)
		if err != nil {
			return nil, err
		}
		w(enc)
	}
	if s := encodeConfirmation(e.GetConfirmation()); s != "" {
		w("STATUS:" + s)
	}
	if s := encodeClassification(e.GetClassification()); s != "" {
		w("CLASS:" + s)
	}
	if s := encodeTransparency(e.GetTransparency()); s != "" {
		w("TRANSP:" + s)
	}
	if e.GetSequence() > 0 {
		w("SEQUENCE:" + strconv.Itoa(int(e.GetSequence())))
	}
	if r := e.GetRecurrence(); r != nil {
		w("RRULE:" + encodeRecurrence(r))
	}
	for _, t := range e.GetExcludedTimes() {
		v, params := encodeTime(t)
		w("EXDATE" + params + ":" + v)
	}
	for _, t := range e.GetAdditionalTimes() {
		v, params := encodeTime(t)
		w("RDATE" + params + ":" + v)
	}
	if o := e.GetOrganizer(); o != nil {
		enc, err := encodeOrganizer(o)
		if err != nil {
			return nil, err
		}
		w(enc)
	}
	for _, a := range e.GetAttendees() {
		enc, err := encodeAttendee(a)
		if err != nil {
			return nil, err
		}
		w(enc)
	}
	for _, x := range e.GetExtensions() {
		enc, err := encodeExtension(x)
		if err != nil {
			return nil, err
		}
		w(enc)
	}
	// Sub-components go last, after every property of the event itself:
	// section 3.6 requires a component's own properties to precede its
	// sub-components. That covers VALARM and equally RFC 9073's PARTICIPANT,
	// VLOCATION and VRESOURCE, which used to be written up among the
	// properties and so put STATUS and RRULE after a nested component.
	for _, a := range e.GetAlarms() {
		enc, err := encodeAlarm(a)
		if err != nil {
			return nil, err
		}
		out = append(out, enc...)
	}
	for _, p := range e.GetParticipants() {
		enc, err := encodeParticipant(p)
		if err != nil {
			return nil, err
		}
		out = append(out, enc...)
	}
	for _, l := range e.GetStructuredLocations() {
		enc, err := encodeLocation(l)
		if err != nil {
			return nil, err
		}
		out = append(out, enc...)
	}
	for _, r := range e.GetResources() {
		enc, err := encodeResource(r)
		if err != nil {
			return nil, err
		}
		out = append(out, enc...)
	}

	w("END:VEVENT")
	w("END:VCALENDAR")
	return out, nil
}

func encodeExtension(x *eventv1.ExtensionProperty) (string, error) {
	var b strings.Builder
	b.WriteString(x.GetKey())

	keys := make([]string, 0, len(x.GetParameters()))
	for k := range x.GetParameters() {
		keys = append(keys, k)
	}
	sort.Strings(keys) // Go map order is not deterministic; output must be.
	for _, k := range keys {
		// A preserved extension carries whatever the source file held, so its
		// parameter values are the least trustworthy in the codec and the ones
		// most in need of the section 3.1 check.
		esc, err := contentline.EscapeParam(x.GetParameters()[k])
		if err != nil {
			return "", fmt.Errorf("%s parameter %s: %w", x.GetKey(), k, err)
		}
		b.WriteString(";" + k + "=" + esc)
	}
	b.WriteByte(':')
	b.WriteString(contentline.JoinList(x.GetValues()))
	return b.String(), nil
}

// encodeLink writes a LINK property, RFC 9253 section 8.2.
//
// VALUE is emitted for the UID and XML-REFERENCE forms and omitted for URI,
// which section 8.2 makes the default. LINKREL is always emitted: section 6.1
// requires it on every LINK.
func encodeLink(l *eventv1.Link) (string, error) {
	var params, value string
	switch v := l.GetValue().(type) {
	case *eventv1.Link_Uri:
		value = v.Uri
	case *eventv1.Link_IcalUid:
		params, value = ";VALUE=UID", v.IcalUid
	case *eventv1.Link_XmlReference:
		params, value = ";VALUE=XML-REFERENCE", v.XmlReference
	}
	// LINKREL and LABEL are both free text in section 6.1, so both routinely
	// contain the colon or comma that section 3.1 requires quoting for.
	for _, p := range []struct{ name, value string }{
		{"LINKREL", l.GetRelation()},
		{"FMTTYPE", l.GetFormatType()},
		{"LABEL", l.GetLabel()},
		{"LANGUAGE", l.GetLanguageCode()},
	} {
		if p.value == "" {
			continue
		}
		esc, err := contentline.EscapeParam(p.value)
		if err != nil {
			return "", fmt.Errorf("LINK %s: %w", p.name, err)
		}
		params += ";" + p.name + "=" + esc
	}
	return "LINK" + params + ":" + contentline.Escape(value), nil
}
