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
		w("DURATION:" + icaltime.EncodeDuration(t.Duration))
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
		w(encodeLink(l))
	}
	// RFC 9073's content properties, then its components.
	for _, s := range e.GetStyledDescriptions() {
		w(encodeStyledDescription(s))
	}
	for _, d := range e.GetStructuredData() {
		w(encodeStructuredData(d))
	}
	for _, p := range e.GetParticipants() {
		out = append(out, encodeParticipant(p)...)
	}
	for _, l := range e.GetStructuredLocations() {
		out = append(out, encodeLocation(l)...)
	}
	for _, r := range e.GetResources() {
		out = append(out, encodeResource(r)...)
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
		w(encodeOrganizer(o))
	}
	for _, a := range e.GetAttendees() {
		w(encodeAttendee(a))
	}
	for _, x := range e.GetExtensions() {
		w(encodeExtension(x))
	}
	// Alarms are sub-components, so they are written last, after every
	// property of the event itself: section 3.6 requires a component's own
	// properties to precede its sub-components.
	for _, a := range e.GetAlarms() {
		out = append(out, encodeAlarm(a)...)
	}

	w("END:VEVENT")
	w("END:VCALENDAR")
	return out, nil
}

func encodeExtension(x *eventv1.ExtensionProperty) string {
	var b strings.Builder
	b.WriteString(x.GetKey())

	keys := make([]string, 0, len(x.GetParameters()))
	for k := range x.GetParameters() {
		keys = append(keys, k)
	}
	sort.Strings(keys) // Go map order is not deterministic; output must be.
	for _, k := range keys {
		b.WriteString(";" + k + "=" + x.GetParameters()[k])
	}
	b.WriteByte(':')
	b.WriteString(contentline.JoinList(x.GetValues()))
	return b.String()
}

// encodeLink writes a LINK property, RFC 9253 section 8.2.
//
// VALUE is emitted for the UID and XML-REFERENCE forms and omitted for URI,
// which section 8.2 makes the default. LINKREL is always emitted: section 6.1
// requires it on every LINK.
func encodeLink(l *eventv1.Link) string {
	var params, value string
	switch v := l.GetValue().(type) {
	case *eventv1.Link_Uri:
		value = v.Uri
	case *eventv1.Link_IcalUid:
		params, value = ";VALUE=UID", v.IcalUid
	case *eventv1.Link_XmlReference:
		params, value = ";VALUE=XML-REFERENCE", v.XmlReference
	}
	if s := l.GetRelation(); s != "" {
		params += ";LINKREL=" + s
	}
	if s := l.GetFormatType(); s != "" {
		params += ";FMTTYPE=" + s
	}
	if s := l.GetLabel(); s != "" {
		params += ";LABEL=" + s
	}
	if s := l.GetLanguageCode(); s != "" {
		params += ";LANGUAGE=" + s
	}
	return "LINK" + params + ":" + contentline.Escape(value)
}
