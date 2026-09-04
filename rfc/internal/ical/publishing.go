// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"encoding/base64"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// RFC 9073's components: PARTICIPANT (section 7.1), VLOCATION (7.2) and
// VRESOURCE (7.3).
//
// All three nest, and section 7.1's grammar is `partprop *locationc
// *resourcec` -- a VLOCATION inside a PARTICIPANT belongs to the participant,
// not to the event. That is the same trap VALARM inside VEVENT already set
// here once, so the decoder keeps the open component in the same stack rather
// than tracking a boolean per type.

func decodeParticipantProperty(p *eventv1.Participant, l contentline.Line) error {
	switch l.Name {
	case "UID":
		p.IcalUid = contentline.Unescape(l.Value)
	case "PARTICIPANT-TYPE":
		// Section 6.2 permits a comma-separated list, plus iana-token and
		// x-name. An unrecognized value is left out rather than mapped to a
		// role the publisher did not state.
		for _, v := range contentline.SplitList(l.Value) {
			if t := participantTypeOf(v); t != eventv1.ParticipantType_PARTICIPANT_TYPE_UNSPECIFIED {
				p.ParticipantTypes = append(p.ParticipantTypes, t)
			}
		}
	case "CALENDAR-ADDRESS":
		p.CalendarAddress = contentline.Unescape(l.Value)
	case "SUMMARY":
		p.Summary = contentline.Unescape(l.Value)
	case "DESCRIPTION":
		p.Description = contentline.Unescape(l.Value)
	case "URL":
		p.Url = contentline.Unescape(l.Value)
	case "GEO":
		g, err := decodeGeo(l.Value)
		if err != nil {
			return err
		}
		p.Position = g
	case "STRUCTURED-DATA":
		p.StructuredData = append(p.StructuredData, decodeStructuredData(l))
	case "STYLED-DESCRIPTION":
		p.StyledDescriptions = append(p.StyledDescriptions, decodeStyledDescription(l))
	}
	// Participant is a value object with no extensions field, so an
	// unmodelled property is dropped -- the same choice Alarm makes, and for
	// the same reason: it has no identity to hang extras on.
	return nil
}

func decodeLocationProperty(loc *eventv1.Location, l contentline.Line) error {
	switch l.Name {
	case "UID":
		loc.IcalUid = contentline.Unescape(l.Value)
	case "NAME":
		loc.DisplayName = contentline.Unescape(l.Value)
	case "DESCRIPTION":
		loc.Description = contentline.Unescape(l.Value)
	case "LOCATION-TYPE":
		loc.LocationTypes = append(loc.LocationTypes, contentline.SplitList(l.Value)...)
	case "GEO":
		g, err := decodeGeo(l.Value)
		if err != nil {
			return err
		}
		loc.Position = g
	case "STRUCTURED-DATA":
		loc.StructuredData = append(loc.StructuredData, decodeStructuredData(l))
	}
	return nil
}

func decodeResourceProperty(r *eventv1.Resource, l contentline.Line) error {
	switch l.Name {
	case "UID":
		r.IcalUid = contentline.Unescape(l.Value)
	case "NAME":
		r.DisplayName = contentline.Unescape(l.Value)
	case "DESCRIPTION":
		r.Description = contentline.Unescape(l.Value)
	case "RESOURCE-TYPE":
		r.ResourceTypes = append(r.ResourceTypes, contentline.SplitList(l.Value)...)
	case "GEO":
		g, err := decodeGeo(l.Value)
		if err != nil {
			return err
		}
		r.Position = g
	case "STRUCTURED-DATA":
		r.StructuredData = append(r.StructuredData, decodeStructuredData(l))
	}
	return nil
}

// decodeStyledDescription reads STYLED-DESCRIPTION, RFC 9073 section 6.5.
func decodeStyledDescription(l contentline.Line) *eventv1.StyledDescription {
	s := &eventv1.StyledDescription{
		FormatType: firstParam(l, "FMTTYPE"),
		Derived:    strings.EqualFold(firstParam(l, "DERIVED"), "TRUE"),
	}
	if strings.EqualFold(firstParam(l, "VALUE"), "URI") {
		s.Value = &eventv1.StyledDescription_Uri{Uri: l.Value}
	} else {
		s.Value = &eventv1.StyledDescription_Text{Text: contentline.Unescape(l.Value)}
	}
	return s
}

// decodeStructuredData reads STRUCTURED-DATA, RFC 9073 section 6.6.
//
// Section 6.6 gives three value types and decides between them with VALUE:
// TEXT (the default), URI, and BINARY. The binary form arrives base64-encoded
// with ENCODING=BASE64 and is decoded here, because a protobuf `bytes` field
// is already binary -- keeping the base64 would store one encoding inside
// another.
func decodeStructuredData(l contentline.Line) *eventv1.StructuredData {
	d := &eventv1.StructuredData{
		FormatType: firstParam(l, "FMTTYPE"),
		Schema:     firstParam(l, "SCHEMA"),
	}
	switch strings.ToUpper(firstParam(l, "VALUE")) {
	case "URI":
		d.Value = &eventv1.StructuredData_Uri{Uri: l.Value}
	case "BINARY":
		if raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(l.Value)); err == nil {
			d.Value = &eventv1.StructuredData_Binary{Binary: raw}
		} else {
			// Undecodable base64 is kept as text rather than dropped: the
			// bytes are still the publisher's data, and losing them silently
			// is worse than storing them in the wrong arm.
			d.Value = &eventv1.StructuredData_Text{Text: contentline.Unescape(l.Value)}
		}
	default:
		d.Value = &eventv1.StructuredData_Text{Text: contentline.Unescape(l.Value)}
	}
	return d
}

func participantTypeOf(v string) eventv1.ParticipantType {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "ACTIVE":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_ACTIVE
	case "INACTIVE":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_INACTIVE
	case "SPONSOR":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_SPONSOR
	case "CONTACT":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_CONTACT
	case "BOOKING-CONTACT":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_BOOKING_CONTACT
	case "EMERGENCY-CONTACT":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_EMERGENCY_CONTACT
	case "PUBLICITY-CONTACT":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_PUBLICITY_CONTACT
	case "PLANNER-CONTACT":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_PLANNER_CONTACT
	case "PERFORMER":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_PERFORMER
	case "SPEAKER":
		return eventv1.ParticipantType_PARTICIPANT_TYPE_SPEAKER
	}
	return eventv1.ParticipantType_PARTICIPANT_TYPE_UNSPECIFIED
}

func participantTypeName(t eventv1.ParticipantType) string {
	switch t {
	case eventv1.ParticipantType_PARTICIPANT_TYPE_ACTIVE:
		return "ACTIVE"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_INACTIVE:
		return "INACTIVE"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_SPONSOR:
		return "SPONSOR"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_CONTACT:
		return "CONTACT"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_BOOKING_CONTACT:
		return "BOOKING-CONTACT"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_EMERGENCY_CONTACT:
		return "EMERGENCY-CONTACT"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_PUBLICITY_CONTACT:
		return "PUBLICITY-CONTACT"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_PLANNER_CONTACT:
		return "PLANNER-CONTACT"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_PERFORMER:
		return "PERFORMER"
	case eventv1.ParticipantType_PARTICIPANT_TYPE_SPEAKER:
		return "SPEAKER"
	}
	return ""
}

// encodeParticipant writes a PARTICIPANT component, RFC 9073 section 7.1,
// with its nested VLOCATION and VRESOURCE sub-components.
func encodeParticipant(p *eventv1.Participant) []string {
	out := []string{"BEGIN:PARTICIPANT"}
	if v := p.GetIcalUid(); v != "" {
		out = append(out, "UID:"+contentline.Escape(v))
	}
	var types []string
	for _, t := range p.GetParticipantTypes() {
		if s := participantTypeName(t); s != "" {
			types = append(types, s)
		}
	}
	if len(types) > 0 {
		out = append(out, "PARTICIPANT-TYPE:"+strings.Join(types, ","))
	}
	if v := p.GetCalendarAddress(); v != "" {
		out = append(out, "CALENDAR-ADDRESS:"+v)
	}
	if v := p.GetSummary(); v != "" {
		out = append(out, "SUMMARY:"+contentline.Escape(v))
	}
	if v := p.GetDescription(); v != "" {
		out = append(out, "DESCRIPTION:"+contentline.Escape(v))
	}
	if v := p.GetUrl(); v != "" {
		out = append(out, "URL:"+v)
	}
	if g := p.GetPosition(); g != nil {
		out = append(out, "GEO:"+encodeGeo(g))
	}
	for _, d := range p.GetStructuredData() {
		out = append(out, encodeStructuredData(d))
	}
	for _, s := range p.GetStyledDescriptions() {
		out = append(out, encodeStyledDescription(s))
	}
	// Nested, per section 7.1's `partprop *locationc *resourcec`.
	for _, l := range p.GetStructuredLocations() {
		out = append(out, encodeLocation(l)...)
	}
	for _, r := range p.GetResources() {
		out = append(out, encodeResource(r)...)
	}
	return append(out, "END:PARTICIPANT")
}

// encodeLocation writes a VLOCATION component, RFC 9073 section 7.2.
func encodeLocation(l *eventv1.Location) []string {
	out := []string{"BEGIN:VLOCATION"}
	if v := l.GetIcalUid(); v != "" {
		out = append(out, "UID:"+contentline.Escape(v))
	}
	if v := l.GetDisplayName(); v != "" {
		out = append(out, "NAME:"+contentline.Escape(v))
	}
	if v := l.GetDescription(); v != "" {
		out = append(out, "DESCRIPTION:"+contentline.Escape(v))
	}
	if v := l.GetLocationTypes(); len(v) > 0 {
		out = append(out, "LOCATION-TYPE:"+contentline.JoinList(v))
	}
	if g := l.GetPosition(); g != nil {
		out = append(out, "GEO:"+encodeGeo(g))
	}
	for _, d := range l.GetStructuredData() {
		out = append(out, encodeStructuredData(d))
	}
	return append(out, "END:VLOCATION")
}

// encodeResource writes a VRESOURCE component, RFC 9073 section 7.3.
func encodeResource(r *eventv1.Resource) []string {
	out := []string{"BEGIN:VRESOURCE"}
	if v := r.GetIcalUid(); v != "" {
		out = append(out, "UID:"+contentline.Escape(v))
	}
	if v := r.GetDisplayName(); v != "" {
		out = append(out, "NAME:"+contentline.Escape(v))
	}
	if v := r.GetDescription(); v != "" {
		out = append(out, "DESCRIPTION:"+contentline.Escape(v))
	}
	if v := r.GetResourceTypes(); len(v) > 0 {
		out = append(out, "RESOURCE-TYPE:"+contentline.JoinList(v))
	}
	if g := r.GetPosition(); g != nil {
		out = append(out, "GEO:"+encodeGeo(g))
	}
	for _, d := range r.GetStructuredData() {
		out = append(out, encodeStructuredData(d))
	}
	return append(out, "END:VRESOURCE")
}

func encodeStyledDescription(s *eventv1.StyledDescription) string {
	var params, value string
	switch v := s.GetValue().(type) {
	case *eventv1.StyledDescription_Text:
		value = contentline.Escape(v.Text)
	case *eventv1.StyledDescription_Uri:
		params, value = ";VALUE=URI", v.Uri
	}
	if f := s.GetFormatType(); f != "" {
		params += ";FMTTYPE=" + contentline.EscapeParam(f)
	}
	if s.GetDerived() {
		params += ";DERIVED=TRUE"
	}
	return "STYLED-DESCRIPTION" + params + ":" + value
}

func encodeStructuredData(d *eventv1.StructuredData) string {
	var params, value string
	switch v := d.GetValue().(type) {
	case *eventv1.StructuredData_Text:
		value = contentline.Escape(v.Text)
	case *eventv1.StructuredData_Uri:
		params, value = ";VALUE=URI", v.Uri
	case *eventv1.StructuredData_Binary:
		// Section 6.6 pairs VALUE=BINARY with ENCODING=BASE64; the bytes are
		// stored decoded, so they are re-encoded on the way out.
		params = ";VALUE=BINARY;ENCODING=BASE64"
		value = base64.StdEncoding.EncodeToString(v.Binary)
	}
	if f := d.GetFormatType(); f != "" {
		params += ";FMTTYPE=" + contentline.EscapeParam(f)
	}
	// SCHEMA is a URI, so it always contains a colon and always needs
	// quoting -- RFC 5545 section 3.1 requires it, and an unquoted one makes
	// the parser take the colon as the value separator.
	if s := d.GetSchema(); s != "" {
		params += ";SCHEMA=" + contentline.EscapeParam(s)
	}
	return "STRUCTURED-DATA" + params + ":" + value
}
