// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"sort"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// ATTENDEE and ORGANIZER, RFC 5545 sections 3.8.4.1 and 3.8.4.3.
//
// The property value is a CAL-ADDRESS -- in practice a mailto: URI -- and
// everything that makes a participant useful is in its parameters. That is
// why the schema has messages for these rather than a bare string.

var roles = map[string]eventv1.ParticipationRole{
	"CHAIR":           eventv1.ParticipationRole_PARTICIPATION_ROLE_CHAIR,
	"REQ-PARTICIPANT": eventv1.ParticipationRole_PARTICIPATION_ROLE_REQUIRED,
	"OPT-PARTICIPANT": eventv1.ParticipationRole_PARTICIPATION_ROLE_OPTIONAL,
	"NON-PARTICIPANT": eventv1.ParticipationRole_PARTICIPATION_ROLE_NON_PARTICIPANT,
}

var participations = map[string]eventv1.Participation{
	"NEEDS-ACTION": eventv1.Participation_PARTICIPATION_NEEDS_ACTION,
	"ACCEPTED":     eventv1.Participation_PARTICIPATION_ACCEPTED,
	"DECLINED":     eventv1.Participation_PARTICIPATION_DECLINED,
	"TENTATIVE":    eventv1.Participation_PARTICIPATION_TENTATIVE,
	"DELEGATED":    eventv1.Participation_PARTICIPATION_DELEGATED,
}

var userTypes = map[string]eventv1.CalendarUserType{
	"INDIVIDUAL": eventv1.CalendarUserType_CALENDAR_USER_TYPE_INDIVIDUAL,
	"GROUP":      eventv1.CalendarUserType_CALENDAR_USER_TYPE_GROUP,
	"RESOURCE":   eventv1.CalendarUserType_CALENDAR_USER_TYPE_RESOURCE,
	"ROOM":       eventv1.CalendarUserType_CALENDAR_USER_TYPE_ROOM,
	"UNKNOWN":    eventv1.CalendarUserType_CALENDAR_USER_TYPE_UNKNOWN,
}

func decodeAttendee(l contentline.Line) *eventv1.Attendee {
	a := &eventv1.Attendee{Address: l.Value}
	if v := l.Params["CN"]; len(v) > 0 {
		a.DisplayName = v[0]
	}
	if v := l.Params["ROLE"]; len(v) > 0 {
		a.Role = roles[strings.ToUpper(v[0])]
	}
	if v := l.Params["PARTSTAT"]; len(v) > 0 {
		a.Participation = participations[strings.ToUpper(v[0])]
	}
	if v := l.Params["CUTYPE"]; len(v) > 0 {
		a.UserType = userTypes[strings.ToUpper(v[0])]
	}
	if v := l.Params["RSVP"]; len(v) > 0 {
		a.Rsvp = strings.EqualFold(v[0], "TRUE")
	}
	// Sections 3.2.5 and 3.2.4. The RFC titles these Delegatees and
	// Delegators, which is where the field names came from -- AIP-140 bans
	// the prepositions in DELEGATED-TO and DELEGATED-FROM.
	a.Delegatees = append(a.Delegatees, l.Params["DELEGATED-TO"]...)
	a.Delegators = append(a.Delegators, l.Params["DELEGATED-FROM"]...)
	return a
}

func encodeAttendee(a *eventv1.Attendee) (string, error) {
	p := map[string][]string{}
	if v := a.GetDisplayName(); v != "" {
		p["CN"] = []string{v}
	}
	if s := nameOfRole(a.GetRole()); s != "" {
		p["ROLE"] = []string{s}
	}
	if s := nameOfParticipation(a.GetParticipation()); s != "" {
		p["PARTSTAT"] = []string{s}
	}
	if s := nameOfUserType(a.GetUserType()); s != "" {
		p["CUTYPE"] = []string{s}
	}
	if a.GetRsvp() {
		p["RSVP"] = []string{"TRUE"}
	}
	if v := a.GetDelegatees(); len(v) > 0 {
		p["DELEGATED-TO"] = v
	}
	if v := a.GetDelegators(); len(v) > 0 {
		p["DELEGATED-FROM"] = v
	}
	rp, err := renderParams(p)
	if err != nil {
		return "", fmt.Errorf("ATTENDEE: %w", err)
	}
	return "ATTENDEE" + rp + ":" + a.GetAddress(), nil
}

func decodeOrganizer(l contentline.Line) *eventv1.Organizer {
	o := &eventv1.Organizer{Address: l.Value}
	if v := l.Params["CN"]; len(v) > 0 {
		o.DisplayName = v[0]
	}
	if v := l.Params["SENT-BY"]; len(v) > 0 {
		o.Sender = v[0]
	}
	return o
}

func encodeOrganizer(o *eventv1.Organizer) (string, error) {
	p := map[string][]string{}
	if v := o.GetDisplayName(); v != "" {
		p["CN"] = []string{v}
	}
	if v := o.GetSender(); v != "" {
		p["SENT-BY"] = []string{v}
	}
	rp, err := renderParams(p)
	if err != nil {
		return "", fmt.Errorf("ORGANIZER: %w", err)
	}
	return "ORGANIZER" + rp + ":" + o.GetAddress(), nil
}

// renderParams writes parameters in sorted order so output is diffable.
//
// Escaping goes through contentline.EscapeParam rather than being repeated
// here: it also refuses the DQUOTE and CR/LF that section 3.1 cannot
// represent. A CN carrying either used to be written straight through, and a
// newline in a display name split the ATTENDEE line so the remainder parsed as
// a fresh property.
func renderParams(p map[string][]string) (string, error) {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		vals := make([]string, len(p[k]))
		for i, v := range p[k] {
			esc, err := contentline.EscapeParam(v)
			if err != nil {
				return "", fmt.Errorf("%s: %w", k, err)
			}
			vals[i] = esc
		}
		b.WriteString(";" + k + "=" + strings.Join(vals, ","))
	}
	return b.String(), nil
}

func nameOfRole(r eventv1.ParticipationRole) string      { return lookup(roles, r) }
func nameOfParticipation(p eventv1.Participation) string { return lookup(participations, p) }
func nameOfUserType(u eventv1.CalendarUserType) string   { return lookup(userTypes, u) }

// lookup reverses one of the tables above, deterministically.
func lookup[T comparable](m map[string]T, want T) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if m[k] == want {
			return k
		}
	}
	return ""
}
