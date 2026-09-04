// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package jscontact

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"
)

// ORG, TITLE, ROLE, NICKNAME, NOTE, BDAY, ANNIVERSARY, RELATED, GRAMGENDER
// and PRONOUNS -- RFC 9555 sections 2.9.4, 2.9.6, 2.5.6, 2.11.4, 2.5.1,
// 2.9.5 and 2.5.4.
//
// Section 2.9.6 is the one that does not round trip cleanly by construction:
// **TITLE and ROLE are two vCard properties and one JSContact object**,
// separated by a `kind` field. Converting to vCard splits them back out, so
// the data survives, but a Card assembled by hand with the wrong `kind` will
// come back as the other property.

func organizationsToCard(c *vcardv1.Contact, card *cardv1.Card) {
	for i, o := range c.GetOrganizations() {
		if card.Organizations == nil {
			card.Organizations = map[string]*cardv1.Organization{}
		}
		out := &cardv1.Organization{
			DisplayName: o.GetValue(),
			Contexts:    contextsToCard(o.GetTypes()),
		}
		for _, u := range o.GetUnits() {
			out.Units = append(out.Units, &cardv1.OrgUnit{DisplayName: u})
		}
		card.Organizations[id("org", i)] = out
	}

	// TITLE and ROLE both become Title, keyed on kind -- section 2.9.6.
	n := 0
	for _, t := range c.GetTitles() {
		if card.Titles == nil {
			card.Titles = map[string]*cardv1.Title{}
		}
		card.Titles[id("t", n)] = &cardv1.Title{
			DisplayName: t,
			Kind:        cardv1.TitleKind_TITLE_KIND_TITLE,
		}
		n++
	}
	for _, r := range c.GetRoles() {
		if card.Titles == nil {
			card.Titles = map[string]*cardv1.Title{}
		}
		card.Titles[id("t", n)] = &cardv1.Title{
			DisplayName: r.GetValue(),
			Kind:        cardv1.TitleKind_TITLE_KIND_ROLE,
		}
		n++
	}

	// NICKNAME, section 2.5.6. A vCard NICKNAME holds a comma-separated
	// list; JSContact gives each nickname its own object, so one property
	// can produce several entries.
	n = 0
	for _, nn := range c.GetNicknames() {
		for _, v := range nn.GetValues() {
			if card.Nicknames == nil {
				card.Nicknames = map[string]*cardv1.Nickname{}
			}
			card.Nicknames[id("nick", n)] = &cardv1.Nickname{
				DisplayName: v,
				Pref:        nn.GetPref(),
				Contexts:    contextsToCard(nn.GetTypes()),
			}
			n++
		}
	}
}

func organizationsToVcard(card *cardv1.Card, c *vcardv1.Contact) {
	for _, k := range sortedKeys(card.GetOrganizations()) {
		o := card.GetOrganizations()[k]
		out := &vcardv1.Organization{
			Value: o.GetDisplayName(),
			Types: contextsToVcard(o.GetContexts()),
		}
		for _, u := range o.GetUnits() {
			out.Units = append(out.Units, u.GetDisplayName())
		}
		c.Organizations = append(c.Organizations, out)
	}

	// The split back out, section 2.9.6.
	for _, k := range sortedKeys(card.GetTitles()) {
		t := card.GetTitles()[k]
		if t.GetKind() == cardv1.TitleKind_TITLE_KIND_ROLE {
			c.Roles = append(c.Roles, &vcardv1.Role{Value: t.GetDisplayName()})
			continue
		}
		// An absent kind means "title", per section 2.2.5 of RFC 9553.
		c.Titles = append(c.Titles, t.GetDisplayName())
	}

	for _, k := range sortedKeys(card.GetNicknames()) {
		n := card.GetNicknames()[k]
		c.Nicknames = append(c.Nicknames, &vcardv1.Nickname{
			Values: []string{n.GetDisplayName()},
			Pref:   n.GetPref(),
			Types:  contextsToVcard(n.GetContexts()),
		})
	}
}

func personalToCard(c *vcardv1.Contact, card *cardv1.Card) {
	// NOTE, section 2.11.4.
	for i, n := range c.GetNotes() {
		if card.Notes == nil {
			card.Notes = map[string]*cardv1.Note{}
		}
		card.Notes[id("note", i)] = &cardv1.Note{Note: n}
	}

	// BDAY and ANNIVERSARY, section 2.5.1. Both become Anniversary objects
	// distinguished by kind, which is also how a death date would arrive --
	// vCard has no DEATHDATE in this model, so that direction only.
	n := 0
	if b := c.GetBirthday(); b != nil {
		if d := dateOrTextToCard(b); d != nil {
			if card.Anniversaries == nil {
				card.Anniversaries = map[string]*cardv1.Anniversary{}
			}
			card.Anniversaries[id("a", n)] = &cardv1.Anniversary{
				Kind:      cardv1.AnniversaryKind_ANNIVERSARY_KIND_BIRTH,
				DateValue: d,
			}
			n++
		}
	}
	if a := c.GetAnniversary(); a != nil {
		if d := dateOrTextToCard(a); d != nil {
			if card.Anniversaries == nil {
				card.Anniversaries = map[string]*cardv1.Anniversary{}
			}
			card.Anniversaries[id("a", n)] = &cardv1.Anniversary{
				Kind:      cardv1.AnniversaryKind_ANNIVERSARY_KIND_WEDDING,
				DateValue: d,
			}
		}
	}

	// RELATED, section 2.9.5. The map key is the related entity's URI.
	for _, r := range c.GetRelations() {
		// The JSContact map key is the related entity itself -- a URI, or the
		// free text where vCard permitted one.
		key := firstNonEmpty(r.GetUri(), r.GetText())
		if key == "" {
			continue
		}
		if card.Relations == nil {
			card.Relations = map[string]*cardv1.Relation{}
		}
		// Merged, not replaced. vCard permits several RELATED properties
		// naming the same entity -- RELATED;TYPE=friend and
		// RELATED;TYPE=colleague for one person is the ordinary way to say
		// both -- and RFC 9555 section 2.9.5 keys the JSContact map on the
		// value, so they collide here. Assigning dropped every type but the
		// last one seen.
		existing, ok := card.Relations[key]
		if !ok {
			card.Relations[key] = &cardv1.Relation{
				Relations: relationTypesToCard(r.GetRelationTypes()),
			}
			continue
		}
		have := map[cardv1.RelationType]bool{}
		for _, t := range existing.GetRelations() {
			have[t] = true
		}
		for _, t := range relationTypesToCard(r.GetRelationTypes()) {
			if !have[t] {
				existing.Relations = append(existing.Relations, t)
				have[t] = true
			}
		}
	}
}

func personalToVcard(card *cardv1.Card, c *vcardv1.Contact) {
	for _, k := range sortedKeys(card.GetNotes()) {
		c.Notes = append(c.Notes, card.GetNotes()[k].GetNote())
	}

	for _, k := range sortedKeys(card.GetAnniversaries()) {
		a := card.GetAnniversaries()[k]
		d := dateToVcard(a.GetDateValue())
		if d == nil {
			continue
		}
		switch a.GetKind() {
		case cardv1.AnniversaryKind_ANNIVERSARY_KIND_BIRTH:
			c.Birthday = d
		case cardv1.AnniversaryKind_ANNIVERSARY_KIND_WEDDING:
			c.Anniversary = d
		}
		// ANNIVERSARY_KIND_DEATH is dropped: RFC 6350 has no DEATHDATE
		// property, and RFC 9554 did not add one. The value survives only if
		// the caller keeps the Card, which is the direction rule 14 stores in
		// anyway.
	}

	for _, k := range sortedKeys(card.GetRelations()) {
		rel := &vcardv1.Relation{
			RelationTypes: relationTypesToVcard(card.GetRelations()[k].GetRelations()),
		}
		// RFC 9555 section 2.9.5 maps a RELATED value to the map key in one
		// direction only, and its own example carries a free-text value --
		// "Please contact my deputy John for any inquiries." -- into a key
		// alongside the URI ones. Coming back it says nothing, so telling
		// them apart is a local decision, not an RFC-derived one: a key with
		// an RFC 3986 scheme is treated as a URI and anything else as text.
		//
		// Emitting every key as a URI, which is what this did, produced
		// RELATED:Please contact my deputy John... with no VALUE=text, and a
		// reader is then obliged to parse that sentence as a URI.
		if isURI(k) {
			rel.Value = &vcardv1.Relation_Uri{Uri: k}
		} else {
			rel.Value = &vcardv1.Relation_Text{Text: k}
		}
		c.Relations = append(c.Relations, rel)
	}
}

// GRAMGENDER and PRONOUNS, section 2.5.4.
func speechToCard(c *vcardv1.Contact, card *cardv1.Card) {
	gg := c.GetGrammaticalGenders()
	pr := c.GetPronouns()
	if len(gg) == 0 && len(pr) == 0 {
		return
	}
	s := &cardv1.Speech{}
	if len(gg) > 0 {
		// JSContact carries one grammatical gender per Card; vCard permits
		// one per language. Section 2.5.4 converts the first and leaves the
		// rest to vCardProps.
		s.GrammaticalGender = grammaticalGenderToCard(gg[0].GetKind())
	}
	for i, p := range pr {
		if s.Pronouns == nil {
			s.Pronouns = map[string]*cardv1.Pronouns{}
		}
		s.Pronouns[id("pr", i)] = &cardv1.Pronouns{
			Pronouns: p.GetValue(),
			Pref:     p.GetPref(),
			Contexts: contextsToCard(p.GetTypes()),
		}
	}
	card.Speech = s
}

func speechToVcard(card *cardv1.Card, c *vcardv1.Contact) {
	s := card.GetSpeech()
	if s == nil {
		return
	}
	if g := grammaticalGenderToVcard(s.GetGrammaticalGender()); g != vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_UNSPECIFIED {
		c.GrammaticalGenders = append(c.GrammaticalGenders, &vcardv1.GrammaticalGender{Kind: g})
	}
	for _, k := range sortedKeys(s.GetPronouns()) {
		p := s.GetPronouns()[k]
		c.Pronouns = append(c.Pronouns, &vcardv1.Pronouns{
			Value: p.GetPronouns(),
			Pref:  p.GetPref(),
			Types: contextsToVcard(p.GetContexts()),
		})
	}
}

// isURI reports whether s begins with an RFC 3986 section 3.1 scheme
// <https://www.rfc-editor.org/rfc/rfc3986.html#section-3.1>:
// ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ) ":".
//
// Deliberately only the scheme. A full URI parse would accept the bare text
// "John" as a relative reference, which is exactly the case this has to
// reject.
func isURI(s string) bool {
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == ':':
			return i > 0
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z':
		case i > 0 && (ch >= '0' && ch <= '9' || ch == '+' || ch == '-' || ch == '.'):
		default:
			return false
		}
	}
	return false
}
