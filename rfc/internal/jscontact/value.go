// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package jscontact

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"
)

// Value-type conversions, RFC 9555 section 2.2, and the RELATED and
// GRAMGENDER value sets from sections 2.9.5 and 2.5.4.

// dateOrTextToCard converts vCard's DATE-AND-OR-TIME to an AnniversaryDate,
// RFC 9555 section 2.2.2.
//
// The text form has nowhere to go. RFC 6350 section 6.2.5 permits a BDAY like
// "circa 1800", and JSContact's PartialDate has no free-text arm -- section
// 2.2.2 converts only the date forms. A text-valued birthday therefore
// produces no anniversary at all rather than a wrong date, and the value
// belongs in vCardProps.
func dateOrTextToCard(d *vcardv1.DateOrText) *cardv1.AnniversaryDate {
	if date := d.GetDate(); date != nil {
		return &cardv1.AnniversaryDate{
			Value: &cardv1.AnniversaryDate_PartialDate{PartialDate: date},
		}
	}
	return nil
}

// dateToVcard is the inverse. A Timestamp-valued anniversary is not
// converted: RFC 6350's DATE-AND-OR-TIME has no instant form, and rendering
// one as a date would silently change what was said.
func dateToVcard(d *cardv1.AnniversaryDate) *vcardv1.DateOrText {
	if pd := d.GetPartialDate(); pd != nil {
		return &vcardv1.DateOrText{
			Value: &vcardv1.DateOrText_Date{Date: pd},
		}
	}
	return nil
}

func relationTypesToCard(ts []vcardv1.RelationType) []cardv1.RelationType {
	var out []cardv1.RelationType
	for _, t := range ts {
		if v, ok := relationToCard[t]; ok {
			out = append(out, v)
		}
	}
	return out
}

func relationTypesToVcard(ts []cardv1.RelationType) []vcardv1.RelationType {
	var out []vcardv1.RelationType
	for _, t := range ts {
		if v, ok := relationToVcard[t]; ok {
			out = append(out, v)
		}
	}
	return out
}

// relationToCard maps RELATED's TYPE values, RFC 9555 section 2.9.5.
//
// Both sides draw from the same IANA registry, so this is a rename rather
// than a mapping decision. A value in one and not the other is omitted rather
// than approximated.
var relationToCard = map[vcardv1.RelationType]cardv1.RelationType{
	vcardv1.RelationType_RELATION_TYPE_ACQUAINTANCE: cardv1.RelationType_RELATION_TYPE_ACQUAINTANCE,
	vcardv1.RelationType_RELATION_TYPE_AGENT:        cardv1.RelationType_RELATION_TYPE_AGENT,
	vcardv1.RelationType_RELATION_TYPE_CHILD:        cardv1.RelationType_RELATION_TYPE_CHILD,
	vcardv1.RelationType_RELATION_TYPE_CO_RESIDENT:  cardv1.RelationType_RELATION_TYPE_CO_RESIDENT,
	vcardv1.RelationType_RELATION_TYPE_CO_WORKER:    cardv1.RelationType_RELATION_TYPE_CO_WORKER,
	vcardv1.RelationType_RELATION_TYPE_COLLEAGUE:    cardv1.RelationType_RELATION_TYPE_COLLEAGUE,
	vcardv1.RelationType_RELATION_TYPE_CONTACT:      cardv1.RelationType_RELATION_TYPE_CONTACT,
	vcardv1.RelationType_RELATION_TYPE_CRUSH:        cardv1.RelationType_RELATION_TYPE_CRUSH,
	vcardv1.RelationType_RELATION_TYPE_DATE:         cardv1.RelationType_RELATION_TYPE_DATE,
	vcardv1.RelationType_RELATION_TYPE_EMERGENCY:    cardv1.RelationType_RELATION_TYPE_EMERGENCY,
	vcardv1.RelationType_RELATION_TYPE_FRIEND:       cardv1.RelationType_RELATION_TYPE_FRIEND,
	vcardv1.RelationType_RELATION_TYPE_KIN:          cardv1.RelationType_RELATION_TYPE_KIN,
	vcardv1.RelationType_RELATION_TYPE_ME:           cardv1.RelationType_RELATION_TYPE_ME,
	vcardv1.RelationType_RELATION_TYPE_MET:          cardv1.RelationType_RELATION_TYPE_MET,
	vcardv1.RelationType_RELATION_TYPE_MUSE:         cardv1.RelationType_RELATION_TYPE_MUSE,
	vcardv1.RelationType_RELATION_TYPE_NEIGHBOR:     cardv1.RelationType_RELATION_TYPE_NEIGHBOR,
	vcardv1.RelationType_RELATION_TYPE_PARENT:       cardv1.RelationType_RELATION_TYPE_PARENT,
	vcardv1.RelationType_RELATION_TYPE_SIBLING:      cardv1.RelationType_RELATION_TYPE_SIBLING,
	vcardv1.RelationType_RELATION_TYPE_SPOUSE:       cardv1.RelationType_RELATION_TYPE_SPOUSE,
	vcardv1.RelationType_RELATION_TYPE_SWEETHEART:   cardv1.RelationType_RELATION_TYPE_SWEETHEART,
}

var relationToVcard = func() map[cardv1.RelationType]vcardv1.RelationType {
	m := make(map[cardv1.RelationType]vcardv1.RelationType, len(relationToCard))
	for k, v := range relationToCard {
		m[v] = k
	}
	return m
}()

// grammaticalGenderToCard maps GRAMGENDER, RFC 9555 section 2.5.4. Both sides
// take the same six values from RFC 9554 section 3.2.
func grammaticalGenderToCard(g vcardv1.GrammaticalGenderKind) cardv1.GrammaticalGender {
	switch g {
	case vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_ANIMATE:
		return cardv1.GrammaticalGender_GRAMMATICAL_GENDER_ANIMATE
	case vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_COMMON:
		return cardv1.GrammaticalGender_GRAMMATICAL_GENDER_COMMON
	case vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_FEMININE:
		return cardv1.GrammaticalGender_GRAMMATICAL_GENDER_FEMININE
	case vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_INANIMATE:
		return cardv1.GrammaticalGender_GRAMMATICAL_GENDER_INANIMATE
	case vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_MASCULINE:
		return cardv1.GrammaticalGender_GRAMMATICAL_GENDER_MASCULINE
	case vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_NEUTER:
		return cardv1.GrammaticalGender_GRAMMATICAL_GENDER_NEUTER
	}
	return cardv1.GrammaticalGender_GRAMMATICAL_GENDER_UNSPECIFIED
}

func grammaticalGenderToVcard(g cardv1.GrammaticalGender) vcardv1.GrammaticalGenderKind {
	switch g {
	case cardv1.GrammaticalGender_GRAMMATICAL_GENDER_ANIMATE:
		return vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_ANIMATE
	case cardv1.GrammaticalGender_GRAMMATICAL_GENDER_COMMON:
		return vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_COMMON
	case cardv1.GrammaticalGender_GRAMMATICAL_GENDER_FEMININE:
		return vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_FEMININE
	case cardv1.GrammaticalGender_GRAMMATICAL_GENDER_INANIMATE:
		return vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_INANIMATE
	case cardv1.GrammaticalGender_GRAMMATICAL_GENDER_MASCULINE:
		return vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_MASCULINE
	case cardv1.GrammaticalGender_GRAMMATICAL_GENDER_NEUTER:
		return vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_NEUTER
	}
	return vcardv1.GrammaticalGenderKind_GRAMMATICAL_GENDER_KIND_UNSPECIFIED
}
