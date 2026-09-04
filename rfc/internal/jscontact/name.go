// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package jscontact

import (
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"
)

// N and FN, RFC 9555 sections 2.5.5 and 2.5.2.
//
// Table 1 of section 2.5.5 gives the component mapping, and two of its rows
// carry a merge rule that is easy to miss:
//
//   - vCard's Honorific suffix maps to "credential", *not* "generation" --
//     even though RFC 9554 added a separate Generation component.
//   - Converting to vCard, "surname2" values are appended to the Family name
//     component and "generation" values to the Honorific suffix, so a reader
//     that only understands RFC 6350 still sees the whole name. Converting
//     from vCard, a value that appears in both the merged and the dedicated
//     component is taken once.
//
// Getting that wrong duplicates a surname on every round trip, which is why
// the dedupe is explicit below rather than implied.

func nameToCard(c *vcardv1.Contact) *cardv1.Name {
	n := c.GetNameComponents()
	fn := c.GetDisplayNames()
	if n == nil && len(fn) == 0 {
		return nil
	}

	out := &cardv1.Name{}
	// FN, section 2.5.2. More than one is unexpected; the RFC says convert
	// one and leave the rest to vCardProps, so the first is taken.
	if len(fn) > 0 {
		out.DisplayName = fn[0]
	}
	if n == nil {
		return out
	}

	// Table 1. Order follows the N structured value left to right, which
	// section 2.5.5 requires when JSCOMPS is absent -- as it is here, since
	// the vCard model does not carry that parameter.
	secondary := n.GetSecondarySurnames()
	generations := n.GetGenerations()

	add := func(kind cardv1.NameComponentKind, values []string, skip []string) {
		for _, v := range values {
			if v == "" || contains(skip, v) {
				continue
			}
			out.Components = append(out.Components, &cardv1.NameComponent{
				Kind:  kind,
				Value: v,
			})
		}
	}

	// "From vCard: ignore any value that also occurs in the Secondary
	// surname component" -- otherwise a name merged on the way out comes
	// back doubled.
	add(cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME, n.GetFamilyName(), secondary)
	add(cardv1.NameComponentKind_NAME_COMPONENT_KIND_GIVEN, n.GetGivenName(), nil)
	add(cardv1.NameComponentKind_NAME_COMPONENT_KIND_GIVEN2, n.GetMiddleNames(), nil)
	add(cardv1.NameComponentKind_NAME_COMPONENT_KIND_TITLE, n.GetHonorificPrefixes(), nil)
	// The same rule for Honorific suffix and Generation.
	add(cardv1.NameComponentKind_NAME_COMPONENT_KIND_CREDENTIAL, n.GetHonorificSuffixes(), generations)
	add(cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME2, secondary, nil)
	add(cardv1.NameComponentKind_NAME_COMPONENT_KIND_GENERATION, generations, nil)

	// Section 2.5.5: with no JSCOMPS parameter, isOrdered is false and
	// defaultSeparator MUST NOT be set. The vCard model has no JSCOMPS, so
	// both stay at their zero values deliberately.

	// PHONETIC and SCRIPT, sections 2.3.15 and 2.3.19.
	out.PhoneticSystem = phoneticToCard(n.GetPhonetic())
	out.PhoneticScript = n.GetScript()
	return out
}

func nameToVcard(card *cardv1.Card, c *vcardv1.Contact) {
	n := card.GetNameComponents()
	if n == nil {
		return
	}
	if v := n.GetDisplayName(); v != "" {
		c.DisplayNames = []string{v}
	}
	if len(n.GetComponents()) == 0 {
		return
	}

	out := &vcardv1.Name{
		Phonetic: phoneticToVcard(n.GetPhoneticSystem()),
		Script:   n.GetPhoneticScript(),
	}
	for _, comp := range n.GetComponents() {
		v := comp.GetValue()
		switch comp.GetKind() {
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME:
			out.FamilyName = append(out.FamilyName, v)
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_GIVEN:
			out.GivenName = append(out.GivenName, v)
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_GIVEN2:
			out.MiddleNames = append(out.MiddleNames, v)
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_TITLE:
			out.HonorificPrefixes = append(out.HonorificPrefixes, v)
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_CREDENTIAL:
			out.HonorificSuffixes = append(out.HonorificSuffixes, v)
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME2:
			out.SecondarySurnames = append(out.SecondarySurnames, v)
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_GENERATION:
			out.Generations = append(out.Generations, v)
		}
		// A "separator" component is dropped: it only has meaning alongside
		// JSCOMPS, which the vCard model does not carry.
	}

	// Table 1's merge rule. A reader that understands only RFC 6350 sees the
	// second surname and the generation in the components it knows; a reader
	// that understands RFC 9554 sees them twice and must dedupe, which is
	// what nameToCard does on the way back.
	out.FamilyName = append(out.FamilyName, out.GetSecondarySurnames()...)
	out.HonorificSuffixes = append(out.HonorificSuffixes, out.GetGenerations()...)

	c.NameComponents = out
}

// assembleDisplayName builds an FN from components when the Card has no
// `full`, because RFC 6350 section 6.2.1 makes FN mandatory and the vCard
// encoder refuses a Contact without one.
//
// Deliberately naive -- given then surname, space separated. Section 2.2.1.1
// of RFC 9553 is explicit that assembling a name correctly needs `isOrdered`
// and the separator components, and inventing a better guess than the RFC
// offers would be worse than an obviously simple one.
func assembleDisplayName(n *cardv1.Name) string {
	if n == nil {
		return ""
	}
	if v := n.GetDisplayName(); v != "" {
		return v
	}
	var given, surname []string
	for _, comp := range n.GetComponents() {
		switch comp.GetKind() {
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_GIVEN:
			given = append(given, comp.GetValue())
		case cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME:
			surname = append(surname, comp.GetValue())
		}
	}
	return strings.TrimSpace(strings.Join(append(given, surname...), " "))
}

func phoneticToCard(p vcardv1.PhoneticSystem) cardv1.PhoneticSystem {
	switch p {
	case vcardv1.PhoneticSystem_PHONETIC_SYSTEM_IPA:
		return cardv1.PhoneticSystem_PHONETIC_SYSTEM_IPA
	case vcardv1.PhoneticSystem_PHONETIC_SYSTEM_PINY:
		return cardv1.PhoneticSystem_PHONETIC_SYSTEM_PINY
	case vcardv1.PhoneticSystem_PHONETIC_SYSTEM_JYUT:
		return cardv1.PhoneticSystem_PHONETIC_SYSTEM_JYUT
	}
	// PHONETIC_SYSTEM_SCRIPT has no JSContact equivalent: RFC 9553 carries
	// the script in `phoneticScript` instead, so the vCard value is implied
	// by that field rather than repeated here.
	return cardv1.PhoneticSystem_PHONETIC_SYSTEM_UNSPECIFIED
}

func phoneticToVcard(p cardv1.PhoneticSystem) vcardv1.PhoneticSystem {
	switch p {
	case cardv1.PhoneticSystem_PHONETIC_SYSTEM_IPA:
		return vcardv1.PhoneticSystem_PHONETIC_SYSTEM_IPA
	case cardv1.PhoneticSystem_PHONETIC_SYSTEM_PINY:
		return vcardv1.PhoneticSystem_PHONETIC_SYSTEM_PINY
	case cardv1.PhoneticSystem_PHONETIC_SYSTEM_JYUT:
		return vcardv1.PhoneticSystem_PHONETIC_SYSTEM_JYUT
	}
	return vcardv1.PhoneticSystem_PHONETIC_SYSTEM_UNSPECIFIED
}

func contains(haystack []string, needle string) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}
