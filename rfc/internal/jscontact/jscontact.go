// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package jscontact

import (
	"fmt"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"
)

// FromVcard converts a vCard Contact to a JSContact Card, RFC 9555 section 2.
//
// The Contact carries no identity of its own -- Vcard does -- so the caller
// is responsible for moving `vcard_uid` onto the Card's `jscontact_uid`, and
// for the resource fields either side.
func FromVcard(c *vcardv1.Contact) (*cardv1.Card, error) {
	if c == nil {
		return nil, fmt.Errorf("cannot convert a nil contact")
	}
	card := &cardv1.Card{
		Kind:     kindToCard(c.GetKind()),
		Keywords: c.GetCategories(),
	}

	// FN, section 2.5.2, and N, section 2.5.5, both land in `name`.
	if n := nameToCard(c); n != nil {
		card.NameComponents = n
	}
	if v := c.GetDefaultLanguageCode(); v != "" {
		card.LanguageCode = v
	}

	addressesToCard(c, card)
	contactMethodsToCard(c, card)
	organizationsToCard(c, card)
	personalToCard(c, card)
	speechToCard(c, card)

	return card, nil
}

// ToVcard converts a JSContact Card back to a vCard Contact, RFC 9555
// section 3.
//
// Not the inverse of FromVcard in the strict sense, and section 1.1 is candid
// about why: the two models disagree about what is one property and what is
// several. TITLE and ROLE are separate vCard properties and one JSContact
// object keyed on `kind`; PHOTO, SOUND and LOGO are three and one. Round
// tripping preserves the data, not the encoding.
func ToVcard(card *cardv1.Card) (*vcardv1.Contact, error) {
	if card == nil {
		return nil, fmt.Errorf("cannot convert a nil card")
	}
	c := &vcardv1.Contact{
		Kind:       kindToVcard(card.GetKind()),
		Categories: card.GetKeywords(),
	}
	if v := card.GetLanguageCode(); v != "" {
		c.DefaultLanguageCode = v
	}

	nameToVcard(card, c)
	addressesToVcard(card, c)
	contactMethodsToVcard(card, c)
	organizationsToVcard(card, c)
	personalToVcard(card, c)
	speechToVcard(card, c)

	// Section 6.2.1 makes FN the one mandatory vCard property, and the
	// encoder rejects a Contact without it. A Card whose name has no `full`
	// still has to produce a usable vCard, so the components are assembled
	// rather than leaving the caller with something that cannot be encoded.
	if len(c.GetDisplayNames()) == 0 {
		if s := assembleDisplayName(card.GetNameComponents()); s != "" {
			c.DisplayNames = []string{s}
		}
	}
	return c, nil
}

// kindToCard maps KIND, RFC 9555 section 2.4.2.
func kindToCard(k vcardv1.Kind) cardv1.Kind {
	switch k {
	case vcardv1.Kind_KIND_INDIVIDUAL:
		return cardv1.Kind_KIND_INDIVIDUAL
	case vcardv1.Kind_KIND_GROUP:
		return cardv1.Kind_KIND_GROUP
	case vcardv1.Kind_KIND_ORG:
		return cardv1.Kind_KIND_ORG
	case vcardv1.Kind_KIND_LOCATION:
		return cardv1.Kind_KIND_LOCATION
	}
	return cardv1.Kind_KIND_UNSPECIFIED
}

// kindToVcard is the inverse. JSContact adds "device" and "application",
// which RFC 6350 section 6.1.4 has no value for; section 2.4.2 leaves the
// vCard KIND unset rather than inventing one, and the value survives in
// jscontact_props.
func kindToVcard(k cardv1.Kind) vcardv1.Kind {
	switch k {
	case cardv1.Kind_KIND_INDIVIDUAL:
		return vcardv1.Kind_KIND_INDIVIDUAL
	case cardv1.Kind_KIND_GROUP:
		return vcardv1.Kind_KIND_GROUP
	case cardv1.Kind_KIND_ORG:
		return vcardv1.Kind_KIND_ORG
	case cardv1.Kind_KIND_LOCATION:
		return vcardv1.Kind_KIND_LOCATION
	}
	return vcardv1.Kind_KIND_UNSPECIFIED
}

// id generates a map key, RFC 9555 section 2.1.2.
//
// Positional and deterministic: the same input yields the same keys, so a
// conversion run twice produces the same Card. Section 1.4.1 of RFC 9553
// restricts an Id to the base64url alphabet, which this satisfies.
func id(prefix string, i int) string {
	return fmt.Sprintf("%s%d", prefix, i+1)
}
