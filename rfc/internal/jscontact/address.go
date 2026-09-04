// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package jscontact

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"
)

// ADR, RFC 9555 section 2.6.1.
//
// The table there has a conditional the other property mappings do not:
// vCard's `extended address` and `street address` convert to "apartment" and
// "name" **only if the ADR is in RFC 6350's original seven-component form**.
// If RFC 9554's extended components are present, those two legacy slots are
// ignored, because the same information is in the new components and reading
// both would duplicate it.
//
// So the conversion has to decide which format it is looking at before it can
// map anything -- `usesExtendedFormat` below -- and that decision is the
// whole difficulty of this file.

// usesExtendedFormat reports whether an Address carries any of RFC 9554
// section 2.1's components, which is what section 2.6.1's "if the ADR
// structured value is of the format defined in [RFC9554]" turns on.
func usesExtendedFormat(a *vcardv1.Address) bool {
	return len(a.GetRooms()) > 0 || len(a.GetApartments()) > 0 ||
		len(a.GetFloors()) > 0 || len(a.GetStreetNumbers()) > 0 ||
		len(a.GetStreets()) > 0 || len(a.GetBuildings()) > 0 ||
		len(a.GetBlocks()) > 0 || len(a.GetSubdistricts()) > 0 ||
		len(a.GetDistricts()) > 0 || len(a.GetLandmarks()) > 0 ||
		len(a.GetDirections()) > 0
}

func addressesToCard(c *vcardv1.Contact, card *cardv1.Card) {
	for i, a := range c.GetAddresses() {
		out := &cardv1.Address{
			Pref:           a.GetPref(),
			FullAddress:    a.GetLabel(),
			Contexts:       contextsToCard(a.GetTypes()),
			PhoneticSystem: phoneticToCard(a.GetPhonetic()),
			PhoneticScript: a.GetScript(),
		}

		add := func(kind cardv1.AddressComponentKind, values []string) {
			for _, v := range values {
				if v == "" {
					continue
				}
				out.Components = append(out.Components, &cardv1.AddressComponent{
					Kind:  kind,
					Value: v,
				})
			}
		}
		add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_POST_OFFICE_BOX, a.GetPostOfficeBox())

		if usesExtendedFormat(a) {
			// RFC 9554's components map one to one. The legacy `extended
			// address` and `street address` slots are ignored here, exactly
			// as section 2.6.1 requires -- their content is already in these.
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_ROOM, a.GetRooms())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_APARTMENT, a.GetApartments())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_FLOOR, a.GetFloors())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_BUILDING, a.GetBuildings())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NUMBER, a.GetStreetNumbers())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NAME, a.GetStreets())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_BLOCK, a.GetBlocks())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_SUBDISTRICT, a.GetSubdistricts())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_DISTRICT, a.GetDistricts())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_LANDMARK, a.GetLandmarks())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_DIRECTION, a.GetDirections())
		} else {
			// The RFC 6350 form: two positional slots stand in for all of it.
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_APARTMENT, a.GetExtendedAddresses())
			add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NAME, a.GetStreetAddresses())
		}

		add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_LOCALITY, a.GetLocalities())
		add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_REGION, a.GetRegions())
		add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_POSTCODE, a.GetPostalCodes())
		add(cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_COUNTRY, a.GetCountries())

		if card.Addresses == nil {
			card.Addresses = map[string]*cardv1.Address{}
		}
		card.Addresses[id("adr", i)] = out
	}
}

func addressesToVcard(card *cardv1.Card, c *vcardv1.Contact) {
	for _, key := range sortedKeys(card.GetAddresses()) {
		a := card.GetAddresses()[key]
		out := &vcardv1.Address{
			Pref:     a.GetPref(),
			Label:    a.GetFullAddress(),
			Types:    contextsToVcard(a.GetContexts()),
			Phonetic: phoneticToVcard(a.GetPhoneticSystem()),
			Script:   a.GetPhoneticScript(),
		}
		for _, comp := range a.GetComponents() {
			v := comp.GetValue()
			switch comp.GetKind() {
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_POST_OFFICE_BOX:
				out.PostOfficeBox = append(out.PostOfficeBox, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_ROOM:
				out.Rooms = append(out.Rooms, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_APARTMENT:
				out.Apartments = append(out.Apartments, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_FLOOR:
				out.Floors = append(out.Floors, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_BUILDING:
				out.Buildings = append(out.Buildings, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NUMBER:
				out.StreetNumbers = append(out.StreetNumbers, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NAME:
				out.Streets = append(out.Streets, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_BLOCK:
				out.Blocks = append(out.Blocks, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_SUBDISTRICT:
				out.Subdistricts = append(out.Subdistricts, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_DISTRICT:
				out.Districts = append(out.Districts, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_LANDMARK:
				out.Landmarks = append(out.Landmarks, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_DIRECTION:
				out.Directions = append(out.Directions, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_LOCALITY:
				out.Localities = append(out.Localities, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_REGION:
				out.Regions = append(out.Regions, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_POSTCODE:
				out.PostalCodes = append(out.PostalCodes, v)
			case cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_COUNTRY:
				out.Countries = append(out.Countries, v)
			}
			// A "separator" component is dropped; it exists for JSCOMPS,
			// which vCard's ADR has no room for here.
		}

		// Section 2.6.1's "To vCard" column: the legacy positional slots are
		// populated as well, so a reader that predates RFC 9554 still sees a
		// usable street address. addressesToCard ignores them whenever the
		// extended components are present, which is what stops the duplicate
		// coming back.
		out.StreetAddresses = append(out.StreetAddresses, out.GetStreets()...)
		out.ExtendedAddresses = append(out.ExtendedAddresses, out.GetApartments()...)

		c.Addresses = append(c.Addresses, out)
	}
}

// contextsToCard maps the TYPE parameter, RFC 9555 section 2.3.22.
func contextsToCard(types []vcardv1.Type) []cardv1.Context {
	var out []cardv1.Context
	for _, t := range types {
		switch t {
		case vcardv1.Type_TYPE_WORK:
			out = append(out, cardv1.Context_CONTEXT_WORK)
		case vcardv1.Type_TYPE_HOME:
			out = append(out, cardv1.Context_CONTEXT_PRIVATE)
		case vcardv1.Type_TYPE_BILLING:
			out = append(out, cardv1.Context_CONTEXT_BILLING)
		case vcardv1.Type_TYPE_DELIVERY:
			out = append(out, cardv1.Context_CONTEXT_DELIVERY)
		}
	}
	return out
}

// contextsToVcard is the inverse. Note the asymmetry section 2.3.22 defines:
// vCard's "home" is JSContact's "private", not a value of its own.
func contextsToVcard(contexts []cardv1.Context) []vcardv1.Type {
	var out []vcardv1.Type
	for _, ctx := range contexts {
		switch ctx {
		case cardv1.Context_CONTEXT_WORK:
			out = append(out, vcardv1.Type_TYPE_WORK)
		case cardv1.Context_CONTEXT_PRIVATE:
			out = append(out, vcardv1.Type_TYPE_HOME)
		case cardv1.Context_CONTEXT_BILLING:
			out = append(out, vcardv1.Type_TYPE_BILLING)
		case cardv1.Context_CONTEXT_DELIVERY:
			out = append(out, vcardv1.Type_TYPE_DELIVERY)
		}
	}
	return out
}
