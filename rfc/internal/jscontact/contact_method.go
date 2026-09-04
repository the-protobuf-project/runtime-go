// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package jscontact

import (
	"sort"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"
)

// EMAIL, TEL, IMPP, SOCIALPROFILE and LANG -- RFC 9555 sections 2.7.1,
// 2.7.6, 2.7.2, 2.7.5 and 2.7.3.
//
// The interesting case is that **two vCard properties converge on one
// JSContact property**: IMPP (RFC 6350 section 6.4.3) and SOCIALPROFILE (RFC
// 9554 section 3.5) both become `onlineServices`. Sections 2.7.2 and 2.7.5
// distinguish them on the way back by whether the entry has a `service`
// value, and that is the only signal available -- so a round trip can move an
// IMPP into SOCIALPROFILE or the reverse. The data survives; the property it
// arrives in may not.

func contactMethodsToCard(c *vcardv1.Contact, card *cardv1.Card) {
	for i, e := range c.GetEmails() {
		if card.Emails == nil {
			card.Emails = map[string]*cardv1.EmailAddress{}
		}
		card.Emails[id("e", i)] = &cardv1.EmailAddress{
			Address:  e.GetValue(),
			Pref:     e.GetPref(),
			Contexts: contextsToCard(e.GetTypes()),
		}
	}

	for i, t := range c.GetTelephones() {
		if card.Phones == nil {
			card.Phones = map[string]*cardv1.Phone{}
		}
		// Section 2.7.6: the value converts as-is, whether it is a tel: URI
		// or free text. JSContact puts no grammar on it either.
		number := t.GetUri()
		if number == "" {
			number = t.GetText()
		}
		card.Phones[id("p", i)] = &cardv1.Phone{
			Number:   number,
			Pref:     t.GetPref(),
			Contexts: contextsToCard(t.GetTypes()),
			Features: featuresToCard(t.GetFeatures()),
		}
	}

	// IMPP, section 2.7.2.
	n := 0
	for _, m := range c.GetInstantMessages() {
		if card.OnlineServices == nil {
			card.OnlineServices = map[string]*cardv1.OnlineService{}
		}
		card.OnlineServices[id("os", n)] = &cardv1.OnlineService{
			Uri:      m.GetValue(),
			Pref:     m.GetPref(),
			Contexts: contextsToCard(m.GetTypes()),
		}
		n++
	}
	// SOCIALPROFILE, section 2.7.5, into the same map.
	for _, s := range c.GetSocialProfiles() {
		if card.OnlineServices == nil {
			card.OnlineServices = map[string]*cardv1.OnlineService{}
		}
		os := &cardv1.OnlineService{
			Service:  s.GetServiceType(),
			Username: s.GetUsername(),
			Pref:     s.GetPref(),
			Contexts: contextsToCard(s.GetTypes()),
		}
		if u := s.GetUri(); u != "" {
			os.Uri = u
		} else {
			// A TEXT-valued SOCIALPROFILE has no URI. Section 2.7.5 keeps the
			// handle in `user`, which is where it already is.
			os.Username = firstNonEmpty(s.GetUsername(), s.GetText())
		}
		card.OnlineServices[id("os", n)] = os
		n++
	}

	// LANG, section 2.7.3.
	for i, l := range c.GetLanguages() {
		if card.PreferredLanguages == nil {
			card.PreferredLanguages = map[string]*cardv1.LanguagePref{}
		}
		card.PreferredLanguages[id("lang", i)] = &cardv1.LanguagePref{
			LanguageCode: l.GetValue(),
			Pref:         l.GetPref(),
			Contexts:     contextsToCard(l.GetTypes()),
		}
	}

	// URL, section 2.11.9, becomes a Link.
	for i, u := range c.GetUrls() {
		if card.Links == nil {
			card.Links = map[string]*cardv1.Link{}
		}
		card.Links[id("l", i)] = &cardv1.Link{
			Uri:      u.GetValue(),
			Pref:     u.GetPref(),
			Contexts: contextsToCard(u.GetTypes()),
		}
	}
}

func contactMethodsToVcard(card *cardv1.Card, c *vcardv1.Contact) {
	for _, k := range sortedKeys(card.GetEmails()) {
		e := card.GetEmails()[k]
		c.Emails = append(c.Emails, &vcardv1.Email{
			Value: e.GetAddress(),
			Pref:  e.GetPref(),
			Types: contextsToVcard(e.GetContexts()),
		})
	}

	for _, k := range sortedKeys(card.GetPhones()) {
		p := card.GetPhones()[k]
		t := &vcardv1.Telephone{
			Pref:     p.GetPref(),
			Types:    contextsToVcard(p.GetContexts()),
			Features: featuresToVcard(p.GetFeatures()),
		}
		// Section 6.4.1 says a TEL SHOULD be a tel: URI, and the vCard model
		// keeps the two forms in a oneof, so the shape is decided here rather
		// than guessed by the encoder.
		if strings.HasPrefix(strings.ToLower(p.GetNumber()), "tel:") {
			t.Value = &vcardv1.Telephone_Uri{Uri: p.GetNumber()}
		} else {
			t.Value = &vcardv1.Telephone_Text{Text: p.GetNumber()}
		}
		c.Telephones = append(c.Telephones, t)
	}

	for _, k := range sortedKeys(card.GetOnlineServices()) {
		os := card.GetOnlineServices()[k]
		// Sections 2.7.2 and 2.7.5: an entry naming a service is a
		// SOCIALPROFILE, one that does not is an IMPP. That is the only
		// distinction available, and it is the RFC's own.
		if os.GetService() != "" || os.GetUsername() != "" {
			sp := &vcardv1.SocialProfile{
				ServiceType: os.GetService(),
				Username:    os.GetUsername(),
				Pref:        os.GetPref(),
				Types:       contextsToVcard(os.GetContexts()),
			}
			if u := os.GetUri(); u != "" {
				sp.Value = &vcardv1.SocialProfile_Uri{Uri: u}
			} else {
				sp.Value = &vcardv1.SocialProfile_Text{Text: os.GetUsername()}
			}
			c.SocialProfiles = append(c.SocialProfiles, sp)
			continue
		}
		c.InstantMessages = append(c.InstantMessages, &vcardv1.InstantMessage{
			Value: os.GetUri(),
			Pref:  os.GetPref(),
			Types: contextsToVcard(os.GetContexts()),
		})
	}

	for _, k := range sortedKeys(card.GetPreferredLanguages()) {
		l := card.GetPreferredLanguages()[k]
		c.Languages = append(c.Languages, &vcardv1.Language{
			Value: l.GetLanguageCode(),
			Pref:  l.GetPref(),
			Types: contextsToVcard(l.GetContexts()),
		})
	}

	for _, k := range sortedKeys(card.GetLinks()) {
		l := card.GetLinks()[k]
		c.Urls = append(c.Urls, &vcardv1.Url{
			Value: l.GetUri(),
			Pref:  l.GetPref(),
			Types: contextsToVcard(l.GetContexts()),
		})
	}
}

func featuresToCard(fs []vcardv1.Feature) []cardv1.PhoneFeature {
	var out []cardv1.PhoneFeature
	for _, f := range fs {
		switch f {
		case vcardv1.Feature_FEATURE_TEXT:
			out = append(out, cardv1.PhoneFeature_PHONE_FEATURE_TEXT)
		case vcardv1.Feature_FEATURE_VOICE:
			out = append(out, cardv1.PhoneFeature_PHONE_FEATURE_VOICE)
		case vcardv1.Feature_FEATURE_FAX:
			out = append(out, cardv1.PhoneFeature_PHONE_FEATURE_FAX)
		case vcardv1.Feature_FEATURE_CELL:
			out = append(out, cardv1.PhoneFeature_PHONE_FEATURE_MOBILE)
		case vcardv1.Feature_FEATURE_VIDEO:
			out = append(out, cardv1.PhoneFeature_PHONE_FEATURE_VIDEO)
		case vcardv1.Feature_FEATURE_PAGER:
			out = append(out, cardv1.PhoneFeature_PHONE_FEATURE_PAGER)
		case vcardv1.Feature_FEATURE_TEXTPHONE:
			out = append(out, cardv1.PhoneFeature_PHONE_FEATURE_TEXTPHONE)
		}
	}
	return out
}

// featuresToVcard is the inverse. Note "main-number" has no vCard TYPE value
// -- RFC 6350 section 6.4.1 registers no equivalent -- so it is dropped here
// rather than mapped to something that means less.
func featuresToVcard(fs []cardv1.PhoneFeature) []vcardv1.Feature {
	var out []vcardv1.Feature
	for _, f := range fs {
		switch f {
		case cardv1.PhoneFeature_PHONE_FEATURE_TEXT:
			out = append(out, vcardv1.Feature_FEATURE_TEXT)
		case cardv1.PhoneFeature_PHONE_FEATURE_VOICE:
			out = append(out, vcardv1.Feature_FEATURE_VOICE)
		case cardv1.PhoneFeature_PHONE_FEATURE_FAX:
			out = append(out, vcardv1.Feature_FEATURE_FAX)
		case cardv1.PhoneFeature_PHONE_FEATURE_MOBILE:
			out = append(out, vcardv1.Feature_FEATURE_CELL)
		case cardv1.PhoneFeature_PHONE_FEATURE_VIDEO:
			out = append(out, vcardv1.Feature_FEATURE_VIDEO)
		case cardv1.PhoneFeature_PHONE_FEATURE_PAGER:
			out = append(out, vcardv1.Feature_FEATURE_PAGER)
		case cardv1.PhoneFeature_PHONE_FEATURE_TEXTPHONE:
			out = append(out, vcardv1.Feature_FEATURE_TEXTPHONE)
		}
	}
	return out
}

// sortedKeys returns a map's keys in order.
//
// Go randomizes map iteration, and a conversion that emitted entries in a
// different order each run would make every round-trip comparison flaky. This
// repository has already shipped that bug once, in the VAVAILABILITY codec --
// see docs/codec-findings-calendar.md.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}
