// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// Decoders for the single-value TYPE/PREF properties: NICKNAME (6.2.3), URL
// (6.7.8), ROLE (6.6.2), IMPP (6.4.3), LANG (6.4.4) and GEO (6.5.2). Each
// message is the same shape as Email -- a value plus TYPE and PREF -- so
// encoding stays inline in contentLines rather than earning its own function
// per property, matching how Email and Address are written there.

// uriValued lists the properties whose VALUE type defaults to uri with no
// text alternative in their own ABNF, so their value is never escaped and
// jcardType/xcardValueType must not fall back to "text" for them.
var uriValued = map[string]bool{"URL": true, "GEO": true, "IMPP": true}

// unescapedTypes lists the jCard/xCard value-type identifiers that section
// 3.4's TEXT escaping does not apply to: URIs, BCP 47 language tags, and the
// utc-offset token TZ's third form uses.
var unescapedTypes = map[string]bool{"uri": true, "language-tag": true, "utc-offset": true}

func decodeNickname(l contentline.Line) *vcardv1.Nickname {
	return &vcardv1.Nickname{
		Values: contentline.SplitList(l.Value),
		Types:  decodeTypes(l),
		Pref:   decodePref(l),
	}
}

func decodeUrl(l contentline.Line) *vcardv1.Url {
	// Section 6.7.8's VALUE type is always uri: unescaped, unlike TEL which
	// has a text alternative.
	return &vcardv1.Url{Value: l.Value, Types: decodeTypes(l), Pref: decodePref(l)}
}

func decodeRole(l contentline.Line) *vcardv1.Role {
	return &vcardv1.Role{Value: contentline.Unescape(l.Value), Types: decodeTypes(l), Pref: decodePref(l)}
}

func decodeInstantMessage(l contentline.Line) *vcardv1.InstantMessage {
	return &vcardv1.InstantMessage{Value: l.Value, Types: decodeTypes(l), Pref: decodePref(l)}
}

func decodeLanguage(l contentline.Line) *vcardv1.Language {
	// Section 6.4.4's VALUE type is language-tag, section 4.3.6, a BCP 47
	// token with nothing section 3.4's TEXT escaping applies to.
	return &vcardv1.Language{Value: l.Value, Types: decodeTypes(l), Pref: decodePref(l)}
}

func decodeGeo(l contentline.Line) *vcardv1.Geo {
	return &vcardv1.Geo{Value: l.Value, Types: decodeTypes(l), Pref: decodePref(l)}
}
