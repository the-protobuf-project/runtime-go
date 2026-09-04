// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// Codec for Timezone, TZ's three-form value, section 6.5.1. Unlike TEL's
// tel: prefix, none of the three forms is detectable from the value alone --
// a bare URI could be any scheme -- so the VALUE parameter is written
// explicitly for the two non-default forms and read back the same way,
// rather than sniffed.

// encodeTimezone renders the value and the VALUE parameter its form needs.
func encodeTimezone(t *vcardv1.Timezone) (value, valueParam string) {
	switch v := t.GetValue().(type) {
	case *vcardv1.Timezone_Text:
		// Section 6.5.1's default; no parameter needed.
		return contentline.Escape(v.Text), ""
	case *vcardv1.Timezone_Uri:
		return v.Uri, ";VALUE=uri"
	case *vcardv1.Timezone_UtcOffset:
		return v.UtcOffset, ";VALUE=utc-offset"
	}
	return "", ""
}

func decodeTimezone(l contentline.Line) *vcardv1.Timezone {
	tz := &vcardv1.Timezone{Types: decodeTypes(l), Pref: decodePref(l)}
	switch valueType(l) {
	case "uri":
		tz.Value = &vcardv1.Timezone_Uri{Uri: l.Value}
	case "utc-offset":
		tz.Value = &vcardv1.Timezone_UtcOffset{UtcOffset: l.Value}
	default:
		tz.Value = &vcardv1.Timezone_Text{Text: contentline.Unescape(l.Value)}
	}
	return tz
}

// valueType reads the VALUE parameter, defaulting to "text" when absent --
// the default every property that uses this helper falls back to unless its
// own ABNF says otherwise (uriValued, LANG). Not used by RELATED: section
// 6.6.6 defaults to uri instead, so decodeRelation checks VALUE directly.
func valueType(l contentline.Line) string {
	if v := l.Params["VALUE"]; len(v) > 0 {
		return strings.ToLower(v[0])
	}
	return "text"
}
