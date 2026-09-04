// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package contentline

import (
	"fmt"
	"strings"
)

type Line struct {
	Group string
	// name is upper-cased for matching, because section 3.3 makes property
	// names case-insensitive.
	Name string
	// rawName is the name exactly as written. An extension must be emitted
	// with the case it arrived in: Apple writes X-ABLabel, and echoing
	// X-ABLABEL is legal but is not the same bytes, which real consumers
	// notice.
	RawName string
	Params  map[string][]string
	Value   string
}

// unfold joins continuation lines. Section 3.2: "any sequence of CRLF
// followed immediately by a single white space character is ignored".
//
// The single space is part of the fold marker and is removed with it --
// dropping the CRLF but keeping the space is the classic bug, and it inserts
// a space into the middle of every folded value.
// Unfold joins continuation lines.
func Unfold(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	s = strings.ReplaceAll(s, "\n ", "")
	s = strings.ReplaceAll(s, "\n\t", "")

	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

// Parse splits one unfolded content line into its parts.
func Parse(raw string) (Line, error) {
	l := Line{Params: map[string][]string{}}

	// The value starts at the first colon that is not inside a quoted
	// parameter value, because a quoted param may legally contain one.
	colon, quoted := -1, false
	for i, r := range raw {
		switch {
		case r == '"':
			quoted = !quoted
		case r == ':' && !quoted:
			colon = i
		}
		if colon >= 0 {
			break
		}
	}
	if colon < 0 {
		return l, fmt.Errorf("content line has no value separator: %q", raw)
	}
	l.Value = raw[colon+1:]

	parts := SplitUnescaped(raw[:colon], ';')
	if len(parts) == 0 || parts[0] == "" {
		return l, fmt.Errorf("content line has no name: %q", raw)
	}

	// [group "."] name
	nameField := parts[0]
	if dot := strings.IndexByte(nameField, '.'); dot >= 0 {
		l.Group, nameField = nameField[:dot], nameField[dot+1:]
	}
	l.RawName = nameField
	l.Name = strings.ToUpper(nameField)

	for _, p := range parts[1:] {
		k, v, found := strings.Cut(p, "=")
		if !found {
			// Section 5: a bare parameter is shorthand for TYPE=value.
			k, v = "TYPE", p
		}
		k = strings.ToUpper(strings.TrimSpace(k))
		// Split before unquoting, not after. SplitUnescaped already honors
		// quoting, so a comma inside a quoted-string stays inside its value --
		// which is what both RFCs require. RFC 5545 section 3.1 is explicit:
		// "Property parameter values that contain the COLON, SEMICOLON, or
		// COMMA character separators MUST be specified as quoted-string text
		// values." Unquoting first and splitting after turned CN="Doe, John"
		// into the single value "Doe", silently losing the given name.
		for _, one := range SplitUnescaped(strings.TrimSpace(v), ',') {
			one = strings.TrimSpace(one)
			quoted := len(one) >= 2 && one[0] == '"' && one[len(one)-1] == '"'
			if quoted {
				one = one[1 : len(one)-1]
			}
			// One documented departure from the ABNF, for the multi-valued
			// parameters only. RFC 6350 section 6.4.1's own example is
			// TEL;TYPE="work,voice" meaning two types, which its section 3.3
			// grammar does not actually permit -- the example and the ABNF
			// disagree. Real vCards follow the example, so a quoted value of a
			// parameter the RFCs define as a list is split again here. Every
			// other parameter is free text and is never split.
			if quoted && multiValuedParams[k] {
				for _, sub := range SplitUnescaped(one, ',') {
					if sub = strings.TrimSpace(sub); sub != "" {
						l.Params[k] = append(l.Params[k], sub)
					}
				}
				continue
			}
			if one != "" {
				l.Params[k] = append(l.Params[k], one)
			}
		}
	}
	return l, nil
}

// SplitUnescaped splits on sep, honoring backslash escapes and quoting.
func SplitUnescaped(s string, sep byte) []string {
	var out []string
	var cur strings.Builder
	quoted := false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' && i+1 < len(s):
			cur.WriteByte(c)
			i++
			cur.WriteByte(s[i])
		case c == '"':
			quoted = !quoted
			cur.WriteByte(c)
		case c == sep && !quoted:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	return append(out, cur.String())
}

// multiValuedParams are the parameters the RFCs define as a comma-separated
// list rather than free text. Only these are split inside a quoted-string;
// see Parse for why that departure exists at all.
//
// vCard RFC 6350: TYPE (section 5.6), PID (section 5.5), SORT-AS (section 5.9).
// iCalendar RFC 5545: DELEGATED-FROM (section 3.2.4), DELEGATED-TO
// (section 3.2.5), MEMBER (section 3.2.11).
//
// Everything absent from this set -- CN, LABEL, LANGUAGE, ALTREP, DIR,
// SENT-BY, FMTTYPE, TZID -- holds one value that may legitimately contain a
// comma.
var multiValuedParams = map[string]bool{
	"TYPE":           true,
	"PID":            true,
	"SORT-AS":        true,
	"DELEGATED-FROM": true,
	"DELEGATED-TO":   true,
	"MEMBER":         true,
}
