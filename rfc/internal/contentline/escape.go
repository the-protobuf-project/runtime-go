// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package contentline

import "strings"

// Unescape reverses TEXT escaping: RFC 6350 section 3.4 and RFC 5545
// section 3.3.11 define the same escape set.
//
// Both \n and \N mean a newline; the RFC permits either case and real
// exporters emit both.
func Unescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 'n', 'N':
			b.WriteByte('\n')
		default:
			// Section 3.4 escapes backslash, comma and semicolon. Anything
			// else keeps its literal character: dropping the backslash and
			// keeping the char is what conforming parsers do.
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// Escape applies TEXT escaping: RFC 6350 section 3.4, RFC 5545 section 3.3.11.
//
// Comma is escaped "even for properties that don't allow multiple instances
// (for consistency)", so this is unconditional. Semicolon likewise: the RFC
// requires it in compound values and permits it elsewhere, and escaping
// always is both legal and simpler than tracking which property is compound.
func Escape(s string) string {
	r := strings.NewReplacer(
		`\`, `\\`,
		"\n", `\n`,
		`,`, `\,`,
		`;`, `\;`,
	)
	return r.Replace(s)
}

// SplitCompound splits a compound value on unescaped semicolons and unescapes
// each component. Used for N and ADR, section 6.2.2 and 6.3.1.
func SplitCompound(s string) []string {
	parts := SplitUnescaped(s, ';')
	for i := range parts {
		parts[i] = Unescape(parts[i])
	}
	return parts
}

// SplitList splits a multi-valued component on unescaped commas.
func SplitList(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range SplitUnescaped(s, ',') {
		if p = Unescape(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// JoinList is the inverse of SplitList.
func JoinList(vs []string) string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = Escape(v)
	}
	return strings.Join(out, ",")
}

// Fold breaks a content line at 75 octets: RFC 6350 section 3.2 and RFC 5545
// section 3.1 give the same limit and continuation marker.
//
// Octets, not runes: the limit is a byte limit, but a Fold must not split a
// multi-byte UTF-8 sequence, so continuation bytes are carried to the next
// line. A naive byte-slice here produces output that looks fine in ASCII and
// corrupts every non-Latin name.
func Fold(s string) string {
	const limit = 75
	if len(s) <= limit {
		return s
	}
	var b strings.Builder
	start := 0
	for start < len(s) {
		width := limit
		if start > 0 {
			width = limit - 1 // the leading space counts toward the limit
		}
		end := start + width
		if end >= len(s) {
			end = len(s)
		} else {
			for end > start && s[end]&0xC0 == 0x80 {
				end--
			}
		}
		if start > 0 {
			b.WriteString("\r\n ")
		}
		b.WriteString(s[start:end])
		start = end
	}
	return b.String()
}

// EscapeParam renders a parameter value, quoting it when the RFC requires.
//
// RFC 5545 section 3.1: "Property parameter values that contain the COLON,
// SEMICOLON, or COMMA character separators MUST be specified as quoted-string
// text values. Property parameter values MUST NOT contain the DQUOTE
// character." RFC 6350 section 3.3 says the same. A DQUOTE in the value has
// nowhere to go, so it is dropped rather than emitted unescaped, which would
// produce a line no conforming parser could read.
func EscapeParam(s string) string {
	s = strings.ReplaceAll(s, `"`, "")
	if strings.ContainsAny(s, ":;,") {
		return `"` + s + `"`
	}
	return s
}
