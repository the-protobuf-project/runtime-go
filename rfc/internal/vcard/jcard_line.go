// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"fmt"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"strings"
)

// jcardToLine converts one jCard property array into a content line, so the
// shared semantic decoder can consume it unchanged.
//
//	[name, parameters, type, value...]   RFC 7095 section 3.3
func jcardToLine(raw any) (contentline.Line, error) {
	arr, ok := raw.([]any)
	if !ok || len(arr) < 4 {
		return contentline.Line{}, fmt.Errorf("jcard property must be an array of at least 4 elements, got %v", raw)
	}

	name, ok := arr[0].(string)
	if !ok {
		return contentline.Line{}, fmt.Errorf("jcard property name is not a string: %v", arr[0])
	}

	l := contentline.Line{Params: map[string][]string{}}
	// A group is carried in the name as "group.property", section 3.3.1.1.
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		l.Group, name = name[:dot], name[dot+1:]
	}
	l.RawName = name
	l.Name = strings.ToUpper(name)

	if params, ok := arr[1].(map[string]any); ok {
		for k, v := range params {
			key := strings.ToUpper(k)
			switch t := v.(type) {
			case string:
				l.Params[key] = []string{t}
			case []any:
				for _, one := range t {
					if s, ok := one.(string); ok {
						l.Params[key] = append(l.Params[key], s)
					}
				}
			}
		}
	}

	valueType, _ := arr[2].(string)
	valueType = strings.ToLower(valueType)

	// Section 5.2: "If the value type specified in jCard is set to 'unknown',
	// the 'VALUE' parameter MUST NOT be specified. The value MUST be taken
	// over in vCard without processing." So exactly one raw string, copied
	// through, and no VALUE=unknown invented on the way -- that identifier is
	// jCard's alone and section 5 reserves it out of vCard entirely.
	if valueType == "unknown" {
		if len(arr) != 4 {
			return contentline.Line{}, fmt.Errorf("%s: an unknown value takes exactly one value, got %d", l.Name, len(arr)-3)
		}
		v, ok := arr[3].(string)
		if !ok {
			return contentline.Line{}, fmt.Errorf("%s: an unknown value must be a string, got %v", l.Name, arr[3])
		}
		l.Value = v
		return l, nil
	}

	// arr[2] is the value-type identifier. It is recorded as a VALUE
	// parameter only when it is not the default, so that a round trip does
	// not invent a VALUE=text on every property. date-and-or-time is BDAY and
	// ANNIVERSARY's own default, so it is not written back either.
	if valueType != "" && valueType != "text" && valueType != "date-and-or-time" {
		if _, seen := l.Params["VALUE"]; !seen {
			l.Params["VALUE"] = []string{valueType}
		}
	}

	l.Value = jcardValue(l.Name, valueType, arr[3:])
	// The inverse of basicToExtendedDate: jCard carries section 3.5.3's
	// extended form, the content line carries vCard's basic one.
	if valueType == "date-and-or-time" || valueType == "date" || valueType == "date-time" {
		l.Value = extendedToBasicDate(l.Value)
	}
	return l, nil
}

// extendedToBasicDate is basicToExtendedDate's inverse. As there, a value it
// does not recognize passes through unchanged rather than half-converted.
func extendedToBasicDate(v string) string {
	d, t, hasTime := strings.Cut(v, "T")

	var out string
	switch {
	case strings.HasPrefix(d, "---") && len(d) == 5: // ---DD
		out = d
	case strings.HasPrefix(d, "--") && len(d) == 7 && d[4] == '-': // --MM-DD
		out = "--" + d[2:4] + d[5:7]
	case strings.HasPrefix(d, "--") && len(d) == 4: // --MM
		out = d
	case len(d) == 10 && d[4] == '-' && d[7] == '-': // YYYY-MM-DD
		out = d[0:4] + d[5:7] + d[8:10]
	case len(d) == 7 && d[4] == '-': // YYYY-MM
		out = d
	case len(d) == 4 && isDigits(d): // YYYY
		out = d
	case d == "":
		out = ""
	default:
		return v
	}
	if !hasTime {
		return out
	}

	zone := ""
	if i := strings.IndexAny(t, "Z+"); i > 0 {
		zone, t = t[i:], t[:i]
	} else if i := strings.LastIndex(t, "-"); i > 0 {
		zone, t = t[i:], t[:i]
	}
	switch {
	case len(t) == 8 && t[2] == ':' && t[5] == ':': // HH:MM:SS
		t = t[0:2] + t[3:5] + t[6:8]
	case len(t) == 5 && t[2] == ':': // HH:MM
		t = t[0:2] + t[3:5]
	case len(t) == 2 && isDigits(t):
	default:
		return v
	}
	return out + "T" + t + strings.ReplaceAll(zone, ":", "")
}

// jcardValue renders jCard's JSON value back into vCard's text form:
// components joined by semicolons, multiple values by commas.
//
// A uri, language-tag or utc-offset value is never escaped. Section 3.4's
// escaping rules cover text values only, and section 6.4.1's own example
// writes "tel:+1-555-555-5555;ext=5555" with a bare semicolon -- escaping it
// would change the URI. This is the one place the two encodings could
// disagree about a value, and the cross-format test is what found it.
func jcardValue(name, valueType string, vals []any) string {
	if unescapedTypes[valueType] && len(vals) == 1 {
		if s, ok := vals[0].(string); ok {
			return s
		}
	}
	// A structured value arrives as a single nested array.
	if len(vals) == 1 {
		if comps, ok := vals[0].([]any); ok && structured[name] {
			parts := make([]string, len(comps))
			for i, c := range comps {
				parts[i] = jcardComponent(c)
			}
			return strings.Join(parts, ";")
		}
	}
	// Otherwise every element is one value of a multi-valued property.
	parts := make([]string, 0, len(vals))
	for _, v := range vals {
		parts = append(parts, jcardComponent(v))
	}
	return strings.Join(parts, ",")
}

// jcardComponent renders one component, which may itself hold several values.
func jcardComponent(v any) string {
	switch t := v.(type) {
	case string:
		return contentline.Escape(t)
	case []any:
		parts := make([]string, len(t))
		for i, one := range t {
			if s, ok := one.(string); ok {
				parts[i] = contentline.Escape(s)
			}
		}
		return strings.Join(parts, ",")
	case nil:
		return ""
	default:
		return contentline.Escape(fmt.Sprint(t))
	}
}
