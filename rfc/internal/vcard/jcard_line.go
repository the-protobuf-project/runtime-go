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

	// arr[2] is the value-type identifier. It is recorded as a VALUE
	// parameter only when it is not the default, so that a round trip does
	// not invent a VALUE=text on every property.
	if t, ok := arr[2].(string); ok && t != "" && t != "text" {
		if _, seen := l.Params["VALUE"]; !seen {
			l.Params["VALUE"] = []string{t}
		}
	}

	valueType, _ := arr[2].(string)
	l.Value = jcardValue(l.Name, strings.ToLower(valueType), arr[3:])
	return l, nil
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
