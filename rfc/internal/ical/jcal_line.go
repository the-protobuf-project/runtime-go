// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// jcalToLine converts one jCal property array into a content line, so the
// shared semantic decoder consumes it unchanged.
//
//	[name, parameters, type, value...]   RFC 7265 section 3.3
func jcalToLine(raw any) (contentline.Line, error) {
	arr, ok := raw.([]any)
	if !ok || len(arr) < 4 {
		return contentline.Line{}, fmt.Errorf("jcal property must be an array of at least 4 elements, got %v", raw)
	}
	name, ok := arr[0].(string)
	if !ok {
		return contentline.Line{}, fmt.Errorf("jcal property name is not a string: %v", arr[0])
	}

	l := contentline.Line{RawName: name, Name: strings.ToUpper(name), Params: map[string][]string{}}
	params, ok := arr[1].(map[string]any)
	if !ok {
		return contentline.Line{}, fmt.Errorf("%s: jcal property parameters are not an object: %v", l.Name, arr[1])
	}
	for k, v := range params {
		key := strings.ToUpper(k)
		switch t := v.(type) {
		case string:
			l.Params[key] = []string{t}
		case []any:
			for _, one := range t {
				if s, isString := one.(string); isString {
					l.Params[key] = append(l.Params[key], s)
				}
			}
		}
	}

	rawType, ok := arr[2].(string)
	if !ok {
		return contentline.Line{}, fmt.Errorf("%s: jcal property type is not a string: %v", l.Name, arr[2])
	}
	typ := strings.ToLower(rawType)
	vals := arr[3:]

	switch typ {
	case "date", "date-time":
		// The type identifier is how jCal says DATE, so it has to become a
		// VALUE parameter again for the shared decoder to see an all-day
		// value. Without this every all-day event decodes as a date-time.
		if typ == "date" {
			l.Params["VALUE"] = []string{"DATE"}
		}
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			s, ok := v.(string)
			if !ok {
				return contentline.Line{}, fmt.Errorf("%s: value is not a string: %v", name, v)
			}
			b, err := extendedToBasic(s)
			if err != nil {
				return contentline.Line{}, fmt.Errorf("%s: %w", name, err)
			}
			parts = append(parts, b)
		}
		l.Value = strings.Join(parts, ",")
	case "recur":
		obj, ok := vals[0].(map[string]any)
		if !ok {
			return contentline.Line{}, fmt.Errorf("%s: recur value is not an object: %v", name, vals[0])
		}
		r, err := objectToRecur(obj)
		if err != nil {
			return contentline.Line{}, fmt.Errorf("%s: %w", name, err)
		}
		l.Value = r
	case "integer":
		l.Value = jcalScalar(vals[0])
	case "float":
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = jcalScalar(v)
		}
		l.Value = strings.Join(parts, ";")
	default:
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			parts = append(parts, contentline.Escape(jcalScalar(v)))
		}
		l.Value = strings.Join(parts, ",")
	}
	return l, nil
}

func jcalScalar(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}
