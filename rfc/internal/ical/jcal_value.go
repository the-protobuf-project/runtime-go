// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// jCal, RFC 7265: application/calendar+json.
//
// Unlike jCard, jCal is not a pure syntax layer. RFC 7265 changes three value
// representations, so this file translates them and the semantic mapping in
// decodeProperty and contentLines is still shared with text/calendar:
//
//	section 3.5.4/3.5.5  dates are ISO 8601 *extended* -- 2026-06-15T09:00:00,
//	                     not iCalendar's basic 20260615T090000
//	section 3.5.10       RECUR is a JSON object, not a string
//	section 3.4.3        GEO is a two-element float array, not "lat;lon"

// jcalType is the value-type identifier for a property, section 3.4.
func jcalType(l contentline.Line) string {
	if v := l.Params["VALUE"]; len(v) > 0 {
		return strings.ToLower(v[0])
	}
	switch l.Name {
	case "DTSTART", "DTEND", "EXDATE", "RDATE", "DTSTAMP", "CREATED", "LAST-MODIFIED":
		return "date-time"
	case "RRULE":
		return "recur"
	case "DURATION":
		return "duration"
	case "SEQUENCE", "REPEAT":
		return "integer"
	case "TRIGGER":
		// Section 3.8.6.3: DURATION by default, DATE-TIME when VALUE says so
		// -- which the VALUE branch above has already handled.
		return "duration"
	case "GEO":
		return "float"
	}
	return "text"
}

// basicToExtended converts iCalendar's basic date form to ISO 8601 extended.
//
//	20260615T090000Z -> 2026-06-15T09:00:00Z
//	20260701         -> 2026-07-01
func basicToExtended(v string) (string, error) {
	utc := strings.HasSuffix(v, "Z")
	v = strings.TrimSuffix(v, "Z")

	d, t, hasTime := strings.Cut(v, "T")
	if len(d) != 8 {
		return "", fmt.Errorf("date %q is not 8 characters", d)
	}
	out := d[0:4] + "-" + d[4:6] + "-" + d[6:8]
	if hasTime {
		if len(t) != 6 {
			return "", fmt.Errorf("time %q is not 6 characters", t)
		}
		out += "T" + t[0:2] + ":" + t[2:4] + ":" + t[4:6]
	}
	if utc {
		out += "Z"
	}
	return out, nil
}

// extendedToBasic is the inverse.
func extendedToBasic(v string) (string, error) {
	utc := strings.HasSuffix(v, "Z")
	v = strings.TrimSuffix(v, "Z")

	d, t, hasTime := strings.Cut(v, "T")
	dp := strings.Split(d, "-")
	if len(dp) != 3 {
		return "", fmt.Errorf("date %q is not YYYY-MM-DD", d)
	}
	out := dp[0] + dp[1] + dp[2]
	if hasTime {
		tp := strings.Split(t, ":")
		if len(tp) != 3 {
			return "", fmt.Errorf("time %q is not HH:MM:SS", t)
		}
		out += "T" + tp[0] + tp[1] + tp[2]
	}
	if utc {
		out += "Z"
	}
	return out, nil
}

// recurToObject turns an RRULE string into jCal's object form, section 3.5.10.
// Rule-part names are lowercased; a part with several values becomes an array.
func recurToObject(v string) map[string]any {
	obj := map[string]any{}
	for _, part := range strings.Split(v, ";") {
		k, val, ok := strings.Cut(part, "=")
		if !ok || k == "" {
			continue
		}
		k = strings.ToLower(k)
		vals := strings.Split(val, ",")
		if len(vals) == 1 {
			obj[k] = numberOrString(vals[0])
			continue
		}
		arr := make([]any, len(vals))
		for i, one := range vals {
			arr[i] = numberOrString(one)
		}
		obj[k] = arr
	}
	return obj
}

// numberOrString keeps numeric rule parts numeric. Section 3.5.10 types
// COUNT, INTERVAL and the BY* numbers as numbers, and BYDAY as a string --
// "-1FR" is not a number and must stay text.
func numberOrString(s string) any {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return s
}

// objectToRecur is the inverse. Parts are emitted in a fixed order so output
// is diffable; section 3.3.10 fixes no order.
func objectToRecur(obj map[string]any) string {
	order := []string{
		"freq", "until", "count", "interval",
		"bysecond", "byminute", "byhour", "byday",
		"bymonthday", "byyearday", "byweekno", "bymonth", "bysetpos", "wkst",
	}
	seen := map[string]bool{}
	var parts []string
	emit := func(k string) {
		v, ok := obj[k]
		if !ok || seen[k] {
			return
		}
		seen[k] = true
		parts = append(parts, strings.ToUpper(k)+"="+recurValue(v))
	}
	for _, k := range order {
		emit(k)
	}
	// Anything the RFC adds later still round-trips, in a stable order.
	rest := make([]string, 0, len(obj))
	for k := range obj {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	for _, k := range rest {
		emit(k)
	}
	return strings.Join(parts, ";")
}

func recurValue(v any) string {
	switch t := v.(type) {
	case []any:
		ss := make([]string, len(t))
		for i, one := range t {
			ss[i] = recurValue(one)
		}
		return strings.Join(ss, ",")
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}
