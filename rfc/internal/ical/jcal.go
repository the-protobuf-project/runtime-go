// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// EncodeJCal serializes an Event as application/calendar+json, RFC 7265.
//
// A jCal component is a three-element array -- [name, properties,
// subcomponents] -- where jCard's is two. The nesting is what carries
// VCALENDAR containing VEVENT, so BEGIN and END have no representation.
func EncodeJCal(e *eventv1.Event) ([]byte, error) {
	raws, err := contentLines(e)
	if err != nil {
		return nil, err
	}

	// A component tree, built generically from BEGIN/END.
	//
	// jCal's own structure is recursive -- every component is
	// [name, properties, subcomponents] -- so the encoder is too. It used to
	// special-case VALARM, which meant RFC 9073's PARTICIPANT, VLOCATION and
	// VRESOURCE were flattened into the calendar's property list: a document
	// that parses as JSON, is not valid jCal, and loses the nesting entirely.
	// Handling the tree rather than a list of known names is what stops the
	// next component from repeating that.
	type frame struct {
		name  string
		props []any
		subs  []any
	}
	var stack []*frame
	var root []any

	for _, raw := range raws {
		l, err := contentline.Parse(raw)
		if err != nil {
			return nil, err
		}
		switch l.Name {
		case "BEGIN":
			stack = append(stack, &frame{
				name:  strings.ToLower(strings.TrimSpace(l.Value)),
				props: []any{},
				subs:  []any{},
			})
			continue
		case "END":
			if len(stack) == 0 {
				return nil, fmt.Errorf("END:%s without a matching BEGIN", l.Value)
			}
			f := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			arr := []any{f.name, f.props, f.subs}
			if len(stack) == 0 {
				root = arr
			} else {
				parent := stack[len(stack)-1]
				parent.subs = append(parent.subs, arr)
			}
			continue
		}
		if len(stack) == 0 {
			return nil, fmt.Errorf("property %s outside any component", l.Name)
		}
		p, err := jcalProperty(l)
		if err != nil {
			return nil, err
		}
		top := stack[len(stack)-1]
		top.props = append(top.props, p)
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("unclosed component %s", stack[len(stack)-1].name)
	}
	if root == nil {
		return nil, fmt.Errorf("no component found")
	}
	return json.Marshal(root)
}

func jcalProperty(l contentline.Line) ([]any, error) {
	typ := jcalType(l)

	params := map[string]any{}
	for k, vs := range l.Params {
		lk := strings.ToLower(k)
		// The type identifier carries what VALUE said, so repeating it as a
		// parameter would round-trip a redundant VALUE onto every property.
		if lk == "value" {
			continue
		}
		if len(vs) == 1 {
			params[lk] = vs[0]
		} else {
			params[lk] = vs
		}
	}

	prop := []any{strings.ToLower(l.RawName), params, typ}

	switch typ {
	case "date", "date-time":
		// EXDATE and RDATE are comma-separated lists sharing one type.
		var vals []any
		for _, one := range contentline.SplitUnescaped(l.Value, ',') {
			ext, err := basicToExtended(strings.TrimSpace(one))
			if err != nil {
				return nil, fmt.Errorf("%s: %w", l.Name, err)
			}
			vals = append(vals, ext)
		}
		return append(prop, vals...), nil
	case "recur":
		return append(prop, recurToObject(l.Value)), nil
	case "integer":
		n, err := strconv.ParseInt(strings.TrimSpace(l.Value), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s: %q is not an integer", l.Name, l.Value)
		}
		return append(prop, n), nil
	case "float":
		// GEO, section 3.4.3: a two-element float array.
		var nums []any
		for _, s := range strings.Split(l.Value, ";") {
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return nil, fmt.Errorf("%s: %q is not a float", l.Name, s)
			}
			nums = append(nums, f)
		}
		return append(prop, nums...), nil
	}

	vals := contentline.SplitList(l.Value)
	if len(vals) == 0 {
		return append(prop, contentline.Unescape(l.Value)), nil
	}
	for _, v := range vals {
		prop = append(prop, v)
	}
	return prop, nil
}
