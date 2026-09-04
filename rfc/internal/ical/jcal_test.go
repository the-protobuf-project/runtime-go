// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
)

// TestJCalCrossFormat: text/calendar and jCal must produce the same Event.
//
// RFC 7265 defines jCal as an alternate encoding of RFC 5545's data model, so
// the two cannot legitimately disagree. This is a stronger test than either
// round trip, because jCal changes three value representations -- extended
// dates, RECUR as an object, GEO as floats -- and a mistake in any of those
// surfaces here rather than staying invisible behind a symmetric bug.
func TestJCalCrossFormat(t *testing.T) {
	paths, _ := filepath.Glob("testdata/*.ics")
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			fromText, err := Decode(string(src))
			if err != nil {
				t.Fatal(err)
			}
			j, err := EncodeJCal(fromText)
			if err != nil {
				t.Fatalf("encode jcal: %v", err)
			}
			fromJSON, err := DecodeJCal(j)
			if err != nil {
				t.Fatalf("decode jcal %s: %v", j, err)
			}
			// RFC 7265 section 3.3 mandates lowercase property names, so an
			// extension key's original case cannot survive the crossing.
			// That is a property of the format, not a defect here -- the
			// same is true of jCard -- and it is the only difference the two
			// encodings may legitimately have.
			opts := cmp.Options{
				protocmp.Transform(),
				protocmp.FilterField(&eventv1.ExtensionProperty{}, "key",
					cmp.Comparer(strings.EqualFold)),
			}
			if diff := cmp.Diff(fromText, fromJSON, opts); diff != "" {
				t.Errorf("text and jCal disagree (-text +json):\n%s\njcal was:\n%s", diff, j)
			}
		})
	}
}

// TestJCalShape checks the wire format against RFC 7265 section 3.
func TestJCalShape(t *testing.T) {
	src, err := os.ReadFile("testdata/zoned.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeJCal(e)
	if err != nil {
		t.Fatal(err)
	}

	var doc []any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	// Section 3.3: a component is [name, properties, subcomponents].
	if len(doc) != 3 {
		t.Fatalf("component has %d elements, want 3", len(doc))
	}
	if doc[0] != "vcalendar" {
		t.Errorf("root name = %v, want vcalendar", doc[0])
	}
	subs, _ := doc[2].([]any)
	if len(subs) != 1 {
		t.Fatalf("want 1 subcomponent, got %d", len(subs))
	}
	vevent, _ := subs[0].([]any)
	if len(vevent) != 3 || vevent[0] != "vevent" {
		t.Fatalf("subcomponent = %v, want a vevent triple", vevent)
	}

	props, _ := vevent[1].([]any)
	byName := map[string][]any{}
	for _, p := range props {
		arr, ok := p.([]any)
		if !ok || len(arr) < 4 {
			t.Errorf("property is not a 4+ element array: %v", p)
			continue
		}
		name, _ := arr[0].(string)
		if name != strings.ToLower(name) {
			t.Errorf("property name %q is not lowercase", name)
		}
		byName[name] = arr
	}

	// Section 3.5.5: date-times are ISO 8601 extended, not iCalendar basic.
	dtstart := byName["dtstart"]
	if dtstart == nil {
		t.Fatal("no dtstart")
	}
	if got, _ := dtstart[3].(string); got != "2026-03-15T09:30:00" {
		t.Errorf("dtstart = %q, want extended format 2026-03-15T09:30:00", got)
	}
	// TZID rides in the parameters object, section 3.5.5.
	if params, _ := dtstart[1].(map[string]any); params["tzid"] != "Europe/London" {
		t.Errorf("dtstart params = %v, want tzid Europe/London", dtstart[1])
	}

	// Section 3.5.10: RECUR is an object, not a string.
	rrule := byName["rrule"]
	if rrule == nil {
		t.Fatal("no rrule")
	}
	obj, ok := rrule[3].(map[string]any)
	if !ok {
		t.Fatalf("rrule value is %T, want an object", rrule[3])
	}
	if obj["freq"] != "MONTHLY" {
		t.Errorf("rrule freq = %v, want MONTHLY", obj["freq"])
	}
	if n, _ := obj["count"].(float64); n != 12 {
		t.Errorf("rrule count = %v, want the number 12", obj["count"])
	}
	if obj["byday"] != "-1FR" {
		t.Errorf("rrule byday = %v, want the string -1FR", obj["byday"])
	}

	// Section 3.4.3: GEO is a float array.
	geo := byName["geo"]
	if geo == nil {
		t.Fatal("no geo")
	}
	if len(geo) != 5 {
		t.Fatalf("geo has %d elements, want name+params+type+2 floats", len(geo))
	}
	if lat, _ := geo[3].(float64); lat != 51.5074 {
		t.Errorf("geo lat = %v, want 51.5074 as a number", geo[3])
	}
}

// TestJCalAllDay: an all-day value is typed "date", and that type is what
// tells the decoder it is a DATE rather than a date-time.
func TestJCalAllDay(t *testing.T) {
	src, err := os.ReadFile("testdata/allday.ics")
	if err != nil {
		t.Fatal(err)
	}
	e, _ := Decode(string(src))
	raw, err := EncodeJCal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"date","2026-07-01"`) {
		t.Errorf("all-day dtstart not typed as date:\n%s", raw)
	}
	back, err := DecodeJCal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if back.GetStart().GetDate() == nil {
		t.Error("all-day value did not survive as a Date through jCal")
	}
}
