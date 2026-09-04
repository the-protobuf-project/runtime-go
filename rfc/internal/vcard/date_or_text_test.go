// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/type/date"
)

// TestPartialDateForms covers section 4.3.1's six reduced-accuracy forms.
// new_properties.vcf only exercises two (--0415 and a full date); the other
// four have no property in this schema's scope that would reach them through
// TestRoundTrip, so they need their own table.
func TestPartialDateForms(t *testing.T) {
	cases := []struct {
		name          string
		year, mo, day int32
		want          string
	}{
		{"full date", 1996, 4, 15, "19960415"},
		{"year and month", 1985, 4, 0, "1985-04"},
		{"year only", 1985, 0, 0, "1985"},
		{"month and day, no year", 0, 4, 15, "--0415"},
		{"month only", 0, 4, 0, "--04"},
		{"day only", 0, 0, 15, "---15"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &date.Date{Year: tc.year, Month: tc.mo, Day: tc.day}
			got := encodePartialDate(d)
			if got != tc.want {
				t.Fatalf("encodePartialDate(%+v) = %q, want %q", tc, got, tc.want)
			}
			back, err := parsePartialDate(got)
			if err != nil {
				t.Fatalf("parsePartialDate(%q): %v", got, err)
			}
			if back.GetYear() != tc.year || back.GetMonth() != tc.mo || back.GetDay() != tc.day {
				t.Errorf("parsePartialDate(%q) = %+v, want year=%d month=%d day=%d",
					got, back, tc.year, tc.mo, tc.day)
			}
		})
	}
}

// TestUTCOffsetForms covers section 4.3.2's zone production as TZ's
// utc-offset form and a DATE-TIME zone suffix both use it.
func TestUTCOffsetForms(t *testing.T) {
	cases := []struct {
		hours, minutes int
		want           string
	}{
		{-5, 0, "-0500"},
		{5, 30, "+0530"},
		{0, 0, "+0000"},
	}
	for _, tc := range cases {
		d := time.Duration(tc.hours)*time.Hour + time.Duration(tc.minutes)*time.Minute
		got := formatUTCOffset(d)
		if got != tc.want {
			t.Errorf("formatUTCOffset(%dh%dm) = %q, want %q", tc.hours, tc.minutes, got, tc.want)
		}
		back, err := parseUTCOffset(got)
		if err != nil {
			t.Fatalf("parseUTCOffset(%q): %v", got, err)
		}
		if back != d {
			t.Errorf("parseUTCOffset(%q) = %v, want %v", got, back, d)
		}
	}
}
