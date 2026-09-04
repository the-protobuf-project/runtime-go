// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"sort"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/genproto/googleapis/type/month"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
)

func encodeRecurrence(r *eventv1.Recurrence) string {
	parts := []string{"FREQ=" + freqName(r.GetFrequency())}

	switch b := r.GetBound().(type) {
	case *eventv1.Recurrence_Until:
		v, _ := encodeTime(b.Until)
		parts = append(parts, "UNTIL="+v)
	case *eventv1.Recurrence_Count:
		parts = append(parts, "COUNT="+strconv.Itoa(int(b.Count)))
	}
	if r.GetInterval() > 0 {
		parts = append(parts, "INTERVAL="+strconv.Itoa(int(r.GetInterval())))
	}

	// Section 3.3.10 fixes no part order, so a deterministic one is chosen
	// here to keep output diffable.
	add := func(name string, ns []int32) {
		if len(ns) == 0 {
			return
		}
		ss := make([]string, len(ns))
		for i, n := range ns {
			ss[i] = strconv.Itoa(int(n))
		}
		parts = append(parts, name+"="+strings.Join(ss, ","))
	}
	add("BYSECOND", r.GetSecondNumbers())
	add("BYMINUTE", r.GetMinuteNumbers())
	add("BYHOUR", r.GetHourNumbers())

	if len(r.GetWeekdays()) > 0 {
		ss := make([]string, 0, len(r.GetWeekdays()))
		for _, wd := range r.GetWeekdays() {
			s := ""
			if wd.GetOrdinal() != 0 {
				s = strconv.Itoa(int(wd.GetOrdinal()))
			}
			ss = append(ss, s+dayName(wd.GetDay()))
		}
		parts = append(parts, "BYDAY="+strings.Join(ss, ","))
	}

	add("BYMONTHDAY", r.GetMonthDays())
	add("BYYEARDAY", r.GetYearDays())
	add("BYWEEKNO", r.GetWeekNumbers())
	if len(r.GetMonths()) > 0 || len(r.GetLeapMonths()) > 0 {
		ss := make([]string, 0, len(r.GetMonths())+len(r.GetLeapMonths()))
		// MONTH_UNSPECIFIED is skipped rather than written: its enum number is
		// 0 and "BYMONTH=0" is not a rule part any consumer can read. The zero
		// only reaches here from a caller-built Recurrence, since the decoder
		// now rejects an out-of-range BYMONTH outright.
		for _, m := range r.GetMonths() {
			if m == month.Month_MONTH_UNSPECIFIED {
				continue
			}
			ss = append(ss, strconv.Itoa(int(m.Number())))
		}
		// RFC 7529 section 4's leap-month suffix.
		for _, m := range r.GetLeapMonths() {
			if m == month.Month_MONTH_UNSPECIFIED {
				continue
			}
			ss = append(ss, strconv.Itoa(int(m.Number()))+"L")
		}
		if len(ss) > 0 {
			parts = append(parts, "BYMONTH="+strings.Join(ss, ","))
		}
	}
	add("BYSETPOS", r.GetSetPositions())

	if d := r.GetWeekStart(); d != dayofweek.DayOfWeek_DAY_OF_WEEK_UNSPECIFIED {
		parts = append(parts, "WKST="+dayName(d))
	}
	// RFC 7529 section 4. SKIP must not appear without RSCALE, so it is
	// emitted inside the RSCALE guard rather than on its own.
	if s := r.GetRscale(); s != "" {
		parts = append(parts, "RSCALE="+s)
		switch r.GetSkip() {
		case eventv1.RecurrenceSkip_RECURRENCE_SKIP_OMIT:
			parts = append(parts, "SKIP=OMIT")
		case eventv1.RecurrenceSkip_RECURRENCE_SKIP_BACKWARD:
			parts = append(parts, "SKIP=BACKWARD")
		case eventv1.RecurrenceSkip_RECURRENCE_SKIP_FORWARD:
			parts = append(parts, "SKIP=FORWARD")
		}
	}
	return strings.Join(parts, ";")
}

// monthOf maps BYMONTH's 1-12 onto google.type.Month, whose enum numbers are
// the same 1-12. The range is enforced by the type rather than by a
// constraint that has to be kept in step with it.
func monthOf(n int32) month.Month {
	if n < 1 || n > 12 {
		return month.Month_MONTH_UNSPECIFIED
	}
	return month.Month(n)
}

func freqName(f eventv1.Frequency) string {
	for k, v := range frequencies {
		if v == f {
			return k
		}
	}
	return ""
}

func dayName(d dayofweek.DayOfWeek) string {
	keys := make([]string, 0, len(weekdays))
	for k := range weekdays {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if weekdays[k] == d {
			return k
		}
	}
	return ""
}
