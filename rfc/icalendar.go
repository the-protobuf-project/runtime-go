package rfc

// icalendar.go is the reading half of the calendar lineage.
//
// One asymmetry with the contact side is worth pointing at, because it looks
// like an oversight and is not: [ICalendarSource] offers both `Event` and
// `Availability`, where every vCard reader offers only `Contact`. A text/calendar
// document is a VCALENDAR that may hold either component, and which one it holds
// is the caller's knowledge rather than the parser's — asking for the wrong one
// is an error, not a guess.

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/availability"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/ical"
)

// ICalendarSource converts a text/calendar document, RFC 5545 <https://www.rfc-editor.org/rfc/rfc5545.html>.
//
// Obtained from [ICalendar].
type ICalendarSource struct {
	text     string
	validate bool
}

// ICalendar begins a conversion from a text/calendar document.
//
//	event, err := rfc.ICalendar(text).Event()
//	avail, err := rfc.ICalendar(text).Availability()
//
// Which method to call is decided by which component the document carries; see
// this file's note on why the parser does not choose.
func ICalendar(text string) *ICalendarSource {
	return &ICalendarSource{text: text}
}

// Validate requires the parsed message to satisfy its buf.validate rules
// before this source returns it.
//
// The check runs after parsing, because that is when there is a message to
// check — see validate.go. It applies to whichever component is asked for:
// [ICalendarSource.Event] validates an Event, [ICalendarSource.Availability]
// an Availability.
func (s *ICalendarSource) Validate() *ICalendarSource {
	s.validate = true
	return s
}

// Event parses the document's VEVENT into an Event.
//
// Fails when the document holds no VEVENT — including when it holds a
// VAVAILABILITY instead, which [ICalendarSource.Availability] reads.
func (s *ICalendarSource) Event() (*eventv1.Event, error) {
	out, err := ical.Decode(s.text)
	if err != nil {
		return nil, fail("icalendar", "event", err)
	}
	if s.validate {
		if err := check("event", out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Availability parses the document's VAVAILABILITY into an Availability,
// RFC 7953 <https://www.rfc-editor.org/rfc/rfc7953.html>.
//
// Fails when the document holds no VAVAILABILITY.
func (s *ICalendarSource) Availability() (*availabilityv1.Availability, error) {
	out, err := availability.Decode(s.text)
	if err != nil {
		return nil, fail("icalendar", "availability", err)
	}
	if s.validate {
		if err := check("availability", out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// JCalSource converts an application/calendar+json document, RFC 7265 <https://www.rfc-editor.org/rfc/rfc7265.html>.
//
// Obtained from [JCal].
type JCalSource struct {
	data     []byte
	validate bool
}

// JCal begins a conversion from an application/calendar+json document.
func JCal(data []byte) *JCalSource {
	return &JCalSource{data: data}
}

// Validate requires the parsed message to satisfy its buf.validate rules
// before this source returns it.
//
// The check runs after parsing, because that is when there is a message to
// check — see validate.go.
func (s *JCalSource) Validate() *JCalSource {
	s.validate = true
	return s
}

// Event parses the document's vevent component into an Event.
func (s *JCalSource) Event() (*eventv1.Event, error) {
	out, err := ical.DecodeJCal(s.data)
	if err != nil {
		return nil, fail("jcal", "event", err)
	}
	if s.validate {
		if err := check("event", out); err != nil {
			return nil, err
		}
	}
	return out, nil
}
