package rfc

// event.go and icalendar.go are the calendar lineage, and it is shorter than
// the contact one for a reason worth stating: there is no canonical model to
// convert to.
//
// JSCalendar is the intended successor to iCalendar, but RFC 8984 is being
// obsoleted by a 2.0 that has no RFC number yet, and the iCalendar-to-JSCalendar
// conversion is itself still a draft. So iCalendar is both the model and the
// format here, and these types encode and decode rather than convert. When
// jscalendarbis publishes, an `EventSource.JSCalendar` belongs next to
// `ContactSource.Card` and nowhere else.

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/availability"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/ical"
)

// EventSource converts an iCalendar Event into the forms RFC 5545 defines.
//
// Obtained from [Event].
type EventSource struct {
	event    *eventv1.Event
	validate bool
}

// Event begins a conversion from an iCalendar Event.
//
//	text, err := rfc.Event(e).ICalendar()
//	data, err := rfc.Event(e).JCal()
func Event(e *eventv1.Event) *EventSource {
	return &EventSource{event: e}
}

// ICalendar renders the event as text/calendar, RFC 5545 <https://www.rfc-editor.org/rfc/rfc5545.html>.
//
// The output is a VCALENDAR wrapping one VEVENT, with the RFC 9073 components
// and RFC 9074 alarm extensions the model carries.
//
// Fails when the event has no DTSTART, which section 3.6.1 requires.
func (s *EventSource) ICalendar() (string, error) {
	if err := s.checked(); err != nil {
		return "", err
	}
	out, err := ical.Encode(s.event)
	return out, fail("event", "icalendar", err)
}

// Validate requires the event to satisfy its buf.validate rules before any
// method on this source produces a result.
//
// Chainable rather than terminal; see validate.go for why.
func (s *EventSource) Validate() *EventSource {
	s.validate = true
	return s
}

// Err reports whether the event satisfies its rules, without converting.
// Returns nil when Validate was not called.
func (s *EventSource) Err() error { return s.checked() }

func (s *EventSource) checked() error {
	if !s.validate {
		return nil
	}
	return check("event", s.event)
}

// JCal renders the event as application/calendar+json, RFC 7265 <https://www.rfc-editor.org/rfc/rfc7265.html>.
func (s *EventSource) JCal() ([]byte, error) {
	if err := s.checked(); err != nil {
		return nil, err
	}
	out, err := ical.EncodeJCal(s.event)
	return out, fail("event", "jcal", err)
}

// AvailabilitySource converts an Availability into the forms RFC 7953 defines.
//
// Obtained from [Availability].
type AvailabilitySource struct {
	availability *availabilityv1.Availability
	validate     bool
}

// Availability begins a conversion from a VAVAILABILITY Availability.
//
//	text, err := rfc.Availability(a).ICalendar()
func Availability(a *availabilityv1.Availability) *AvailabilitySource {
	return &AvailabilitySource{availability: a}
}

// Validate requires the availability to satisfy its buf.validate rules before
// any method on this source produces a result.
//
// Chainable rather than terminal; see validate.go for why.
func (s *AvailabilitySource) Validate() *AvailabilitySource {
	s.validate = true
	return s
}

// Err reports whether the availability satisfies its rules, without
// converting. Returns nil when Validate was not called.
func (s *AvailabilitySource) Err() error { return s.checked() }

func (s *AvailabilitySource) checked() error {
	if !s.validate {
		return nil
	}
	return check("availability", s.availability)
}

// ICalendar renders the availability as text/calendar, RFC 7953 <https://www.rfc-editor.org/rfc/rfc7953.html>.
//
// Note the polarity the format defines: the VAVAILABILITY's whole range is busy,
// and each AVAILABLE sub-component carves free time out of it. An Availability
// with no periods renders as "never schedulable in this range", which is a
// legitimate thing to say and not an empty document.
//
// Fails when the availability has no UID, which section 3.1 requires.
func (s *AvailabilitySource) ICalendar() (string, error) {
	if err := s.checked(); err != nil {
		return "", err
	}
	out, err := availability.Encode(s.availability)
	return out, fail("availability", "icalendar", err)
}
