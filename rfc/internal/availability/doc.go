// Package availability converts between text/calendar VAVAILABILITY
// components (RFC 7953) and the protobuf.rfc7953.availability.v1 schema.
//
// A separate Go package from ical for the same reason the schema is a
// separate proto package: AIP-215 forbids a cross-package message reference,
// so Availability's CalendarTime is a distinct Go type from the Event one
// even though the two are structurally identical. The parsing primitives that
// do not depend on those types live in internal/icaltime and are shared.
package availability
