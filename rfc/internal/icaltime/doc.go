// Package icaltime parses the iCalendar value types that are not tied to any
// one schema package: DATE, DATE-TIME and DURATION.
//
// These live here because their results are google.* types --
// google.type.Date, google.type.DateTime, google.protobuf.Duration -- which
// every package may reference. The thin wrappers that box them into a
// package's own CalendarTime cannot be shared, because AIP-215 forces each
// proto package to define its own; those stay with their codec.
//
// The split matters: it keeps the AIP-215 duplication tax to the wrapper,
// rather than copying the whole of RFC 5545 section 3.3's grammar per
// package.
package icaltime
