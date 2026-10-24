// Package caltime reads and writes the RFC 5545 times the calendar schemas
// carry -- a DATE, or a DATE-TIME that is floating, fixed to a UTC offset or
// pinned to an IANA zone -- as Go instants, keeping the form each was written
// in so that a time derived from it can be written back the same way.
//
// It is shared by the caldav handler and its in-memory reference store, which
// is why it lives under caldav/internal rather than in either of them.
package caltime
