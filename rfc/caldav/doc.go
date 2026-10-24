// Package caldav serves calendars over CalDAV, RFC 4791, from any store that
// speaks the RFC protobuf types: RFC 5545 calendars and events and RFC 7953
// availability.
//
// The package owns the protocol and nothing else. WebDAV and CalDAV methods,
// XML bodies, preconditions, entity tags and iCalendar encoding live here;
// every read and write goes through the [Store] the consumer implements, so a
// calendar a client edits is the same record the consumer's own API serves.
//
//	h := caldav.New(store, caldav.WithName("bookings"))
//	mux.Handle("/dav/", h)
//	mux.Handle("/.well-known/caldav", h.WellKnown())
//
// # What it answers
//
//   - OPTIONS, with DAV: 1, 3, calendar-access.
//   - PROPFIND (Depth 0 and 1; infinity is answered as 1) on the root, the one
//     principal, the calendar home, calendars and objects, with allprop,
//     propname and named properties, CalendarServer's getctag included.
//   - PROPPATCH of a calendar's displayname and calendar-description; any
//     other property fails the whole request, RFC 4918 section 9.2.
//   - MKCALENDAR, with the display name and description the body may set.
//   - REPORT calendar-query (component, time-range and UID text-match
//     filters, calendar-data with expand), calendar-multiget and
//     free-busy-query. A filter it cannot evaluate is refused with
//     CALDAV:supported-filter, never silently widened; sync-collection is
//     refused with DAV:supported-report, which tells a client to fall back.
//   - GET and HEAD of an object, or of a whole calendar as one .ics export.
//   - PUT of an object: create or full replace, honoring If-Match and
//     If-None-Match (412), checking C:valid-calendar-data,
//     C:valid-calendar-object-resource, C:supported-calendar-component,
//     C:max-resource-size and C:no-uid-conflict.
//   - DELETE of an object, or of a calendar with every member.
//
// # Calendar objects
//
// An [Object] is one calendar object resource, RFC 4791 section 4.1: a single
// event, a recurring series together with the overrides of its instances
// (they share its UID, so they are one object), or an availability. Its entity
// tag covers every component in it, so editing one instance changes the
// object's tag.
//
// A PUT carrying RECURRENCE-ID;RANGE=THISANDFUTURE (RFC 5545 section 3.2.13)
// is handed to the store as [PutRequest.Split]: the store cuts the series
// before that instance and stores the rest as a new object. Because what it
// stored is not what was sent, the response carries no ETag, RFC 4791 section
// 5.3.4, and the client reads the object again.
//
// # The store
//
// The store does the work that depends on how calendars are kept: grouping
// overrides into their series, atomic writes, expanding recurrences (a
// time-range filter or an expand element is answered from
// [Store.Instances]) and computing free/busy ([Store.FreeBusy]). The memstore
// package is a complete in-memory implementation, for tests and examples.
//
// # Authentication
//
// There is none: every request acts as the single principal,
// /principals/default/. Put the handler behind whatever authenticates the
// consumer's other APIs.
package caldav
