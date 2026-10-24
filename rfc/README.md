# rfc

`github.com/the-protobuf-project/runtime-go/rfc` converts between the IETF's
contact and calendar formats and the protobuf schemas published at
[buf.build/the-protobuf-project/rfc](https://buf.build/the-protobuf-project/rfc),
and serves those schemas over CalDAV.

| Package | What it does |
| --- | --- |
| `rfc` | vCard, xCard, jCard, JSContact, iCalendar, jCal and VAVAILABILITY in and out of the RFC messages: `rfc.ICalendar(text).Validate().Events()`, `rfc.Event(e).ICalendar()`. See the package doc. |
| `rfc/caldav` | A CalDAV server (RFC 4791) over the RFC 5545 / RFC 7953 messages, with all storage behind a `Store` interface. |
| `rfc/caldav/memstore` | An in-memory `caldav.Store`: the reference implementation, for tests and examples. |

## CalDAV

```go
h := caldav.New(store,
	caldav.WithName("bookings"),             // root and principal display name
	caldav.WithProductID("-//acme//cal//EN"), // PRODID on every VCALENDAR served
	caldav.WithErrorStatus(statusOf),         // map the store's own errors to HTTP statuses
)
mux.Handle(h.Prefix()+"/", h)                 // "/dav/" unless WithPrefix says otherwise
mux.Handle("/.well-known/caldav", h.WellKnown())
```

The handler owns the protocol: PROPFIND, PROPPATCH, MKCALENDAR, REPORT
(calendar-query with time-range, UID text-match and expand; calendar-multiget;
free-busy-query), GET/HEAD (an object, or a whole calendar as one `.ics`),
PUT and DELETE, entity tags with `If-Match` / `If-None-Match` (412), the RFC
4791 preconditions (`valid-calendar-data`, `valid-calendar-object-resource`,
`supported-calendar-component`, `max-resource-size`, `no-uid-conflict`),
CalendarServer's `getctag`, and XML multistatus encoding. sync-collection is
not offered; clients fall back to a PROPFIND of etags.

The consumer implements `caldav.Store`:

```go
type Store interface {
	Calendars(ctx context.Context) ([]*calendarv1.Calendar, error)
	Calendar(ctx context.Context, id string) (*calendarv1.Calendar, error)
	CreateCalendar(ctx context.Context, c *calendarv1.Calendar) error
	UpdateCalendar(ctx context.Context, c *calendarv1.Calendar, paths []string) error
	DeleteCalendar(ctx context.Context, c *calendarv1.Calendar, members []Object) error

	Objects(ctx context.Context, calendar string) ([]Object, error)
	Object(ctx context.Context, calendar, id string) (Object, error)
	ObjectByUID(ctx context.Context, calendar, uid string) (Object, error)
	PutObject(ctx context.Context, req PutRequest) (altered bool, err error)
	DeleteObject(ctx context.Context, calendar string, o Object) error

	Instances(ctx context.Context, c *calendarv1.Calendar, o Object, window Period) ([]*eventv1.Event, error)
	FreeBusy(ctx context.Context, c *calendarv1.Calendar, window Period) (FreeBusy, error)
}
```

An `Object` is one calendar object resource: a single event, a recurring
series with the overrides of its instances, or an availability. A PUT whose
body carries `RECURRENCE-ID;RANGE=THISANDFUTURE` reaches the store as
`PutRequest.Split`; the store cuts the series and returns `altered = true`, and
the handler answers without an ETag (RFC 4791 section 5.3.4). Missing records
are errors matching `caldav.ErrNotFound`, failed etag guards
`caldav.ErrPreconditionFailed`; a store returns a `*caldav.Error` to pick the
response itself (e.g. 403 `valid-calendar-object-resource` for a split it
refuses).

There is no authentication: every request acts as the single principal. Put
the handler behind whatever authenticates the rest of the service.

## Testing

```sh
go test -race ./...
golangci-lint run ./...
```

`caldav/caldav_test.go` drives a calendar client's whole session over HTTP
against `memstore` — discovery, MKCALENDAR, zoned recurring series, overrides,
expand, free-busy with RFC 7953 availability, THISANDFUTURE splits and their
retries; `caldav/protocol_test.go` checks the protocol edges one by one.
