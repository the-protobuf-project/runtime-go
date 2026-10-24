package caldav

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store is everything the handler needs from persistence. The consumer
// implements it over whatever holds its calendars; the handler owns the
// protocol and never reaches past it.
//
// Calendars are named "calendars/{id}" and addressed in URLs by that id.
// A calendar or object that does not exist -- or is soft-deleted, which is
// the same thing to a CalDAV client -- is reported as an error matching
// [ErrNotFound] (or one the [WithErrorStatus] mapper maps to 404).
//
// Writes guard on etags: a record handed to a write carries the etag it was
// read with, and a store that finds it changed fails with an error matching
// [ErrPreconditionFailed].
type Store interface {
	// Calendars returns every live calendar.
	Calendars(ctx context.Context) ([]*calendarv1.Calendar, error)
	// Calendar returns the live calendar with id.
	Calendar(ctx context.Context, id string) (*calendarv1.Calendar, error)
	// CreateCalendar creates c, whose Name is "calendars/{id}". A calendar
	// already holding that name fails with ErrAlreadyExists.
	CreateCalendar(ctx context.Context, c *calendarv1.Calendar) error
	// UpdateCalendar writes the fields of c named by paths (display_name,
	// description) to the calendar c.Name, guarded by c.Etag.
	UpdateCalendar(ctx context.Context, c *calendarv1.Calendar, paths []string) error
	// DeleteCalendar deletes c and every one of members, its objects, in one
	// atomic write: a failure leaves the collection whole.
	DeleteCalendar(ctx context.Context, c *calendarv1.Calendar, members []Object) error

	// Objects returns every live object in the calendar with id, in a stable
	// order (the calendar's ctag hashes them in it). An override is part of
	// its series' object; one whose series is not in the calendar is an
	// object of its own.
	Objects(ctx context.Context, calendar string) ([]Object, error)
	// Object returns the live object named id in the calendar: an event with
	// the overrides of its instances, or an availability. An override that
	// belongs to a series in the calendar is not an object of its own.
	Object(ctx context.Context, calendar, id string) (Object, error)
	// ObjectByUID returns the live object in the calendar whose series,
	// single event or availability carries the iCalendar UID uid. The
	// handler uses it for RFC 4791's no-uid-conflict precondition.
	ObjectByUID(ctx context.Context, calendar, uid string) (Object, error)
	// PutObject writes one calendar object resource atomically: everything
	// in req lands, or nothing does. See [PutRequest]. altered reports that
	// what was stored differs from what was sent (a split series), so the
	// handler withholds the ETag, RFC 4791 section 5.3.4.
	PutObject(ctx context.Context, req PutRequest) (altered bool, err error)
	// DeleteObject deletes o -- an availability, or an event with every
	// override in it -- from the calendar with id, atomically.
	DeleteObject(ctx context.Context, calendar string, o Object) error

	// Instances returns o's occurrences overlapping window, in c's zone: a
	// recurring series' instances with its overrides applied (a moved one at
	// its new time, a cancelled one absent), each carrying the
	// original_start that identifies it; any other event itself, when it
	// overlaps. A zero-length occurrence overlaps a window containing its
	// instant, RFC 4791 section 9.9. It answers the calendar-query
	// time-range filter and the calendar-data expand element.
	Instances(ctx context.Context, c *calendarv1.Calendar, o Object, window Period) ([]*eventv1.Event, error)
	// FreeBusy returns c's busy time in window, for the free-busy-query
	// REPORT, RFC 4791 section 7.10.
	FreeBusy(ctx context.Context, c *calendarv1.Calendar, window Period) (FreeBusy, error)
}

// Object is one calendar object resource, RFC 4791 section 4.1: an event or
// an availability. Exactly one of Event and Availability is set. A recurring
// event carries the overrides of its instances with it: they share its UID,
// so they are one object.
type Object struct {
	// Name is the object's resource name within its calendar, the last
	// segment of its href ("{uuid}.ics").
	Name         string
	Event        *eventv1.Event
	Overrides    []*eventv1.Event
	Availability *availabilityv1.Availability
}

// ETag is the object's entity tag. With overrides it covers each of them as
// well as the series, so editing one instance changes the object's tag.
func (o Object) ETag() string {
	if o.Availability != nil {
		return o.Availability.GetEtag()
	}
	if len(o.Overrides) == 0 {
		return o.Event.GetEtag()
	}
	parts := []string{o.Event.GetEtag()}
	for _, e := range o.Overrides {
		parts = append(parts, e.GetName()+"\x00"+e.GetEtag())
	}
	slices.Sort(parts[1:])
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:16])
}

// UID is the iCalendar UID the object is published under.
func (o Object) UID() string {
	if o.Availability != nil {
		return uidOf(o.Availability.GetIcalUid(), o.Availability.GetUid(), o.Availability.GetName())
	}
	return uidOf(o.Event.GetIcalUid(), o.Event.GetUid(), o.Event.GetName())
}

func (o Object) updateTime() *timestamppb.Timestamp {
	if o.Event != nil {
		return o.Event.GetUpdateTime()
	}
	return o.Availability.GetUpdateTime()
}

func (o Object) contentType() string {
	if o.Event != nil {
		return "text/calendar; charset=utf-8; component=VEVENT"
	}
	return "text/calendar; charset=utf-8; component=VAVAILABILITY"
}

// PutRequest is a PUT of one calendar object resource, decoded and checked:
// the body is valid iCalendar, its components share one UID, the request's
// If-Match/If-None-Match held against Existing, and the component type
// matches what Existing holds.
//
// Exactly one of Event and Availability is set. Messages arrive as the
// client sent them: server-owned fields (name, etag, times) are the store's
// to set.
type PutRequest struct {
	// Calendar is the collection being written into.
	Calendar *calendarv1.Calendar
	// Name is the object's resource name, the last segment of its href.
	Name string
	// Existing is the object being replaced, nil for a create. Its etags are
	// what the write must guard on.
	Existing *Object

	// Event is the series or single event. Overrides are the overrides of
	// single instances the body carries, RFC 5545 section 3.8.4.4: the
	// object's stored overrides become exactly these -- one for an instance
	// already overridden replaces it, a new one is created, one no longer
	// sent is deleted so its instance comes back.
	Event     *eventv1.Event
	Overrides []*eventv1.Event
	// Split, when set, is a RECURRENCE-ID;RANGE=THISANDFUTURE override, RFC
	// 5545 section 3.2.13: the series is cut before that instance and the
	// rest becomes a series of its own, a new object. A split the store
	// cannot perform should fail with an [*Error] carrying status 403 and
	// [ValidCalendarObjectResource].
	Split *eventv1.Event

	Availability *availabilityv1.Availability
}

// Period is a half-open span of time [Start, End).
type Period struct {
	Start, End time.Time
}

// Duration is how long p lasts.
func (p Period) Duration() time.Duration { return p.End.Sub(p.Start) }

// FreeBusy is a calendar's busy time in a window, by RFC 5545 FBTYPE.
type FreeBusy struct {
	// Busy is FBTYPE=BUSY.
	Busy []Period
	// Tentative is FBTYPE=BUSY-TENTATIVE.
	Tentative []Period
	// Unavailable is FBTYPE=BUSY-UNAVAILABLE: time the calendar's RFC 7953
	// components do not offer.
	Unavailable []Period
}

// ctag summarizes a calendar's state for CalendarServer's getctag: it changes
// whenever any member, or the calendar itself, does. It is derived from etags
// the store already rotates on every write, so it needs no storage.
func ctag(c *calendarv1.Calendar, objs []Object) string {
	h := sha256.New()
	h.Write([]byte(c.GetEtag()))
	for _, o := range objs {
		h.Write([]byte{0})
		h.Write([]byte(o.Name))
		h.Write([]byte{0})
		h.Write([]byte(o.ETag()))
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}
