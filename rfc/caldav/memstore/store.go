package memstore

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc7953/availability/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/internal/caltime"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store is an in-memory [caldav.Store]. It is safe for concurrent use; every
// message it hands out is a copy, and every write is atomic.
type Store struct {
	mu        sync.Mutex
	now       func() time.Time
	seq       uint64
	calendars map[string]*entry // by calendar id
	order     []string          // calendar ids, in creation order
}

// entry is one calendar and the objects in it.
type entry struct {
	cal     *calendarv1.Calendar
	objects map[string]caldav.Object
	order   []string // object names, in creation order
}

var _ caldav.Store = (*Store)(nil)

// Option configures a [Store].
type Option func(*Store)

// WithClock sets the clock create and update times are read from.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// New returns an empty store.
func New(opts ...Option) *Store {
	s := &Store{now: time.Now, calendars: map[string]*entry{}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// etag mints a new entity tag. Callers hold s.mu.
func (s *Store) etag() string {
	s.seq++
	return strconv.FormatUint(s.seq, 36)
}

func (s *Store) stampNow() *timestamppb.Timestamp { return timestamppb.New(s.now()) }

func notFound(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{caldav.ErrNotFound}, args...)...)
}

func cloneObject(o caldav.Object) caldav.Object {
	out := caldav.Object{Name: o.Name}
	if o.Event != nil {
		out.Event = proto.Clone(o.Event).(*eventv1.Event)
	}
	for _, e := range o.Overrides {
		out.Overrides = append(out.Overrides, proto.Clone(e).(*eventv1.Event))
	}
	if o.Availability != nil {
		out.Availability = proto.Clone(o.Availability).(*availabilityv1.Availability)
	}
	return out
}

// Calendars implements [caldav.Store].
func (s *Store) Calendars(context.Context) ([]*calendarv1.Calendar, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*calendarv1.Calendar, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, proto.Clone(s.calendars[id].cal).(*calendarv1.Calendar))
	}
	return out, nil
}

// Calendar implements [caldav.Store].
func (s *Store) Calendar(_ context.Context, id string) (*calendarv1.Calendar, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.calendars[id]
	if !ok {
		return nil, notFound("calendar %s", id)
	}
	return proto.Clone(e.cal).(*calendarv1.Calendar), nil
}

// CreateCalendar implements [caldav.Store].
func (s *Store) CreateCalendar(_ context.Context, c *calendarv1.Calendar) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := lastSegment(c.GetName())
	if _, ok := s.calendars[id]; ok {
		return fmt.Errorf("%w: calendar %s", caldav.ErrAlreadyExists, id)
	}
	c = proto.Clone(c).(*calendarv1.Calendar)
	c.Name, c.Etag = caldav.CalendarName(id), s.etag()
	c.CreateTime, c.UpdateTime = s.stampNow(), s.stampNow()
	s.calendars[id] = &entry{cal: c, objects: map[string]caldav.Object{}}
	s.order = append(s.order, id)
	return nil
}

// UpdateCalendar implements [caldav.Store].
func (s *Store) UpdateCalendar(_ context.Context, c *calendarv1.Calendar, paths []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.calendars[lastSegment(c.GetName())]
	if !ok {
		return notFound("calendar %s", c.GetName())
	}
	if c.GetEtag() != "" && c.GetEtag() != e.cal.GetEtag() {
		return fmt.Errorf("%w: calendar %s changed", caldav.ErrPreconditionFailed, c.GetName())
	}
	next := proto.Clone(e.cal).(*calendarv1.Calendar)
	for _, p := range paths {
		switch p {
		case "display_name":
			next.DisplayName = c.GetDisplayName()
		case "description":
			next.Description = c.GetDescription()
		default:
			return fmt.Errorf("%w: calendar field %q is not writable", caldav.ErrInvalid, p)
		}
	}
	next.Etag, next.UpdateTime = s.etag(), s.stampNow()
	e.cal = next
	return nil
}

// DeleteCalendar implements [caldav.Store].
func (s *Store) DeleteCalendar(_ context.Context, c *calendarv1.Calendar, _ []caldav.Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := lastSegment(c.GetName())
	e, ok := s.calendars[id]
	if !ok {
		return notFound("calendar %s", id)
	}
	if c.GetEtag() != "" && c.GetEtag() != e.cal.GetEtag() {
		return fmt.Errorf("%w: calendar %s changed", caldav.ErrPreconditionFailed, id)
	}
	delete(s.calendars, id)
	s.order = slices.DeleteFunc(s.order, func(x string) bool { return x == id })
	return nil
}

func (s *Store) entry(id string) (*entry, error) {
	e, ok := s.calendars[id]
	if !ok {
		return nil, notFound("calendar %s", id)
	}
	return e, nil
}

// Objects implements [caldav.Store]: events first, then availabilities, each
// in creation order.
func (s *Store) Objects(_ context.Context, calendar string) ([]caldav.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.entry(calendar)
	if err != nil {
		return nil, err
	}
	var events, avails []caldav.Object
	for _, name := range e.order {
		o := cloneObject(e.objects[name])
		if o.Event != nil {
			events = append(events, o)
		} else {
			avails = append(avails, o)
		}
	}
	return append(events, avails...), nil
}

// Object implements [caldav.Store].
func (s *Store) Object(_ context.Context, calendar, id string) (caldav.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.entry(calendar)
	if err != nil {
		return caldav.Object{}, err
	}
	o, ok := e.objects[id]
	if !ok {
		return caldav.Object{}, notFound("object %s in calendar %s", id, calendar)
	}
	return cloneObject(o), nil
}

// ObjectByUID implements [caldav.Store].
func (s *Store) ObjectByUID(_ context.Context, calendar, uid string) (caldav.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.entry(calendar)
	if err != nil {
		return caldav.Object{}, err
	}
	for _, name := range e.order {
		o := e.objects[name]
		if (o.Event != nil && o.Event.GetIcalUid() == uid) || (o.Availability != nil && o.Availability.GetIcalUid() == uid) {
			return cloneObject(o), nil
		}
	}
	return caldav.Object{}, notFound("UID %s in calendar %s", uid, calendar)
}

// PutObject implements [caldav.Store]. The new state is built aside and
// swapped in under the lock, so a failure leaves the calendar as it was.
func (s *Store) PutObject(_ context.Context, req caldav.PutRequest) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := lastSegment(req.Calendar.GetName())
	e, err := s.entry(id)
	if err != nil {
		return false, err
	}
	stored, exists := e.objects[req.Name]
	switch {
	case exists && req.Existing == nil:
		return false, fmt.Errorf("%w: object %s was created meanwhile", caldav.ErrPreconditionFailed, req.Name)
	case !exists && req.Existing != nil:
		return false, notFound("object %s in calendar %s", req.Name, id)
	case exists && stored.ETag() != req.Existing.ETag():
		return false, fmt.Errorf("%w: object %s changed", caldav.ErrPreconditionFailed, req.Name)
	}
	now := s.stampNow()
	createTime := now
	if exists {
		createTime = createTimeOf(stored)
	}
	if req.Availability != nil {
		a := proto.Clone(req.Availability).(*availabilityv1.Availability)
		a.Name, a.Etag, a.CreateTime, a.UpdateTime = req.Calendar.GetName()+"/availabilities/"+req.Name, s.etag(), createTime, now
		a.DeleteTime, a.ExpireTime = nil, nil
		s.put(e, caldav.Object{Name: req.Name, Availability: a})
		return false, nil
	}

	eventName := req.Calendar.GetName() + "/events/" + req.Name
	series := s.stamped(proto.Clone(req.Event).(*eventv1.Event), eventName, createTime, now)
	overrides := cloneEvents(req.Overrides)
	if req.Split == nil {
		s.put(e, caldav.Object{Name: req.Name, Event: series, Overrides: s.stampAll(overrides, eventName, now)})
		return false, nil
	}

	loc := caltime.Zone(req.Calendar.GetTimeZone().GetId())
	p, err := planSplit(series, overrides, req.Split, loc)
	if err != nil {
		return false, err
	}
	writes := []caldav.Object{{Name: req.Name, Event: p.truncated, Overrides: s.stampAll(p.kept, eventName, now)}}
	if p.tail == nil {
		writes[0].Overrides = append(writes[0].Overrides, s.stampAll(p.rehomed, eventName, now)...)
	} else {
		tailObject := lastSegment(p.tail.GetName())
		tailCreate := now
		if old, ok := e.objects[tailObject]; ok {
			if old.Event == nil {
				return false, refuse("the split's new series would replace %s, which is not an event", tailObject)
			}
			tailCreate = createTimeOf(old)
		}
		tail := s.stamped(p.tail, p.tail.GetName(), tailCreate, now)
		writes = append(writes, caldav.Object{Name: tailObject, Event: tail, Overrides: s.stampAll(p.rehomed, tail.GetName(), now)})
	}
	for _, o := range writes {
		s.put(e, o)
	}
	return true, nil
}

// put stores o in e, keeping its place in the order when it replaces one.
func (s *Store) put(e *entry, o caldav.Object) {
	if _, ok := e.objects[o.Name]; !ok {
		e.order = append(e.order, o.Name)
	}
	e.objects[o.Name] = o
}

// stamped gives ev the server-owned fields of a write: its name, a new etag,
// and its create and update times.
func (s *Store) stamped(ev *eventv1.Event, name string, created, now *timestamppb.Timestamp) *eventv1.Event {
	ev.Name, ev.Etag, ev.CreateTime, ev.UpdateTime = name, s.etag(), created, now
	ev.DeleteTime, ev.ExpireTime = nil, nil
	return ev
}

// stampAll names each override after its series and the instance it
// replaces, so a rewrite of the same instance keeps its name.
func (s *Store) stampAll(overrides []*eventv1.Event, series string, now *timestamppb.Timestamp) []*eventv1.Event {
	out := make([]*eventv1.Event, 0, len(overrides))
	for _, o := range overrides {
		name := series + "/overrides/" + mustRead(o.GetOriginalStart()).UTC().Format(caltime.UTCStamp)
		out = append(out, s.stamped(o, name, now, now))
	}
	return out
}

func mustRead(ct *eventv1.CalendarTime) time.Time {
	t, _, _ := caltime.Read(ct, time.UTC)
	return t
}

func cloneEvents(in []*eventv1.Event) []*eventv1.Event {
	out := make([]*eventv1.Event, 0, len(in))
	for _, e := range in {
		out = append(out, proto.Clone(e).(*eventv1.Event))
	}
	return out
}

func createTimeOf(o caldav.Object) *timestamppb.Timestamp {
	if o.Availability != nil {
		return o.Availability.GetCreateTime()
	}
	return o.Event.GetCreateTime()
}

// DeleteObject implements [caldav.Store].
func (s *Store) DeleteObject(_ context.Context, calendar string, o caldav.Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.entry(calendar)
	if err != nil {
		return err
	}
	stored, ok := e.objects[o.Name]
	if !ok {
		return notFound("object %s in calendar %s", o.Name, calendar)
	}
	if stored.ETag() != o.ETag() {
		return fmt.Errorf("%w: object %s changed", caldav.ErrPreconditionFailed, o.Name)
	}
	delete(e.objects, o.Name)
	e.order = slices.DeleteFunc(e.order, func(x string) bool { return x == o.Name })
	return nil
}

// Instances implements [caldav.Store].
func (s *Store) Instances(_ context.Context, c *calendarv1.Calendar, o caldav.Object, window caldav.Period) ([]*eventv1.Event, error) {
	if o.Event == nil {
		return nil, nil
	}
	return instances(o.Event, o.Overrides, caltime.Zone(c.GetTimeZone().GetId()), window)
}

func lastSegment(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return name[i+1:]
		}
	}
	return name
}
