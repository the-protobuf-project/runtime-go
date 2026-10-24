package caldav

import (
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultPrefix is where the CalDAV tree is mounted unless [WithPrefix] says
// otherwise.
const DefaultPrefix = "/dav"

// DefaultMaxWindow bounds a free-busy-query window, and closes an open-ended
// time-range: an unbounded rule cannot be expanded to infinity.
const DefaultMaxWindow = 400 * 24 * time.Hour

// Handler answers CalDAV requests over a [Store].
type Handler struct {
	store       Store
	prefix      string
	name        string
	prodID      string
	maxWindow   time.Duration
	now         func() time.Time
	errorStatus func(error) int
}

// Option configures a [Handler].
type Option func(*Handler)

// WithPrefix mounts the tree under prefix ("/dav" by default). It has no
// trailing slash.
func WithPrefix(prefix string) Option {
	return func(h *Handler) { h.prefix = strings.TrimSuffix(prefix, "/") }
}

// WithName is the display name of the root and the principal ("CalDAV" by
// default).
func WithName(name string) Option { return func(h *Handler) { h.name = name } }

// WithProductID is the PRODID every VCALENDAR the handler writes carries.
func WithProductID(prodID string) Option { return func(h *Handler) { h.prodID = prodID } }

// WithMaxWindow bounds a free-busy-query window and closes open time-ranges
// ([DefaultMaxWindow] by default).
func WithMaxWindow(d time.Duration) Option { return func(h *Handler) { h.maxWindow = d } }

// WithClock sets the clock DTSTAMPs are read from.
func WithClock(now func() time.Time) Option { return func(h *Handler) { h.now = now } }

// WithErrorStatus maps a store's own errors to HTTP statuses, consulted after
// [*Error] and the package's sentinels. It returns 0 for an error it does not
// know, which is then a 500. A store error mapped to 404 counts as not found.
func WithErrorStatus(f func(error) int) Option { return func(h *Handler) { h.errorStatus = f } }

// New returns a Handler over store.
func New(store Store, opts ...Option) *Handler {
	h := &Handler{
		store:     store,
		prefix:    DefaultPrefix,
		name:      "CalDAV",
		prodID:    "-//the-protobuf-project//runtime-go CalDAV//EN",
		maxWindow: DefaultMaxWindow,
		now:       time.Now,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Prefix is the path the tree is mounted under.
func (h *Handler) Prefix() string { return h.prefix }

// methods are the HTTP methods CalDAV uses: RFC 4918's WebDAV methods, RFC
// 4791's MKCALENDAR, and the plain HTTP ones on calendar object resources.
var methods = []string{
	http.MethodOptions, "PROPFIND", "PROPPATCH", "REPORT", "MKCALENDAR",
	http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete,
}

// Methods lists the HTTP methods the handler answers, for a router that
// registers routes per method.
func Methods() []string { return append([]string(nil), methods...) }

// WellKnown answers /.well-known/caldav with a redirect to the tree, RFC 6764
// section 5: clients probe it first. Mount it for GET, HEAD, PROPFIND and
// OPTIONS.
func (h *Handler) WellKnown() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, h.prefix+"/", http.StatusMovedPermanently)
	})
}

// ServeHTTP dispatches one CalDAV request. Requests outside the prefix are
// 404s.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, ok := h.parsePath(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}
	var err error
	switch r.Method {
	case http.MethodOptions:
		options(w)
	case "PROPFIND":
		err = h.propfind(w, r, t)
	case "PROPPATCH":
		err = h.proppatch(w, r, t)
	case "REPORT":
		err = h.report(w, r, t)
	case "MKCALENDAR":
		err = h.mkcalendar(w, r, t)
	case http.MethodGet, http.MethodHead:
		err = h.get(w, r, t)
	case http.MethodPut:
		err = h.put(w, r, t)
	case http.MethodDelete:
		err = h.delete(w, r, t)
	default:
		w.Header().Set("Allow", strings.Join(methods, ", "))
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	if err != nil {
		h.writeError(w, err)
	}
}

func options(w http.ResponseWriter) {
	// RFC 4791 section 5.1: calendar-access in the DAV header is how a
	// client learns this is a CalDAV server. Class 3 is RFC 4918's.
	w.Header().Set("DAV", "1, 3, calendar-access")
	w.Header().Set("Allow", strings.Join(methods, ", "))
	w.WriteHeader(http.StatusOK)
}

// kind is what a request path names.
type kind int

const (
	kindRoot kind = iota
	kindPrincipal
	kindHome
	kindCalendar
	kindObject
)

// target is a parsed request path.
type target struct {
	kind     kind
	calendar string // calendar id, for kindCalendar and kindObject
	object   string // object name, for kindObject
}

// parsePath reads an escaped request path. Segments are unescaped one by one,
// so a name holding an encoded slash stays one segment.
//
//	{prefix}/                            root
//	{prefix}/principals/default/         the one principal
//	{prefix}/calendars/                  calendar home: every calendar
//	{prefix}/calendars/{calendar}/       a calendar collection
//	{prefix}/calendars/{calendar}/{obj}  an event, or an RFC 7953 availability
func (h *Handler) parsePath(escaped string) (target, bool) {
	rest, ok := strings.CutPrefix(escaped, h.prefix)
	if !ok || (rest != "" && rest[0] != '/') {
		return target{}, false
	}
	var segs []string
	for _, s := range strings.Split(rest, "/") {
		if s == "" {
			continue
		}
		u, err := url.PathUnescape(s)
		if err != nil || u == "" {
			return target{}, false
		}
		segs = append(segs, u)
	}
	switch {
	case len(segs) == 0:
		return target{kind: kindRoot}, true
	case len(segs) == 2 && segs[0] == "principals" && segs[1] == "default":
		return target{kind: kindPrincipal}, true
	case len(segs) >= 1 && segs[0] == "calendars":
		switch len(segs) {
		case 1:
			return target{kind: kindHome}, true
		case 2:
			return target{kind: kindCalendar, calendar: segs[1]}, true
		case 3:
			return target{kind: kindObject, calendar: segs[1], object: segs[2]}, true
		}
	}
	return target{}, false
}

// href is the canonical path of t: collections end in a slash.
func (h *Handler) href(t target) string {
	switch t.kind {
	case kindPrincipal:
		return h.principalPath()
	case kindHome:
		return h.homePath()
	case kindCalendar:
		return h.calendarHref(t.calendar)
	case kindObject:
		return h.objectHref(t.calendar, t.object)
	}
	return h.prefix + "/"
}

func (h *Handler) principalPath() string { return h.prefix + "/principals/default/" }
func (h *Handler) homePath() string      { return h.prefix + "/calendars/" }

func (h *Handler) calendarHref(id string) string {
	return h.prefix + "/calendars/" + url.PathEscape(id) + "/"
}

func (h *Handler) objectHref(calendar, name string) string {
	return h.calendarHref(calendar) + url.PathEscape(name)
}

// CalendarName is the resource name of the calendar with id.
func CalendarName(id string) string { return "calendars/" + id }
