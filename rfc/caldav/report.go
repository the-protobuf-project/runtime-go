package caldav

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/internal/caltime"
)

// report answers REPORT, RFC 4791 section 7: the three reports this server
// advertises. Anything else is refused with DAV:supported-report, which is
// how a client learns to fall back (sync-collection to a PROPFIND of etags).
func (h *Handler) report(w http.ResponseWriter, r *http.Request, t target) error {
	body, err := readXML(r)
	if err != nil {
		return err
	}
	switch {
	case body.is(nsCalDAV, "calendar-query"):
		return h.calendarQuery(r.Context(), w, t, body)
	case body.is(nsCalDAV, "calendar-multiget"):
		return h.calendarMultiget(r.Context(), w, body)
	case body.is(nsCalDAV, "free-busy-query"):
		return h.freeBusyQuery(r.Context(), w, t, body)
	}
	return Errorf(http.StatusForbidden, SupportedReport, "unsupported REPORT")
}

// dataRequest is what a report asks to see of each match.
type dataRequest struct {
	props  []xml.Name
	expand *Period // C:expand inside C:calendar-data
}

func (h *Handler) readDataRequest(body *node) (dataRequest, error) {
	var d dataRequest
	p := body.child(nsDAV, "prop")
	if p == nil {
		return d, nil
	}
	d.props = p.propNames()
	if cd := p.child(nsCalDAV, "calendar-data"); cd != nil {
		if ex := cd.child(nsCalDAV, "expand"); ex != nil {
			sp, err := h.timeRange(ex, true)
			if err != nil {
				return d, err
			}
			d.expand = &sp
		}
	}
	return d, nil
}

// filter is the part of a calendar-query filter this server evaluates:
// which component, which time, and which UID.
type filter struct {
	component string  // "VEVENT", "VAVAILABILITY", or "" for any
	none      bool    // the component is asked to be absent: nothing matches
	window    *Period // time-range, nil for any
	uid       string  // prop-filter UID text-match, "" for any
	uidNegate bool
}

// readFilter parses C:filter, RFC 4791 section 9.7. What it cannot evaluate
// it refuses with CALDAV:supported-filter rather than ignores: a filter
// silently widened returns objects the client said it did not want.
func (h *Handler) readFilter(body *node) (filter, error) {
	var f filter
	fl := body.child(nsCalDAV, "filter")
	if fl == nil {
		return f, nil
	}
	cal := fl.child(nsCalDAV, "comp-filter")
	if cal == nil || cal.attr("name") != "VCALENDAR" {
		return f, Errorf(http.StatusForbidden, "C:valid-filter", "a filter starts with comp-filter VCALENDAR")
	}
	comp := cal.child(nsCalDAV, "comp-filter")
	if comp == nil {
		return f, nil
	}
	f.component = strings.ToUpper(comp.attr("name"))
	for _, c := range comp.Nodes {
		switch {
		case c.is(nsCalDAV, "is-not-defined"):
			f.none = true
		case c.is(nsCalDAV, "time-range"):
			sp, err := h.timeRange(&c, false)
			if err != nil {
				return f, err
			}
			f.window = &sp
		case c.is(nsCalDAV, "prop-filter") && strings.EqualFold(c.attr("name"), "UID"):
			tm := c.child(nsCalDAV, "text-match")
			if tm == nil {
				return f, Errorf(http.StatusForbidden, "C:supported-filter", "a UID prop-filter needs a text-match")
			}
			f.uid, f.uidNegate = tm.Text, tm.attr("negate-condition") == "yes"
		default:
			return f, Errorf(http.StatusForbidden, "C:supported-filter",
				"filter element %s is not supported; time-range and UID text-match are", c.XMLName.Local)
		}
	}
	return f, nil
}

// timeRange reads the start and end attributes of a time-range or expand
// element, UTC DATE-TIMEs, RFC 4791 section 9.9. An open end is bounded at
// the handler's max window from the other, since an unbounded rule
// cannot be expanded to infinity; expand requires both ends.
func (h *Handler) timeRange(n *node, both bool) (Period, error) {
	parse := func(attr string) (time.Time, bool, error) {
		v := n.attr(attr)
		if v == "" {
			return time.Time{}, false, nil
		}
		t, err := time.Parse(utcStamp, v)
		if err != nil {
			return time.Time{}, false, Errorf(http.StatusBadRequest, "C:valid-filter", "%s %q is not a UTC DATE-TIME", attr, v)
		}
		return t, true, nil
	}
	start, hasStart, err := parse("start")
	if err != nil {
		return Period{}, err
	}
	end, hasEnd, err := parse("end")
	if err != nil {
		return Period{}, err
	}
	switch {
	case both && (!hasStart || !hasEnd):
		return Period{}, Errorf(http.StatusBadRequest, "C:valid-filter", "%s needs both start and end", n.XMLName.Local)
	case !hasStart && !hasEnd:
		return Period{}, Errorf(http.StatusBadRequest, "C:valid-filter", "time-range needs start or end")
	case !hasStart:
		start = end.Add(-h.maxWindow)
	case !hasEnd:
		end = start.Add(h.maxWindow)
	}
	if !end.After(start) {
		return Period{}, Errorf(http.StatusBadRequest, "C:valid-filter", "time-range end is not after start")
	}
	return Period{Start: start, End: end}, nil
}

// calendarQuery answers calendar-query, RFC 4791 section 7.8.
func (h *Handler) calendarQuery(ctx context.Context, w http.ResponseWriter, t target, body *node) error {
	if t.kind != kindCalendar && t.kind != kindObject {
		return Errorf(http.StatusForbidden, "", "calendar-query runs on a calendar or a calendar object")
	}
	data, err := h.readDataRequest(body)
	if err != nil {
		return err
	}
	f, err := h.readFilter(body)
	if err != nil {
		return err
	}
	c, err := h.calendar(ctx, t.calendar)
	if err != nil {
		return err
	}
	var objs []Object
	if t.kind == kindObject {
		o, oerr := h.store.Object(ctx, t.calendar, t.object)
		if oerr != nil {
			return oerr
		}
		objs = []Object{o}
	} else if objs, err = h.store.Objects(ctx, t.calendar); err != nil {
		return err
	}
	var out []response
	for _, o := range objs {
		ok, err := h.matches(ctx, f, c, o)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		resp, err := h.objectResponse(ctx, c, o, data)
		if err != nil {
			return err
		}
		out = append(out, resp)
	}
	writeMultistatus(w, out)
	return nil
}

func (h *Handler) matches(ctx context.Context, f filter, c *calendarv1.Calendar, o Object) (bool, error) {
	if f.none {
		return false, nil
	}
	switch f.component {
	case "VEVENT":
		if o.Event == nil {
			return false, nil
		}
	case "VAVAILABILITY":
		if o.Availability == nil {
			return false, nil
		}
	case "":
	default:
		return false, nil // VTODO, VJOURNAL: this server holds none
	}
	if f.uid != "" {
		uid := o.UID()
		// RFC 4791 section 9.7.5: text-match is a case-insensitive substring
		// match under the default i;ascii-casemap collation.
		if strings.Contains(strings.ToLower(uid), strings.ToLower(f.uid)) == f.uidNegate {
			return false, nil
		}
	}
	// An availability's range is not narrowed by time here: one with no end
	// covers every window, and RFC 7953 clients fetch the whole set anyway.
	if f.window == nil || o.Event == nil {
		return true, nil
	}
	instances, err := h.store.Instances(ctx, c, o, *f.window)
	if err != nil {
		return false, err
	}
	return len(instances) > 0, nil
}

// objectResponse renders one matched object with the properties asked for:
// calendar-data in full, or expanded to its instances in the asked window.
func (h *Handler) objectResponse(ctx context.Context, c *calendarv1.Calendar, o Object, data dataRequest) (response, error) {
	res := h.objectResource(lastSegment(c.GetName()), o)
	resp := response{href: res.href}
	names := data.props
	if len(names) == 0 {
		names = []xml.Name{dav("getetag")}
	}
	for _, n := range names {
		if n != caldav("calendar-data") {
			if v, ok := res.props(n); ok {
				resp.found = append(resp.found, prop{name: n, inner: v})
			} else {
				resp.missing = append(resp.missing, n)
			}
			continue
		}
		text, err := h.objectData(ctx, c, o, data.expand)
		if err != nil {
			return resp, err
		}
		resp.found = append(resp.found, prop{name: n, inner: escapeText(text)})
	}
	return resp, nil
}

func (h *Handler) objectData(ctx context.Context, c *calendarv1.Calendar, o Object, expand *Period) (string, error) {
	if expand == nil || o.Event == nil || o.Event.GetRecurrence() == nil {
		block, err := objectComponent(o)
		return h.calendarObject(block), err
	}
	instances, err := h.store.Instances(ctx, c, o, *expand)
	if err != nil {
		return "", err
	}
	blocks, err := instanceComponents(o.Event, instances, caltime.Zone(c.GetTimeZone().GetId()))
	if err != nil {
		return "", err
	}
	return h.calendarObject(blocks...), nil
}

// calendarMultiget answers calendar-multiget, RFC 4791 section 7.9 -- the
// BatchGetEvents of CalDAV. A missing href is a 404 response, not a failure
// of the report.
func (h *Handler) calendarMultiget(ctx context.Context, w http.ResponseWriter, body *node) error {
	data, err := h.readDataRequest(body)
	if err != nil {
		return err
	}
	var out []response
	for _, n := range body.Nodes {
		if !n.is(nsDAV, "href") {
			continue
		}
		href := strings.TrimSpace(n.Text)
		if u, err := url.Parse(href); err == nil {
			href = u.EscapedPath()
		}
		t, ok := h.parsePath(href)
		if !ok || t.kind != kindObject {
			out = append(out, response{href: href, status: http.StatusNotFound})
			continue
		}
		c, err := h.calendar(ctx, t.calendar)
		if err != nil {
			out = append(out, response{href: href, status: http.StatusNotFound})
			continue
		}
		o, err := h.store.Object(ctx, t.calendar, t.object)
		if err != nil {
			out = append(out, response{href: href, status: http.StatusNotFound})
			continue
		}
		resp, err := h.objectResponse(ctx, c, o, data)
		if err != nil {
			return err
		}
		out = append(out, resp)
	}
	writeMultistatus(w, out)
	return nil
}

// freeBusyQuery answers free-busy-query, RFC 4791 section 7.10, from the
// store's free/busy computation.
func (h *Handler) freeBusyQuery(ctx context.Context, w http.ResponseWriter, t target, body *node) error {
	if t.kind != kindCalendar {
		return Errorf(http.StatusForbidden, "", "free-busy-query runs on a calendar collection")
	}
	tr := body.child(nsCalDAV, "time-range")
	if tr == nil {
		return Errorf(http.StatusBadRequest, "C:valid-filter", "free-busy-query needs a time-range")
	}
	window, err := h.timeRange(tr, true)
	if err != nil {
		return err
	}
	if window.Duration() > h.maxWindow {
		return Errorf(http.StatusForbidden, MaxDateTime, "window longer than %v", h.maxWindow)
	}
	c, err := h.calendar(ctx, t.calendar)
	if err != nil {
		return err
	}
	fb, err := h.store.FreeBusy(ctx, c, window)
	if err != nil {
		return err
	}
	text := h.freeBusyObject(fb, window, h.now())
	w.Header().Set("Content-Type", MediaType)
	w.WriteHeader(http.StatusOK)
	_, err = io.WriteString(w, text)
	return err
}
