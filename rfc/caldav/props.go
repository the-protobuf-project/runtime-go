package caldav

import (
	"context"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func dav(local string) xml.Name    { return xml.Name{Space: nsDAV, Local: local} }
func caldav(local string) xml.Name { return xml.Name{Space: nsCalDAV, Local: local} }
func cs(local string) xml.Name     { return xml.Name{Space: nsCS, Local: local} }

// resource is one thing a PROPFIND can describe, with the properties it has.
type resource struct {
	href  string
	props func(xml.Name) (string, bool)
	// names lists the properties allprop and propname report.
	names []xml.Name
}

// privileges are what the one principal may do everywhere, RFC 3744 section
// 5.4 and RFC 4791 section 6.1.1. Clients read this to decide whether a
// calendar is writable, so it is answered even without ACL support.
const privileges = "<D:privilege><D:read/></D:privilege><D:privilege><D:write/></D:privilege>" +
	"<D:privilege><D:write-properties/></D:privilege><D:privilege><D:write-content/></D:privilege>" +
	"<D:privilege><D:bind/></D:privilege><D:privilege><D:unbind/></D:privilege>" +
	"<D:privilege><D:read-current-user-privilege-set/></D:privilege><D:privilege><C:read-free-busy/></D:privilege>"

// common answers the properties every resource has.
func (h *Handler) common(name xml.Name) (string, bool) {
	switch name {
	case dav("current-user-principal"):
		return hrefXML(h.principalPath()), true
	case dav("current-user-privilege-set"):
		return privileges, true
	}
	return "", false
}

var commonNames = []xml.Name{dav("current-user-principal"), dav("current-user-privilege-set")}

func (h *Handler) collectionResource(href, displayName string, extra ...string) resource {
	types := "<D:collection/>" + strings.Join(extra, "")
	return resource{
		href:  href,
		names: append([]xml.Name{dav("resourcetype"), dav("displayname")}, commonNames...),
		props: func(n xml.Name) (string, bool) {
			switch n {
			case dav("resourcetype"):
				return types, true
			case dav("displayname"):
				return escapeText(displayName), true
			}
			return h.common(n)
		},
	}
}

func (h *Handler) principalResource() resource {
	r := h.collectionResource(h.principalPath(), h.name, "<D:principal/>")
	base := r.props
	r.names = append(r.names, dav("principal-URL"), caldav("calendar-home-set"))
	r.props = func(n xml.Name) (string, bool) {
		switch n {
		case dav("principal-URL"):
			return hrefXML(h.principalPath()), true
		case caldav("calendar-home-set"):
			return hrefXML(h.homePath()), true
		}
		return base(n)
	}
	return r
}

// supportedReports is the report set calendars and the home advertise.
const supportedReports = "<D:supported-report><D:report><C:calendar-query/></D:report></D:supported-report>" +
	"<D:supported-report><D:report><C:calendar-multiget/></D:report></D:supported-report>" +
	"<D:supported-report><D:report><C:free-busy-query/></D:report></D:supported-report>"

// calendarResource describes a calendar collection. objs are its live
// members, for getctag, when read is true; an empty calendar's are none, and
// still have a ctag. Unread, getctag is not offered.
func (h *Handler) calendarResource(c *calendarv1.Calendar, objs []Object, read bool) resource {
	id := lastSegment(c.GetName())
	r := h.collectionResource(h.calendarHref(id), c.GetDisplayName(), "<C:calendar/>")
	base := r.props
	r.names = append(r.names, dav("owner"), dav("supported-report-set"), dav("getlastmodified"),
		caldav("calendar-description"), caldav("supported-calendar-component-set"),
		caldav("supported-calendar-data"), cs("getctag"))
	limits := map[xml.Name]string{}
	if v := c.GetMaxResourceSize(); v > 0 {
		limits[caldav("max-resource-size")] = strconv.FormatInt(v, 10)
	}
	if v := c.GetMinDateTime(); v != nil {
		limits[caldav("min-date-time")] = v.AsTime().UTC().Format(utcStamp)
	}
	if v := c.GetMaxDateTime(); v != nil {
		limits[caldav("max-date-time")] = v.AsTime().UTC().Format(utcStamp)
	}
	if v := c.GetMaxInstances(); v > 0 {
		limits[caldav("max-instances")] = strconv.Itoa(int(v))
	}
	if v := c.GetMaxAttendeesPerInstance(); v > 0 {
		limits[caldav("max-attendees-per-instance")] = strconv.Itoa(int(v))
	}
	for n := range limits {
		r.names = append(r.names, n)
	}
	r.props = func(n xml.Name) (string, bool) {
		if v, ok := limits[n]; ok {
			return v, true
		}
		switch n {
		case dav("owner"):
			return hrefXML(h.principalPath()), true
		case dav("supported-report-set"):
			return supportedReports, true
		case dav("getlastmodified"):
			return httpDate(c.GetUpdateTime()), c.GetUpdateTime() != nil
		case caldav("calendar-description"):
			return escapeText(c.GetDescription()), c.GetDescription() != ""
		case caldav("supported-calendar-component-set"):
			return `<C:comp name="VEVENT"/><C:comp name="VAVAILABILITY"/>`, true
		case caldav("supported-calendar-data"):
			return `<C:calendar-data content-type="text/calendar" version="2.0"/>`, true
		case cs("getctag"):
			return ctag(c, objs), read
		}
		return base(n)
	}
	return r
}

// objectResource describes a calendar object resource.
func (h *Handler) objectResource(calendar string, o Object) resource {
	updated := o.updateTime()
	return resource{
		href: h.objectHref(calendar, o.Name),
		names: append([]xml.Name{dav("resourcetype"), dav("getetag"), dav("getcontenttype"), dav("getlastmodified")},
			commonNames...),
		props: func(n xml.Name) (string, bool) {
			switch n {
			case dav("resourcetype"):
				return "", true
			case dav("getetag"):
				return escapeText(quoteETag(o.ETag())), true
			case dav("getcontenttype"):
				return escapeText(o.contentType()), true
			case dav("getlastmodified"):
				return httpDate(updated), updated != nil
			}
			return h.common(n)
		},
	}
}

func httpDate(t *timestamppb.Timestamp) string {
	if t == nil {
		return ""
	}
	return t.AsTime().UTC().Format(http.TimeFormat)
}

// propfind answers PROPFIND, RFC 4918 section 9.1.
func (h *Handler) propfind(w http.ResponseWriter, r *http.Request, t target) error {
	body, err := readXML(r)
	if err != nil {
		return err
	}
	// An empty body is allprop, section 9.1.
	mode, asked := "allprop", []xml.Name(nil)
	if body != nil {
		switch {
		case !body.is(nsDAV, "propfind"):
			return Errorf(http.StatusBadRequest, "", "PROPFIND body must be DAV:propfind")
		case body.child(nsDAV, "propname") != nil:
			mode = "propname"
		case body.child(nsDAV, "prop") != nil:
			mode, asked = "prop", body.child(nsDAV, "prop").propNames()
		}
	}
	// Depth "infinity" is answered as 1: this tree is at most three deep
	// below the home, and section 9.1 lets a server decline infinity.
	depth := r.Header.Get("Depth")
	children := depth == "1" || strings.EqualFold(depth, "infinity")

	resources, err := h.resources(r.Context(), t, children, mode == "allprop" || containsName(asked, cs("getctag")))
	if err != nil {
		return err
	}
	out := make([]response, 0, len(resources))
	for _, res := range resources {
		resp := response{href: res.href}
		switch mode {
		case "propname":
			for _, n := range res.names {
				resp.found = append(resp.found, prop{name: n})
			}
		case "allprop":
			for _, n := range res.names {
				if v, ok := res.props(n); ok {
					resp.found = append(resp.found, prop{name: n, inner: v})
				}
			}
		default:
			for _, n := range asked {
				if v, ok := res.props(n); ok {
					resp.found = append(resp.found, prop{name: n, inner: v})
				} else {
					resp.missing = append(resp.missing, n)
				}
			}
		}
		out = append(out, resp)
	}
	writeMultistatus(w, out)
	return nil
}

func containsName(names []xml.Name, n xml.Name) bool {
	for _, x := range names {
		if x == n {
			return true
		}
	}
	return false
}

// resources returns the resource t names and, with children, its members.
// wantCtag reads each calendar's members so getctag can be answered.
func (h *Handler) resources(ctx context.Context, t target, children, wantCtag bool) ([]resource, error) {
	switch t.kind {
	case kindRoot:
		out := []resource{h.collectionResource(h.prefix+"/", h.name)}
		if children {
			out = append(out, h.collectionResource(h.homePath(), "Calendars"))
		}
		return out, nil
	case kindPrincipal:
		return []resource{h.principalResource()}, nil
	case kindHome:
		home := h.collectionResource(h.homePath(), "Calendars")
		base := home.props
		home.names = append(home.names, dav("supported-report-set"))
		home.props = func(n xml.Name) (string, bool) {
			if n == dav("supported-report-set") {
				return supportedReports, true
			}
			return base(n)
		}
		out := []resource{home}
		if children {
			cals, err := h.store.Calendars(ctx)
			if err != nil {
				return nil, err
			}
			for _, c := range cals {
				var objs []Object
				if wantCtag {
					if objs, err = h.store.Objects(ctx, lastSegment(c.GetName())); err != nil {
						return nil, err
					}
				}
				out = append(out, h.calendarResource(c, objs, wantCtag))
			}
		}
		return out, nil
	case kindCalendar:
		c, err := h.calendar(ctx, t.calendar)
		if err != nil {
			return nil, err
		}
		var objs []Object
		if wantCtag || children {
			if objs, err = h.store.Objects(ctx, t.calendar); err != nil {
				return nil, err
			}
		}
		out := []resource{h.calendarResource(c, objs, wantCtag || children)}
		if children {
			for _, o := range objs {
				out = append(out, h.objectResource(t.calendar, o))
			}
		}
		return out, nil
	}
	if _, err := h.calendar(ctx, t.calendar); err != nil {
		return nil, err
	}
	o, err := h.store.Object(ctx, t.calendar, t.object)
	if err != nil {
		return nil, err
	}
	return []resource{h.objectResource(t.calendar, o)}, nil
}

// proppatch answers PROPPATCH on a calendar, RFC 4918 section 9.2. Only the
// display name and description are writable; anything else fails the whole
// request, as section 9.2 requires of a partial failure.
func (h *Handler) proppatch(w http.ResponseWriter, r *http.Request, t target) error {
	if t.kind != kindCalendar {
		return Errorf(http.StatusForbidden, "", "only calendar collections take PROPPATCH")
	}
	body, err := readXML(r)
	if err != nil {
		return err
	}
	if !body.is(nsDAV, "propertyupdate") {
		return Errorf(http.StatusBadRequest, "", "PROPPATCH body must be DAV:propertyupdate")
	}
	c, err := h.calendar(r.Context(), t.calendar)
	if err != nil {
		return err
	}
	update := &calendarv1.Calendar{Name: c.GetName(), Etag: c.GetEtag()}
	var paths []string
	var changed, refused []xml.Name
	for _, op := range body.Nodes {
		set := op.is(nsDAV, "set")
		if !set && !op.is(nsDAV, "remove") {
			continue
		}
		for _, p := range op.child(nsDAV, "prop").Nodes {
			switch p.XMLName {
			case dav("displayname"):
				if set {
					update.DisplayName = strings.TrimSpace(p.Text)
				}
				paths, changed = append(paths, "display_name"), append(changed, p.XMLName)
			case caldav("calendar-description"):
				if set {
					update.Description = strings.TrimSpace(p.Text)
				}
				paths, changed = append(paths, "description"), append(changed, p.XMLName)
			default:
				refused = append(refused, p.XMLName)
			}
		}
	}
	resp := response{href: h.href(t)}
	if len(refused) > 0 {
		resp.failed = map[int][]xml.Name{http.StatusForbidden: refused}
		if len(changed) > 0 {
			resp.failed[http.StatusFailedDependency] = changed
		}
		writeMultistatus(w, []response{resp})
		return nil
	}
	if len(paths) > 0 {
		if err := h.store.UpdateCalendar(r.Context(), update, paths); err != nil {
			return err
		}
	}
	for _, n := range changed {
		resp.found = append(resp.found, prop{name: n})
	}
	writeMultistatus(w, []response{resp})
	return nil
}

// mkcalendar answers MKCALENDAR, RFC 4791 section 5.3.1: CreateCalendar, with
// the display name and description the body may set.
func (h *Handler) mkcalendar(w http.ResponseWriter, r *http.Request, t target) error {
	if t.kind != kindCalendar {
		return Errorf(http.StatusForbidden, "", "calendars are created under %s", h.homePath())
	}
	exists := Errorf(http.StatusMethodNotAllowed, ResourceMustBeNull, "%s already exists", h.href(t))
	if _, err := h.calendar(r.Context(), t.calendar); err == nil {
		return exists
	} else if !h.notFound(err) {
		return err
	}
	body, err := readXML(r)
	if err != nil {
		return err
	}
	c := &calendarv1.Calendar{Name: CalendarName(t.calendar), DisplayName: t.calendar}
	if body != nil {
		if set := body.child(nsDAV, "set"); set != nil {
			props := set.child(nsDAV, "prop")
			if n := props.child(nsDAV, "displayname"); n != nil {
				c.DisplayName = strings.TrimSpace(n.Text)
			}
			if n := props.child(nsCalDAV, "calendar-description"); n != nil {
				c.Description = strings.TrimSpace(n.Text)
			}
		}
	}
	if err := h.store.CreateCalendar(r.Context(), c); err != nil {
		if h.statusOf(err) == http.StatusPreconditionFailed {
			return exists
		}
		return err
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusCreated)
	return nil
}
