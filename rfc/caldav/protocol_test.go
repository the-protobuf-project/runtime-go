package caldav_test

// Litmus-style protocol checks: one behavior each, the edges a WebDAV or
// CalDAV client relies on.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/memstore"
)

const (
	nsDecl = `xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"`
	cal    = "/dav/calendars/work/"
)

func vcal(components ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\n" + strings.Join(components, "") + "END:VCALENDAR\r\n"
}

func vevent(uid, start, end string, extra ...string) string {
	return "BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20270101T000000Z\r\nDTSTART:" + start + "\r\nDTEND:" + end + "\r\n" +
		strings.Join(extra, "") + "END:VEVENT\r\n"
}

// withCalendar is a server holding one empty calendar, "work".
func withCalendar(t *testing.T, opts ...caldav.Option) (*memstore.Store, davClient) {
	t.Helper()
	store, c := newServer(t, opts...)
	c.want("MKCALENDAR", cal, "", http.StatusCreated)
	return store, c
}

func TestOptions(t *testing.T) {
	_, c := newServer(t)
	for _, path := range []string{"/dav", "/dav/", cal, cal + "x.ics"} {
		resp, _ := c.want("OPTIONS", path, "", http.StatusOK)
		if dav := resp.Header.Get("DAV"); dav != "1, 3, calendar-access" {
			t.Fatalf("OPTIONS %s: DAV %q", path, dav)
		}
		for _, m := range []string{"PROPFIND", "REPORT", "MKCALENDAR", "PUT", "DELETE"} {
			if !strings.Contains(resp.Header.Get("Allow"), m) {
				t.Fatalf("OPTIONS %s: Allow %q lacks %s", path, resp.Header.Get("Allow"), m)
			}
		}
	}
}

func TestUnknownPathsAndMethods(t *testing.T) {
	_, c := withCalendar(t)
	c.want("PROPFIND", "/dav/nowhere/", "", http.StatusNotFound)
	c.want("GET", "/dav/calendars/work/a/b", "", http.StatusNotFound)
	resp, _ := c.want("PATCH", cal, "", http.StatusMethodNotAllowed)
	if !strings.Contains(resp.Header.Get("Allow"), "PROPFIND") {
		t.Fatalf("405 without Allow: %v", resp.Header)
	}
	c.want("GET", "/dav/calendars/", "", http.StatusMethodNotAllowed)
	c.want("PROPFIND", "/dav/calendars/missing/", "", http.StatusNotFound, "Depth", "0")
	c.want("GET", cal+"missing.ics", "", http.StatusNotFound)
	c.want("DELETE", cal+"missing.ics", "", http.StatusNotFound)
	c.want("DELETE", "/dav/calendars/", "", http.StatusForbidden)
}

func TestPropfindModes(t *testing.T) {
	_, c := withCalendar(t)
	_, out := c.want("PROPFIND", cal, `<D:propfind xmlns:D="DAV:"><D:propname/></D:propfind>`, http.StatusMultiStatus, "Depth", "0")
	mustContain(t, "propname", out, "<D:resourcetype/>", "<D:displayname/>", "<CS:getctag/>", "<C:supported-calendar-component-set/>")

	_, out = c.want("PROPFIND", cal, "", http.StatusMultiStatus, "Depth", "0")
	mustContain(t, "allprop", out, "<D:collection/><C:calendar/>", "<CS:getctag>", `<C:comp name="VEVENT"/>`, "<D:current-user-privilege-set>")

	_, out = c.want("PROPFIND", cal, `<D:propfind xmlns:D="DAV:" xmlns:X="urn:example"><D:prop><D:displayname/><X:color/></D:prop></D:propfind>`, http.StatusMultiStatus, "Depth", "0")
	mustContain(t, "unknown prop", out, "<D:displayname>work</D:displayname>", `<X:color xmlns:X="urn:example"/>`, "404 Not Found")

	// Depth infinity is answered as 1.
	c.want("PUT", cal+"a.ics", vcal(vevent("a", "20270104T100000Z", "20270104T110000Z")), http.StatusCreated)
	for depth, want := range map[string]int{"0": 1, "1": 2, "infinity": 2} {
		_, ms := c.want("PROPFIND", cal, `<D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`, http.StatusMultiStatus, "Depth", depth)
		if n := strings.Count(ms, "<D:response>"); n != want {
			t.Fatalf("Depth %s: %d responses, want %d", depth, n, want)
		}
	}
	_, out = c.want("PROPFIND", "/dav/", "", http.StatusMultiStatus, "Depth", "1")
	mustContain(t, "root", out, "<D:href>/dav/</D:href>", "<D:href>/dav/calendars/</D:href>")
}

func TestMalformedBodies(t *testing.T) {
	_, c := withCalendar(t)
	c.want("PROPFIND", cal, "<D:propfind", http.StatusBadRequest)
	c.want("PROPFIND", cal, `<D:prop xmlns:D="DAV:"/>`, http.StatusBadRequest)
	c.want("PROPPATCH", cal, `<D:propfind xmlns:D="DAV:"/>`, http.StatusBadRequest)
	c.want("REPORT", cal, "not xml", http.StatusBadRequest)
}

func TestMkcalendar(t *testing.T) {
	_, c := withCalendar(t)
	_, out := c.want("MKCALENDAR", cal, "", http.StatusMethodNotAllowed)
	mustContain(t, "existing", out, "<D:resource-must-be-null/>")
	c.want("MKCALENDAR", "/dav/calendars/", "", http.StatusForbidden)
	c.want("MKCALENDAR", cal+"x.ics", "", http.StatusForbidden)
	c.want("MKCALENDAR", "/dav/calendars/team/", `<C:mkcalendar `+nsDecl+`><D:set><D:prop><D:displayname>R&amp;D &lt;team&gt;</D:displayname></D:prop></D:set></C:mkcalendar>`, http.StatusCreated)
	_, out = c.want("PROPFIND", "/dav/calendars/team/", "", http.StatusMultiStatus, "Depth", "0")
	mustContain(t, "escaped name", out, "<D:displayname>R&amp;D &lt;team&gt;</D:displayname>")
}

func TestProppatch(t *testing.T) {
	store, c := withCalendar(t)
	c.want("PROPPATCH", "/dav/calendars/", `<D:propertyupdate xmlns:D="DAV:"/>`, http.StatusForbidden)
	_, out := c.want("PROPPATCH", cal, `<D:propertyupdate `+nsDecl+`><D:set><D:prop><C:calendar-description>Desk</C:calendar-description></D:prop></D:set><D:remove><D:prop><D:displayname/></D:prop></D:remove></D:propertyupdate>`, http.StatusMultiStatus)
	mustContain(t, "proppatch", out, "<C:calendar-description/>", "<D:displayname/>", "200 OK")
	got, err := store.Calendar(context.Background(), "work")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetDescription() != "Desk" || got.GetDisplayName() != "" {
		t.Fatalf("after PROPPATCH: %q / %q", got.GetDisplayName(), got.GetDescription())
	}
}

func TestPutPreconditions(t *testing.T) {
	_, c := withCalendar(t)
	body := vcal(vevent("one@example.com", "20270104T100000Z", "20270104T110000Z"))

	c.want("PUT", "/dav/calendars/missing/a.ics", body, http.StatusConflict)
	c.want("PUT", cal, body, http.StatusMethodNotAllowed)
	c.want("PUT", cal+"a.ics", body, http.StatusPreconditionFailed, "If-Match", "*")
	c.want("PUT", cal+"a.ics", body, http.StatusPreconditionFailed, "If-Match", `"x"`)
	resp, _ := c.want("PUT", cal+"a.ics", body, http.StatusCreated, "If-None-Match", "*")
	etag := resp.Header.Get("ETag")

	// A UID names one object in a collection, RFC 4791 section 5.3.2.1.
	_, out := c.want("PUT", cal+"b.ics", body, http.StatusForbidden)
	mustContain(t, "no-uid-conflict", out, "<C:no-uid-conflict><D:href>/dav/calendars/work/a.ics</D:href></C:no-uid-conflict>")
	// The same UID at the same name is a replace.
	c.want("PUT", cal+"a.ics", body, http.StatusNoContent, "If-Match", etag)

	// An event cannot become an availability in place.
	hours := vcal("BEGIN:VAVAILABILITY\r\nUID:h\r\nDTSTAMP:20270101T000000Z\r\nDTSTART:20270101T000000Z\r\n" +
		"BEGIN:AVAILABLE\r\nUID:h-1\r\nDTSTAMP:20270101T000000Z\r\nDTSTART:20270101T080000Z\r\nDTEND:20270101T200000Z\r\nEND:AVAILABLE\r\nEND:VAVAILABILITY\r\n")
	_, out = c.want("PUT", cal+"a.ics", hours, http.StatusForbidden)
	mustContain(t, "type change", out, "valid-calendar-object-resource")

	_, out = c.want("PUT", cal+"big.ics", strings.Repeat("x", 1<<20+1), http.StatusForbidden)
	mustContain(t, "too big", out, "<C:max-resource-size/>")
	_, out = c.want("PUT", cal+"bad.ics", vcal("BEGIN:VTODO\r\nUID:t\r\nEND:VTODO\r\n"), http.StatusForbidden)
	mustContain(t, "vtodo", out, "<C:supported-calendar-component/>")
}

func TestGetConditional(t *testing.T) {
	_, c := withCalendar(t)
	resp, _ := c.want("PUT", cal+"a.ics", vcal(vevent("a", "20270104T100000Z", "20270104T110000Z", "SUMMARY:A & B\r\n")), http.StatusCreated)
	etag := resp.Header.Get("ETag")
	c.want("GET", cal+"a.ics", "", http.StatusNotModified, "If-None-Match", etag)
	resp, out := c.want("HEAD", cal+"a.ics", "", http.StatusOK)
	if out != "" || resp.Header.Get("Content-Length") == "0" || resp.Header.Get("ETag") != etag {
		t.Fatalf("HEAD: body %q, headers %v", out, resp.Header)
	}
	resp, out = c.want("GET", cal, "", http.StatusOK)
	mustContain(t, "export", out, "SUMMARY:A & B", "PRODID:-//the-protobuf-project//runtime-go CalDAV//EN")
	if !strings.Contains(resp.Header.Get("Content-Disposition"), `filename="work.ics"`) {
		t.Fatalf("export disposition %q", resp.Header.Get("Content-Disposition"))
	}
	ctag := resp.Header.Get("ETag")
	c.want("PUT", cal+"b.ics", vcal(vevent("b", "20270105T100000Z", "20270105T110000Z")), http.StatusCreated)
	if resp, _ := c.want("GET", cal, "", http.StatusOK); resp.Header.Get("ETag") == ctag {
		t.Fatal("the calendar's tag did not change when a member was added")
	}
	c.want("DELETE", cal, "", http.StatusPreconditionFailed, "If-Match", ctag)
}

func TestReports(t *testing.T) {
	_, c := withCalendar(t)
	c.want("PUT", cal+"a.ics", vcal(vevent("Alpha-1", "20270104T100000Z", "20270104T110000Z")), http.StatusCreated)
	query := func(filter string) string {
		return `<C:calendar-query ` + nsDecl + `><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` + filter + `</C:comp-filter></C:filter></C:calendar-query>`
	}
	count := func(body string) int {
		_, out := c.want("REPORT", cal, body, http.StatusMultiStatus, "Depth", "1")
		return strings.Count(out, "<D:response>")
	}
	for name, tc := range map[string]struct {
		filter string
		want   int
	}{
		"any":            {"", 1},
		"vevent":         {`<C:comp-filter name="VEVENT"/>`, 1},
		"vtodo":          {`<C:comp-filter name="VTODO"/>`, 0},
		"vavailability":  {`<C:comp-filter name="VAVAILABILITY"/>`, 0},
		"is-not-defined": {`<C:comp-filter name="VEVENT"><C:is-not-defined/></C:comp-filter>`, 0},
		"uid substring":  {`<C:comp-filter name="VEVENT"><C:prop-filter name="UID"><C:text-match>alpha</C:text-match></C:prop-filter></C:comp-filter>`, 1},
		"uid negated":    {`<C:comp-filter name="VEVENT"><C:prop-filter name="UID"><C:text-match negate-condition="yes">alpha</C:text-match></C:prop-filter></C:comp-filter>`, 0},
		"in range":       {`<C:comp-filter name="VEVENT"><C:time-range start="20270104T103000Z"/></C:comp-filter>`, 1},
		"before":         {`<C:comp-filter name="VEVENT"><C:time-range end="20270104T100000Z"/></C:comp-filter>`, 0},
	} {
		if got := count(query(tc.filter)); got != tc.want {
			t.Errorf("%s: %d matches, want %d", name, got, tc.want)
		}
	}
	_, out := c.want("REPORT", cal, `<C:calendar-query `+nsDecl+`><C:filter><C:comp-filter name="VEVENT"/></C:filter></C:calendar-query>`, http.StatusForbidden)
	mustContain(t, "valid-filter", out, "<C:valid-filter/>")
	c.want("REPORT", cal, query(`<C:comp-filter name="VEVENT"><C:time-range start="2027"/></C:comp-filter>`), http.StatusBadRequest)
	c.want("REPORT", cal, query(`<C:comp-filter name="VEVENT"><C:time-range/></C:comp-filter>`), http.StatusBadRequest)
	c.want("REPORT", "/dav/calendars/", query(""), http.StatusForbidden)

	// calendar-query on an object; expand of a single event leaves it whole.
	_, out = c.want("REPORT", cal+"a.ics", `<C:calendar-query `+nsDecl+`><D:prop><C:calendar-data><C:expand start="20270101T000000Z" end="20270201T000000Z"/></C:calendar-data></D:prop></C:calendar-query>`, http.StatusMultiStatus, "Depth", "0")
	mustContain(t, "object query", out, "UID:Alpha-1", "DTSTART:20270104T100000Z")

	// multiget takes absolute URLs as well as paths.
	_, out = c.want("REPORT", cal, `<C:calendar-multiget `+nsDecl+`><D:prop><D:getetag/></D:prop><D:href>`+c.base+cal+`a.ics</D:href><D:href>/elsewhere</D:href></C:calendar-multiget>`, http.StatusMultiStatus)
	mustContain(t, "multiget", out, "<D:href>"+cal+"a.ics</D:href><D:propstat>", "<D:href>/elsewhere</D:href><D:status>HTTP/1.1 404 Not Found</D:status>")

	// sync-collection is not offered: the client falls back to PROPFIND.
	_, out = c.want("REPORT", cal, `<D:sync-collection xmlns:D="DAV:"><D:sync-token/></D:sync-collection>`, http.StatusForbidden)
	mustContain(t, "sync-collection", out, "<D:supported-report/>")
}

func TestFreeBusyQuery(t *testing.T) {
	_, c := withCalendar(t)
	c.want("PUT", cal+"a.ics", vcal(vevent("a", "20270104T100000Z", "20270104T110000Z")), http.StatusCreated)
	c.want("PUT", cal+"t.ics", vcal(vevent("t", "20270104T120000Z", "20270104T130000Z", "STATUS:TENTATIVE\r\n")), http.StatusCreated)
	c.want("PUT", cal+"free.ics", vcal(vevent("f", "20270104T140000Z", "20270104T150000Z", "TRANSP:TRANSPARENT\r\n")), http.StatusCreated)
	fbq := func(start, end string) string {
		return `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="` + start + `" end="` + end + `"/></C:free-busy-query>`
	}
	_, out := c.want("REPORT", cal, fbq("20270104T000000Z", "20270105T000000Z"), http.StatusOK)
	mustContain(t, "free-busy", out, "BEGIN:VFREEBUSY", "DTSTART:20270104T000000Z", "DTEND:20270105T000000Z",
		"FREEBUSY;FBTYPE=BUSY:20270104T100000Z/20270104T110000Z",
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20270104T120000Z/20270104T130000Z")
	if strings.Contains(out, "T140000Z") {
		t.Fatalf("a transparent event blocks time:\n%s", out)
	}
	c.want("REPORT", cal+"a.ics", fbq("20270104T000000Z", "20270105T000000Z"), http.StatusForbidden)
	c.want("REPORT", cal, `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"/>`, http.StatusBadRequest)
	c.want("REPORT", cal, fbq("20270104T000000Z", "20270103T000000Z"), http.StatusBadRequest)
	_, out = c.want("REPORT", cal, fbq("20270101T000000Z", "20290101T000000Z"), http.StatusForbidden)
	mustContain(t, "too long", out, "<C:max-date-time/>")
}

// A split the store refuses is the precondition a client understands, and
// leaves the object as it was.
func TestRefusedSplit(t *testing.T) {
	_, c := withCalendar(t)
	series := vevent("s", "20270104T100000Z", "20270104T110000Z", "RRULE:FREQ=WEEKLY;COUNT=4\r\n")
	resp, _ := c.want("PUT", cal+"s.ics", vcal(series), http.StatusCreated)
	before := resp.Header.Get("ETag")
	notAnInstance := "BEGIN:VEVENT\r\nUID:s\r\nDTSTAMP:20270101T000000Z\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20270105T100000Z\r\n" +
		"DTSTART:20270105T120000Z\r\nDTEND:20270105T130000Z\r\nEND:VEVENT\r\n"
	_, out := c.want("PUT", cal+"s.ics", vcal(series, notAnInstance), http.StatusForbidden)
	mustContain(t, "refused split", out, "valid-calendar-object-resource")
	if after, _ := c.want("GET", cal+"s.ics", "", http.StatusOK); after.Header.Get("ETag") != before {
		t.Fatal("a refused split changed the object")
	}
	// A split at the first instance edits the whole series in place.
	first := "BEGIN:VEVENT\r\nUID:s\r\nDTSTAMP:20270101T000000Z\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20270104T100000Z\r\n" +
		"DTSTART:20270104T120000Z\r\nDTEND:20270104T130000Z\r\nSUMMARY:Later\r\nEND:VEVENT\r\n"
	resp, _ = c.want("PUT", cal+"s.ics", vcal(series, first), http.StatusNoContent)
	if resp.Header.Get("ETag") != "" {
		t.Fatal("a split PUT returned an ETag")
	}
	_, out = c.want("GET", cal+"s.ics", "", http.StatusOK)
	mustContain(t, "split at first", out, "DTSTART:20270104T120000Z", "RRULE:FREQ=WEEKLY;COUNT=4", "SUMMARY:Later")
	_, out = c.want("PROPFIND", cal, `<D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`, http.StatusMultiStatus, "Depth", "1")
	if n := strings.Count(out, "<D:response>"); n != 2 {
		t.Fatalf("a split at the first instance made a new object:\n%s", out)
	}
}

// failing wraps a store whose calendar lookups fail with its own error.
type failing struct {
	caldav.Store
	err error
}

func (f failing) Calendar(context.Context, string) (*calendarv1.Calendar, error) { return nil, f.err }

var errBusy = errors.New("backend busy")

func TestErrorMapping(t *testing.T) {
	for name, tc := range map[string]struct {
		opts []caldav.Option
		want int
	}{
		"unmapped": {nil, http.StatusInternalServerError},
		"mapped": {[]caldav.Option{caldav.WithErrorStatus(func(err error) int {
			if errors.Is(err, errBusy) {
				return http.StatusServiceUnavailable
			}
			return 0
		})}, http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			h := caldav.New(failing{Store: memstore.New(), err: errBusy}, tc.opts...)
			c := serve(t, h)
			c.want("GET", cal+"a.ics", "", tc.want)
		})
	}
	// A store error mapped to 404 is not-found: a PUT into it is a conflict.
	h := caldav.New(failing{Store: memstore.New(), err: errBusy}, caldav.WithErrorStatus(func(error) int { return http.StatusNotFound }))
	serve(t, h).want("PUT", cal+"a.ics", vcal(vevent("a", "20270104T100000Z", "20270104T110000Z")), http.StatusConflict)
}

func TestPrefixAndName(t *testing.T) {
	_, c := newServer(t, caldav.WithPrefix("/caldav/"), caldav.WithName("bookings"), caldav.WithProductID("-//x//y//EN"))
	resp, _ := c.want("PROPFIND", "/.well-known/caldav", "", http.StatusMovedPermanently)
	if loc := resp.Header.Get("Location"); loc != "/caldav/" {
		t.Fatalf("well-known redirects to %q", loc)
	}
	_, out := c.want("PROPFIND", "/caldav/principals/default/", "", http.StatusMultiStatus, "Depth", "0")
	mustContain(t, "principal", out, "<D:displayname>bookings</D:displayname>", "<C:calendar-home-set><D:href>/caldav/calendars/</D:href>")
	c.want("MKCALENDAR", "/caldav/calendars/c/", "", http.StatusCreated)
	c.want("PUT", "/caldav/calendars/c/a.ics", vcal(vevent("a", "20270104T100000Z", "20270104T110000Z")), http.StatusCreated)
	_, out = c.want("GET", "/caldav/calendars/c/a.ics", "", http.StatusOK)
	mustContain(t, "prodid", out, "PRODID:-//x//y//EN")
	c.want("PROPFIND", "/dav/", "", http.StatusNotFound)
}
