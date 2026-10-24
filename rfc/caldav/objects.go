package caldav

import (
	"context"
	"io"
	"net/http"
	"strconv"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/calendar/v1"
)

// calendar returns the live calendar id.
func (h *Handler) calendar(ctx context.Context, id string) (*calendarv1.Calendar, error) {
	return h.store.Calendar(ctx, id)
}

// get answers GET and HEAD. An object is served as its own VCALENDAR; a
// calendar collection as one VCALENDAR holding every member, which is the
// whole calendar as a .ics file -- the export a subscribing client reads.
func (h *Handler) get(w http.ResponseWriter, r *http.Request, t target) error {
	ctx := r.Context()
	var body, etag string
	switch t.kind {
	case kindObject:
		if _, err := h.calendar(ctx, t.calendar); err != nil {
			return err
		}
		o, err := h.store.Object(ctx, t.calendar, t.object)
		if err != nil {
			return err
		}
		block, err := objectComponent(o)
		if err != nil {
			return err
		}
		body, etag = h.calendarObject(block), o.ETag()
	case kindCalendar:
		c, err := h.calendar(ctx, t.calendar)
		if err != nil {
			return err
		}
		objs, err := h.store.Objects(ctx, t.calendar)
		if err != nil {
			return err
		}
		blocks := make([]string, 0, len(objs))
		for _, o := range objs {
			block, err := objectComponent(o)
			if err != nil {
				return err
			}
			blocks = append(blocks, block)
		}
		body, etag = h.calendarObject(blocks...), ctag(c, objs)
		w.Header().Set("Content-Disposition", `attachment; filename="`+t.calendar+`.ics"`)
	default:
		return Errorf(http.StatusMethodNotAllowed, "", "GET is served on calendars and calendar objects")
	}
	w.Header().Set("Content-Type", MediaType)
	w.Header().Set("ETag", quoteETag(etag))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if v := r.Header.Get("If-None-Match"); v != "" && matchesIf(v, etag, true) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = io.WriteString(w, body)
	}
	return nil
}

// put answers PUT of a calendar object resource, RFC 4791 section 5.3.2: a
// create when the name is new, a full replace when it exists. If-Match and
// If-None-Match are honored, so a client's conditional write cannot clobber
// someone else's.
func (h *Handler) put(w http.ResponseWriter, r *http.Request, t target) error {
	if t.kind != kindObject {
		return Errorf(http.StatusMethodNotAllowed, "", "PUT writes calendar objects; MKCALENDAR creates calendars")
	}
	ctx := r.Context()
	// RFC 4791 section 5.3.2: a PUT into a collection that is not there is a
	// conflict, not a 404 on the object.
	cal, err := h.calendar(ctx, t.calendar)
	if err != nil {
		if h.notFound(err) {
			return Errorf(http.StatusConflict, "", "calendar %s does not exist", t.calendar)
		}
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		return err
	}
	if len(raw) > maxBody {
		return Errorf(http.StatusForbidden, MaxResourceSize, "calendar object over %d bytes", maxBody)
	}
	d, err := decode(string(raw))
	if err != nil {
		return err
	}

	existing, err := h.store.Object(ctx, t.calendar, t.object)
	exists := err == nil
	if err != nil && !h.notFound(err) {
		return err
	}
	if perr := checkPreconditions(r, existing.ETag(), exists); perr != nil {
		return perr
	}
	if exists && (existing.Event != nil) != (d.event != nil) {
		return Errorf(http.StatusForbidden, ValidCalendarObjectResource,
			"%s holds a different component type; delete it first", t.object)
	}
	// RFC 4791 section 5.3.2.1: a UID names one object in a collection.
	if uid := d.uid(); uid != "" {
		other, uerr := h.store.ObjectByUID(ctx, t.calendar, uid)
		switch {
		case uerr == nil && other.Name != t.object:
			return &Error{Status: http.StatusForbidden, Precondition: NoUIDConflict,
				Href:    h.objectHref(t.calendar, other.Name),
				Message: "UID " + uid + " is already in use by " + other.Name}
		case uerr != nil && !h.notFound(uerr):
			return uerr
		}
	}

	req := PutRequest{
		Calendar:     cal,
		Name:         t.object,
		Event:        d.event,
		Overrides:    d.overrides,
		Availability: d.avail,
	}
	if exists {
		req.Existing = &existing
	}
	if len(d.splits) > 0 {
		req.Split = d.splits[0]
	}
	altered, err := h.store.PutObject(ctx, req)
	if err != nil {
		return err
	}
	// RFC 4791 section 5.3.4: a server that stores something other than what
	// was sent -- a split series -- must not return an ETag, so the client
	// fetches the object again.
	if !altered {
		written, err := h.store.Object(ctx, t.calendar, t.object)
		if err != nil {
			return err
		}
		w.Header().Set("ETag", quoteETag(written.ETag()))
	}
	if exists {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	return nil
}

// delete answers DELETE. A calendar takes its members with it, as a WebDAV
// collection must.
func (h *Handler) delete(w http.ResponseWriter, r *http.Request, t target) error {
	ctx := r.Context()
	switch t.kind {
	case kindObject:
		if _, err := h.calendar(ctx, t.calendar); err != nil {
			return err
		}
		o, err := h.store.Object(ctx, t.calendar, t.object)
		if err != nil {
			return err
		}
		if err := checkPreconditions(r, o.ETag(), true); err != nil {
			return err
		}
		if err := h.store.DeleteObject(ctx, t.calendar, o); err != nil {
			return err
		}
	case kindCalendar:
		c, err := h.calendar(ctx, t.calendar)
		if err != nil {
			return err
		}
		objs, err := h.store.Objects(ctx, t.calendar)
		if err != nil {
			return err
		}
		if err := checkPreconditions(r, ctag(c, objs), true); err != nil {
			return err
		}
		if err := h.store.DeleteCalendar(ctx, c, objs); err != nil {
			return err
		}
	default:
		return Errorf(http.StatusForbidden, "", "%s cannot be deleted", h.href(t))
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
