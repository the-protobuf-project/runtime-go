package caldav

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// Sentinel errors a [Store] returns (or wraps) so the handler can answer with
// the status each means. A store whose errors are its own can map them with
// [WithErrorStatus] instead.
var (
	// ErrNotFound: the calendar or object does not exist. 404, and the cue
	// for a PUT to create.
	ErrNotFound = errors.New("caldav: not found")
	// ErrAlreadyExists: a create met an existing record. 412, or 405
	// DAV:resource-must-be-null for MKCALENDAR.
	ErrAlreadyExists = errors.New("caldav: already exists")
	// ErrPreconditionFailed: an etag guard failed. 412.
	ErrPreconditionFailed = errors.New("caldav: precondition failed")
	// ErrConflict: the write conflicts with the state of the store. 409.
	ErrConflict = errors.New("caldav: conflict")
	// ErrInvalid: the store refused what it was given. 400.
	ErrInvalid = errors.New("caldav: invalid")
)

// Preconditions and postconditions, RFC 4918 section 16 and RFC 4791
// section 5.3.2.1, as the element an [Error] names.
const (
	ValidCalendarData           = "C:valid-calendar-data"
	ValidCalendarObjectResource = "C:valid-calendar-object-resource"
	SupportedCalendarComponent  = "C:supported-calendar-component"
	NoUIDConflict               = "C:no-uid-conflict"
	MaxResourceSize             = "C:max-resource-size"
	MaxDateTime                 = "C:max-date-time"
	ValidFilter                 = "C:valid-filter"
	SupportedFilter             = "C:supported-filter"
	SupportedReport             = "D:supported-report"
	ResourceMustBeNull          = "D:resource-must-be-null"
)

// Error is an HTTP failure with an optional precondition element naming the
// condition that failed, rendered as a DAV:error body. A store returns one to
// choose the response itself, e.g. a split it refuses.
type Error struct {
	Status int
	// Precondition is the element, e.g. [ValidCalendarObjectResource];
	// empty for a plain-text error.
	Precondition string
	// Href, with [NoUIDConflict], names the resource already holding the UID.
	Href    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// Errorf returns an [*Error].
func Errorf(status int, precondition, format string, args ...any) error {
	return &Error{Status: status, Precondition: precondition, Message: fmt.Sprintf(format, args...)}
}

// statusOf is the HTTP status err means: an *Error's own, a sentinel's, the
// configured mapper's, else 500.
func (h *Handler) statusOf(err error) int {
	var e *Error
	switch {
	case errors.As(err, &e):
		return e.Status
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrPreconditionFailed):
		return http.StatusPreconditionFailed
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	case errors.Is(err, ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, context.Canceled):
		return 499
	}
	if h.errorStatus != nil {
		if s := h.errorStatus(err); s != 0 {
			return s
		}
	}
	return http.StatusInternalServerError
}

func (h *Handler) notFound(err error) bool {
	return err != nil && h.statusOf(err) == http.StatusNotFound
}

// writeError renders err: an *Error as itself, anything else as the status
// statusOf gives it.
func (h *Handler) writeError(w http.ResponseWriter, err error) {
	var e *Error
	if !errors.As(err, &e) {
		e = &Error{Status: h.statusOf(err), Message: err.Error()}
	}
	if e.Precondition == "" {
		http.Error(w, e.Message, e.Status)
		return
	}
	inner := "<" + e.Precondition + "/>"
	if e.Href != "" {
		inner = "<" + e.Precondition + ">" + hrefXML(e.Href) + "</" + e.Precondition + ">"
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(e.Status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>`+"\n"+
		`<D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">%s<D:responsedescription>%s</D:responsedescription></D:error>`,
		inner, escapeText(e.Message))
}
