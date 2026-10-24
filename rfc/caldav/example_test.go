package caldav_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/memstore"
)

// A CalDAV server over the in-memory store: mount the handler under its
// prefix and the RFC 6764 well-known path beside it.
func Example() {
	h := caldav.New(memstore.New(), caldav.WithName("bookings"))
	mux := http.NewServeMux()
	mux.Handle(h.Prefix()+"/", h)
	mux.Handle("/.well-known/caldav", h.WellKnown())

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), "MKCALENDAR", "/dav/calendars/court-1/", nil))
	fmt.Println(rec.Code)

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/dav/", nil))
	fmt.Println(rec.Header().Get("DAV"))
	// Output:
	// 201
	// 1, 3, calendar-access
}
