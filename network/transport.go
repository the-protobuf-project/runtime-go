package network

import (
	"net/http"
	"time"
)

// newPooledClient returns an http.Client with a dedicated pooled transport.
// A client built without a Transport shares http.DefaultTransport, which keeps
// only 2 idle connections per host — so concurrent requests to one endpoint
// (the normal shape for a GraphQL or REST backend) constantly open and close
// TCP connections. A dedicated transport sized for single-host traffic lets
// the pool actually reuse connections, and keeps this module's clients from
// contending on the process-wide default transport.
func newPooledClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100
	return &http.Client{Timeout: timeout, Transport: transport}
}
