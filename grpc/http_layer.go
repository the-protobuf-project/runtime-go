package grpc

import (
	"fmt"
	"math/rand"
	"net/http"

	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// serveHandler is what the HTTP/1.1 and HTTP/3 listeners actually serve.
//
// Without [WithHTTPHandler] it is the grpc-gateway mux, unchanged. With one it is that
// handler mounted at the root, with /health kept alongside it — a caller replacing the
// gateway should not have to remember to re-add the endpoint a load balancer probes.
//
// Both listeners go through here so they cannot drift: an HTTP/3 client and an HTTP/1.1
// client hitting the same path run the same routing.
func (s *HybridServer) serveHandler() http.Handler {
	if s.httpHandler == nil {
		return s.mux
	}

	// Go's ServeMux prefers the more specific pattern, so "/health" wins over "/" without
	// the mounted handler needing to know the endpoint exists.
	mux := http.NewServeMux()
	mux.Handle("/", s.httpHandler)
	mux.Handle("/health", healthzHandler())
	return mux
}

// healthzHandler answers GET /health with 200 and a small JSON body.
//
// It reports that the process is answering, which is what the path is asked for. A check that
// probed dependencies would fail a server that is up and merely waiting on something else,
// and take it out of rotation for a fault it does not have.
func healthzHandler() http.HandlerFunc {
	messages := []string{
		"I'm doing fine, bro. Don't worry. 🌱",
		"Still alive and kicking. 🚀",
		"All systems nominal. 👍",
		"Healthy as a horse. 🐴",
		"Feeling great, thanks for asking. 😎",
	}

	return func(w http.ResponseWriter, r *http.Request) {
		shared.Telemetry().Logger.Debugf("HTTP: GET /health from %s", r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ok","message":"%s"}`, messages[rand.Intn(len(messages))])
	}
}
