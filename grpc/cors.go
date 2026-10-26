package grpc

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// CORSPolicy is the cross-origin resource sharing policy [WithCORS] applies to
// the HTTP listeners, so a browser on another origin can call the REST gateway.
//
// The zero value of each list takes its default, except AllowedOrigins: a
// policy naming no origin allows none, and [CORSPolicy.Validate] refuses it.
type CORSPolicy struct {
	// AllowedOrigins are the origins allowed to call, each an exact
	// scheme://host[:port] ("https://app.example.com"), or "*" for any origin.
	// Matching ignores case and a trailing slash.
	AllowedOrigins []string
	// AllowedMethods answer a preflight's Access-Control-Request-Method. Empty
	// means [DefaultCORSAllowedMethods].
	AllowedMethods []string
	// AllowedHeaders answer a preflight's Access-Control-Request-Headers. Empty
	// means [DefaultCORSAllowedHeaders]; "*" allows whatever a preflight asks
	// for, by echoing it back.
	AllowedHeaders []string
	// ExposedHeaders are the response headers a browser lets the calling
	// script read, beyond the CORS-safelisted ones.
	ExposedHeaders []string
	// AllowCredentials lets a browser send cookies and HTTP authentication and
	// read the response. It cannot be combined with the "*" origin.
	AllowCredentials bool
	// MaxAge is how long a browser may cache a preflight's answer; 0 sends no
	// Access-Control-Max-Age, leaving the browser's own default.
	MaxAge time.Duration
}

// DefaultCORSAllowedMethods returns the methods a policy allows when it names
// none: every verb the REST gateway's google.api.http bindings use, plus
// OPTIONS.
func DefaultCORSAllowedMethods() []string {
	return []string{
		http.MethodGet, http.MethodPost, http.MethodPatch,
		http.MethodPut, http.MethodDelete, http.MethodOptions,
	}
}

// DefaultCORSAllowedHeaders returns the request headers a policy allows when
// it names none: what a GAPIC client's browser (REST fallback) transport
// sends -- Content-Type, the x-goog-* call metadata, and the credentials it
// may carry (Authorization, an API key).
func DefaultCORSAllowedHeaders() []string {
	return []string{
		"Content-Type",
		"Authorization",
		"X-Goog-Api-Client",
		"X-Goog-Request-Params",
		"X-Goog-User-Project",
		"X-Api-Key",
	}
}

// Validate reports whether p can be served: at least one origin, each "*" or
// an absolute scheme://host[:port] with no path, query or fragment, and no
// "*" origin when credentials are allowed -- a browser refuses that pairing,
// and echoing any origin instead would hand every site the caller's
// credentials.
func (p CORSPolicy) Validate() error {
	if len(p.AllowedOrigins) == 0 {
		return errors.New("cors: no allowed origins")
	}
	for _, o := range p.AllowedOrigins {
		if o == "*" {
			if p.AllowCredentials {
				return errors.New(`cors: the "*" origin cannot be combined with allowed credentials; list the origins instead`)
			}
			continue
		}
		if err := validateOrigin(o); err != nil {
			return err
		}
	}
	if p.MaxAge < 0 {
		return fmt.Errorf("cors: negative max age %s", p.MaxAge)
	}
	return nil
}

// validateOrigin checks one non-wildcard origin is scheme://host[:port].
func validateOrigin(o string) error {
	u, err := url.Parse(strings.TrimSuffix(o, "/"))
	if err != nil {
		return fmt.Errorf("cors: origin %q: %w", o, err)
	}
	if u.Scheme == "" || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return fmt.Errorf("cors: origin %q: want scheme://host[:port], such as https://app.example.com", o)
	}
	return nil
}

// CORS returns middleware applying p to the handler it wraps, or the error
// [CORSPolicy.Validate] finds. It is what [WithCORS] installs, exported for a
// caller composing its own handler.
//
// A preflight -- OPTIONS carrying Origin and Access-Control-Request-Method --
// is answered here and never reaches the wrapped handler: 204 with the
// policy's methods, headers, credentials and max age for an allowed origin,
// 403 with no CORS headers otherwise. Any other request from an allowed
// origin passes through with Access-Control-Allow-Origin (and the exposed
// headers and credentials flag) added; from a disallowed origin it passes
// through unchanged, and the browser withholds the response. Every response
// carries Vary: Origin, so a shared cache never serves one origin's answer to
// another.
func CORS(p CORSPolicy) (func(http.Handler) http.Handler, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	c := newCORS(p)
	return c.wrap, nil
}

// cors is a validated policy with its header values precomputed.
type cors struct {
	anyOrigin    bool
	origins      map[string]struct{}
	methods      string
	headers      string
	anyHeader    bool
	exposed      string
	credentials  bool
	maxAge       string
	allowMethods map[string]struct{}
}

// newCORS precomputes p's header values. p is assumed valid.
func newCORS(p CORSPolicy) *cors {
	methods := p.AllowedMethods
	if len(methods) == 0 {
		methods = DefaultCORSAllowedMethods()
	}
	headers := p.AllowedHeaders
	if len(headers) == 0 {
		headers = DefaultCORSAllowedHeaders()
	}
	c := &cors{
		origins:      make(map[string]struct{}, len(p.AllowedOrigins)),
		allowMethods: make(map[string]struct{}, len(methods)),
		anyHeader:    slices.Contains(headers, "*"),
		exposed:      strings.Join(p.ExposedHeaders, ", "),
		credentials:  p.AllowCredentials,
	}
	for _, o := range p.AllowedOrigins {
		if o == "*" {
			c.anyOrigin = true
			continue
		}
		c.origins[normalizeOrigin(o)] = struct{}{}
	}
	upper := make([]string, 0, len(methods))
	for _, m := range methods {
		m = strings.ToUpper(strings.TrimSpace(m))
		upper = append(upper, m)
		c.allowMethods[m] = struct{}{}
	}
	c.methods = strings.Join(upper, ", ")
	c.headers = strings.Join(headers, ", ")
	if p.MaxAge > 0 {
		c.maxAge = strconv.Itoa(int(p.MaxAge / time.Second))
	}
	return c
}

// normalizeOrigin lower-cases o and drops a trailing slash: scheme and host
// are case-insensitive, and a configured "https://a.example/" means the same
// origin a browser sends as "https://a.example".
func normalizeOrigin(o string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(o), "/"))
}

// allowed reports whether origin may call.
func (c *cors) allowed(origin string) bool {
	if c.anyOrigin {
		return true
	}
	_, ok := c.origins[normalizeOrigin(origin)]
	return ok
}

// allowOrigin is the Access-Control-Allow-Origin value for an allowed origin:
// "*" for a wildcard policy without credentials, else the origin itself.
func (c *cors) allowOrigin(origin string) string {
	if c.anyOrigin && !c.credentials {
		return "*"
	}
	return origin
}

// wrap is the middleware [CORS] returns.
func (c *cors) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			c.preflight(w, r, origin)
			return
		}
		if c.allowed(origin) {
			h.Set("Access-Control-Allow-Origin", c.allowOrigin(origin))
			if c.credentials {
				h.Set("Access-Control-Allow-Credentials", "true")
			}
			if c.exposed != "" {
				h.Set("Access-Control-Expose-Headers", c.exposed)
			}
		}
		next.ServeHTTP(w, r)
	})
}

// preflight answers an OPTIONS preflight without calling the wrapped handler.
func (c *cors) preflight(w http.ResponseWriter, r *http.Request, origin string) {
	h := w.Header()
	h.Add("Vary", "Access-Control-Request-Method")
	h.Add("Vary", "Access-Control-Request-Headers")
	method := strings.ToUpper(r.Header.Get("Access-Control-Request-Method"))
	_, methodOK := c.allowMethods[method]
	if !c.allowed(origin) || !methodOK {
		shared.Telemetry().Logger.Debugf("CORS: refused preflight from origin %q for %s %s", origin, method, r.URL.Path)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	h.Set("Access-Control-Allow-Origin", c.allowOrigin(origin))
	h.Set("Access-Control-Allow-Methods", c.methods)
	if c.anyHeader {
		if requested := r.Header.Get("Access-Control-Request-Headers"); requested != "" {
			h.Set("Access-Control-Allow-Headers", requested)
		}
	} else {
		h.Set("Access-Control-Allow-Headers", c.headers)
	}
	if c.credentials {
		h.Set("Access-Control-Allow-Credentials", "true")
	}
	if c.maxAge != "" {
		h.Set("Access-Control-Max-Age", c.maxAge)
	}
	w.WriteHeader(http.StatusNoContent)
}

// WithCORS applies p to the HTTP/1.1 and HTTP/3 listeners -- the REST gateway,
// a [WithHTTPHandler] replacement, and /health alike -- so browser clients on
// the allowed origins can call them. Preflights are answered before any
// handler runs. An invalid policy (see [CORSPolicy.Validate]) makes Start
// fail.
//
// The MCP and A2A listeners are separate servers and are not covered.
func WithCORS(p CORSPolicy) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithCORS: allowing origins %v (credentials=%t)", p.AllowedOrigins, p.AllowCredentials)
		policy := p
		s.cors = &policy
	}
}
