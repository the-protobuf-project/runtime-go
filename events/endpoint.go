package events

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"
)

// Reasons CheckEndpoint refuses an endpoint for.
const (
	ReasonInvalidURI        = "INVALID_URI"
	ReasonURIHasCredentials = "URI_HAS_CREDENTIALS" //nolint:gosec // a reason name, not a credential
	ReasonURINotHTTPS       = "URI_NOT_HTTPS"
	ReasonURINotPublic      = "URI_NOT_PUBLIC"
)

// EndpointPolicy is what endpoints a server sends to.
type EndpointPolicy struct {
	// AllowInsecure accepts http:// endpoints on loopback hosts, for
	// development against a local receiver.
	AllowInsecure bool
	// AllowPrivateNetworks accepts endpoints on loopback, private and
	// link-local addresses.
	AllowPrivateNetworks bool
}

// EndpointError is an endpoint CheckEndpoint refused: why, as a stable
// reason, what is wrong and what to do about it.
type EndpointError struct {
	Reason string
	What   string
	Fix    string
}

func (e *EndpointError) Error() string { return e.What + ". " + e.Fix }

// CheckEndpoint validates an endpoint a client chose: an absolute URL without
// credentials, https (or http on loopback when p.AllowInsecure), and not a
// literal non-public address unless p.AllowPrivateNetworks. A host name is
// checked when it is dialed (PublicOnly). It returns an *EndpointError.
func CheckEndpoint(uri string, p EndpointPolicy) error {
	u, err := url.Parse(uri)
	switch {
	case err != nil || u.Host == "":
		return &EndpointError{ReasonInvalidURI,
			fmt.Sprintf("uri %q is not an absolute URL", uri),
			"Send the full endpoint, scheme and host included, e.g. https://example.com/hooks"}
	case u.User != nil:
		return &EndpointError{ReasonURIHasCredentials,
			"uri carries a username or password",
			"Remove them: every delivery is signed with the webhook's secret (Standard Webhooks), so verify that signature instead"}
	case u.Scheme == "https":
	case u.Scheme == "http" && p.AllowInsecure && loopback(u.Hostname()):
	default:
		return &EndpointError{ReasonURINotHTTPS,
			fmt.Sprintf("uri %q is not https", uri),
			"Serve the endpoint over https; plain http is accepted only for localhost, and only when the server allows insecure endpoints (development)"}
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && !IsPublic(ip) && !p.AllowPrivateNetworks {
		return &EndpointError{ReasonURINotPublic,
			fmt.Sprintf("uri points at %s, which is not a public address", u.Hostname()),
			"Deliveries go only to the public internet; expose the endpoint publicly, or allow private networks on the server (development)"}
	}
	return nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// NewClient is the client deliveries are sent with by default: a 10-second
// timeout, no redirects followed, no proxy (one would make the connection,
// and the address check would see the proxy instead of the endpoint) and,
// unless allowPrivate, connections to public addresses only.
func NewClient(allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if !allowPrivate {
		dialer.Control = PublicOnly
	}
	return &http.Client{
		Timeout:       10 * time.Second,
		Transport:     &http.Transport{DialContext: dialer.DialContext, TLSHandshakeTimeout: 5 * time.Second},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// PublicOnly is a net.Dialer Control that refuses connections to anything but
// a public address (IsPublic). It runs at connect time, on the address DNS
// actually returned, so a name that resolves somewhere private is caught
// however it is spelled.
func PublicOnly(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if ip := net.ParseIP(host); ip == nil || !IsPublic(ip) {
		return fmt.Errorf("refusing to connect to %s: not a public address", host)
	}
	return nil
}

// cgnat is RFC 6598's shared address space, private in all but name.
var cgnat = &net.IPNet{IP: net.IP{100, 64, 0, 0}, Mask: net.CIDRMask(10, 32)}

// IsPublic reports whether ip is on the public internet: not loopback,
// private, link-local, multicast, carrier-grade NAT or unspecified -- where an
// endpoint's URL would reach into the server's own network.
func IsPublic(ip net.IP) bool {
	return !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() &&
		!ip.IsInterfaceLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified() && !cgnat.Contains(ip)
}
