package mqtt

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// schemes are the address schemes autopaho dials, by whether they are secure.
var schemes = map[string]bool{
	"mqtt": false, "tcp": false, "ws": false,
	"tls": true, "ssl": true, "mqtts": true, "mqtt+ssl": true, "tcps": true, "wss": true,
}

// parseServer turns an address into the URL a connection dials.
//
// A bare host:port is the address this package has always taken, and stays
// plain MQTT unless a TLS configuration says otherwise. A port left off a TCP
// address is the one the protocol registers: 1883 in the clear, 8883 over TLS.
func parseServer(address string, tlsCfg *tls.Config) (*url.URL, error) {
	if address == "" {
		return nil, fmt.Errorf("mqtt: no broker address given")
	}
	if !strings.Contains(address, "://") {
		scheme := "mqtt"
		if tlsCfg != nil {
			scheme = "tls"
		}
		address = scheme + "://" + address
	}

	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("mqtt: %q is not a broker address: %w", address, err)
	}
	secure, ok := schemes[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("mqtt: %q uses the %q scheme; use mqtt://, tls://, mqtts://, ws:// or wss://", address, u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("mqtt: %q names no host", address)
	}
	if tlsCfg != nil && !secure {
		return nil, fmt.Errorf("mqtt: a TLS configuration was given for %q, which would connect in the clear; use tls:// or wss://", address)
	}

	if u.Port() == "" && u.Scheme != "ws" && u.Scheme != "wss" {
		port := "1883"
		if secure {
			port = "8883"
		}
		u.Host = net.JoinHostPort(u.Hostname(), port)
	}
	return u, nil
}
