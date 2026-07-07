// URL construction shared by all client types.
package network

import (
	"fmt"
	"net/url"
)

// buildFullURL constructs a full URL from URLOptions using the path at pathIndex.
// It validates the scheme (http, https, ws, wss), a non-empty host, a non-empty
// paths list, and that pathIndex is in range. Query parameters from Params are
// appended when present.
func buildFullURL(urlOptions URLOptions, pathIndex int) (string, error) {
	switch urlOptions.Scheme {
	case HTTP, HTTPS, WS, WSS:
	default:
		return "", fmt.Errorf("invalid URL scheme: %s. Must be 'http', 'https', 'ws', or 'wss'", urlOptions.Scheme)
	}
	if urlOptions.Host == "" {
		return "", fmt.Errorf("host cannot be empty")
	}
	if len(urlOptions.Paths) == 0 {
		return "", fmt.Errorf("paths array cannot be empty")
	}
	if pathIndex < 0 || pathIndex >= len(urlOptions.Paths) {
		return "", fmt.Errorf("pathIndex %d out of bounds for paths array of length %d", pathIndex, len(urlOptions.Paths))
	}

	u := url.URL{Scheme: string(urlOptions.Scheme), Host: urlOptions.Host}

	// Ensure the path starts with a forward slash without re-encoding it.
	path := urlOptions.Paths[pathIndex]
	if len(path) > 0 && path[0] != '/' {
		path = "/" + path
	}
	u.Path = path

	if len(urlOptions.Params) > 0 {
		query := u.Query()
		for key, value := range urlOptions.Params {
			query.Set(key, value)
		}
		u.RawQuery = query.Encode()
	}

	return u.String(), nil
}

// URLFromStd converts a parsed *url.URL into URLOptions: scheme, host, and a single path
// (defaulting to "/"), with any query parameters copied into Params. It lets generated
// clients connect using the standard library's url.Parse output directly.
func URLFromStd(u *url.URL) URLOptions {
	opts := URLOptions{Scheme: URLScheme(u.Scheme), Host: u.Host}
	path := u.Path
	if path == "" {
		path = "/"
	}
	opts.Paths = []string{path}
	if q := u.Query(); len(q) > 0 {
		opts.Params = make(map[string]string, len(q))
		for key, values := range q {
			if len(values) > 0 {
				opts.Params[key] = values[0]
			}
		}
	}
	return opts
}

// websocketURL derives a ws/wss URLOptions copy from an http/https URLOptions,
// used to open a subscription transport against the same host and path.
func websocketURL(in URLOptions) URLOptions {
	out := in
	switch in.Scheme {
	case HTTPS, WSS:
		out.Scheme = WSS
	default:
		out.Scheme = WS
	}
	return out
}
