// Package network provides GraphQL, HTTP and WebSocket clients behind one
// factory.
//
// NewConnection builds each with the same connection options, and optionally
// verifies the server is reachable before the first call.
//
// All three client types are created via NewConnection(clientType) and configured
// with WithOpts(opts):
//
//   - GraphQL: queries, mutations, and subscriptions against a GraphQL endpoint.
//   - HTTP: GET, POST, PUT, PATCH, DELETE with retries and context support.
//   - WebSocket: full-duplex Send/Receive with optional auto-reconnect and keepalive.
//
// By default Connect verifies the target is reachable before returning; set
// ConnectionOptions.SkipConnectivityCheck to skip the HTTP/GraphQL check. Errors
// are always returned to the caller; the package performs no logging.
package network
