// Package network provides GraphQL, HTTP, and WebSocket clients behind a single
// factory (NewConnection) with consistent connection options and optional
// connectivity verification.
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
