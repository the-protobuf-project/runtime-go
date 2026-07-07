# Network Package

A comprehensive Go networking package providing unified interfaces for HTTP, GraphQL, and WebSocket clients with built-in retry logic, logging, and telemetry.

## Features

- **HTTP Client**: Full REST API support (GET, POST, PUT, PATCH, DELETE)
- **GraphQL Client**: Query and mutation support with variables
- **WebSocket Client**: Real-time bidirectional communication
- **Context Support**: Proper timeout and cancellation handling
- **Auto-Retry**: Configurable retry logic with exponential backoff
- **Structured Logging**: Built-in logging via Pulse
- **Telemetry**: OpenTelemetry integration
- **Thread-Safe**: Concurrent operations supported

## Installation

```bash
go get github.com/machanirobotics/loom/go/network
```

## Quick Start

### HTTP Client

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    "github.com/machanirobotics/loom/go/network"
)

func main() {
    // Create HTTP client
    conn, _ := network.NewConnection(network.HTTPConnClient)
    defer conn.Close()
    
    // Configure connection
    opts := network.ConnectionOptions{
        URL: network.URLOptions{
            Scheme: network.HTTPS,
            Host:   "api.example.com",
            Paths:  []string{"/users"},
        },
        Timeout: 10 * time.Second,
        Headers: map[string]string{
            "Authorization": "Bearer token",
        },
        RetryDelay: 2 * time.Second,
    }
    
    conn.WithOpts(opts)
    httpClient, _ := conn.AsHTTPConnectionType()
    
    // Make request
    ctx := context.Background()
    respChan := httpClient.Request(ctx, network.GET, opts.URL, nil, nil, 0, 3)
    resp := <-respChan
    
    if resp.Error != nil {
        fmt.Printf("Error: %v\n", resp.Error)
        return
    }
    
    fmt.Printf("Response: %s\n", resp.Data)
}
```

### GraphQL Client

```go
// Create GraphQL client
conn, _ := network.NewConnection(network.GraphQLConnClient)
defer conn.Close()

opts := network.ConnectionOptions{
    URL: network.URLOptions{
        Scheme: network.HTTPS,
        Host:   "api.example.com",
        Paths:  []string{"/graphql"},
    },
    Timeout: 15 * time.Second,
}

conn.WithOpts(opts)
gqlClient, _ := conn.AsGraphQLConnectionType()

// Query with variables
var query struct {
    User struct {
        ID   string
        Name string
    } `graphql:"user(id: $id)"`
}

variables := map[string]interface{}{
    "id": network.ID("123"),
}

resultChan := gqlClient.Query(&query, variables)
result := <-resultChan

if result.Error != nil {
    fmt.Printf("Error: %v\n", result.Error)
    return
}

fmt.Printf("User: %s\n", query.User.Name)

// Mutation with input type
type CreateUserInput struct {
    Name  string `json:"name"`
    Email string `json:"email"`
}

var mutation struct {
    CreateUser struct {
        ID   string
        Name string
    } `graphql:"createUser(input: $input)"`
}

input := CreateUserInput{
    Name:  "John",
    Email: "john@example.com",
}

variables = map[string]interface{}{
    "input": input,
}

resultChan = gqlClient.Mutation(&mutation, variables)
result = <-resultChan
```

#### GraphQL Scalar Types

The network package provides convenient type aliases for GraphQL scalars:

```go
// Use these types for GraphQL variables
network.ID("123")        // GraphQL ID type
network.String("text")   // GraphQL String type
network.Int(42)          // GraphQL Int type
network.Float(3.14)      // GraphQL Float type
network.Boolean(true)    // GraphQL Boolean type
```

### WebSocket Client

```go
// Create WebSocket client
conn, _ := network.NewConnection(network.WebsocketConnClient)
defer conn.Close()

opts := network.ConnectionOptions{
    URL: network.URLOptions{
        Scheme: network.WSS,
        Host:   "ws.example.com",
        Paths:  []string{"/ws"},
    },
    Timeout: 10 * time.Second,
}

conn.WithOpts(opts)
wsClient, _ := conn.AsWebSocketConnectionType()

// Enable auto-reconnect
wsClient.SetAutoReconnect(true, 5*time.Second)

// Listen for messages
ctx := context.Background()
errChan := wsClient.Listen(ctx, func(messageType int, message []byte) {
    fmt.Printf("Received: %s\n", message)
})

// Send messages
wsClient.Send(websocket.TextMessage, []byte("Hello"))

// Wait for errors
if err := <-errChan; err != nil {
    fmt.Printf("Connection error: %v\n", err)
}
```

## Configuration

### Connection Options

```go
type ConnectionOptions struct {
    URL        URLOptions        // URL configuration (scheme, host, paths, params)
    Timeout    time.Duration     // Request/connection timeout; 0 uses DefaultTimeout (10s)
    Headers    map[string]string // HTTP headers (and WebSocket handshake headers)
    Retries    int               // Max retries for HTTP Request
    RetryDelay time.Duration     // Delay between retries (default 2s)

    // SkipConnectivityCheck: when true, skip the initial reachability check for
    // HTTP and GraphQL. Use when the server is known to be up or the first
    // request will act as the check. Ignored for WebSocket (connection = Dial).
    SkipConnectivityCheck bool

    // GraphQLConnectivityQuery: override the query used to verify the GraphQL
    // server is reachable. Empty = use DefaultGraphQLConnectivityQuery.
    // Set for strict servers that limit introspection.
    GraphQLConnectivityQuery string
}
```

### Connectivity Verification

By default, `WithOpts` (and each client’s `Connect`) verifies that the target is reachable before returning:

- **GraphQL**: Sends a small introspection query (default `query { __typename }`, or `GraphQLConnectivityQuery` if set). On failure, `Connect` returns an error.
- **HTTP**: Sends a **HEAD** request to the configured URL. If the server returns **405 Method Not Allowed**, a **GET** is sent instead and the response body is drained (up to 1 MiB) so the connection can be reused. On network or unreachable host, `Connect` returns an error.
- **WebSocket**: Connection is established by **Dial**. There is no separate “check”; `SkipConnectivityCheck` is ignored.

Set `SkipConnectivityCheck: true` when you know the server is up or when the first request will effectively act as the check (e.g. to avoid an extra round-trip or when the server does not support HEAD/introspection). Use `GraphQLConnectivityQuery` to supply a custom query for the GraphQL connectivity check (e.g. `network.DefaultGraphQLConnectivityQuery` or a server-specific query).

### Defaults

- **DefaultTimeout**: 10 seconds. Used when `ConnectionOptions.Timeout` is zero or negative.
- **RetryDelay**: If zero, 2 seconds is used between HTTP retries.
- **GraphQL connectivity query**: `network.DefaultGraphQLConnectivityQuery` (`query { __typename }`) unless `GraphQLConnectivityQuery` is set.

### URL Options

```go
type URLOptions struct {
    Scheme URLScheme         // http, https, ws, wss
    Host   string            // example.com:8080
    Paths  []string          // ["/api", "/v1"]
    Params map[string]string // Query parameters
}
```

## Debug Logging

Enable debug logging by setting the environment variable:

```bash
export LOOM_DEBUG_TRACE=development
```

### Log Levels

- `production` - Errors and warnings only
- `development` - Info, warnings, and errors
- `debug` - All logs including debug messages

### Telemetry Configuration

```bash
# Enable OTLP telemetry
export LOOM_OTLP_ENABLED=true
export LOOM_OTLP_HOST=localhost
export LOOM_OTLP_PORT=4317
```

## Error Handling

### HTTP Client

```go
resp := <-httpClient.Request(ctx, network.GET, urlOpts, nil, nil, 0, 3)
if resp.Error != nil {
    // Handle error
    log.Printf("Request failed: %v", resp.Error)
    return
}
// Use resp.Data
```

### GraphQL Client

```go
result := <-gqlClient.Query(&query, variables)
if result.Error != nil {
    // Handle error
    log.Printf("Query failed: %v", result.Error)
    return
}
// Use result.Response or the filled query struct
```

### WebSocket Client

```go
errChan := wsClient.Listen(ctx, handleMessage)
if err := <-errChan; err != nil {
    if err == context.Canceled {
        // Normal shutdown
    } else {
        // Connection error
        log.Printf("WebSocket error: %v", err)
    }
}
```

## Advanced Features

### Context Cancellation

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

respChan := httpClient.Request(ctx, network.GET, urlOpts, nil, nil, 0, 0)
select {
case resp := <-respChan:
    // Handle response
case <-ctx.Done():
    // Handle timeout
}
```

### Retry Configuration

```go
opts := network.ConnectionOptions{
    Retries:    3,
    RetryDelay: 2 * time.Second,
}

// Exponential backoff can be implemented by adjusting RetryDelay
```

### WebSocket Auto-Reconnect

```go
wsClient.SetAutoReconnect(true, 5*time.Second)

// Client will automatically reconnect on connection loss
errChan := wsClient.Listen(ctx, handleMessage)
```

### Custom Headers

```go
opts := network.ConnectionOptions{
    Headers: map[string]string{
        "Authorization": "Bearer token",
        "Content-Type":  "application/json",
        "User-Agent":    "MyApp/1.0",
    },
}
```

## Examples

### Local GraphQL Server & Client

Complete working example with gqlgen server that properly supports GraphQL variables:

```bash
# Terminal 1: Start gqlgen GraphQL server
cd examples/graphql/server
go run server.go generated.go models_gen.go resolver.go schema.resolvers.go

# Terminal 2: Run client examples
go run ./examples/graphql
```

**Server Features:**
- **gqlgen-based GraphQL server** on `http://localhost:8080/query`
- **Full GraphQL variables support** - proper implementation
- In-memory database with Users and Posts
- Full CRUD operations (Create, Read, Update, Delete)
- **GraphQL Playground UI** at `http://localhost:8080/` for interactive testing
- Type-safe resolvers with Go code generation
- Thread-safe operations with mutex locks
- Production-ready GraphQL implementation

**Client Examples:**
All examples use **GraphQL variables** (proper approach):
- Query all users (no variables)
- Query user by ID with posts (with `$id` variable)
- Create user mutation (with `$input` variable)
- Create post mutation (with `$input` variable)
- Update user mutation (with `$input` variable)
- Verify all data

**GraphQL Variables:**
The client demonstrates proper GraphQL variable usage:
```go
// Query with variables
var query struct {
    User struct {
        ID   string
        Name string
    } `graphql:"user(id: $id)"`
}

variables := map[string]interface{}{
    "id": network.ID("1"),
}

resultChan := client.Query(&query, variables)
```

**Mutation with Input Types:**
```go
type CreateUserInput struct {
    Name  string `json:"name"`
    Email string `json:"email"`
}

var mutation struct {
    CreateUser struct {
        ID   string
        Name string
    } `graphql:"createUser(input: $input)"`
}

input := CreateUserInput{
    Name:  "John",
    Email: "john@example.com",
}

variables := map[string]interface{}{
    "input": input,
}

resultChan := client.Mutation(&mutation, variables)
```

**GraphQL Playground:**
Visit `http://localhost:8080/` in your browser to use the interactive GraphQL Playground for testing queries and mutations with variables.

### GraphQL API (Simplified)

The GraphQL client has been simplified - context is handled internally:

```go
// Query - just pass query struct and variables
resultChan := client.Query(&query, variables)
result := <-resultChan

// Mutation - just pass mutation struct and variables  
resultChan := client.Mutation(&mutation, variables)
result := <-resultChan
```

**No need to pass context or response parameters!**

## Testing

```bash
# Run all tests
go test -v ./...

# Run specific test
go test -v -run TestHTTPClient

# Run with coverage
go test -v -cover ./...
```

## Architecture

### Package Structure

```
network/
├── http.go           # HTTP client implementation
├── graphql.go        # GraphQL client implementation
├── websocket.go      # WebSocket client implementation
├── network.go        # Core types and factory
├── shared/
│   └── pulse.go      # Logging and telemetry
└── example/          # Example applications
```

### Design Patterns

- **Factory Pattern**: `NewConnection()` creates appropriate client
- **Interface-Based**: Common `Client` interface for all types
- **Type Casting**: Safe type conversion methods
- **Context Propagation**: All methods accept context
- **Channel-Based**: Async operations return channels

## Performance

### HTTP Client
- Connection pooling via `http.Client`
- Configurable timeouts
- Retry with exponential backoff

### GraphQL Client
- Efficient struct-based queries
- Variable support for dynamic queries
- Batching support (via underlying HTTP)

### WebSocket Client
- Ping/pong keepalive (30s interval)
- Auto-reconnection on disconnect
- Thread-safe operations
- Graceful shutdown

## Security

### Best Practices

1. **Never commit tokens** to version control
2. **Use environment variables** for sensitive data
3. **Enable TLS** for production (HTTPS/WSS)
4. **Validate inputs** before making requests
5. **Handle errors properly** - don't expose internals

### Token Management

```go
// Good: Use environment variables
token := os.Getenv("API_TOKEN")

// Good: Use .env files (with .gitignore)
// Bad: Hard-coded tokens in code
```

## Troubleshooting

### Connection Timeouts

```go
// Increase timeout
opts.Timeout = 30 * time.Second
```

### SSL/TLS Errors

```go
// For local development only
// Use HTTP instead of HTTPS
opts.URL.Scheme = network.HTTP
```

### Rate Limiting

```go
// Add delay between requests
time.Sleep(1 * time.Second)

// Or use RetryDelay
opts.RetryDelay = 5 * time.Second
```

### Debug Logging

```bash
# Enable verbose logging
export LOOM_DEBUG_TRACE=debug

# Check logs
go run main.go 2>&1 | grep "network"
```