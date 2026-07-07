# Network Package Examples

This directory contains comprehensive examples for all three network client types: HTTP, GraphQL, and WebSocket.

## Directory Structure

```
example/
├── README.md              # This file
├── EXAMPLES_SUMMARY.md    # Quick start guide
├── http/
│   └── main.go           # HTTP client examples
├── graphql/
│   ├── main.go           # Main entry point
│   ├── query.go          # GraphQL query examples
│   ├── mutation.go       # GraphQL mutation examples
│   └── README.md         # GraphQL guide
└── websocket/
    └── main.go           # WebSocket client examples
```

## Running Examples

### HTTP Client Examples

The HTTP examples use [httpbin.org](https://httpbin.org) - a free HTTP request & response service.

```bash
cd example/http
go run main.go
```

**Features demonstrated:**
- Simple GET requests
- GET with query parameters
- POST with JSON body
- Automatic retries on failure
- Context cancellation/timeout
- Synchronous vs asynchronous requests

### GraphQL Client Examples (Queries + Mutations)

The GraphQL examples demonstrate both **queries** and **mutations** in one organized package:
- **Queries**: [Rick and Morty GraphQL API](https://rickandmortyapi.com/graphql) (no auth)
- **Mutations**: [GitHub GraphQL API](https://docs.github.com/en/graphql) (requires token)

```bash
cd example/graphql

# Run all examples (queries + mutations)
go run *.go

# With GitHub token for real mutations
export GITHUB_TOKEN=your_token_here
go run *.go
```

**File Structure:**
- `main.go` - Client initialization and orchestration
- `query.go` - 4 query examples (Rick and Morty API)
- `mutation.go` - 3 mutation examples (GitHub API)

**Features demonstrated:**
- Simple queries with hardcoded values
- Queries with variables
- Complex nested queries (characters with episodes)
- Pagination support
- Add star to repository (mutation)
- Create a gist (mutation)
- Update user status (mutation)
- Authentication with tokens
- Demo mode without API

**Note:** Queries work without authentication. Mutations require a GitHub Personal Access Token. See the [GraphQL README](graphql/README.md) for setup instructions.

### WebSocket Client Examples

The WebSocket examples use [echo.websocket.org](https://echo.websocket.org) - a free WebSocket echo service.

```bash
cd example/websocket
go run main.go
```

**Features demonstrated:**
- Simple echo connection
- Sending and receiving messages
- Auto-reconnection on disconnect
- Graceful shutdown with signal handling
- Context-based cancellation
- Ping/pong keepalive (automatic)

## Public APIs Used

### HTTP: httpbin.org
- **URL**: https://httpbin.org
- **Features**: HTTP request testing, various endpoints for different scenarios
- **No authentication required**

### GraphQL: Rick and Morty API
- **URL**: https://rickandmortyapi.com/graphql
- **Features**: Character, episode, and location data from the TV show
- **No authentication required**
- **GraphQL Playground**: https://rickandmortyapi.com/graphql

### WebSocket: echo.websocket.org
- **URL**: wss://echo.websocket.org
- **Features**: Echoes back any message sent to it
- **No authentication required**

## Example Output

### HTTP Example
```
=== HTTP Client Examples ===

1. Simple GET Request
   URL: https://httpbin.org/get
   Success! Response length: 312 bytes
   Response preview: {
     "args": {},
     "headers": {
       "Accept-Encoding": "gzip",
       ...
```

### GraphQL Example
```
=== GraphQL Client Examples ===
Using Rick and Morty GraphQL API

1. Simple Query - Get Character by ID
   Fetching Rick Sanchez (ID: 1)
   Success!
   Name: Rick Sanchez
   Status: Alive
   Species: Human
   Gender: Male
```

### WebSocket Example
```
=== WebSocket Client Examples ===
Using WebSocket Echo Server

1. Simple WebSocket Echo
   Connecting to wss://echo.websocket.org
   Connected!
   Sending: Hello, WebSocket!
   Received (type 1): Hello, WebSocket!
   Echo successful!
```

## Additional Resources

### GraphQL Queries for Rick and Morty API

Here are some useful queries you can try:

**Get all characters:**
```graphql
query {
  characters {
    results {
      id
      name
      status
      species
    }
  }
}
```

**Search by name:**
```graphql
query {
  characters(filter: { name: "Rick" }) {
    results {
      id
      name
      status
    }
  }
}
```

**Get character with episodes:**
```graphql
query {
  character(id: 1) {
    name
    episode {
      name
      episode
    }
  }
}
```

### Testing Your Own APIs

To test with your own APIs, simply modify the connection options:

```go
opts := network.ConnectionOptions{
    URL: network.URLOptions{
        Scheme: network.HTTPS,
        Host:   "your-api.com",
        Paths:  []string{"/your-endpoint"},
    },
    Timeout: 10 * time.Second,
    Headers: map[string]string{
        "Authorization": "Bearer YOUR_TOKEN",
    },
}
```

## Troubleshooting

### Connection Timeouts
If you're experiencing timeouts, try increasing the timeout value:
```go
Timeout: 30 * time.Second,
```

### SSL/TLS Errors
The examples use HTTPS/WSS by default. If testing with local servers, use HTTP/WS:
```go
Scheme: network.HTTP,  // or network.WS for WebSocket
```

### Rate Limiting
Public APIs may have rate limits. If you encounter 429 errors, add delays between requests:
```go
time.Sleep(1 * time.Second)
```

## Need Help?

- Check the main package documentation
- Review the `IMPROVEMENTS.md` file for API details
- Look at the test files for more usage examples
