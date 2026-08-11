# Redis Client

A simple and efficient Go client for Redis with support for key-value, document, cache, and stream operations.

## Installation

```bash
go get github.com/machanirobotics/loom/go/redis
```

## Quick Examples

### 1. Key-Value Store

```go
// Set and get simple key-value pairs
doc := redis.Document{
    Data: map[string]interface{}{
        "key": "username",
        "value": "john_doe",
    },
}
created, err := manager.Document.KV.Create(doc)
value, err := manager.Document.KV.Get("username")
```

### 2. Cache Document Store

```go
// Store and retrieve structured data
type User struct {
    Name  string `json:"name"`
    Email string `json:"email"`
}

userDoc := redis.Document{
    Data: User{
        Name: "John",
        Email: "john@example.com",
    },
    TTL: 30,
}
created, err := manager.Document.Create(userDoc)

var result User
doc, err := manager.Document.Get(created.ID)
if err := doc.Decode(&result); err != nil {
    log.Fatal(err)
}
```

### 3. Stream Processing

```go
// Create a stream
stream, err := manager.Channel.Stream.Create(redis.Stream{
    Name:        "notifications",
    Description: "User notifications stream",
    Subjects:    []string{"user.login", "user.logout"},
})

// Publish to a stream
err = stream.Publisher.Publish("user.login", map[string]interface{}{
    "user_id": 123,
    "timestamp": time.Now().Unix(),
})

// Subscribe to a stream
msgs, err := stream.Subscriber.Subscribe("user.login")
go func() {
    for msg := range msgs {
        log.Printf("Received: %+v", msg)
    }
}()
```

## Configuration

### Environment Variables

```bash
REDIS_HOST=localhost     # Redis server host
REDIS_PORT=6379         # Redis server port
REDIS_PASSWORD=         # Redis password (if any)
REDIS_DB=0              # Database number
```

### Programmatic Configuration

```go
client, err := redis.NewRedisClient(redis.RedisClientOptions{
    Address:  "localhost",
    Port:     "6379",
    Password: "your-password",
    Database: "0",
})
```
