# GraphQL Test Server

A simple GraphQL server with in-memory database for testing queries and mutations.

## Features

- In-memory database (no external dependencies)
- Users and Posts with relationships
- Full CRUD operations (Create, Read, Update, Delete)
- Time fields (createdAt, updatedAt) using `time.Time`
- CORS enabled for browser testing

## Running the Server

```bash
cd example/graphql-server
go mod download
go run main.go
```

Server will start at: `http://localhost:8080/graphql`

## Data Model

### User
- `id`: String
- `name`: String
- `email`: String
- `createdAt`: DateTime
- `updatedAt`: DateTime
- `posts`: [Post]

### Post
- `id`: String
- `title`: String
- `content`: String
- `authorId`: String
- `createdAt`: DateTime
- `updatedAt`: DateTime
- `author`: User

## Example Queries

### Get All Users
```graphql
{
  users {
    id
    name
    email
    createdAt
    updatedAt
  }
}
```

### Get User by ID with Posts
```graphql
{
  user(id: "1") {
    id
    name
    email
    createdAt
    posts {
      id
      title
      createdAt
    }
  }
}
```

### Get All Posts with Authors
```graphql
{
  posts {
    id
    title
    content
    createdAt
    updatedAt
    author {
      id
      name
      email
    }
  }
}
```

### Get Post by ID
```graphql
{
  post(id: "1") {
    id
    title
    content
    createdAt
    author {
      name
      email
    }
  }
}
```

## Example Mutations

### Create User
```graphql
mutation {
  createUser(name: "John Doe", email: "john@example.com") {
    id
    name
    email
    createdAt
    updatedAt
  }
}
```

### Update User
```graphql
mutation {
  updateUser(id: "1", name: "Alice Updated", email: "alice.new@example.com") {
    id
    name
    email
    updatedAt
  }
}
```

### Delete User
```graphql
mutation {
  deleteUser(id: "2")
}
```

### Create Post
```graphql
mutation {
  createPost(
    title: "My New Post"
    content: "This is the content of my post"
    authorId: "1"
  ) {
    id
    title
    content
    createdAt
    author {
      name
    }
  }
}
```

### Update Post
```graphql
mutation {
  updatePost(
    id: "1"
    title: "Updated Title"
    content: "Updated content"
  ) {
    id
    title
    content
    updatedAt
  }
}
```

### Delete Post
```graphql
mutation {
  deletePost(id: "2")
}
```

## Initial Data

The server starts with seeded data:

**Users:**
- ID: 1, Name: Alice Johnson, Email: alice@example.com
- ID: 2, Name: Bob Smith, Email: bob@example.com

**Posts:**
- ID: 1, Title: "Getting Started with GraphQL", Author: Alice
- ID: 2, Title: "Building REST APIs", Author: Bob

## Testing with cURL

### Query Example
```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ users { id name email createdAt } }"}'
```

### Mutation Example
```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "mutation { createUser(name: \"Test User\", email: \"test@example.com\") { id name createdAt } }"}'
```

## Testing with Network Package

See `example/graphql-local/` for examples using this server with the network package.

## Features Demonstrated

1. **Queries**: Read operations
2. **Mutations**: Create, Update, Delete operations
3. **Relationships**: Users have Posts, Posts have Authors
4. **Time Fields**: All entities have createdAt and updatedAt timestamps
5. **Thread Safety**: Uses sync.RWMutex for concurrent access
6. **Error Handling**: Proper error messages for invalid operations

## Notes

- Data is stored in memory and will be lost when server restarts
- No authentication/authorization (for testing only)
- CORS enabled for all origins (for testing only)
- Port 8080 is hardcoded (change in main.go if needed)
