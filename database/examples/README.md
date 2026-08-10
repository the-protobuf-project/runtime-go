# Database examples

One runnable program per shape the contract supports. Each is small on purpose:
the point is what the database does with a generated descriptor, not how a
descriptor is made — that lives once in [`internal/model`](./internal/model).

| Example | Backend | Needs | Shows |
| --- | --- | --- | --- |
| [`sql`](./sql) | SQLite (in memory) | nothing | records, migrations from the descriptor, transactions, a typed view |
| [`keyvalue`](./keyvalue) | Redis | `redis` | records with no query language, and the capabilities it honestly lacks |
| [`document`](./document) | MongoDB | `mongodb` | server-side filtering, bulk reads, and a change stream |
| [`graph`](./graph) | ArangoDB | `arangodb` | records *and* the connections between them, in one transaction |
| [`timeseries`](./timeseries) | TimescaleDB | `timescaledb` | a hypertable, a window, and a reduction that runs in the database |
| [`cached`](./cached) | SQLite + Redis | `redis` | a read-through cache in front of a store |

`sql` runs with nothing installed. For the rest:

```sh
docker compose -f ../docker/compose.yaml up -d mongodb arangodb timescaledb
docker compose -f ../../cache/docker/compose.yaml up -d redis

go run ./sql
go run ./graph
```

Each program creates what it needs and drops it on the way out, so they can be
run repeatedly and in any order.

## Why this is a separate module

The examples import the database module by its published path rather than by a
relative one, so they exercise what an outside consumer actually writes — an
example that reached into the package next door would compile under conditions
no user has.

They are also commands rather than library code, so keeping them out of the
module means `go build ./...` on the library never depends on whether a demo
still compiles.

## The shape every example shares

```go
client, _ := <backend>.NewClient(ctx, <backend>.Config{…})  // you own it
p := <backend>.NewProvider(client)
db, _ := p.SetDatabase(ctx, "tenant_a")                     // nothing is reachable until now
defer db.Close()

db.Schema.EnsureSchema(ctx, res)   // the schema comes from the descriptor
db.Create(ctx, res, book)
```

What differs between them is only which capabilities are real. Every example
ends by asking for one its backend does not have, so the refusal is visible
rather than theoretical:

```
graph on SQL: database: unimplemented: gorm is not a graph
transactions: database: unimplemented: redis cannot run a transaction
```
