# Database examples

Two sets, because there are two ways to use this module and they suit different
programs.

## [`simple/`](./simple) — a Go struct is the schema

Start here. No protobuf, no descriptors, no generator.

```go
type Book struct {
    ID    string `db:"id,pk"`
    Title string `db:"title,unique"`
    Year  int32  `db:"published_year"`
}

books, _ := database.Collection[Book](db, "books")
books.EnsureSchema(ctx)

id, _ := books.Create(ctx, Book{ID: "books/dune", Title: "Dune", Year: 1965})
b, _ := books.Get(ctx, id)   // already a Book
```

| Example | Backend | Needs |
| --- | --- | --- |
| [`simple/sql`](./simple/sql) | SQLite | nothing |
| [`simple/mongodb`](./simple/mongodb) | MongoDB | `mongodb` |
| [`simple/timescale`](./simple/timescale) | TimescaleDB | `timescaledb` |

`simple/sql` and `simple/mongodb` store the **same struct with the same calls** —
only the lines that open a client differ. That pair is where the portability
claim is checked rather than asserted.

## [`store/`](./store) — a proto message is the schema

For a program whose schema already comes from a proto file, where protorm
generates the descriptor and the gRPC adapter serves it. Same rows, same tables,
nothing hidden.

| Example | Backend | Shows |
| --- | --- | --- |
| [`store/sql`](./store/sql) | SQLite | records, migrations, transactions, typed views |
| [`store/keyvalue`](./store/keyvalue) | Redis | records without a query language |
| [`store/document`](./store/document) | MongoDB | filtering, bulk, change streams |
| [`store/graph`](./store/graph) | ArangoDB | records *and* the edges between them |
| [`store/timeseries`](./store/timeseries) | TimescaleDB | hypertables, windows, reductions |
| [`store/cached`](./store/cached) | SQLite + Redis | a read-through cache over a store |

## Running them

`simple/sql` and `store/sql` need nothing. For the rest:

```sh
docker compose -f ../docker/compose.yaml up -d mongodb arangodb timescaledb
docker compose -f ../../cache/docker/compose.yaml up -d redis

go run ./simple/mongodb
go run ./store/graph
```

Each creates what it needs and drops it on the way out, so they can be run
repeatedly and in any order.

## Which to use

Use `simple/` unless your schema is already a proto file. It derives the same
descriptor the generator would emit, so a struct stored through it and a message
stored through `store` are the same rows — and `Coll.Resource()` hands you the
descriptor whenever you need to reach the layer underneath for a graph edge, a
time-series window, or a transaction.

Use `store/` when the proto is the source of truth, when the gRPC adapter is
serving the same resources, or when a chain driver is involved — those all speak
descriptors already, and going through `simple/` would derive one you already
have.
