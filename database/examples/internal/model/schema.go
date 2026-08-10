package model

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/the-protobuf-project/runtime-go/database"
)

// file is the proto these examples would have generated from.
//
// Written out rather than compiled from a .proto so the module needs no
// generator to build — the shape is what matters here, not how it was produced.
var file = &descriptorpb.FileDescriptorProto{
	Name:       proto.String("examples/v1/model.proto"),
	Package:    proto.String("examples.v1"),
	Syntax:     proto.String("proto3"),
	Dependency: []string{"google/protobuf/timestamp.proto"},
	MessageType: []*descriptorpb.DescriptorProto{
		{
			Name: proto.String("Book"),
			Field: []*descriptorpb.FieldDescriptorProto{
				str("id", 1), str("title", 2), i32("published_year", 3),
			},
		},
		{
			Name: proto.String("Author"),
			Field: []*descriptorpb.FieldDescriptorProto{
				str("id", 1), str("name", 2),
			},
		},
		{
			Name: proto.String("Wrote"),
			Field: []*descriptorpb.FieldDescriptorProto{
				str("id", 1), str("role", 2),
			},
		},
		{
			Name: proto.String("Reading"),
			Field: []*descriptorpb.FieldDescriptorProto{
				str("id", 1), str("sensor", 2), f64("celsius", 3), ts("observed_at", 4),
			},
		},
	},
}

// BookResource describes a Book: an id, a title people search by, and a year.
//
// The title is unique so the examples have something for a backend to enforce —
// which is the whole of what "unique" means here, since a descriptor that
// claimed it without a store behind it would be the quiet lie the contract
// exists to avoid.
func BookResource() *database.Resource {
	return &database.Resource{
		Name: "Book", Table: "books", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(bookMD) },
		Columns: []database.Column{
			{Name: "id", Field: "id", Kind: database.KindString, SQLType: "TEXT", PrimaryKey: true, NotNull: true},
			{Name: "title", Field: "title", Kind: database.KindString, SQLType: "TEXT", Unique: true},
			{Name: "published_year", Field: "published_year", Kind: database.KindInt, SQLType: "INTEGER"},
		},
	}
}

// AuthorResource describes an Author.
func AuthorResource() *database.Resource {
	return &database.Resource{
		Name: "Author", Table: "authors", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(authorMD) },
		Columns: []database.Column{
			{Name: "id", Field: "id", Kind: database.KindString, SQLType: "TEXT", PrimaryKey: true, NotNull: true},
			{Name: "name", Field: "name", Kind: database.KindString, SQLType: "TEXT"},
		},
	}
}

// WroteResource describes the connection from an Author to a Book.
//
// IsEdge is what tells a graph backend to store it as a connection rather than
// as a record — an edge collection on ArangoDB, a relationship type on Neo4j.
// Every other backend ignores it.
func WroteResource() *database.Resource {
	return &database.Resource{
		Name: "Wrote", Table: "wrote", PKColumn: "id", IsEdge: true,
		New: func() proto.Message { return dynamicpb.NewMessage(wroteMD) },
		Columns: []database.Column{
			{Name: "id", Field: "id", Kind: database.KindString},
			{Name: "role", Field: "role", Kind: database.KindString},
		},
	}
}

// ReadingResource describes a measurement.
//
// The id is the lookup key but not a SQL primary key, which is what TimescaleDB
// requires: every unique index has to contain the partitioning column, and this
// one is partitioned on observed_at. See the timescale example.
func ReadingResource() *database.Resource {
	return &database.Resource{
		Name: "Reading", Table: "readings", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(readingMD) },
		Columns: []database.Column{
			{Name: "id", Field: "id", Kind: database.KindString, SQLType: "TEXT", NotNull: true},
			{Name: "sensor", Field: "sensor", Kind: database.KindString, SQLType: "TEXT"},
			{Name: "celsius", Field: "celsius", Kind: database.KindFloat, SQLType: "DOUBLE PRECISION"},
			{Name: "observed_at", Field: "observed_at", Kind: database.KindTimestamp, SQLType: "TIMESTAMPTZ", NotNull: true},
		},
	}
}

// Registry indexes every resource, which the graph half needs to turn a
// resource name back into a collection.
func Registry() *database.Registry {
	return database.NewRegistry(
		*BookResource(), *AuthorResource(), *WroteResource(), *ReadingResource(),
	)
}

func str(name string, n int32) *descriptorpb.FieldDescriptorProto {
	return field(name, n, descriptorpb.FieldDescriptorProto_TYPE_STRING, "")
}

func i32(name string, n int32) *descriptorpb.FieldDescriptorProto {
	return field(name, n, descriptorpb.FieldDescriptorProto_TYPE_INT32, "")
}

func f64(name string, n int32) *descriptorpb.FieldDescriptorProto {
	return field(name, n, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, "")
}

func ts(name string, n int32) *descriptorpb.FieldDescriptorProto {
	return field(name, n, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".google.protobuf.Timestamp")
}

func field(name string, n int32, kind descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
	f := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(n),
		Type:   kind.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
	if typeName != "" {
		f.TypeName = proto.String(typeName)
	}
	return f
}
