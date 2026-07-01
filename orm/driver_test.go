package orm_test

import (
	"context"
	"errors"
	"testing"

	"github.com/glebarez/sqlite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/orm"
	"github.com/the-protobuf-project/runtime-go/store"
)

// bookMD builds a dynamic Book descriptor (id, title, published_year, genre) so
// the driver can be exercised without generated proto types.
func bookMD(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("bookstore/v1/orm_test.proto"),
		Package: proto.String("bookstore.v1"),
		Syntax:  proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Genre"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("GENRE_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("GENRE_FICTION"), Number: proto.Int32(1)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Book"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("title"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("published_year"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("genre"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(), TypeName: proto.String(".bookstore.v1.Genre"), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

func bookRes(md protoreflect.MessageDescriptor) *store.Resource {
	return &store.Resource{
		Name:     "Book",
		Table:    "books",
		PKColumn: "id",
		New:      func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "title", Field: "title", Kind: store.KindString, NotNull: true},
			{Name: "published_year", Field: "published_year", Kind: store.KindInt},
			{Name: "genre", Field: "genre", Kind: store.KindEnum, NotNull: true},
		},
	}
}

func newBook(md protoreflect.MessageDescriptor, id, title string, year int32, genre int32) proto.Message {
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	f := md.Fields()
	m.Set(f.ByName("id"), protoreflect.ValueOfString(id))
	m.Set(f.ByName("title"), protoreflect.ValueOfString(title))
	m.Set(f.ByName("published_year"), protoreflect.ValueOfInt32(year))
	m.Set(f.ByName("genre"), protoreflect.ValueOfEnum(protoreflect.EnumNumber(genre)))
	return msg
}

func setup(t *testing.T) (*orm.Driver, *store.Resource, protoreflect.MessageDescriptor) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.Exec(`CREATE TABLE books (
		id TEXT PRIMARY KEY,
		title TEXT NOT NULL,
		published_year INTEGER,
		genre INTEGER NOT NULL
	)`).Error; err != nil {
		t.Fatalf("create table: %v", err)
	}
	md := bookMD(t)
	return orm.New(db), bookRes(md), md
}

func TestDriverCRUD(t *testing.T) {
	ctx := context.Background()
	d, res, md := setup(t)

	if _, err := d.Create(ctx, res, newBook(md, "books/dune", "Dune", 1965, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := d.Get(ctx, res, "books/dune")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if title := got.ProtoReflect().Get(md.Fields().ByName("title")).String(); title != "Dune" {
		t.Fatalf("title = %q, want Dune", title)
	}

	ok, err := d.Exists(ctx, res, "books/dune")
	if err != nil || !ok {
		t.Fatalf("Exists = %v, %v; want true", ok, err)
	}

	// Update the title.
	if _, err := d.Update(ctx, res, newBook(md, "books/dune", "Dune (1965)", 1965, 1)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ = d.Get(ctx, res, "books/dune")
	if title := got.ProtoReflect().Get(md.Fields().ByName("title")).String(); title != "Dune (1965)" {
		t.Fatalf("after update title = %q", title)
	}

	// Second record + List.
	if _, err := d.Create(ctx, res, newBook(md, "books/hobbit", "The Hobbit", 1937, 1)); err != nil {
		t.Fatalf("Create 2: %v", err)
	}
	lr, err := d.List(ctx, res, store.ListOptions{PageSize: 10, OrderBy: "id"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lr.Items) != 2 || lr.Total != 2 {
		t.Fatalf("List items=%d total=%d, want 2/2", len(lr.Items), lr.Total)
	}

	if err := d.Delete(ctx, res, "books/dune"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := d.Get(ctx, res, "books/dune"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get after delete err = %v, want ErrNotFound", err)
	}
}

func TestDriverErrorMapping(t *testing.T) {
	ctx := context.Background()
	d, res, md := setup(t)

	if _, err := d.Create(ctx, res, newBook(md, "books/dune", "Dune", 1965, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := d.Create(ctx, res, newBook(md, "books/dune", "Dup", 1965, 1)); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate Create err = %v, want ErrAlreadyExists", err)
	}
	if _, err := d.Update(ctx, res, newBook(md, "books/missing", "X", 2000, 1)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Update missing err = %v, want ErrNotFound", err)
	}
	if err := d.Delete(ctx, res, "books/missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Delete missing err = %v, want ErrNotFound", err)
	}
}

// TestDriverManagedColumns proves the driver fills a synthesized ULID primary
// key and audit timestamps the proto message does not carry — the case that lets
// the dynamic runtime write to a protorm schema with ID_STRATEGY_ULID +
// timestamps.
func TestDriverManagedColumns(t *testing.T) {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Exec(`CREATE TABLE notes (
		id TEXT PRIMARY KEY,
		body TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	)`).Error; err != nil {
		t.Fatalf("create table: %v", err)
	}

	// A Note message carrying only `body`; id/created_at/updated_at are managed.
	fileProto := &descriptorpb.FileDescriptorProto{
		Name: proto.String("notes/v1/notes_test.proto"), Package: proto.String("notes.v1"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Note"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("body"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("descriptor: %v", err)
	}
	md := fd.Messages().Get(0)
	res := &store.Resource{
		Name: "Note", Table: "notes", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "", Kind: store.KindString, PrimaryKey: true, NotNull: true, Generated: "ulid"},
			{Name: "body", Field: "body", Kind: store.KindString, NotNull: true},
			{Name: "created_at", Field: "", Kind: store.KindTimestamp, NotNull: true, AutoCreate: true},
			{Name: "updated_at", Field: "", Kind: store.KindTimestamp, NotNull: true, AutoUpdate: true},
		},
	}

	d := orm.New(db)
	note := dynamicpb.NewMessage(md)
	note.ProtoReflect().Set(md.Fields().ByName("body"), protoreflect.ValueOfString("hello"))
	if _, err := d.Create(ctx, res, note); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var rows []map[string]any
	if err := db.Table("notes").Find(&rows).Error; err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if id, _ := rows[0]["id"].(string); len(id) != 26 {
		t.Fatalf("generated id = %q (len %d), want a 26-char ULID", id, len(id))
	}
	if rows[0]["created_at"] == nil || rows[0]["updated_at"] == nil {
		t.Fatalf("audit timestamps not set: %#v", rows[0])
	}
}

func TestDriverListPagination(t *testing.T) {
	ctx := context.Background()
	d, res, md := setup(t)
	for _, id := range []string{"a", "b", "c"} {
		if _, err := d.Create(ctx, res, newBook(md, id, id, 2000, 1)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	first, err := d.List(ctx, res, store.ListOptions{PageSize: 2, OrderBy: "id"})
	if err != nil {
		t.Fatalf("List page1: %v", err)
	}
	if len(first.Items) != 2 || first.NextPageToken == "" {
		t.Fatalf("page1 items=%d next=%q, want 2 + token", len(first.Items), first.NextPageToken)
	}
	second, err := d.List(ctx, res, store.ListOptions{PageSize: 2, OrderBy: "id", PageToken: first.NextPageToken})
	if err != nil {
		t.Fatalf("List page2: %v", err)
	}
	if len(second.Items) != 1 || second.NextPageToken != "" {
		t.Fatalf("page2 items=%d next=%q, want 1 + no token", len(second.Items), second.NextPageToken)
	}
}
