package store_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/the-protobuf-project/runtime-go/store"
)

// bookMessage builds a dynamic "Book" message descriptor at runtime (no codegen)
// so the bridge can be exercised against a real protoreflect message covering a
// string PK, an optional int32 (presence), an enum, and a Timestamp.
func bookMessage(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	optional := true
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("bookstore/v1/book_test.proto"),
		Package:    proto.String("bookstore.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
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
				{Name: proto.String("published_year"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Proto3Optional: &optional, OneofIndex: proto.Int32(0)},
				{Name: proto.String("genre"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(), TypeName: proto.String(".bookstore.v1.Genre"), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("create_time"), Number: proto.Int32(5), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".google.protobuf.Timestamp"), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
			OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("_published_year")}},
		}},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build file descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

func bookResource(md protoreflect.MessageDescriptor) store.Resource {
	return store.Resource{
		Name:     "Book",
		Schema:   "bookstore_v1",
		Table:    "books",
		PKColumn: "id",
		New:      func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "title", Field: "title", Kind: store.KindString, NotNull: true},
			{Name: "published_year", Field: "published_year", Kind: store.KindInt},
			{Name: "genre", Field: "genre", Kind: store.KindEnum, NotNull: true},
			{Name: "create_time", Field: "create_time", Kind: store.KindTimestamp, NotNull: true},
		},
	}
}

func TestBridgeRoundTrip(t *testing.T) {
	md := bookMessage(t)
	res := bookResource(md)

	orig := dynamicpb.NewMessage(md)
	m := orig.ProtoReflect()
	fields := md.Fields()
	m.Set(fields.ByName("id"), protoreflect.ValueOfString("books/dune"))
	m.Set(fields.ByName("title"), protoreflect.ValueOfString("Dune"))
	m.Set(fields.ByName("published_year"), protoreflect.ValueOfInt32(1965))
	m.Set(fields.ByName("genre"), protoreflect.ValueOfEnum(1))
	m.Set(fields.ByName("create_time"), protoreflect.ValueOfMessage(timestamppb.New(time.Unix(1700000000, 0)).ProtoReflect()))

	cols, err := store.MessageToColumns(&res, orig)
	if err != nil {
		t.Fatalf("MessageToColumns: %v", err)
	}
	if cols["id"] != "books/dune" || cols["title"] != "Dune" {
		t.Fatalf("unexpected string columns: %#v", cols)
	}
	if cols["published_year"] != int64(1965) {
		t.Fatalf("published_year = %#v, want int64(1965)", cols["published_year"])
	}
	if cols["genre"] != int32(1) {
		t.Fatalf("genre = %#v, want int32(1)", cols["genre"])
	}
	if _, ok := cols["create_time"].(time.Time); !ok {
		t.Fatalf("create_time = %T, want time.Time", cols["create_time"])
	}

	got, err := store.ColumnsToMessage(&res, cols)
	if err != nil {
		t.Fatalf("ColumnsToMessage: %v", err)
	}
	if !proto.Equal(orig, got) {
		t.Fatalf("round-trip mismatch:\n orig = %v\n got  = %v", orig, got)
	}

	key, err := store.KeyOf(&res, got)
	if err != nil || key != "books/dune" {
		t.Fatalf("KeyOf = %q, %v; want books/dune", key, err)
	}
}

func TestBridgeOptionalUnsetIsNull(t *testing.T) {
	md := bookMessage(t)
	res := bookResource(md)

	// published_year left unset; it has presence and is not NotNull -> nil.
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	m.Set(md.Fields().ByName("id"), protoreflect.ValueOfString("books/x"))

	cols, err := store.MessageToColumns(&res, msg)
	if err != nil {
		t.Fatalf("MessageToColumns: %v", err)
	}
	if cols["published_year"] != nil {
		t.Fatalf("unset optional published_year = %#v, want nil", cols["published_year"])
	}
}
