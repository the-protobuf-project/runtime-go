package hints

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// shop builds a small API in its own registry: shelves and the books on
// them, with resource and HTTP annotations, the way generated code would
// register them.
func shop(t *testing.T) *protoregistry.Files {
	t.Helper()
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	msg := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	field := func(name string, n int32) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(n), Type: str, Label: opt}
	}
	ref := func(name string, n int32, typ string) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(n), Type: msg, Label: opt, TypeName: proto.String(".shop.v1." + typ)}
	}
	resource := func(name, typ, pattern, singular, plural string) *descriptorpb.DescriptorProto {
		o := &descriptorpb.MessageOptions{}
		proto.SetExtension(o, annotations.E_Resource, &annotations.ResourceDescriptor{Type: typ, Pattern: []string{pattern}, Singular: singular, Plural: plural})
		return &descriptorpb.DescriptorProto{Name: proto.String(name), Field: []*descriptorpb.FieldDescriptorProto{field("name", 1)}, Options: o}
	}
	request := func(name string, fields ...*descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{Name: proto.String(name), Field: fields}
	}
	method := func(name, in string, rule *annotations.HttpRule) *descriptorpb.MethodDescriptorProto {
		o := &descriptorpb.MethodOptions{}
		proto.SetExtension(o, annotations.E_Http, rule)
		return &descriptorpb.MethodDescriptorProto{Name: proto.String(name), InputType: proto.String(".shop.v1." + in), OutputType: proto.String(".shop.v1.Shelf"), Options: o}
	}
	get := func(p string) *annotations.HttpRule {
		return &annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: p}}
	}
	post := func(p string) *annotations.HttpRule {
		return &annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: p}}
	}
	patch := func(p string) *annotations.HttpRule {
		return &annotations.HttpRule{Pattern: &annotations.HttpRule_Patch{Patch: p}}
	}
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("shop/v1/shop.proto"),
		Package: proto.String("shop.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			resource("Shelf", "shop.example.com/Shelf", "shelves/{shelf}", "shelf", "shelves"),
			resource("Book", "shop.example.com/Book", "shelves/{shelf}/books/{book}", "book", "books"),
			resource("BookCase", "shop.example.com/BookCase", "bookCases/{book_case}", "bookCase", "bookCases"),
			request("GetShelfRequest", field("name", 1)),
			request("ListShelvesRequest"),
			request("GetBookRequest", field("name", 1)),
			request("ListBooksRequest", field("parent", 1)),
			request("CreateBookRequest", field("parent", 1), field("book_id", 2), ref("book", 3, "Book")),
			request("UpdateShelfRequest", ref("shelf", 1, "Shelf")),
			request("UndeleteShelfRequest", field("name", 1)),
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("ShopService"),
			Method: []*descriptorpb.MethodDescriptorProto{
				method("GetShelf", "GetShelfRequest", get("/v1/{name=shelves/*}")),
				method("ListShelves", "ListShelvesRequest", get("/v1/shelves")),
				method("GetBook", "GetBookRequest", get("/v1/{name=shelves/*/books/*}")),
				method("ListBooks", "ListBooksRequest", get("/v1/{parent=shelves/*}/books")),
				method("CreateBook", "CreateBookRequest", post("/v1/{parent=shelves/*}/books")),
				method("UpdateShelf", "UpdateShelfRequest", patch("/v1/{shelf.name=shelves/*}")),
				method("UndeleteShelf", "UndeleteShelfRequest", post("/v1/{name=shelves/*}:undelete")),
			},
		}},
	}
	file, err := protodesc.NewFile(fd, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	files := &protoregistry.Files{}
	if err := files.RegisterFile(file); err != nil {
		t.Fatal(err)
	}
	return files
}

// req builds a request of type name with string fields set from kv, and a
// nested resource's name from "<field>.name".
func req(t *testing.T, files *protoregistry.Files, name string, kv ...string) proto.Message {
	t.Helper()
	d, err := files.FindDescriptorByName(protoreflect.FullName("shop.v1." + name))
	if err != nil {
		t.Fatal(err)
	}
	m := dynamicpb.NewMessage(d.(protoreflect.MessageDescriptor))
	for i := 0; i < len(kv); i += 2 {
		path, val := kv[i], kv[i+1]
		if outer, inner, ok := strings.Cut(path, "."); ok {
			fd := m.Descriptor().Fields().ByName(protoreflect.Name(outer))
			sub := m.Mutable(fd).Message()
			sub.Set(sub.Descriptor().Fields().ByName(protoreflect.Name(inner)), protoreflect.ValueOfString(val))
			continue
		}
		m.Set(m.Descriptor().Fields().ByName(protoreflect.Name(path)), protoreflect.ValueOfString(val))
	}
	return m
}

func TestRewrite(t *testing.T) {
	files := shop(t)
	h := New(Options{Product: "shop", Files: files})
	ctx := context.Background()
	const svc = "/shop.v1.ShopService/"
	for _, tc := range []struct {
		name   string
		method string
		req    proto.Message
		err    error
		want   []string // substrings of the rewritten message
		same   bool     // left as it was
	}{
		{"a bare not found names the resource and how to list",
			svc + "GetShelf", req(t, files, "GetShelfRequest", "name", "shelves/ghost"),
			status.Error(codes.NotFound, "Not found."),
			[]string{`Shelf "shelves/ghost" does not exist`, "ListShelves (GET /v1/shelves)"}, false},
		{"a nested resource lists under its parent",
			svc + "GetBook", req(t, files, "GetBookRequest", "name", "shelves/s/books/b"),
			status.Error(codes.NotFound, "not found"),
			[]string{`Book "shelves/s/books/b"`, "ListBooks (GET /v1/shelves/s/books)"}, false},
		{"a stale etag says to read again",
			svc + "UpdateShelf", req(t, files, "UpdateShelfRequest", "shelf.name", "shelves/s"),
			status.Error(codes.Aborted, "conflict: Message: row changed"),
			[]string{"changed after you read it", "GetShelf (GET /v1/shelves/s)"}, false},
		{"a taken id says to choose another",
			svc + "CreateBook", req(t, files, "CreateBookRequest", "parent", "shelves/s", "book_id", "b1"),
			status.Error(codes.AlreadyExists, "already exists: duplicate key"),
			[]string{`"shelves/s/books/b1" already exists`, "Choose another id", "GetBook (GET /v1/shelves/s/books/b1)"}, false},
		{"undelete of a live record says there is nothing to restore",
			svc + "UndeleteShelf", req(t, files, "UndeleteShelfRequest", "name", "shelves/s"),
			status.Error(codes.AlreadyExists, "shelves/s is not deleted"),
			[]string{"is not deleted, so there is nothing to restore"}, false},
		{"a GET of a custom method says to POST",
			svc + "GetShelf", req(t, files, "GetShelfRequest", "name", "shelves/s:undelete"),
			status.Error(codes.NotFound, "not found"),
			[]string{"custom methods are called with POST", "not a shelf name"}, false},
		{"a camel-cased kind reads as words",
			svc + "GetShelf", req(t, files, "GetShelfRequest", "name", "bookCases/c"),
			status.Error(codes.NotFound, "not found"),
			[]string{`Book case "bookCases/c" does not exist`}, false},
		{"a name of no known kind still reads",
			svc + "GetShelf", req(t, files, "GetShelfRequest", "name", "lamps/l"),
			status.Error(codes.NotFound, "not found"),
			[]string{`resource "lamps/l" does not exist`}, false},
		{"a not found that already names what is missing is left alone",
			svc + "GetBook", req(t, files, "GetBookRequest", "name", "shelves/s/books/b"),
			status.Error(codes.NotFound, `Shelf "shelves/s" does not exist.`),
			nil, true},
		{"an error that is not a status is left alone",
			svc + "GetShelf", req(t, files, "GetShelfRequest", "name", "shelves/s"),
			context.Canceled, nil, true},
		{"an internal error carries no internals",
			svc + "CreateBook", req(t, files, "CreateBookRequest"),
			status.Error(codes.Internal, "ERROR: null value in column \"title\""),
			[]string{"shop failed unexpectedly while handling CreateBook", "fault on our side", "the time and the call"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := h.Rewrite(ctx, tc.method, tc.req, tc.err)
			msg := status.Convert(got).Message()
			if tc.same {
				if got != tc.err { //nolint:errorlint // identity: left untouched
					t.Fatalf("rewrote %q into %q", status.Convert(tc.err).Message(), msg)
				}
				return
			}
			for _, w := range tc.want {
				if !strings.Contains(msg, w) {
					t.Errorf("message %q lacks %q", msg, w)
				}
			}
			if strings.Contains(msg, "duplicate key") || strings.Contains(msg, "null value") || strings.Contains(msg, "Message: row") {
				t.Errorf("message leaks the backend: %q", msg)
			}
		})
	}
}

// A service that described its error with its own reason keeps its message;
// a generic reason is rewritten, and the hint keeps its details.
func TestRewriteKeepsDescribedErrors(t *testing.T) {
	files := shop(t)
	h := New(Options{Files: files})
	r := req(t, files, "CreateBookRequest", "parent", "shelves/s", "book_id", "b")
	st, _ := status.New(codes.AlreadyExists, "Book b already exists and was made from another order.").
		WithDetails(&errdetails.ErrorInfo{Reason: "BOOK_ID_TAKEN"})
	err := st.Err()
	if got := h.Rewrite(context.Background(), "/shop.v1.ShopService/CreateBook", r, err); got != err { //nolint:errorlint // identity
		t.Fatalf("rewrote a described error: %v", got)
	}
	st, _ = status.New(codes.AlreadyExists, "already exists").WithDetails(&errdetails.ErrorInfo{Reason: "ALREADY_EXISTS", Domain: "shop"})
	got := status.Convert(h.Rewrite(context.Background(), "/shop.v1.ShopService/CreateBook", r, st.Err()))
	var info *errdetails.ErrorInfo
	var res *errdetails.ResourceInfo
	for _, d := range got.Details() {
		switch d := d.(type) {
		case *errdetails.ErrorInfo:
			info = d
		case *errdetails.ResourceInfo:
			res = d
		}
	}
	if info.GetReason() != "ALREADY_EXISTS" || res.GetResourceName() != "shelves/s/books/b" || res.GetResourceType() != "shop.example.com/Book" {
		t.Fatalf("details %v %v", info, res)
	}
}

func TestInternalCarriesTraceID(t *testing.T) {
	h := New(Options{Files: shop(t)})
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{1}})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	msg := status.Convert(h.Rewrite(ctx, "/x.Y/Z", nil, status.Error(codes.Unknown, "boom"))).Message()
	if !strings.Contains(msg, "trace "+sc.TraceID().String()) || !strings.HasPrefix(msg, "The service failed") {
		t.Fatalf("message %q", msg)
	}
}

func TestInterceptor(t *testing.T) {
	files := shop(t)
	ic := New(Options{Files: files}).Interceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/shop.v1.ShopService/GetShelf"}
	_, err := ic(context.Background(), req(t, files, "GetShelfRequest", "name", "shelves/x"), info,
		func(context.Context, any) (any, error) { return nil, status.Error(codes.NotFound, "not found") })
	if !strings.Contains(status.Convert(err).Message(), `Shelf "shelves/x" does not exist`) {
		t.Fatalf("interceptor: %v", err)
	}
	resp, err := ic(context.Background(), nil, info, func(context.Context, any) (any, error) { return "ok", nil })
	if resp != "ok" || err != nil {
		t.Fatalf("success: %v %v", resp, err)
	}
}

func TestCall(t *testing.T) {
	h := New(Options{Files: shop(t)})
	if got, want := h.Call("UndeleteShelf", map[string]string{"name": "shelves/s"}), "UndeleteShelf (POST /v1/shelves/s:undelete)"; got != want {
		t.Fatalf("Call = %q, want %q", got, want)
	}
	if got := h.Call("NoSuchMethod", nil); got != "NoSuchMethod" {
		t.Fatalf("an unknown method renders as %q", got)
	}
	if got := h.TypeOf("shelves/s/books/b"); got != "shop.example.com/Book" {
		t.Fatalf("TypeOf = %q", got)
	}
	// The package-level helpers read the global registry, which knows no shop.
	if TypeOf("shelves/s") != "" || Call("GetShelf", nil) != "GetShelf" {
		t.Fatal("the global registry knows the test's protos")
	}
}
