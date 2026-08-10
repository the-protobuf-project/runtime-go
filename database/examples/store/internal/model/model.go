package model

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Descriptors, built once. protodesc registers the file globally, so building
// it twice in one process is an error rather than a duplicate.
var (
	bookMD    protoreflect.MessageDescriptor
	authorMD  protoreflect.MessageDescriptor
	wroteMD   protoreflect.MessageDescriptor
	readingMD protoreflect.MessageDescriptor
)

func init() {
	fd, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	if err != nil {
		panic("model: cannot build descriptors: " + err.Error())
	}
	bookMD = fd.Messages().ByName("Book")
	authorMD = fd.Messages().ByName("Author")
	wroteMD = fd.Messages().ByName("Wrote")
	readingMD = fd.Messages().ByName("Reading")
}

// Book is a record every example stores.
func Book(id, title string, year int32) proto.Message {
	m := dynamicpb.NewMessage(bookMD)
	set(m, "id", protoreflect.ValueOfString(id))
	set(m, "title", protoreflect.ValueOfString(title))
	set(m, "published_year", protoreflect.ValueOfInt32(year))
	return m
}

// Author is a record the graph example connects books to.
func Author(id, name string) proto.Message {
	m := dynamicpb.NewMessage(authorMD)
	set(m, "id", protoreflect.ValueOfString(id))
	set(m, "name", protoreflect.ValueOfString(name))
	return m
}

// Wrote is an edge — a resource like any other, which is what lets one
// generator describe both a record and a connection.
func Wrote(role string) proto.Message {
	m := dynamicpb.NewMessage(wroteMD)
	set(m, "role", protoreflect.ValueOfString(role))
	return m
}

// Reading is a measurement, for the time-series example.
func Reading(id, sensor string, celsius float64, at time.Time) proto.Message {
	m := dynamicpb.NewMessage(readingMD)
	set(m, "id", protoreflect.ValueOfString(id))
	set(m, "sensor", protoreflect.ValueOfString(sensor))
	set(m, "celsius", protoreflect.ValueOfFloat64(celsius))
	set(m, "observed_at", protoreflect.ValueOfMessage(timestamppb.New(at).ProtoReflect()))
	return m
}

// Field reads one field back out of a message, for printing.
func Field(msg proto.Message, name string) string {
	if msg == nil {
		return "<nil>"
	}
	fd := msg.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return "<no field " + name + ">"
	}
	return fmt.Sprint(msg.ProtoReflect().Get(fd).Interface())
}

func set(m *dynamicpb.Message, name string, v protoreflect.Value) {
	m.Set(m.Descriptor().Fields().ByName(protoreflect.Name(name)), v)
}
