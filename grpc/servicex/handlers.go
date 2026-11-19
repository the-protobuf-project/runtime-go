package servicex

// handlers.go is the handler model's share of the runtime: what a server
// built from <Service>Handlers, rather than over store repositories, needs.

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// MissingHandlers is the error New<Service>Server returns when the
// <Service>Handlers it is given leave an RPC without a handler: a server is
// never built that would answer one UNIMPLEMENTED by omission.
type MissingHandlers struct {
	// Service is the gRPC service's full name.
	Service string
	// Methods are the RPCs without a handler, in proto order.
	Methods []string
}

func (e *MissingHandlers) Error() string {
	return e.Service + " has no handler for " + strings.Join(e.Methods, ", ")
}

// Mask is the update mask an Update handler receives: the canonical paths
// the server resolved (proto names, OUTPUT_ONLY ones dropped), or "*" for
// every mutable field.
func Mask(paths []string) *fieldmaskpb.FieldMask {
	if len(paths) == 0 {
		return &fieldmaskpb.FieldMask{Paths: []string{"*"}}
	}
	return &fieldmaskpb.FieldMask{Paths: slices.Clone(paths)}
}

// MergeMutable copies the fields of src that paths selects onto dst, every
// field for an empty mask, leaving name and the outputOnly fields as dst
// has them: the stored record with an update applied, which the hooks see,
// a validate_only update returns, and the handler writes.
func MergeMutable(dst, src proto.Message, paths, outputOnly []string) {
	d, s := dst.ProtoReflect(), src.ProtoReflect()
	fields := s.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())
		if name == "name" || slices.Contains(outputOnly, name) || !Touches(paths, name) {
			continue
		}
		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}
	}
}
