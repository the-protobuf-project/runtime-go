package store

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Kind classifies a column's value domain independent of any one backend's type
// system, so a driver decides how to encode it (a SQL column type, an ABI type,
// a GraphQL scalar) without parsing provider-specific type strings. It is the
// runtime counterpart of the chain type system the generator uses.
type Kind int

const (
	// KindUnknown is the zero value; treat as KindString.
	KindUnknown Kind = iota
	KindString
	KindInt  // signed integer (proto int32/int64/sint*)
	KindUint // unsigned integer (proto uint32/uint64/fixed*)
	KindBool
	KindBytes
	KindFloat
	KindTimestamp // google.protobuf.Timestamp; bridged as time.Time
	KindEnum      // bridged as the enum's int32 number
)

// Column describes one field of a resource: how it appears in the proto message
// (Field) and how it maps onto a backend column (Name, Kind, SQLType). The
// bridge reads/writes the proto value through Field; drivers use Name/Kind to
// talk to their backend, and the relational driver additionally uses SQLType.
type Column struct {
	// Name is the backend column identifier (snake_case), e.g. "author_id".
	Name string

	// Field is the proto field name carrying this column's value, e.g. "author_id".
	// May differ from Name when a backend renames; the bridge keys off Field.
	Field protoreflect.Name

	// Kind is the backend-neutral value domain used to encode/decode the value.
	Kind Kind

	// SQLType is the canonical SQL type from protorm's IR (e.g. "VARCHAR(255)",
	// "TIMESTAMPTZ"). Consumed by the relational driver; empty for chain-only
	// resources whose type lives entirely in Kind.
	SQLType string

	// PrimaryKey marks the key column (exactly one per resource is expected).
	PrimaryKey bool

	// NotNull is true when the field is required; a false value lets the bridge
	// store NULL for an unset optional field.
	NotNull bool

	// Unique marks a column under a unique constraint (drives ErrAlreadyExists).
	Unique bool

	// Generated names a value-generation strategy ("ulid" or "uuid") for a
	// synthesized key column the proto message does not carry. When set and Field
	// is empty, a driver fills the value on Create.
	Generated string

	// AutoCreate marks a column the driver sets to the current time on Create
	// (a created_at/create_time audit column).
	AutoCreate bool

	// AutoUpdate marks a column the driver sets to the current time on Create and
	// Update (an updated_at/update_time audit column).
	AutoUpdate bool
}

// Managed reports whether a driver, not the proto message, supplies this
// column's value (a generated key or an audit timestamp). Such columns are
// skipped by the bridge and filled by the driver.
func (c Column) Managed() bool {
	return c.Generated != "" || c.AutoCreate || c.AutoUpdate
}

// ForeignKey records a column that references another resource's key, mirroring
// schema.ForeignKey. Drivers that enforce or index relations consume it; the
// relational driver and the subgraph generator use it, the plain CRUD path
// ignores it.
type ForeignKey struct {
	Column          string // referencing column in this resource
	ReferencedName  string // Name of the referenced Resource
	ReferencedField string // referenced column, usually its primary key
}

// Resource is the runtime descriptor for one proto resource message — the
// runtime mirror of protorm's schema.Table. New constructs an empty message of
// the resource's concrete type so Get and List can populate and return it
// without the driver importing the generated package.
type Resource struct {
	// Name is the resource identifier (the proto message simple name, e.g. "Book"),
	// used as the registry key and in adapter dispatch.
	Name string

	// Schema is the logical namespace the resource lives in (e.g. "bookstore_v1").
	Schema string

	// Table is the backend table/contract name (e.g. "books").
	Table string

	// PKColumn is the Name of the primary-key Column.
	PKColumn string

	// New returns a freshly allocated, empty message of the resource's type.
	// Generated descriptors wire this to func() proto.Message { return &pb.Book{} }.
	New func() proto.Message

	// SchemaVersion is the 0x-prefixed layout fingerprint the resource was
	// generated with. A chain driver compares it to the deployed contract's
	// SCHEMA_VERSION and refuses a mismatch. Empty disables the check.
	SchemaVersion string

	Columns []Column
	FKs     []ForeignKey
}

// PK returns the primary-key column and whether one is defined.
func (r *Resource) PK() (Column, bool) {
	for _, c := range r.Columns {
		if c.Name == r.PKColumn {
			return c, true
		}
	}
	return Column{}, false
}

// LookupColumn returns the column with the given backend name.
func (r *Resource) LookupColumn(name string) (Column, bool) {
	for _, c := range r.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}
