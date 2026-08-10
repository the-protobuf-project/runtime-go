// Package model builds the resource descriptors the examples store.
//
// In a real program these come from protorm: the proto file is the schema, and
// the generator emits both the Go type and the [store.Resource] that
// describes it. There is no generator in this module, so the descriptors are
// built here by hand from a dynamic message.
//
// That is the only reason this package exists. Every example would otherwise
// open with forty lines of descriptor construction, which is the part a reader
// least needs to see — the point of each example is what the database does with
// a descriptor, not how one is made.
package model
