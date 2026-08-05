// Package codec encodes values for storage and hashes them for deduplication.
//
// It is internal because it is machinery: callers hand their own models to the
// operations above and never see the bytes.
package codec
