package grpc

import "google.golang.org/grpc/codes"

// StatusCodeType is an alias for the standard gRPC status code type,
// codes.Code, for consistency within this package.
type StatusCodeType = codes.Code

// StatusCode provides a convenient, namespaced struct to access standard gRPC
// status codes without needing to import the "google.golang.org/grpc/codes"
// package directly.
var StatusCode = struct {
	// OK indicates the operation completed successfully.
	OK StatusCodeType
	// Unknown represents an unknown error. This is used when an error is returned
	// from a status that belongs to a different address space.
	Unknown StatusCodeType
	// Canceled indicates the operation was canceled, typically by the caller.
	Canceled StatusCodeType
	// InvalidArgument indicates the client specified an invalid argument.
	// Note that this differs from FailedPrecondition.
	InvalidArgument StatusCodeType
	// DeadlineExceeded indicates the deadline for an operation expired before it
	// could complete.
	DeadlineExceeded StatusCodeType
	// NotFound indicates a requested entity (e.g., file or directory) was not found.
	NotFound StatusCodeType
	// AlreadyExists indicates an attempt to create an entity failed because it
	// already exists.
	AlreadyExists StatusCodeType
	// PermissionDenied indicates the caller does not have permission to execute
	// the specified operation.
	PermissionDenied StatusCodeType
	// ResourceExhausted indicates some resource has been exhausted, perhaps a
	// per-user quota, or the entire file system is out of space.
	ResourceExhausted StatusCodeType
	// FailedPrecondition indicates the system is not in a state required for the
	// operation's execution (e.g., deleting a non-empty directory).
	FailedPrecondition StatusCodeType
	// Aborted indicates the operation was aborted, often due to a concurrency
	// issue like a sequencer check failure or transaction abort.
	Aborted StatusCodeType
	// OutOfRange indicates an operation was attempted beyond the valid range
	// (e.g., seeking or reading past the end of a file).
	OutOfRange StatusCodeType
	// Unimplemented indicates the operation is not implemented or not supported
	// in this service.
	Unimplemented StatusCodeType
	// Internal indicates an internal server error. This means some invariants
	// expected by the underlying system have been broken.
	Internal StatusCodeType
	// Unavailable indicates the service is currently unavailable. This is most
	// likely a transient condition and may be corrected by retrying.
	Unavailable StatusCodeType
	// DataLoss indicates unrecoverable data loss or corruption.
	DataLoss StatusCodeType
	// Unauthenticated indicates the request does not have valid authentication
	// credentials for the operation.
	Unauthenticated StatusCodeType
}{
	OK:                 codes.OK,
	Unknown:            codes.Unknown,
	Canceled:           codes.Canceled,
	InvalidArgument:    codes.InvalidArgument,
	DeadlineExceeded:   codes.DeadlineExceeded,
	NotFound:           codes.NotFound,
	AlreadyExists:      codes.AlreadyExists,
	PermissionDenied:   codes.PermissionDenied,
	ResourceExhausted:  codes.ResourceExhausted,
	FailedPrecondition: codes.FailedPrecondition,
	Aborted:            codes.Aborted,
	OutOfRange:         codes.OutOfRange,
	Unimplemented:      codes.Unimplemented,
	Internal:           codes.Internal,
	Unavailable:        codes.Unavailable,
	DataLoss:           codes.DataLoss,
	Unauthenticated:    codes.Unauthenticated,
}
