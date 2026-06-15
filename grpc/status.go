package grpc

import (
	"fmt"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Status is an alias for the gRPC status object, re-exported for convenience.
type Status = status.Status

// Convert is a wrapper for status.Convert that converts an error to a
// gRPC Status object. If the error is already a Status, it is returned as is.
func Convert(err error) *Status {
	return status.Convert(err)
}

// FromError is a wrapper for status.FromError that extracts a Status object
// from a standard error. The boolean return value indicates whether the
// conversion was successful.
func FromError(err error) (*Status, bool) {
	return status.FromError(err)
}

// Code safely returns the status code from a Status object. If the provided
// status is nil, it returns codes.OK.
func Code(s *Status) codes.Code {
	if s == nil {
		return codes.OK
	}
	return status.Code(s.Err())
}

// Message safely returns the message from a Status object. If the provided
// status is nil, it returns an empty string.
func Message(s *Status) string {
	if s == nil {
		return ""
	}
	return s.Message()
}

// New creates a new Status object with the given code and message.
func New(c codes.Code, msg string) *Status {
	return status.New(c, msg)
}

// Newf creates a new Status object with a formatted message, analogous to fmt.Sprintf.
func Newf(c codes.Code, format string, a ...any) *Status {
	return status.New(c, fmt.Sprintf(format, a...))
}

// Error creates a new error object with the given gRPC status code and message.
// If the code is codes.OK, it returns nil.
func Error(c codes.Code, msg string) error {
	return New(c, msg).Err()
}

// Errorf creates a new error object with a formatted gRPC status message.
// If the code is codes.OK, it returns nil.
func Errorf(c codes.Code, format string, a ...any) error {
	return Error(c, fmt.Sprintf(format, a...))
}

// ErrorProto converts a protobuf Status message into a Go error. If the status
// code is OK, it returns nil.
func ErrorProto(s *spb.Status) error {
	return FromProto(s).Err()
}

// FromProto converts a protobuf Status message into a Go Status object.
func FromProto(s *spb.Status) *Status {
	return status.FromProto(s)
}
