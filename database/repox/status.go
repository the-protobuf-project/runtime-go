package repox

// status.go renders the repositories' reference errors as gRPC statuses, the
// one part of a generated server's error handling that has to know them.

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	"github.com/the-protobuf-project/runtime-go/grpc/problem"
)

// ReferenceStatus renders a repository reference integrity error:
// FAILED_PRECONDITION with an ErrorInfo in the store's domain
// (FOREIGN_KEY_VIOLATION, REFERENCE_NOT_FOUND, RESOURCE_REFERENCED). It is a
// problem.Translator handler: a store-backed server's translator Handles it.
func ReferenceStatus(err error) (error, bool) {
	var e *ReferenceError
	if !errors.As(err, &e) {
		return nil, false
	}
	reason := e.Reason()
	msg := problem.Sentence(e.Error())
	if reason == ReasonForeignKey {
		msg = "The write names a resource that does not exist, or removes one that something still refers to"
		if e.Constraint != "" {
			msg += " (constraint " + e.Constraint + ")"
		}
		msg = problem.Sentence(msg + "; create the referenced resource first, or remove what refers to it")
	}
	metadata := map[string]string{}
	for k, v := range map[string]string{"constraint": e.Constraint, "field": e.Field, "resource_type": e.ResourceType, "resource_name": e.Name} {
		if v != "" {
			metadata[k] = v
		}
	}
	st := status.New(codes.FailedPrecondition, msg)
	details := []protoadapt.MessageV1{&errdetails.ErrorInfo{Reason: reason, Domain: ErrorDomain, Metadata: metadata}}
	if e.Name != "" {
		details = append(details, &errdetails.PreconditionFailure{Violations: []*errdetails.PreconditionFailure_Violation{
			{Type: reason, Subject: e.Name, Description: msg},
		}})
	}
	if detailed, derr := st.WithDetails(details...); derr == nil {
		return detailed.Err(), true
	}
	return st.Err(), true
}
