package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// statusName returns the canonical SCREAMING_SNAKE google.rpc.Code name for a
// gRPC code (e.g. "NOT_FOUND"), which is the conventional form for REST error
// bodies. gRPC code values are aligned with google.rpc.Code.
func statusName(c codes.Code) string {
	return code.Code(int32(c)).String()
}

// HTTPStatusFromCode converts a gRPC status code into the canonical HTTP
// response status, re-exported from grpc-gateway for caller convenience.
//
//	OK                 -> 200    Canceled           -> 499
//	InvalidArgument    -> 400    DeadlineExceeded   -> 504
//	NotFound           -> 404    AlreadyExists      -> 409
//	PermissionDenied   -> 403    Unauthenticated    -> 401
//	ResourceExhausted  -> 429    FailedPrecondition -> 400
//	Aborted            -> 409    OutOfRange         -> 400
//	Unimplemented      -> 501    Unavailable        -> 503
//	Internal/Unknown/DataLoss -> 500
func HTTPStatusFromCode(code codes.Code) int {
	return runtime.HTTPStatusFromCode(code)
}

// gatewayError is the JSON envelope returned by the HTTP/REST gateway for any
// failed request. Field names are single words, so they are identical under
// both camelCase and snake_case marshaling.
type gatewayError struct {
	Error gatewayErrorBody `json:"error"`
}

type gatewayErrorBody struct {
	// Code is the HTTP status code (e.g. 404), so REST clients can branch on it
	// without needing to understand gRPC codes.
	Code int `json:"code"`
	// Status is the canonical google.rpc.Code name (e.g. "NOT_FOUND").
	Status string `json:"status"`
	// Message is the human-readable error message from the gRPC status.
	Message string `json:"message"`
	// Details carries any structured google.rpc.Status details, omitted when empty.
	Details []json.RawMessage `json:"details,omitempty"`
}

// httpErrorHandler is a runtime.ErrorHandlerFunc that maps a gRPC error to the
// proper HTTP status code and writes a consistent JSON envelope. It also honors
// runtime.HTTPStatusError so routing errors can override the status, and sets
// WWW-Authenticate for Unauthenticated responses.
func httpErrorHandler(ctx context.Context, mux *runtime.ServeMux, m runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	var customStatus *runtime.HTTPStatusError
	if errors.As(err, &customStatus) {
		err = customStatus.Err
	}

	s := status.Convert(err)
	httpStatus := runtime.HTTPStatusFromCode(s.Code())
	if customStatus != nil {
		httpStatus = customStatus.HTTPStatus
	}

	shared.Pulse.Logger.Debugf("HTTP gateway error: %s %s -> %d (%s): %s",
		r.Method, r.URL.Path, httpStatus, s.Code(), s.Message())

	// Marshal each status detail through the gateway's configured marshaler so
	// field naming (camelCase / snake_case) stays consistent with the codec.
	var details []json.RawMessage
	for _, d := range s.Proto().GetDetails() {
		if buf, derr := m.Marshal(d); derr == nil {
			details = append(details, buf)
		} else {
			shared.Pulse.Logger.Debugf("HTTP gateway error: failed to marshal detail: %v", derr)
		}
	}

	body := gatewayError{Error: gatewayErrorBody{
		Code:    httpStatus,
		Status:  statusName(s.Code()),
		Message: s.Message(),
		Details: details,
	}}

	w.Header().Del("Trailer")
	w.Header().Del("Transfer-Encoding")
	w.Header().Set("Content-Type", "application/json")
	if s.Code() == codes.Unauthenticated {
		w.Header().Set("WWW-Authenticate", s.Message())
	}

	buf, merr := json.Marshal(body)
	if merr != nil {
		shared.Pulse.Logger.Debugf("HTTP gateway error: failed to marshal envelope: %v", merr)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":500,"status":"INTERNAL","message":"failed to marshal error"}}`))
		return
	}

	w.WriteHeader(httpStatus)
	if _, werr := w.Write(buf); werr != nil {
		shared.Pulse.Logger.Debugf("HTTP gateway error: failed to write response: %v", werr)
	}
}
