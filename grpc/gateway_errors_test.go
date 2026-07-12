package grpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPStatusFromCode(t *testing.T) {
	cases := map[codes.Code]int{
		codes.OK:                 http.StatusOK,
		codes.InvalidArgument:    http.StatusBadRequest,
		codes.NotFound:           http.StatusNotFound,
		codes.AlreadyExists:      http.StatusConflict,
		codes.PermissionDenied:   http.StatusForbidden,
		codes.Unauthenticated:    http.StatusUnauthorized,
		codes.ResourceExhausted:  http.StatusTooManyRequests,
		codes.FailedPrecondition: http.StatusBadRequest,
		codes.Unimplemented:      http.StatusNotImplemented,
		codes.Internal:           http.StatusInternalServerError,
		codes.Unavailable:        http.StatusServiceUnavailable,
		codes.DeadlineExceeded:   http.StatusGatewayTimeout,
	}
	for code, want := range cases {
		if got := HTTPStatusFromCode(code); got != want {
			t.Errorf("HTTPStatusFromCode(%v) = %d, want %d", code, got, want)
		}
	}
}

// decodeEnvelope runs httpErrorHandler against err and returns the recorder and
// decoded envelope.
func decodeEnvelope(t *testing.T, err error) (*httptest.ResponseRecorder, gatewayError) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/things/42", nil)
	httpErrorHandler(context.Background(), nil, &runtime.JSONPb{}, rec, req, err)

	var env gatewayError
	if derr := json.Unmarshal(rec.Body.Bytes(), &env); derr != nil {
		t.Fatalf("response body is not valid JSON: %v\nbody: %s", derr, rec.Body.String())
	}
	return rec, env
}

func TestHTTPErrorHandler_MapsCodeAndEnvelope(t *testing.T) {
	rec, env := decodeEnvelope(t, status.Error(codes.NotFound, "thing 42 not found"))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if env.Error.Code != http.StatusNotFound {
		t.Errorf("envelope code = %d, want %d", env.Error.Code, http.StatusNotFound)
	}
	if env.Error.Status != "NOT_FOUND" {
		t.Errorf("envelope status = %q, want NOT_FOUND", env.Error.Status)
	}
	if env.Error.Message != "thing 42 not found" {
		t.Errorf("envelope message = %q", env.Error.Message)
	}
	if env.Error.Details != nil {
		t.Errorf("expected no details, got %v", env.Error.Details)
	}
}

func TestHTTPErrorHandler_Unauthenticated(t *testing.T) {
	rec, _ := decodeEnvelope(t, status.Error(codes.Unauthenticated, "Bearer realm=example"))

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if wa := rec.Header().Get("WWW-Authenticate"); wa != "Bearer realm=example" {
		t.Errorf("WWW-Authenticate = %q", wa)
	}
}

func TestHTTPErrorHandler_HTTPStatusErrorOverride(t *testing.T) {
	err := &runtime.HTTPStatusError{
		HTTPStatus: http.StatusTeapot,
		Err:        status.Error(codes.Unknown, "i am a teapot"),
	}
	rec, env := decodeEnvelope(t, err)

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
	if env.Error.Code != http.StatusTeapot {
		t.Errorf("envelope code = %d, want %d", env.Error.Code, http.StatusTeapot)
	}
}

func TestHTTPErrorHandler_PlainError(t *testing.T) {
	// A non-status error converts to codes.Unknown -> 500.
	rec, env := decodeEnvelope(t, context.DeadlineExceeded)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if env.Error.Status != "UNKNOWN" {
		t.Errorf("envelope status = %q, want UNKNOWN", env.Error.Status)
	}
}
