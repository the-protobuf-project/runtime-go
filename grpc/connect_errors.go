package grpc

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// connectCodes names each gRPC code as the Connect protocol spells it, with
// the HTTP status a Connect error carries.
var connectCodes = map[codes.Code]struct {
	name string
	http int
}{
	codes.Canceled:           {"canceled", 499},
	codes.Unknown:            {"unknown", http.StatusInternalServerError},
	codes.InvalidArgument:    {"invalid_argument", http.StatusBadRequest},
	codes.DeadlineExceeded:   {"deadline_exceeded", http.StatusGatewayTimeout},
	codes.NotFound:           {"not_found", http.StatusNotFound},
	codes.AlreadyExists:      {"already_exists", http.StatusConflict},
	codes.PermissionDenied:   {"permission_denied", http.StatusForbidden},
	codes.ResourceExhausted:  {"resource_exhausted", http.StatusTooManyRequests},
	codes.FailedPrecondition: {"failed_precondition", http.StatusBadRequest},
	codes.Aborted:            {"aborted", http.StatusConflict},
	codes.OutOfRange:         {"out_of_range", http.StatusBadRequest},
	codes.Unimplemented:      {"unimplemented", http.StatusNotImplemented},
	codes.Internal:           {"internal", http.StatusInternalServerError},
	codes.Unavailable:        {"unavailable", http.StatusServiceUnavailable},
	codes.DataLoss:           {"data_loss", http.StatusInternalServerError},
	codes.Unauthenticated:    {"unauthenticated", http.StatusUnauthorized},
}

// connectError is the Connect protocol's JSON error body.
type connectError struct {
	Code    string          `json:"code"`
	Message string          `json:"message,omitempty"`
	Details []connectDetail `json:"details,omitempty"`
}

// connectDetail is one google.rpc.Status detail: its type name without the
// type.googleapis.com/ prefix, and its protobuf bytes in base64.
type connectDetail struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// writeConnectError writes st as a Connect error. httpStatus 0 takes the
// status Connect assigns to the code. Header and trailer metadata from the
// failed call are sent as headers, as Connect unary does.
func writeConnectError(w http.ResponseWriter, httpStatus int, st *status.Status, header, trailer metadata.MD) {
	c, ok := connectCodes[st.Code()]
	if !ok {
		c = connectCodes[codes.Unknown]
	}
	if httpStatus == 0 {
		httpStatus = c.http
	}
	body := connectError{Code: c.name, Message: st.Message()}
	for _, d := range st.Proto().GetDetails() {
		body.Details = append(body.Details, connectDetail{
			Type:  strings.TrimPrefix(d.GetTypeUrl(), "type.googleapis.com/"),
			Value: base64.RawStdEncoding.EncodeToString(d.GetValue()),
		})
	}
	writeMetadata(w.Header(), header, "")
	writeMetadata(w.Header(), trailer, "Trailer-")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(body)
}
