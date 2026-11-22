package grpc

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// rawCodec passes already-encoded protobuf bytes through gRPC untouched, so
// the bridge forwards a binary Connect body without knowing its type.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("connect: rawCodec cannot marshal %T", v)
	}
	return mem.BufferSlice{mem.SliceBuffer(b)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	out, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("connect: rawCodec cannot unmarshal into %T", v)
	}
	*out = data.Materialize()
	return nil
}

// connectBody is one of the two Connect unary encodings.
type connectBody struct {
	contentType string
	// request turns the HTTP body into the protobuf bytes of the method's input.
	request func(method string, body []byte) ([]byte, error)
	// response turns the method's protobuf output into the HTTP body.
	response func(method string, body []byte) ([]byte, error)
}

func passthrough(_ string, b []byte) ([]byte, error) { return b, nil }

var (
	binaryBody = connectBody{contentType: "application/proto", request: passthrough, response: passthrough}
	jsonBody   = connectBody{contentType: "application/json", request: jsonToProto, response: protoToJSON}
)

// connectFormat picks the encoding from a Content-Type, ignoring parameters
// such as charset.
func connectFormat(contentType string) (connectBody, bool) {
	mediaType, _, _ := strings.Cut(contentType, ";")
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "application/proto", "application/protobuf":
		return binaryBody, true
	case "application/json":
		return jsonBody, true
	}
	return connectBody{}, false
}

// messageTypes resolves /pkg.Service/Method to its input and output types.
func messageTypes(method string) (in, out protoreflect.MessageType, err error) {
	name := protoreflect.FullName(strings.ReplaceAll(strings.TrimPrefix(method, "/"), "/", "."))
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	md, ok := desc.(protoreflect.MethodDescriptor)
	if err != nil || !ok {
		return nil, nil, status.Errorf(codes.Unimplemented, "%s is not a known method", name)
	}
	if in, err = protoregistry.GlobalTypes.FindMessageByName(md.Input().FullName()); err != nil {
		return nil, nil, status.Errorf(codes.Unimplemented, "no Go type for %s", md.Input().FullName())
	}
	if out, err = protoregistry.GlobalTypes.FindMessageByName(md.Output().FullName()); err != nil {
		return nil, nil, status.Errorf(codes.Unimplemented, "no Go type for %s", md.Output().FullName())
	}
	return in, out, nil
}

func jsonToProto(method string, body []byte) ([]byte, error) {
	in, _, err := messageTypes(method)
	if err != nil {
		return nil, err
	}
	msg := in.New().Interface()
	if len(body) > 0 {
		if err := protojson.Unmarshal(body, msg); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "decoding the request: %v", err)
		}
	}
	return proto.Marshal(msg)
}

func protoToJSON(method string, body []byte) ([]byte, error) {
	_, out, err := messageTypes(method)
	if err != nil {
		return nil, err
	}
	msg := out.New().Interface()
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, status.Errorf(codes.Internal, "decoding the response: %v", err)
	}
	return protojson.Marshal(msg)
}

// skippedHeaders are HTTP or protocol headers that are not request metadata.
var skippedHeaders = map[string]bool{
	"host": true, "content-type": true, "content-length": true, "content-encoding": true,
	"accept-encoding": true, "connection": true, "keep-alive": true, "te": true,
	"transfer-encoding": true, "upgrade": true, "user-agent": true, "accept": true,
}

// requestMetadata turns request headers into gRPC metadata, as a gRPC client
// would have sent them. Values of -bin keys are base64 on the wire.
func requestMetadata(h http.Header) metadata.MD {
	md := metadata.MD{}
	for key, values := range h {
		k := strings.ToLower(key)
		if skippedHeaders[k] || strings.HasPrefix(k, "connect-") || strings.HasPrefix(k, "grpc-") {
			continue
		}
		for _, v := range values {
			if strings.HasSuffix(k, "-bin") {
				decoded, err := decodeBinaryHeader(v)
				if err != nil {
					continue
				}
				v = string(decoded)
			}
			md.Append(k, v)
		}
	}
	return md
}

// writeMetadata sets gRPC metadata as response headers, each key prefixed.
func writeMetadata(h http.Header, md metadata.MD, prefix string) {
	for key, values := range md {
		for _, v := range values {
			if strings.HasSuffix(key, "-bin") {
				v = base64.RawStdEncoding.EncodeToString([]byte(v))
			}
			h.Add(prefix+key, v)
		}
	}
}

// decodeBinaryHeader accepts padded and unpadded base64, as Connect allows.
func decodeBinaryHeader(v string) ([]byte, error) {
	return base64.RawStdEncoding.DecodeString(strings.TrimRight(v, "="))
}
