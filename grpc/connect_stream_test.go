package grpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// streamingService answers grpc_testing.TestService's streaming calls.
type streamingService struct {
	grpc_testing.UnimplementedTestServiceServer
}

func payload(size int32) *grpc_testing.Payload {
	return &grpc_testing.Payload{Body: make([]byte, size)}
}

// StreamingOutputCall sends one reply per response parameter. A negative size
// fails the call after the replies before it, to test a mid-stream error.
func (streamingService) StreamingOutputCall(req *grpc_testing.StreamingOutputCallRequest, stream grpc.ServerStreamingServer[grpc_testing.StreamingOutputCallResponse]) error {
	stream.SetTrailer(metadata.Pairs("x-replies", fmt.Sprint(len(req.GetResponseParameters()))))
	for _, p := range req.GetResponseParameters() {
		if p.GetSize() < 0 {
			return status.Error(codes.FailedPrecondition, "negative size")
		}
		if err := stream.Send(&grpc_testing.StreamingOutputCallResponse{Payload: payload(p.GetSize())}); err != nil {
			return err
		}
	}
	return nil
}

// StreamingInputCall adds up the sizes of every request's payload.
func (streamingService) StreamingInputCall(stream grpc.ClientStreamingServer[grpc_testing.StreamingInputCallRequest, grpc_testing.StreamingInputCallResponse]) error {
	var total int32
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&grpc_testing.StreamingInputCallResponse{AggregatedPayloadSize: total})
		}
		if err != nil {
			return err
		}
		total += int32(len(req.GetPayload().GetBody()))
	}
}

// FullDuplexCall answers each request as it arrives.
func (streamingService) FullDuplexCall(stream grpc.BidiStreamingServer[grpc_testing.StreamingOutputCallRequest, grpc_testing.StreamingOutputCallResponse]) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		size := req.GetResponseParameters()[0].GetSize()
		if err := stream.Send(&grpc_testing.StreamingOutputCallResponse{Payload: payload(size)}); err != nil {
			return err
		}
	}
}

// streamingServer starts a HybridServer with Connect and the streaming
// service, and returns its base URL and an HTTP/2 (h2c) client.
func streamingServer(t *testing.T) (string, *http.Client) {
	t.Helper()
	opts := mcpOpts(t)
	opts.EnableMCP, opts.EnableHTTP = false, true
	startHybrid(t, NewHybridServer(opts, WithConnect(), WithGRPCServers(func(s *grpc.Server) {
		grpc_testing.RegisterTestServiceServer(s, streamingService{})
	})))
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	return fmt.Sprintf("http://127.0.0.1:%d/grpc.testing.TestService/", opts.HTTP.Port),
		&http.Client{Transport: &http.Transport{Protocols: protocols}}
}

func envelope(t *testing.T, m proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := writeEnvelope(&buf, 0, data); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// endOfStream is Connect's end-stream envelope.
type endOfStream struct {
	Error    *connectError       `json:"error"`
	Metadata map[string][]string `json:"metadata"`
}

// readStream reads replies until the end-stream envelope.
func readStream(t *testing.T, body io.Reader) ([][]byte, endOfStream) {
	t.Helper()
	var replies [][]byte
	for {
		flags, data, err := readEnvelope(body)
		if err != nil {
			t.Fatalf("reading envelope: %v", err)
		}
		if flags&flagEndStream != 0 {
			var end endOfStream
			if err := json.Unmarshal(data, &end); err != nil {
				t.Fatalf("end-stream %q: %v", data, err)
			}
			return replies, end
		}
		replies = append(replies, data)
	}
}

// streamPost starts a streaming call and returns the response body, closed
// when the test ends.
func streamPost(t *testing.T, client *http.Client, url, contentType string, body io.Reader) io.Reader {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, url, body)
	req.Header.Set("Content-Type", contentType)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != contentType {
		t.Fatalf("status %d, content type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	return resp.Body
}

func TestConnectStream_ServerStreaming(t *testing.T) {
	base, client := streamingServer(t)
	req := &grpc_testing.StreamingOutputCallRequest{ResponseParameters: []*grpc_testing.ResponseParameters{{Size: 1}, {Size: 2}, {Size: 3}}}
	body := streamPost(t, client, base+"StreamingOutputCall", "application/connect+proto", bytes.NewReader(envelope(t, req)))
	replies, end := readStream(t, body)
	if len(replies) != 3 || end.Error != nil || end.Metadata["x-replies"][0] != "3" {
		t.Fatalf("got %d replies, end %+v", len(replies), end)
	}
	var last grpc_testing.StreamingOutputCallResponse
	if err := proto.Unmarshal(replies[2], &last); err != nil || len(last.GetPayload().GetBody()) != 3 {
		t.Fatalf("last reply %v (%v)", &last, err)
	}
}

func TestConnectStream_ErrorMidStream(t *testing.T) {
	base, client := streamingServer(t)
	req := &grpc_testing.StreamingOutputCallRequest{ResponseParameters: []*grpc_testing.ResponseParameters{{Size: 1}, {Size: -1}}}
	body := streamPost(t, client, base+"StreamingOutputCall", "application/connect+proto", bytes.NewReader(envelope(t, req)))
	replies, end := readStream(t, body)
	if len(replies) != 1 || end.Error == nil || end.Error.Code != "failed_precondition" {
		t.Fatalf("got %d replies, end %+v", len(replies), end)
	}
}

func TestConnectStream_ClientStreamingJSON(t *testing.T) {
	base, client := streamingServer(t)
	var requests bytes.Buffer
	for _, b64 := range []string{"AAA=", "AAAAAA==", "AA=="} { // 2, 4 and 1 bytes
		_ = writeEnvelope(&requests, 0, []byte(`{"payload":{"body":"`+b64+`"}}`))
	}
	body := streamPost(t, client, base+"StreamingInputCall", "application/connect+json", &requests)
	replies, end := readStream(t, body)
	if len(replies) != 1 || end.Error != nil || string(replies[0]) != `{"aggregatedPayloadSize":7}` {
		t.Fatalf("replies %q, end %+v", replies, end)
	}
}

// Bidi is full duplex: each reply is read before the next request is written.
func TestConnectStream_BidiIsFullDuplex(t *testing.T) {
	base, client := streamingServer(t)
	pr, pw := io.Pipe()
	resp := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"FullDuplexCall", pr)
		req.Header.Set("Content-Type", "application/connect+proto")
		r, err := client.Do(req)
		if err != nil {
			t.Errorf("POST: %v", err)
			close(resp)
			return
		}
		resp <- r
	}()
	send := func(size int32) {
		req := &grpc_testing.StreamingOutputCallRequest{ResponseParameters: []*grpc_testing.ResponseParameters{{Size: size}}}
		if _, err := pw.Write(envelope(t, req)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	send(5)
	r, ok := <-resp
	if !ok {
		return
	}
	defer func() { _ = r.Body.Close() }()
	for _, size := range []int32{5, 8} {
		if size == 8 {
			send(8)
		}
		_, data, err := readEnvelope(r.Body)
		var reply grpc_testing.StreamingOutputCallResponse
		if err != nil || proto.Unmarshal(data, &reply) != nil || len(reply.GetPayload().GetBody()) != int(size) {
			t.Fatalf("reply for %d: %v (%v)", size, &reply, err)
		}
	}
	_ = pw.Close()
	if replies, end := readStream(t, r.Body); len(replies) != 0 || end.Error != nil {
		t.Fatalf("after close: %d replies, end %+v", len(replies), end)
	}
}
