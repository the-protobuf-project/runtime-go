package grpc

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Envelope flags of the Connect streaming protocol.
const (
	flagCompressed = 0x01
	flagEndStream  = 0x02
)

// streamFormat picks a streaming encoding: application/connect+proto or
// application/connect+json, each message in its own envelope.
func streamFormat(contentType string) (connectBody, bool) {
	mediaType, _, _ := strings.Cut(contentType, ";")
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "application/connect+proto":
		f := binaryBody
		f.contentType = "application/connect+proto"
		return f, true
	case "application/connect+json":
		f := jsonBody
		f.contentType = "application/connect+json"
		return f, true
	}
	return connectBody{}, false
}

// serveStream serves a server-streaming, client-streaming or bidi call. The
// bridge does not need to know which: it opens one stream to the gRPC server,
// sends every request envelope as a message while relaying every reply as an
// envelope, and ends with Connect's end-stream envelope carrying the status
// and trailers. Request and response flow at the same time, so bidi works
// over HTTP/2 (and HTTP/1.1 where the client allows full duplex).
func (b *connectBridge) serveStream(w http.ResponseWriter, r *http.Request, format connectBody) {
	method := r.URL.Path
	ctx, cancel, err := connectContext(r)
	if err != nil {
		startStream(w, format, nil)
		endStream(w, status.New(codes.InvalidArgument, err.Error()), nil)
		return
	}
	defer cancel()
	if enc := r.Header.Get("Connect-Content-Encoding"); enc != "" && enc != "identity" {
		startStream(w, format, nil)
		endStream(w, status.Newf(codes.Unimplemented, "content encoding %q is not supported", enc), nil)
		return
	}
	_ = http.NewResponseController(w).EnableFullDuplex()

	desc := &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}
	stream, err := b.conn.NewStream(ctx, desc, method, grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		startStream(w, format, nil)
		endStream(w, status.Convert(err), nil)
		return
	}

	// The request side runs alongside the replies. A bad request envelope
	// cancels the call and is what the caller is told, so it is recorded
	// before the cancel it causes.
	sendErr := make(chan error, 1)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		if err := pumpRequests(r.Body, stream, format, method); err != nil {
			sendErr <- err
			cancel()
		}
	}()

	header, _ := stream.Header()
	startStream(w, format, header)
	final := relayReplies(w, stream, cancel, format, method)
	// The call is over, whether or not the caller has finished sending: a
	// server can end a stream early, and a deadline can too. So the end-stream
	// envelope does not wait for the request side.
	select {
	case err := <-sendErr:
		final = status.Convert(err)
	default:
	}
	endStream(w, final, stream.Trailer())
	cancel()
	releaseRequest(w, r.Body, sent)
}

// releaseRequest unblocks the request pump and waits for it: the body must
// not be read once the handler returns.
func releaseRequest(w http.ResponseWriter, body io.Closer, sent <-chan struct{}) {
	select {
	case <-sent:
		return
	default:
	}
	// On HTTP/1.1 Close waits for the read in progress, so the read is expired
	// first; Close is for a writer without deadlines.
	if err := http.NewResponseController(w).SetReadDeadline(time.Now()); err != nil {
		_ = body.Close()
	}
	<-sent
}

// pumpRequests sends each request envelope to the gRPC stream, then closes
// the sending side.
func pumpRequests(body io.Reader, stream grpc.ClientStream, format connectBody, method string) error {
	for {
		flags, data, err := readEnvelope(body)
		if errors.Is(err, io.EOF) {
			return stream.CloseSend()
		}
		if err != nil {
			return err
		}
		if flags&flagCompressed != 0 {
			return status.Error(codes.Unimplemented, "compressed messages are not supported")
		}
		msg, err := format.request(method, data)
		if err != nil {
			return err
		}
		if err := stream.SendMsg(msg); err != nil {
			// The server ended the call; its status arrives on the reply side.
			return nil //nolint:nilerr // the reply side reports the call's status
		}
	}
}

// relayReplies writes each reply as an envelope until the stream ends, and
// returns its status. When it has to give up first it cancels the call and
// waits for it to end, which is when its trailers may be read.
func relayReplies(w http.ResponseWriter, stream grpc.ClientStream, cancel context.CancelFunc, format connectBody, method string) *status.Status {
	flusher := http.NewResponseController(w)
	for {
		var msg []byte
		err := stream.RecvMsg(&msg)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return status.Convert(err)
		}
		out, err := format.response(method, msg)
		if err != nil {
			abandon(stream, cancel)
			return status.Convert(err)
		}
		if err := writeEnvelope(w, 0, out); err != nil {
			abandon(stream, cancel)
			return status.New(codes.Canceled, "the client went away")
		}
		_ = flusher.Flush()
	}
}

// abandon cancels the call and drains it.
func abandon(stream grpc.ClientStream, cancel context.CancelFunc) {
	cancel()
	for {
		var msg []byte
		if stream.RecvMsg(&msg) != nil {
			return
		}
	}
}

// readEnvelope reads one envelope: a flags byte, a big-endian length and
// that many bytes. A clean end of the body before a new envelope is io.EOF.
func readEnvelope(r io.Reader) (byte, []byte, error) {
	var prefix [5]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		return 0, nil, status.Errorf(codes.InvalidArgument, "truncated envelope: %v", err)
	}
	size := binary.BigEndian.Uint32(prefix[1:])
	if size > connectMaxBody {
		return 0, nil, status.Errorf(codes.ResourceExhausted, "message of %d bytes exceeds %d", size, connectMaxBody)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, status.Errorf(codes.InvalidArgument, "truncated envelope: %v", err)
	}
	return prefix[0], data, nil
}

func writeEnvelope(w io.Writer, flags byte, data []byte) error {
	var prefix [5]byte
	prefix[0] = flags
	binary.BigEndian.PutUint32(prefix[1:], uint32(len(data))) // bounded by connectMaxBody
	if _, err := w.Write(prefix[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// startStream writes a streaming response's status line and headers. A
// Connect stream is always 200; its outcome is in the end-stream envelope.
func startStream(w http.ResponseWriter, format connectBody, header metadata.MD) {
	writeMetadata(w.Header(), header, "")
	w.Header().Set("Content-Type", format.contentType)
	w.WriteHeader(http.StatusOK)
	_ = http.NewResponseController(w).Flush()
}

// endStream writes the end-stream envelope: the error, when the call failed,
// and the trailers as metadata.
func endStream(w http.ResponseWriter, st *status.Status, trailer metadata.MD) {
	end := struct {
		Error    *connectError       `json:"error,omitempty"`
		Metadata map[string][]string `json:"metadata,omitempty"`
	}{}
	if st != nil && st.Code() != codes.OK {
		body := connectErrorBody(st)
		end.Error = &body
	}
	if len(trailer) > 0 {
		// gRPC metadata keys are lowercase; http.Header would capitalize them.
		end.Metadata = map[string][]string{}
		for key, values := range trailer {
			for _, v := range values {
				if strings.HasSuffix(key, "-bin") {
					v = base64.RawStdEncoding.EncodeToString([]byte(v))
				}
				end.Metadata[key] = append(end.Metadata[key], v)
			}
		}
	}
	data, err := json.Marshal(end)
	if err != nil {
		data = []byte(fmt.Sprintf(`{"error":{"code":"internal","message":%q}}`, err.Error()))
	}
	_ = writeEnvelope(w, flagEndStream, data)
	_ = http.NewResponseController(w).Flush()
}
