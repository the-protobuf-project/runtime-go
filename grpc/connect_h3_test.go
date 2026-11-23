package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/protobuf/proto"
)

// selfSigned writes a certificate and key for 127.0.0.1, and returns their
// paths and a pool that trusts them.
func selfSigned(t *testing.T) (certFile, keyFile string, roots *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots = x509.NewCertPool()
	roots.AddCert(cert)
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	for file, block := range map[string]*pem.Block{
		certFile: {Type: "CERTIFICATE", Bytes: der},
		keyFile:  {Type: "PRIVATE KEY", Bytes: keyDER},
	} {
		if err := os.WriteFile(file, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return certFile, keyFile, roots
}

// Over HTTP/3 bidi is full duplex too, and a call the server ends early does
// not wait for the client.
func TestConnectStream_HTTP3(t *testing.T) {
	certFile, keyFile, roots := selfSigned(t)
	opts := mcpOpts(t)
	opts.EnableMCP, opts.EnableHTTP, opts.ExperimentalHttp3 = false, true, true
	startHybrid(t, NewHybridServer(opts, WithConnect(), WithCertificates(certFile, keyFile),
		WithGRPCServers(func(s *grpc.Server) { grpc_testing.RegisterTestServiceServer(s, streamingService{}) })))

	transport := &http3.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}}
	t.Cleanup(func() { _ = transport.Close() })
	base := fmt.Sprintf("https://127.0.0.1:%d/grpc.testing.TestService/", opts.HTTP.Port+1)

	open := func(t *testing.T, method string, first []byte) (io.ReadCloser, *io.PipeWriter) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)
		pr, pw := io.Pipe()
		t.Cleanup(func() { _ = pw.Close() })
		go func() { _, _ = pw.Write(first) }()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+method, pr)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/connect+proto")
		resp, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("POST %s: %v", method, err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp.Body, pw
	}
	sized := func(size int32) []byte {
		return envelope(t, &grpc_testing.StreamingOutputCallRequest{
			ResponseParameters: []*grpc_testing.ResponseParameters{{Size: size}},
		})
	}

	t.Run("bidi is full duplex", func(t *testing.T) {
		body, pw := open(t, "FullDuplexCall", sized(5))
		for _, size := range []int32{5, 8} {
			if size == 8 {
				if _, err := pw.Write(sized(size)); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			_, data, err := readEnvelope(body)
			if err != nil {
				t.Fatalf("reply for %d: %v", size, err)
			}
			var reply grpc_testing.StreamingOutputCallResponse
			if err := proto.Unmarshal(data, &reply); err != nil || len(reply.GetPayload().GetBody()) != int(size) {
				t.Fatalf("reply for %d: %v (%v)", size, &reply, err)
			}
		}
		_ = pw.Close()
		if replies, end := readStream(t, body); len(replies) != 0 || end.Error != nil {
			t.Fatalf("after close: %d replies, end %+v", len(replies), end)
		}
	})

	t.Run("the server ends first", func(t *testing.T) {
		body, _ := open(t, "NoSuchMethod", []byte{0, 0, 0, 0, 0})
		if _, end := readStream(t, body); end.Error == nil || end.Error.Code != "unimplemented" {
			t.Fatalf("end %+v", end)
		}
	})
}
