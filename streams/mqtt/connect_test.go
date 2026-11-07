package mqtt_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

// selfSigned returns a certificate for 127.0.0.1 and a pool that trusts it.
func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test broker"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

func TestTLS(t *testing.T) {
	cert, pool := selfSigned(t)
	addr, _ := testBroker(t, broker{tls: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}})
	trust := streamsmqtt.WithTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})

	// A bare address goes over TLS once a configuration is given.
	_, m := declare(t, connect(t, addr, trust), subject)
	roundTrip(t, m)

	if _, err := streamsmqtt.Connect(t.Context(), "tls://"+addr); err == nil {
		t.Error("Connect trusted a certificate nothing vouches for")
	}
	if _, err := streamsmqtt.Connect(t.Context(), "mqtt://"+addr, trust); err == nil {
		t.Error("Connect accepted a TLS configuration for a plain mqtt:// address")
	}
}

func TestCredentials(t *testing.T) {
	addr, _ := testBroker(t, broker{ledger: &auth.Ledger{Auth: auth.AuthRules{
		{Username: "plant", Password: "s3cret", Allow: true},
	}}})

	_, m := declare(t, connect(t, addr, streamsmqtt.WithCredentials("plant", "s3cret")), subject)
	roundTrip(t, m)

	if _, err := streamsmqtt.Connect(t.Context(), addr, streamsmqtt.WithCredentials("plant", "wrong")); err == nil {
		t.Error("Connect succeeded with the wrong password")
	}
}

func TestUseSharesTheCallersConnection(t *testing.T) {
	addr, _ := testBroker(t, broker{})
	server, _ := url.Parse("mqtt://" + addr)

	cm, err := autopaho.NewConnection(t.Context(), autopaho.ClientConfig{
		ServerUrls: []*url.URL{server}, KeepAlive: 30,
		ClientConfig: paho.ClientConfig{ClientID: "shared"},
	})
	if err != nil {
		t.Fatalf("NewConnection: %v", err)
	}
	t.Cleanup(func() { _ = cm.Disconnect(context.Background()) })

	s, err := streamsmqtt.Use(cm, streamsmqtt.WithServer(addr))
	if err != nil {
		t.Fatalf("Use: %v", err)
	}
	_, m := declare(t, s, subject)
	roundTrip(t, m)

	// Closing what it did not open is a no-op.
	if err := s.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := m.Publish(t.Context(), subject, event{}); err != nil {
		t.Errorf("Publish after Close on a Use'd connection: %v", err)
	}

	// Without a server there is nothing for a subscription to dial.
	bare, _ := streamsmqtt.Use(cm)
	_, bm := declare(t, bare, subject)
	if _, err := bm.Subscribe(t.Context(), subject); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Subscribe without WithServer = %v, want ErrUnsupported", err)
	}
}

// roundTrip publishes one value and waits for it to come back.
func roundTrip(t *testing.T, m streams.Manager) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := m.Publish(ctx, subject, event{User: "ada"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recv(t, ch, 10*time.Second); got.ID != id {
		t.Fatalf("delivered %q, want %q", got.ID, id)
	}
}
