//go:build live

package pubsub_test

// OPC UA PubSub over the secure broker the MQTT e2e suite uses: mutual TLS,
// then a username and password, under ACLs that give each user only the
// topics below its own name. Start the broker, from streams/, with
//
//	docker/tls/generate.sh            # once, if docker/tls/out is empty
//	docker compose -p tpp-streams-e2e-mqtt -f docker/compose.mqtt.secure.yaml up -d --wait
//	go test -race -tags live ./opcua/pubsub -run E2E
//
// Part 14 topics start with a prefix, opcua by default; under these ACLs the
// prefix is e2e-app/<run>/opcua, so the topic tree is the one the user owns.
// MQTT_E2E_PASSWORD_ADDR (default 127.0.0.1:18884), STREAMS_E2E_TLS_DIR
// (default ../../docker/tls/out) and MQTT_E2E_CONTAINER (default
// tpp-streams-mqtt-secure) point it elsewhere. Without the broker the tests
// skip.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/streams/opcua/pubsub"
)

var (
	e2eApp   = [2]string{"e2e-app", "e2e-app-secret"}
	e2eOther = [2]string{"e2e-other", "e2e-other-secret"}
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func e2eAddr(t *testing.T) string {
	t.Helper()
	addr := envOr("MQTT_E2E_PASSWORD_ADDR", "127.0.0.1:18884")
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Skipf("no secure broker at %s (start docker/compose.mqtt.secure.yaml): %v", addr, err)
	}
	_ = conn.Close()
	return addr
}

// e2eTLS is mutual TLS: the test CA, the test client certificate.
func e2eTLS(t *testing.T) *tls.Config {
	t.Helper()
	dir := envOr("STREAMS_E2E_TLS_DIR", filepath.Join("..", "..", "docker", "tls", "out"))
	ca, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Skipf("no test CA (run docker/tls/generate.sh): %v", err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key"))
	if err != nil {
		t.Skipf("no test client certificate (run docker/tls/generate.sh): %v", err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}}
}

// e2ePrefix is a prefix no earlier run used, inside user's topic tree, so
// retained metadata from a previous run is never read as this one's.
func e2ePrefix(user string) string {
	return user + "/pubsub-" + strings.ToLower(core.NewID()) + "/opcua"
}

// e2eConnect is a PubSub provider as user publishing under prefix.
func e2eConnect(t *testing.T, user [2]string, prefix string) *pubsub.Provider {
	t.Helper()
	return connectTo(t, e2eAddr(t), "plant-e2e",
		pubsub.WithPrefix(prefix), pubsub.WithTLS(e2eTLS(t)), pubsub.WithCredentials(user[0], user[1]))
}

func TestE2EPubSubRoundTripsOverMutualTLSAndAPassword(t *testing.T) {
	p := e2eConnect(t, e2eApp, e2ePrefix(e2eApp[0]))
	m := declare(t, p, "line-a", "boiler")
	all := subscribe(t, m, "boiler")

	id := publish(t, m, "boiler/Temperature", 21.5)
	got := next(t, all)
	if got.ID != id || got.Subject != "boiler/Temperature" || value(t, got) != 21.5 {
		t.Fatalf("%+v", got)
	}

	publish(t, m, "boiler", pubsub.NewDataSet().With("Temperature", 22.0).With("Running", true))
	if value(t, next(t, all)) != 22.0 || value(t, next(t, all)) != true {
		t.Fatal("the DataSet did not arrive whole")
	}

	values := make([]any, 20)
	for i := range values {
		values[i] = int32(i)
	}
	ids, err := m.(streams.Batch).PublishBatch(t.Context(), "boiler/Count", values)
	if err != nil {
		t.Fatal(err)
	}
	for n, id := range ids {
		got := next(t, all)
		if got.ID != id || value(t, got) != int32(n) {
			t.Fatalf("%d: %+v", n, got)
		}
	}
	if p.SequenceGaps() != 0 {
		t.Fatal(p.SequenceGaps())
	}
}

func TestE2EMetadataIsRetainedUnderTheACLs(t *testing.T) {
	prefix := e2ePrefix(e2eApp[0])
	p := e2eConnect(t, e2eApp, prefix)
	declare(t, p, "line-a", "boiler")
	version, err := p.PublishMetadata(t.Context(), "line-a", "boiler", pubsub.NewDataSet().With("Temperature", 1.0).Metadata("boiler"))
	if err != nil {
		t.Fatal(err)
	}
	// A second provider, connecting later, reads the retained message.
	reader := e2eConnect(t, e2eApp, prefix)
	if _, err = reader.Create(t.Context(), streams.Stream{ID: "plant-e2e/line-a", Subjects: []string{"boiler"}}); err != nil {
		t.Fatal(err)
	}
	read, err := reader.Metadata(t.Context(), "plant-e2e/line-a", "boiler")
	if err != nil || *read.MetaData.ConfigurationVersion != version || read.MetaData.Fields[0].Name != "Temperature" {
		t.Fatalf("%+v %v", read, err)
	}
}

func TestE2ETheACLsHold(t *testing.T) {
	// Part 14's bare opcua/... tree is nobody's: the broker refuses the
	// publish with reason 0x87, and the error says so.
	bare := e2eConnect(t, e2eApp, "opcua")
	m := declare(t, bare, "line-a", "w/f")
	_, err := m.Publish(t.Context(), "w/f", int32(1))
	if err == nil || !strings.Contains(err.Error(), "NotAuthorized") {
		t.Fatalf("publishing outside the user's tree: %v", err)
	}

	// Another user may subscribe to e2e-app's topics, but is handed nothing.
	prefix := e2ePrefix(e2eApp[0])
	app := e2eConnect(t, e2eApp, prefix)
	writer := declare(t, app, "line-a", "w/f")
	snoop := e2eConnect(t, e2eOther, prefix)
	stolen := subscribe(t, declare(t, snoop, "line-a", "w/f"), "w/f")
	own := subscribe(t, writer, "w/f")
	publish(t, writer, "w/f", int32(7))
	if value(t, next(t, own)) != int32(7) {
		t.Fatal("the owner did not receive its own message")
	}
	select {
	case got := <-stolen:
		t.Fatalf("the ACL let e2e-other read %+v", got)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestE2EAWrongPasswordIsRefused(t *testing.T) {
	_, err := pubsub.Connect(t.Context(), e2eAddr(t),
		pubsub.WithPrefix(e2ePrefix(e2eApp[0])), pubsub.WithTLS(e2eTLS(t)), pubsub.WithCredentials(e2eApp[0], "wrong"))
	if err == nil {
		t.Fatal("the broker accepted a wrong password")
	}
	if errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("a refused login is not a settled unsupported answer: %v", err)
	}
}

// brokerSub is the broker's own subscriber inside its container (on its
// certificate-identity listener, which has no ACLs), as MQTT 5, printing each
// message's user properties and topic: another tool reading what the
// provider wrote.
func brokerSub(ctx context.Context, topic string) *exec.Cmd {
	return exec.CommandContext(ctx, "docker", "exec", envOr("MQTT_E2E_CONTAINER", "tpp-streams-mqtt-secure"),
		"mosquitto_sub", "-h", "localhost", "-p", "8883", //nolint:misspell // the broker binary's name
		"--cafile", "/certs/ca.crt", "--cert", "/certs/client.crt", "--key", "/certs/client.key",
		"-V", "mqttv5", "-F", "%P %t", "-t", topic, "-C", "1", "-W", "15")
}

// Headers ride as MQTT 5 user properties beside the Part 14 JSON, through the
// ACLs, and another MQTT 5 tool on the broker sees them.
func TestE2EHeadersAreUserPropertiesAnotherToolSees(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI to run the broker's subscriber")
	}
	prefix := e2ePrefix(e2eApp[0])
	p := e2eConnect(t, e2eApp, prefix)
	s, err := p.Create(t.Context(), streams.Stream{ID: "line-h", Subjects: []string{"boiler"}}.WithHeader("lang", "go"))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := p.Bind(t.Context(), s.ID)
	all := subscribe(t, m, "boiler")

	opts := []streams.Option{streams.Header("site", "plant-3"), streams.Expires(30 * time.Second)}
	publish(t, m, "boiler/Temperature", 21.5, opts...)
	got := next(t, all)
	if value(t, got) != 21.5 || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-3" {
		t.Fatalf("%+v", got)
	}

	// The retained metadata carries the stream's headers; the tool started
	// afterwards is handed it at once.
	if _, err = p.PublishMetadata(t.Context(), "line-h", "boiler", pubsub.NewDataSet().With("Temperature", 1.0).Metadata("boiler")); err != nil {
		t.Fatal(err)
	}
	topic := prefix + "/json/metadata/plant-e2e/line-h/boiler"
	out, err := brokerSub(t.Context(), topic).CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "lang:go "+topic {
		t.Fatalf("the broker's subscriber: %q %v", out, err)
	}

	// Live data: the tool subscribes first; publish until it has heard one.
	topic = prefix + "/json/data/plant-e2e/line-h/boiler"
	cmd := brokerSub(t.Context(), topic)
	var buf strings.Builder
	cmd.Stdout = &buf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var waitErr error
publishing:
	for i := 0; i < 50; i++ {
		publish(t, m, "boiler/Temperature", 2.0, opts...)
		select {
		case waitErr = <-done:
			break publishing
		case <-time.After(300 * time.Millisecond):
		}
	}
	if waitErr != nil || strings.TrimSpace(buf.String()) != "lang:go site:plant-3 "+topic {
		t.Fatalf("the broker's subscriber: %q %v", buf.String(), waitErr)
	}

	// Retain on data is refused by name.
	if _, err := m.Publish(t.Context(), "boiler/Temperature", 1.0, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) {
		t.Fatal(err)
	}
}
