//go:build live

package main

// OPC UA client/server. plain is docker/compose.opcua.yaml (SecurityPolicy
// None, anonymous); tls is docker/compose.opcua.secure.yaml, reached as the
// e2e suite reaches it: Basic256Sha256 with SignAndEncrypt, the client
// application certificate the server trusts, the server's own certificate
// pinned, and the e2e-operator user's password.
//
// Run it with --message opcua: the nodes are compose.opcua.nodes.json's
// bench.s00..bench.s09, Int64 tags in opc-plc's namespace, so at most ten
// subjects. A notification is a monitored item's data change, delivered in
// the subscription's publishing cycle, so the publishing interval bounds
// latency from below; OPCUA_PUBLISHING_MS (default 20) sets it. OPCUA_URL
// (plain, default opc.tcp://127.0.0.1:14840), OPCUA_E2E_URL (tls, default
// opc.tcp://127.0.0.1:14841) and STREAMS_E2E_TLS_DIR (default docker/tls/out)
// point it elsewhere.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"github.com/the-protobuf-project/runtime-go/streams/opcua"
)

// opcuaUser is the e2e suite's user, from docker/compose.opcua.secure.yaml.
var opcuaUser = [2]string{"e2e-operator", "e2e-operator-secret"}

// connectOPCUA connects a session for profile (anonymous with no security, or
// Basic256Sha256 SignAndEncrypt with the server pinned and a user password),
// waiting up to 15s for it.
func connectOPCUA(ctx context.Context, profile string) (*conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	publishing, err := strconv.Atoi(env("OPCUA_PUBLISHING_MS", "20"))
	if err != nil {
		return nil, fmt.Errorf("OPCUA_PUBLISHING_MS: %w", err)
	}
	opts := []opcua.Option{opcua.WithPublishingInterval(time.Duration(publishing) * time.Millisecond)}
	var url string
	switch profile {
	case "plain":
		url = env("OPCUA_URL", "opc.tcp://127.0.0.1:14840")
	case "tls":
		url = env("OPCUA_E2E_URL", "opc.tcp://127.0.0.1:14841")
		client, serr := opcuaSecure(ctx, url)
		if serr != nil {
			return nil, serr
		}
		opts = append(opts, opcua.WithClientOptions(client...))
	default:
		return nil, unknownProfile("opcua", profile)
	}
	s, err := opcua.Connect(ctx, url, opts...)
	if err != nil {
		return nil, fmt.Errorf("opcua: cannot connect to %s: %w", url, err)
	}
	return one(s), nil
}

// opcuaSecure is the client configuration of the tls profile: the server's
// Basic256Sha256 SignAndEncrypt endpoint, its certificate pinned to the one
// docker/tls/out/opcua holds, the client application certificate, and the
// user's password.
func opcuaSecure(ctx context.Context, url string) ([]gopcua.Option, error) {
	certs := filepath.Join(certDir("STREAMS_E2E_TLS_DIR"), "opcua")
	pinned, err := os.ReadFile(filepath.Join(certs, "server.der"))
	if err != nil {
		return nil, fmt.Errorf("opcua: cannot read the pinned server certificate (run docker/compose.opcua.secure.sh): %w", err)
	}
	endpoints, err := gopcua.GetEndpoints(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("opcua: cannot list the endpoints of %s: %w", url, err)
	}
	ep, err := gopcua.SelectEndpoint(endpoints, ua.SecurityPolicyURIBasic256Sha256, ua.MessageSecurityModeSignAndEncrypt)
	if err != nil {
		return nil, fmt.Errorf("opcua: %s offers no Basic256Sha256 SignAndEncrypt endpoint: %w", url, err)
	}
	if !bytes.Equal(ep.ServerCertificate, pinned) {
		return nil, fmt.Errorf("opcua: %s presented a certificate other than the pinned %s", url, filepath.Join(certs, "server.der"))
	}
	// The server advertises its container's hostname; the bench dials the
	// forwarded port it was given.
	ep.EndpointURL = url
	return []gopcua.Option{
		gopcua.ApplicationName("tpp-streams bench"),
		gopcua.ApplicationURI("urn:tpp-streams:e2e-client"),
		gopcua.CertificateFile(filepath.Join(certs, "client.der")),
		gopcua.PrivateKeyFile(filepath.Join(certs, "client.pem")),
		gopcua.SecurityFromEndpoint(ep, ua.UserTokenTypeUserName),
		gopcua.AuthUsername(opcuaUser[0], opcuaUser[1]),
		gopcua.RequestTimeout(10 * time.Second),
	}, nil
}
