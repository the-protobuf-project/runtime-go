//go:build live

package main

// ZeroMQ: brokerless, so the bench binds a publisher and connects a subscriber
// to it over loopback TCP. There is no TLS profile: go-zeromq implements
// neither TLS nor CURVE.

import (
	"context"
	"fmt"
	"net"

	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/zeromq"
)

// connectZeroMQ binds a publisher on a free loopback port and connects a
// subscriber to it; only plain exists.
func connectZeroMQ(ctx context.Context, profile string) (*conn, error) {
	if profile != "plain" {
		return nil, unknownProfile("zeromq", profile)
	}
	// The provider does not report the port an ephemeral bind took, so one
	// is chosen here first.
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("tcp://%s", l.Addr())
	_ = l.Close()
	pub, err := zeromq.Publish(ctx, endpoint, zeromq.WithCodec(protobuf.Codec))
	if err != nil {
		return nil, err
	}
	sub, err := zeromq.Subscribe(ctx, endpoint, zeromq.WithCodec(protobuf.Codec))
	if err != nil {
		return nil, err
	}
	return &conn{publisher: pub, subscriber: sub}, nil
}
