package batchconn

import (
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
)

// maxPending is how many bytes a connection queues for the network before a
// write waits for room.
const maxPending = 1 << 20

// closeLinger is how long Close waits for queued bytes (a DISCONNECT, a last
// PUBACK) to reach the network before closing the connection anyway.
const closeLinger = time.Second

// conn writes to a connection from one goroutine, gathering what
// writers queue while a write is in flight into the next one.
//
// paho writes every packet on its own, as a header and a body: over TLS that
// is two records and two system calls per packet, and every PUBACK a consumer
// sends costs the same. Gathered, a burst of packets leaves in a few writes,
// as rumqttc's do in runtime-rs. A lone packet is written at once, so nothing
// waits for company.
//
// It is a [sync.Locker], which paho takes for the length of one packet, so
// the parts of one packet are queued together and never interleave with
// another's.
type conn struct {
	net.Conn
	packet sync.Mutex

	mu      sync.Mutex
	changed *sync.Cond // something queued, room made, or the end reached
	pending []byte
	err     error
	closing bool
	done    chan struct{}
}

func newConn(nc net.Conn) *conn {
	c := &conn{Conn: nc, done: make(chan struct{})}
	c.changed = sync.NewCond(&c.mu)
	go c.flush()
	return c
}

// Lock and Unlock hold the connection for one packet's writes.
func (c *conn) Lock()   { c.packet.Lock() }
func (c *conn) Unlock() { c.packet.Unlock() }

// Write queues p. It reports a failure of an earlier write, which is how a
// dropped connection reaches the writer.
func (c *conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.err == nil && !c.closing && len(c.pending) >= maxPending {
		c.changed.Wait()
	}
	switch {
	case c.err != nil:
		return 0, c.err
	case c.closing:
		return 0, net.ErrClosed
	}
	c.pending = append(c.pending, p...)
	c.changed.Broadcast()
	return len(p), nil
}

// flush writes whatever is queued, in turn, until the connection closes or a
// write fails.
func (c *conn) flush() {
	defer close(c.done)
	var out []byte
	for {
		c.mu.Lock()
		for len(c.pending) == 0 && !c.closing {
			c.changed.Wait()
		}
		if len(c.pending) == 0 {
			c.mu.Unlock()
			return
		}
		out, c.pending = c.pending, out[:0]
		c.changed.Broadcast()
		c.mu.Unlock()

		if _, err := c.Conn.Write(out); err != nil {
			c.mu.Lock()
			c.err = err
			c.pending = nil
			c.changed.Broadcast()
			c.mu.Unlock()
			// So the reader learns of it as well.
			_ = c.Conn.Close()
			return
		}
	}
}

// Close writes what is queued, waiting up to closeLinger, then closes the
// connection.
func (c *conn) Close() error {
	c.mu.Lock()
	c.closing = true
	c.changed.Broadcast()
	c.mu.Unlock()
	select {
	case <-c.done:
	case <-time.After(closeLinger):
	}
	return c.Conn.Close()
}

// Dialer returns autopaho's AttemptConnection for server, dialing as
// autopaho does (over TLS with tlsCfg when secure) and wrapping the connection
// so its writes are gathered. It returns nil for a websocket address or when
// all_proxy names a proxy, which autopaho then dials itself.
func Dialer(server *url.URL, secure bool, tlsCfg *tls.Config) func(context.Context, autopaho.ClientConfig, *url.URL) (net.Conn, error) {
	if os.Getenv("all_proxy") != "" || strings.HasPrefix(strings.ToLower(server.Scheme), "ws") {
		return nil
	}
	return func(ctx context.Context, _ autopaho.ClientConfig, u *url.URL) (net.Conn, error) {
		var (
			nc  net.Conn
			err error
		)
		if secure {
			d := tls.Dialer{Config: tlsCfg}
			nc, err = d.DialContext(ctx, "tcp", u.Host)
		} else {
			var d net.Dialer
			nc, err = d.DialContext(ctx, "tcp", u.Host)
		}
		if err != nil {
			return nil, err
		}
		return newConn(nc), nil
	}
}
