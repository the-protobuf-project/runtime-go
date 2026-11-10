package batchconn

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Packets written concurrently, each in parts under the lock as paho writes
// them, arrive whole: every packet's parts together, each writer's packets
// in its order.
func TestConnKeepsPacketsWhole(t *testing.T) {
	client, server := net.Pipe()
	c := newConn(client)

	const writers, packets = 8, 200
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(server)
		got <- b
	}()

	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for n := range packets {
				c.Lock()
				_, _ = fmt.Fprintf(c, "<%d:", w)
				_, _ = fmt.Fprintf(c, "%d>", n)
				c.Unlock()
			}
		})
	}
	wg.Wait()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	next := make([]int, writers)
	for _, packet := range bytes.SplitAfter(<-got, []byte(">")) {
		if len(packet) == 0 {
			continue
		}
		var w, n int
		if _, err := fmt.Sscanf(string(packet), "<%d:%d>", &w, &n); err != nil {
			t.Fatalf("packet %q is not whole: %v", packet, err)
		}
		if n != next[w] {
			t.Fatalf("writer %d: packet %d arrived, want %d", w, n, next[w])
		}
		next[w]++
	}
	for w, n := range next {
		if n != packets {
			t.Fatalf("writer %d: %d packets arrived, want %d", w, n, packets)
		}
	}
}

// Close writes what was still queued before closing.
func TestConnCloseWritesWhatIsQueued(t *testing.T) {
	client, server := net.Pipe()
	c := newConn(client)
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(server)
		got <- b
	}()
	if _, err := c.Write([]byte("disconnect")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = c.Close()
	if b := <-got; string(b) != "disconnect" {
		t.Fatalf("read %q after Close, want the queued write", b)
	}
	if _, err := c.Write([]byte("x")); err == nil {
		t.Fatal("a write after Close succeeded")
	}
}

// A write that fails on the network is reported to the next writer.
func TestConnReportsAFailedWrite(t *testing.T) {
	client, server := net.Pipe()
	_ = server.Close()
	c := newConn(client)
	defer func() { _ = c.Close() }()

	_, _ = c.Write([]byte("lost"))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.Write([]byte("next")); err != nil {
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("Write = %v, want the network's error", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the failed write was never reported")
}
