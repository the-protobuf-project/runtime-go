package batchconn

import (
	"bytes"
	"io"
	"net"
	"testing"
)

// packet frames body as an MQTT packet of type typ.
func packet(typ byte, body []byte) []byte {
	out := []byte{typ << 4}
	n := len(body)
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

// The DISCONNECT's reason is found however the stream is cut into reads, and
// only a DISCONNECT's, not a byte that happens to look like one in a payload.
func TestDisconnectReasonAcrossAnySplit(t *testing.T) {
	big := bytes.Repeat([]byte{packetDisconnect << 4, 0x8e}, 100) // a payload of fake DISCONNECTs
	stream := bytes.Join([][]byte{
		packet(2, []byte{0, 0, 0}),                   // CONNACK
		packet(3, append([]byte{0, 1, 't'}, big...)), // PUBLISH, 200-byte body
		packet(13, nil),                              // PINGRESP, empty
		packet(packetDisconnect, []byte{0x8e}),       // DISCONNECT: session taken over
	}, nil)
	for split := 1; split <= len(stream); split++ {
		var got []byte
		d := disconnects{report: func(r byte) { got = append(got, r) }}
		for at := 0; at < len(stream); at += split {
			d.read(stream[at:min(at+split, len(stream))])
		}
		if !bytes.Equal(got, []byte{0x8e}) {
			t.Fatalf("reads of %d bytes: reasons %x, want 8e", split, got)
		}
	}
}

func TestDisconnectWithoutAReasonIsSuccess(t *testing.T) {
	var got []byte
	d := disconnects{report: func(r byte) { got = append(got, r) }}
	d.read(packet(packetDisconnect, nil))
	if !bytes.Equal(got, []byte{0}) {
		t.Fatalf("reasons %x, want 00", got)
	}
}

func TestMalformedFramingTurnsTheWatchOff(t *testing.T) {
	d := disconnects{report: func(byte) { t.Fatal("reported after malformed framing") }}
	d.read([]byte{0x30, 0xff, 0xff, 0xff, 0xff, 0x7f}) // a remaining length of five bytes
	d.read(packet(packetDisconnect, []byte{0x8e}))
	if !d.off {
		t.Fatal("still watching")
	}
}

// A connection from the dialer reports the reason as the client reads it.
func TestConnReportsTheReasonAsItIsRead(t *testing.T) {
	client, server := net.Pipe()
	reasons := make(chan byte, 1)
	c := newConn(client, func(r byte) { reasons <- r })
	t.Cleanup(func() { _ = c.Close() })
	go func() { _, _ = server.Write(packet(packetDisconnect, []byte{0x8e})) }()
	if _, err := io.ReadFull(c, make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-reasons:
		if r != 0x8e {
			t.Fatalf("reason %x, want 8e", r)
		}
	default:
		t.Fatal("the reason was not reported by the time the read returned")
	}
}
