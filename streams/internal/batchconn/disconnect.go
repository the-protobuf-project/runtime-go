package batchconn

// packetDisconnect is the MQTT DISCONNECT packet type, in the first byte's
// high nibble.
const packetDisconnect = 14

// disconnects watches the bytes a connection reads for a DISCONNECT from the
// server, and reports its reason code before the client acts on it.
//
// autopaho hands a server's DISCONNECT to OnServerDisconnect on a goroutine of
// its own and starts reconnecting at once, so a callback cannot stop that
// reconnect reliably: a connection whose session another one took may dial
// again and take it back. Seen here, on the read path, the reason is known
// before the client has even parsed the packet.
//
// It follows packet boundaries across reads of any size, reading only each
// fixed header and the first byte after a DISCONNECT's. Malformed framing turns
// it off; the client reports that itself.
type disconnects struct {
	report func(reason byte)
	header []byte // the fixed header being read: type byte, then remaining length
	skip   int    // bytes left of the current packet's body
	reason bool   // the next body byte is a DISCONNECT's reason code
	off    bool
}

func (d *disconnects) read(p []byte) {
	for len(p) > 0 && !d.off {
		if d.skip > 0 {
			if d.reason {
				d.reason = false
				d.report(p[0])
			}
			n := min(d.skip, len(p))
			d.skip -= n
			p = p[n:]
			continue
		}
		d.header = append(d.header, p[0])
		p = p[1:]
		length, complete, ok := remainingLength(d.header[1:])
		if !ok {
			d.off = true
			return
		}
		if !complete {
			continue
		}
		if d.header[0]>>4 == packetDisconnect {
			if length == 0 {
				d.report(0) // no reason code means success
			} else {
				d.reason = true
			}
		}
		d.header = d.header[:0]
		d.skip = length
	}
}

// remainingLength decodes MQTT's variable byte integer. complete reports
// whether all of it has been read; ok is false once it runs past four bytes.
func remainingLength(b []byte) (length int, complete, ok bool) {
	multiplier := 1
	for i, c := range b {
		if i == 4 {
			return 0, false, false
		}
		length += int(c&0x7f) * multiplier
		if c&0x80 == 0 {
			return length, true, true
		}
		multiplier *= 128
	}
	return 0, false, len(b) < 4
}
