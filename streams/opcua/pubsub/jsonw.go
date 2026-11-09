package pubsub

import (
	"strconv"
	"unicode/utf8"
)

// jsonWriter writes one JSON document by hand, so the bytes are exactly the
// ones runtime-rs writes for the same message: the same member order, the
// same number forms, the same escapes. encoding/json would reorder nothing
// here, but it escapes <, > and & and writes floats in its own way, and a
// message that differs by a byte from the Rust provider's is harder to
// compare on the wire than it needs to be.
type jsonWriter struct {
	buf []byte
	// first is, per open object or array, whether nothing has been written
	// into it yet.
	first []bool
	// named is set between a member name and its value.
	named bool
}

// value prepares for a value: a comma after an earlier array element, nothing
// after a member name.
func (w *jsonWriter) value() {
	if w.named {
		w.named = false
		return
	}
	if n := len(w.first); n > 0 {
		if !w.first[n-1] {
			w.buf = append(w.buf, ',')
		}
		w.first[n-1] = false
	}
}

func (w *jsonWriter) beginObject() {
	w.value()
	w.buf = append(w.buf, '{')
	w.first = append(w.first, true)
}

func (w *jsonWriter) endObject() {
	w.first = w.first[:len(w.first)-1]
	w.buf = append(w.buf, '}')
}

func (w *jsonWriter) beginArray() {
	w.value()
	w.buf = append(w.buf, '[')
	w.first = append(w.first, true)
}

func (w *jsonWriter) endArray() {
	w.first = w.first[:len(w.first)-1]
	w.buf = append(w.buf, ']')
}

// name writes a member name; the next call writes its value.
func (w *jsonWriter) name(n string) {
	w.value()
	w.quote(n)
	w.buf = append(w.buf, ':')
	w.named = true
}

func (w *jsonWriter) str(s string) {
	w.value()
	w.quote(s)
}

func (w *jsonWriter) null() {
	w.value()
	w.buf = append(w.buf, "null"...)
}

func (w *jsonWriter) boolean(b bool) {
	w.value()
	w.buf = strconv.AppendBool(w.buf, b)
}

func (w *jsonWriter) int(n int64) {
	w.value()
	w.buf = strconv.AppendInt(w.buf, n, 10)
}

func (w *jsonWriter) uint(n uint64) {
	w.value()
	w.buf = strconv.AppendUint(w.buf, n, 10)
}

// raw writes a number already in JSON form.
func (w *jsonWriter) raw(number string) {
	w.value()
	w.buf = append(w.buf, number...)
}

// quote writes s as a JSON string with the escapes struson, the writer under
// async-opcua, uses: the two that must be, the short forms of five control
// characters, and \u00XX in upper-case hex for the rest of them. Everything
// else, '/' and non-ASCII included, is written as it is.
func (w *jsonWriter) quote(s string) {
	const hex = "0123456789ABCDEF"
	w.buf = append(w.buf, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				// A Rust string cannot hold this; the replacement character is
				// what it would have been read as.
				w.buf = append(w.buf, "�"...)
			} else {
				w.buf = append(w.buf, s[i:i+size]...)
			}
			i += size
			continue
		}
		switch c {
		case '"':
			w.buf = append(w.buf, `\"`...)
		case '\\':
			w.buf = append(w.buf, `\\`...)
		case '\b':
			w.buf = append(w.buf, `\b`...)
		case '\f':
			w.buf = append(w.buf, `\f`...)
		case '\n':
			w.buf = append(w.buf, `\n`...)
		case '\r':
			w.buf = append(w.buf, `\r`...)
		case '\t':
			w.buf = append(w.buf, `\t`...)
		default:
			if c < 0x20 {
				w.buf = append(w.buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			} else {
				w.buf = append(w.buf, c)
			}
		}
		i++
	}
	w.buf = append(w.buf, '"')
}
