package rabbitmq

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// Headers and expiry in AMQP basic properties. These are pure functions, kept
// apart from the broker code so the mapping between [streams.Headers] and an
// AMQP field table can be tested without a broker.

// reservedPrefix is the header-name prefix RabbitMQ keeps for itself:
// x-delivery-count, x-death, x-first-death-*, the plugins' x-delay and
// x-deduplication-header, and AMQP 1.0's x-opt-* annotations.
const reservedPrefix = "x-"

// reservedNames are header names RabbitMQ reads as routing: sender-selected
// distribution sends a message to every routing key listed in CC and BCC, so
// setting one would change where the message goes.
var reservedNames = [...]string{"CC", "BCC"}

// maxShort is AMQP's limit on a short string: a name, a routing key, a
// message id, a field-table key.
const maxShort = 255

// maxExpiry is the longest expiry sent, in milliseconds (2^32 - 1, about 49.7
// days): RabbitMQ's documented range for a TTL. Older brokers close the
// channel on a larger expiration; newer ones take it without complaint, so a
// client cannot rely on either, and a larger expiry is refused here instead.
const maxExpiry = math.MaxUint32

// reserved reports whether key is one RabbitMQ acts on rather than carries.
func reserved(key string) bool {
	if strings.HasPrefix(key, reservedPrefix) {
		return true
	}
	for _, name := range reservedNames {
		if key == name {
			return true
		}
	}
	return false
}

// checkHeaders refuses a header RabbitMQ would act on rather than carry, and a
// key too long for an AMQP field name. The caller has validated them.
func checkHeaders(h streams.Headers) error {
	if err := h.RejectReserved("RabbitMQ", reservedPrefix); err != nil {
		return err
	}
	for _, key := range h.Keys() {
		for _, name := range reservedNames {
			if key == name {
				return fmt.Errorf("%w: header key %q is RabbitMQ's sender-selected distribution, which would route the message to the keys it lists", streams.ErrInvalidHeader, key)
			}
		}
		if len(key) > maxShort {
			return fmt.Errorf("%w: header key %q… is %d bytes, and AMQP limits a field name to %d", streams.ErrInvalidHeader, key[:32], len(key), maxShort)
		}
	}
	return nil
}

// checkStream refuses default headers no publish could send, so a bad one
// fails at the call that set it rather than at every publish after.
func checkStream(in streams.Stream) error {
	if err := in.Headers.Validate(); err != nil {
		return err
	}
	return checkHeaders(in.Headers)
}

// table is the field table carrying h, every value an AMQP long string, or nil
// for no headers, so a header-less publish sends no table at all. The caller
// has run [checkHeaders].
func table(h streams.Headers) amqp.Table {
	if len(h) == 0 {
		return nil
	}
	t := make(amqp.Table, len(h))
	for k, v := range h {
		t[k] = v
	}
	return t
}

// received is the headers a delivery arrived with, or nil for none.
//
// Only string values are headers in this contract, so a value of another AMQP
// type (a number or table some other client set) is left out, as are the
// names RabbitMQ reserves: what it adds on the way, x-delivery-count on a
// quorum queue or x-death after dead-lettering, is the broker's bookkeeping
// rather than something the publisher said.
func received(t amqp.Table) streams.Headers {
	var h streams.Headers
	for k, v := range t {
		s, ok := v.(string)
		if !ok || reserved(k) {
			continue
		}
		if h == nil {
			h = make(streams.Headers, len(t))
		}
		h[k] = s
	}
	return h
}

// expiration is the expiration property for d: whole milliseconds as a
// decimal string, rounded up so a sub-millisecond expiry is not read as "0",
// which RabbitMQ takes to mean "deliver now or drop".
func expiration(d time.Duration) (string, error) {
	ms := (d + time.Millisecond - 1) / time.Millisecond
	if ms > maxExpiry {
		return "", fmt.Errorf("%w: an expiry of %s is beyond RabbitMQ's per-message limit of %d ms", streams.ErrUnsupported, d, uint64(maxExpiry))
	}
	return strconv.FormatInt(int64(ms), 10), nil
}

// short refuses a name over AMQP's 255-byte short-string limit before it
// reaches the wire, where amqp091 would fail the frame half-written.
func short(what, value string) error {
	if len(value) > maxShort {
		return fmt.Errorf("%w: the %s is %d bytes, and AMQP limits names and routing keys to %d", streams.ErrUnsupported, what, len(value), maxShort)
	}
	return nil
}
