package rabbitmq

import "strings"

// matches reports whether a routing key satisfies an AMQP binding key.
//
// Words are separated by `.`. Within a binding key, `*` stands for exactly one
// word and `#` for zero or more — so `user.#` matches `user`, `user.created`
// and `user.eu.created` alike.
//
// That `#` matches zero words is why this is not [core.Matches]: NATS's `>`
// requires at least one token, and reading one broker's wildcards with the
// other's rules is how a publish succeeds against a subject nothing is bound
// to. Unlike MQTT's `#`, AMQP's may appear anywhere in the key.
func matches(binding, key string) bool {
	if binding == key {
		return true
	}

	b := strings.Split(binding, ".")
	k := strings.Split(key, ".")

	// `#` can absorb any number of words, so the tail after it has to be tried
	// at every remaining position — which is what makes this a search rather
	// than a walk.
	var match func(bi, ki int) bool
	match = func(bi, ki int) bool {
		if bi == len(b) {
			return ki == len(k)
		}
		switch b[bi] {
		case "#":
			for skip := ki; skip <= len(k); skip++ {
				if match(bi+1, skip) {
					return true
				}
			}
			return false
		case "*":
			return ki < len(k) && match(bi+1, ki+1)
		default:
			return ki < len(k) && b[bi] == k[ki] && match(bi+1, ki+1)
		}
	}
	return match(0, 0)
}

// hasWildcard reports whether a subject carries a binding character, which
// makes it something to bind to rather than publish on.
func hasWildcard(subject string) bool {
	for _, word := range strings.Split(subject, ".") {
		if word == "*" || word == "#" {
			return true
		}
	}
	return false
}
