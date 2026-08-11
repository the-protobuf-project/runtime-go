package mqtt

import "strings"

// matches reports whether topic satisfies an MQTT topic filter.
//
// Levels are separated by `/`. Within a filter, `+` stands for exactly one
// level and `#` for the remaining levels — including none, so `a/#` matches `a`
// as well as `a/b/c`. That last part is where MQTT differs from NATS, whose `>`
// requires at least one token, which is why this is not [core.Matches].
//
// `#` is only a wildcard as the final level; anywhere else it is a literal that
// no publishable topic can contain, so the filter simply matches nothing.
func matches(filter, topic string) bool {
	if filter == topic {
		return true
	}

	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")

	for i, level := range f {
		if level == "#" {
			return i == len(f)-1
		}
		if i >= len(t) {
			return false
		}
		if level != "+" && level != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// hasWildcard reports whether a subject carries a filter character, which makes
// it something to subscribe to rather than publish on.
func hasWildcard(subject string) bool {
	return strings.ContainsAny(subject, "+#")
}
