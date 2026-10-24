package caldav

import (
	"net/http"
	"strings"
)

// quoteETag renders an entity tag for an ETag header or getetag property.
func quoteETag(etag string) string { return `"` + etag + `"` }

// matchesIf reports whether stored satisfies an If-Match or If-None-Match
// header value: "*" matches anything that exists, otherwise any listed
// entity tag. Weak tags compare by value, which is all a calendar client
// sends.
func matchesIf(header, stored string, exists bool) bool {
	if header == "*" {
		return exists
	}
	if !exists {
		return false
	}
	for _, t := range splitTags(header) {
		if t == stored {
			return true
		}
	}
	return false
}

func splitTags(header string) []string {
	parts := strings.Split(header, ",")
	out := make([]string, 0, len(parts))
	for _, t := range parts {
		t = strings.TrimPrefix(strings.TrimSpace(t), "W/")
		out = append(out, strings.Trim(t, `"`))
	}
	return out
}

// checkPreconditions applies If-Match and If-None-Match to a resource that
// exists with etag stored (or does not exist), RFC 9110 section 13.
func checkPreconditions(r *http.Request, stored string, exists bool) error {
	if v := r.Header.Get("If-Match"); v != "" && !matchesIf(v, stored, exists) {
		return Errorf(http.StatusPreconditionFailed, "", "If-Match %s does not match", v)
	}
	if v := r.Header.Get("If-None-Match"); v != "" && matchesIf(v, stored, exists) {
		return Errorf(http.StatusPreconditionFailed, "", "If-None-Match %s matches", v)
	}
	return nil
}
