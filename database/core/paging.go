package core

import "strconv"

// DefaultPageSize bounds a List that names none.
//
// A default is not a nicety. Without one, a caller that forgets a page size gets
// the whole table, which works in development and takes the process down the
// first time the table is large.
const DefaultPageSize = 50

// PageSize resolves the size for one listing.
func PageSize(requested int32) int64 {
	if requested <= 0 {
		return DefaultPageSize
	}
	return int64(requested)
}

// DecodeToken reads an offset-based page token.
//
// A malformed token reads as offset zero rather than as an error. The token is
// opaque to the caller by contract, so a bad one is a caller's mistake in
// handling something it was told not to interpret — restarting the listing is a
// more useful answer than refusing it.
func DecodeToken(token string) int64 {
	if token == "" {
		return 0
	}
	n, err := strconv.ParseInt(token, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// EncodeToken returns the token for the next page, or "" when this page was the
// last one.
//
// Taking total means the last page carries no token, so a caller's loop ends on
// the page that filled rather than on an extra request that returns nothing.
func EncodeToken(offset, returned, total int64) string {
	next := offset + returned
	if next >= total {
		return ""
	}
	return strconv.FormatInt(next, 10)
}
