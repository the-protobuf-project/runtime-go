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

// FetchLimit is how many rows to read for one page.
//
// One more than asked when no total is being computed. Without a count there is
// no other way to know whether another page exists, and a caller that simply
// stopped on a short page would drop the last page whenever it happened to come
// out exactly full.
func FetchLimit(limit int64, omitTotal bool) int64 {
	if omitTotal {
		return limit + 1
	}
	return limit
}

// TrimPage cuts an over-fetched page back to size and returns the token for the
// next one.
//
// It takes the rows rather than just their count so the extra row read by
// [FetchLimit] is dropped here, in one place, rather than in every driver — a
// page that returned the probe row would be off by one in a way that only shows
// up at a page boundary.
func TrimPage[T any](rows []T, offset, limit, total int64, omitTotal bool) ([]T, string) {
	if !omitTotal {
		return rows, EncodeToken(offset, int64(len(rows)), total)
	}
	if int64(len(rows)) > limit {
		return rows[:limit], strconv.FormatInt(offset+limit, 10)
	}
	return rows, ""
}

// NoTotal is what [ListResult.Total] carries when it was not computed.
const NoTotal int64 = -1
