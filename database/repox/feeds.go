package repox

// feeds.go is what the generated change feeds and store adapters share with
// runtime-go's events and idempotency packages: the translation of a feed's
// query into a repository list, and of the repositories' sentinels into the
// stores' own.

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/the-protobuf-project/runtime-go/events"
)

// FeedInput translates a feed's query into a repository list: an AIP-160
// filter on update_time and name, ordered by update_time, read as far as the
// query's mask.
func FeedInput(q events.Query) ListInput {
	var terms []string
	if !q.Since.IsZero() {
		op := ">"
		if q.SinceInclusive {
			op = ">="
		}
		terms = append(terms, fmt.Sprintf("update_time %s %s", op, strconv.Quote(q.Since.UTC().Format(time.RFC3339Nano))))
	}
	if !q.Until.IsZero() {
		terms = append(terms, fmt.Sprintf("update_time <= %s", strconv.Quote(q.Until.UTC().Format(time.RFC3339Nano))))
	}
	if len(q.Names) > 0 {
		names := make([]string, len(q.Names))
		for i, n := range q.Names {
			names[i] = "name = " + strconv.Quote(n)
		}
		terms = append(terms, "("+strings.Join(names, " OR ")+")")
	}
	in := ListInput{
		PageSize:    q.PageSize,
		PageToken:   q.PageToken,
		ReadMask:    q.ReadMask,
		ShowDeleted: q.ShowDeleted,
		Filter:      strings.Join(terms, " AND "),
	}
	if q.OrderByUpdateTime {
		in.OrderBy = "update_time"
	}
	return in
}

// Lifecycle is events.Lifecycle over a record's timestamps.
func Lifecycle(create, update, del *timestamppb.Timestamp) string {
	var d time.Time
	if del != nil {
		d = del.AsTime()
	}
	return events.Lifecycle(create.AsTime(), update.AsTime(), d)
}

// Time is t as a time, zero for none.
func Time(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

// Timestamp is t as a timestamp, nil for zero.
func Timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// StoreErr restates a repository sentinel as the store sentinel a runtime-go
// store reports: notFound, alreadyExists and conflict for repox's, the error
// unchanged otherwise.
func StoreErr(err, notFound, alreadyExists, conflict error) error {
	switch {
	case err == nil:
		return nil
	case notFound != nil && errors.Is(err, ErrNotFound):
		return fmt.Errorf("%w: %w", notFound, err)
	case alreadyExists != nil && errors.Is(err, ErrAlreadyExists):
		return fmt.Errorf("%w: %w", alreadyExists, err)
	case conflict != nil && errors.Is(err, ErrConflict):
		return fmt.Errorf("%w: %w", conflict, err)
	}
	return err
}

// Scope is the ancestor of name that ends with the collection collection:
// Scope("organizationalUnits/o/holds/h", "organizationalUnits") is
// "organizationalUnits/o"; "" when name has no such ancestor.
func Scope(name, collection string) string {
	parts := strings.Split(name, "/")
	for i := 0; i+1 < len(parts); i += 2 {
		if parts[i] == collection {
			return strings.Join(parts[:i+2], "/")
		}
	}
	return ""
}
