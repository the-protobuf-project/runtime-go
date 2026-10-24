package events

import (
	"context"
	"time"
)

// Change is one version of one record, as a feed read it.
type Change struct {
	// Name and Etag identify the version; NewFeed fills them from the record.
	Name string
	Etag string
	// Type is the CloudEvent type, e.g. "shop.order.paid". Empty is a write
	// nobody is sent (see [Lifecycle]); it still moves the cursor past it.
	Type string
	// Time is the record's update time.
	Time time.Time
	// Scope is what the record belongs to, matched against [Webhook.Scope];
	// empty for a record of no scope, which goes only to webhooks of every
	// scope.
	Scope string
}

// Query is one list call a feed makes. Translate it to the backend's list
// call: an AIP-160 filter on update_time and name, an AIP-132 order_by, an
// AIP-157 read mask and AIP-164 show_deleted.
type Query struct {
	PageSize  int32
	PageToken string
	// ReadMask lists the fields to read; nil reads whole records.
	ReadMask []string
	// Since bounds update_time below: after Since, or at or after it when
	// SinceInclusive. Zero is unbounded.
	Since          time.Time
	SinceInclusive bool
	// Until bounds update_time above, inclusive. Zero is unbounded.
	Until time.Time
	// OrderByUpdateTime orders the page by update_time, oldest first.
	OrderByUpdateTime bool
	// Names limits the page to these records. A query with names fetches
	// records whole, for the changes that are sent.
	Names []string
	// ShowDeleted includes soft-deleted records. A fetch always sets it; a
	// feed over soft-deleted records sets it in its List.
	ShowDeleted bool
}

// Record is what every feed's records have: a name and an etag that changes
// with every write. Generated proto messages have both getters.
type Record interface {
	GetName() string
	GetEtag() string
}

// FeedSpec describes a feed over records of type R.
type FeedSpec[R Record] struct {
	// ID names the feed's cursor. Keep it stable.
	ID string
	// ReadMask is what a change read reads of every record: enough to tell
	// what happened, to whom, and whether it was sent already -- name, etag,
	// create and update times, and whatever Change looks at.
	ReadMask []string
	// List lists a page of records.
	List func(ctx context.Context, q Query) ([]R, string, error)
	// Change reads a record, read as far as ReadMask, into a change. Name
	// and Etag are filled in after.
	Change func(r R) Change
	// Data renders a whole record as the CloudEvent's JSON data.
	Data func(r R) ([]byte, error)
}

// Feed is one kind of record whose changes are sent. Build one with NewFeed.
type Feed struct {
	id    string
	read  func(ctx context.Context, q Query) ([]Change, string, error)
	fetch func(ctx context.Context, names []string) (map[string]fetched, error)
}

// fetched is a whole record, ready to render.
type fetched struct {
	etag string
	data func() ([]byte, error)
}

// ID is the feed's id.
func (f Feed) ID() string { return f.id }

// NewFeed is a feed over spec.List: pages are read as far as spec.ReadMask,
// each record read into a change by spec.Change, and whole records fetched by
// name for the changes that are sent.
func NewFeed[R Record](spec FeedSpec[R]) Feed {
	return Feed{
		id: spec.ID,
		read: func(ctx context.Context, q Query) ([]Change, string, error) {
			q.ReadMask = spec.ReadMask
			page, next, err := spec.List(ctx, q)
			out := make([]Change, len(page))
			for i, r := range page {
				c := spec.Change(r)
				c.Name, c.Etag = r.GetName(), r.GetEtag()
				out[i] = c
			}
			return out, next, err
		},
		fetch: func(ctx context.Context, names []string) (map[string]fetched, error) {
			page, _, err := spec.List(ctx, Query{PageSize: int32(len(names)), ShowDeleted: true, Names: names})
			out := make(map[string]fetched, len(page))
			for _, r := range page {
				out[r.GetName()] = fetched{etag: r.GetEtag(), data: func() ([]byte, error) { return spec.Data(r) }}
			}
			return out, err
		},
	}
}
