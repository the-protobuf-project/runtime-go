package events

import "time"

// The event a record with no state of its own reports (see Lifecycle).
const (
	Created = "created"
	Updated = "updated"
	Deleted = "deleted"
)

// Lifecycles are the events Lifecycle reports, in order.
var Lifecycles = []string{Created, Updated, Deleted}

// DeleteSlack is how far a delete's update time may trail its delete time and
// still be the delete. A soft delete writes both at one instant; a row deleted
// by code that wrote them apart has them a moment apart.
const DeleteSlack = time.Second

// Lifecycle is what last happened to a record with no state of its own, from
// its create, update and (AIP-164) delete times, a zero delete time meaning
// not deleted: a soft delete, else its creation when it has not changed since,
// else an update. A write to a record already deleted -- a purger stamping its
// expire_time -- is none of these and returns "", which no feed sends: the
// deletion was sent when it happened.
func Lifecycle(create, update, del time.Time) string {
	switch {
	case !del.IsZero() && update.After(del.Add(DeleteSlack)):
		return ""
	case !del.IsZero():
		return Deleted
	case create.Equal(update):
		return Created
	}
	return Updated
}
