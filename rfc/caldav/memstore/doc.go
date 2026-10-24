// Package memstore is an in-memory [caldav.Store]: the reference
// implementation of the store contract, for tests, examples and a CalDAV
// server that needs nothing durable.
//
//	store := memstore.New()
//	h := caldav.New(store)
//
// It implements the whole contract: overrides grouped into their series'
// object, atomic writes guarded by etags, RECURRENCE-ID;RANGE=THISANDFUTURE
// splits (the series cut before the instance, the rest a new object named
// "{name}_R{instant}.ics" under the UID "{uid}_R{instant}"), recurrence
// expansion and free/busy, RFC 7953 availability included.
//
// Deletes are hard: nothing is kept for an undelete.
//
// The expander covers what calendar clients write: FREQ=DAILY, WEEKLY,
// MONTHLY and YEARLY with INTERVAL, COUNT, UNTIL, BYDAY (without ordinals),
// BYMONTHDAY and BYMONTH, plus EXDATE and RDATE. Anything else fails with
// [ErrUnsupportedRule]. A production store will usually answer
// [caldav.Store.Instances] and [caldav.Store.FreeBusy] from its own engine.
package memstore
