// Package events sends a service's changes to webhooks: a change-feed
// dispatcher that reads changes from the records themselves, records one
// delivery per webhook and change, and POSTs each as a signed CloudEvent until
// its endpoint accepts it or attempts run out.
//
// # Why a change feed
//
// Changes are read from the records, not announced by the writes that make
// them. Nothing that writes a record needs to know webhooks exist, and no
// change can be lost between a write and its announcement: a record's
// update_time and etag are the change log, and a soft delete (AIP-164) is a
// change like any other. A [Feed] reads one kind of record ordered by update
// time from a stored [Cursor], reading each record only as far as a read mask
// (AIP-157) until some webhook wants it.
//
// # What the consumer supplies
//
//   - A [Feed] per kind of record, built with [NewFeed] over any list call that
//     filters and orders by update time and honors a read mask. The feed says
//     what each record's event type is (see [Lifecycle] for records with no
//     state of their own) and renders its CloudEvent data.
//   - A [Store] for webhooks, deliveries and feed cursors.
//
// # Guarantees
//
//   - Each round rereads [Options.Lag] before the cursor, for writes that
//     commit after a later one was read; a seen-set keeps a reread from
//     recording a change twice, and a settled cursor stops an idle feed from
//     rereading the same window every round.
//   - A round takes only changes up to its read of the webhooks, less
//     [Options.HookSettle], so a webhook being created as the round starts is
//     never skipped.
//   - A delivery's name is derived from the webhook and the change, so two
//     servers recording the same change collide instead of sending twice; a
//     delivery is claimed (etag-guarded) before it is sent.
//   - Every POST is signed per Standard Webhooks ([Sign]); after a rotation the
//     previous secret signs too for [Options.RotationGrace]. Secrets are sealed
//     at rest with a [Sealer].
//   - The default client connects only to public addresses ([PublicOnly]),
//     follows no redirects and uses no proxy; [CheckEndpoint] refuses unsafe
//     endpoints when a webhook is created.
//
// # Example
//
//	feed := events.NewFeed(events.FeedSpec[*pb.Order]{
//	    ID:       "orders",
//	    ReadMask: []string{"name", "etag", "create_time", "update_time", "state"},
//	    List:     func(ctx context.Context, q events.Query) ([]*pb.Order, string, error) { ... },
//	    Change: func(o *pb.Order) events.Change {
//	        return events.Change{Type: "shop.order." + strings.ToLower(o.GetState().String()), Time: o.GetUpdateTime().AsTime()}
//	    },
//	    Data: func(o *pb.Order) ([]byte, error) { return protojson.Marshal(o) },
//	})
//	d := events.NewDispatcher(store, []events.Feed{feed}, events.Options{Source: "//shop.example.com"})
//	d.Start(ctx, 5*time.Second, func(err error) { log.Print(err) })
package events
