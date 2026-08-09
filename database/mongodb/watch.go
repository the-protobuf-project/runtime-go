package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/the-protobuf-project/runtime-go/database"
)

var _ database.Watcher = (*Driver)(nil)

// Watch delivers a change per event until ctx is done.
//
// This is a change stream, which is the reason to reach for MongoDB over a
// key-value store for anything that has to react to writes: the alternative
// without it is polling, and polling a collection turns one slow query into a
// permanent load floor.
//
// # It needs a replica set
//
// Change streams read the oplog, which a standalone server does not keep. The
// error says so rather than the channel simply never delivering.
//
// # What a resume token is worth
//
// [database.WatchOptions.Resume] restarts just after a change already seen, so a
// consumer that restarts does not miss what happened while it was gone — as long
// as that point is still in the oplog. Past that the server refuses the token
// and this reports it, rather than silently restarting from now and leaving a
// gap nobody knows the size of.
func (d *Driver) Watch(ctx context.Context, res *database.Resource, opts database.WatchOptions) (<-chan database.Change, error) {
	if res == nil {
		return nil, fmt.Errorf("mongodb: Watch needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return nil, err
	}

	// updateLookup so an update carries the document as it now is, rather than
	// only the fields that changed — the contract's Change.Message is the record
	// after the change, and a partial document could not satisfy it.
	stream := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if opts.Resume != "" {
		token, terr := decodeResume(opts.Resume)
		if terr != nil {
			return nil, terr
		}
		stream = stream.SetResumeAfter(token)
	}

	cs, err := coll.Watch(ctx, bson.A{}, stream)
	if err != nil {
		return nil, fmt.Errorf("mongodb: cannot watch %s (change streams need a replica set): %w", res.Table, err)
	}

	out := make(chan database.Change)
	go func() {
		defer close(out)
		defer func() { _ = cs.Close(context.WithoutCancel(ctx)) }()

		for cs.Next(ctx) {
			change, ok := decodeChange(res, cs.Current)
			if !ok {
				continue // an event this contract has no vocabulary for
			}
			select {
			case out <- change:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// changeEvent is the shape of one change stream document, narrowed to what the
// contract carries.
type changeEvent struct {
	OperationType string `bson:"operationType"`
	DocumentKey   bson.M `bson:"documentKey"`
	FullDocument  bson.M `bson:"fullDocument"`
	ID            bson.M `bson:"_id"`
}

// decodeChange turns one change stream event into the contract's shape,
// reporting whether it was one this contract can express.
func decodeChange(res *database.Resource, raw bson.Raw) (database.Change, bool) {
	var ev changeEvent
	if err := bson.Unmarshal(raw, &ev); err != nil {
		return database.Change{}, false
	}

	var kind database.ChangeKind
	switch ev.OperationType {
	case "insert":
		kind = database.ChangeCreated
	case "update", "replace":
		kind = database.ChangeUpdated
	case "delete":
		kind = database.ChangeDeleted
	default:
		// drop, rename, invalidate: real events, but not changes to a record,
		// and inventing a Kind for them would make every consumer handle a case
		// it has no answer for.
		return database.Change{}, false
	}

	change := database.Change{Kind: kind, Resume: encodeResume(ev.ID)}
	if id, ok := ev.DocumentKey["_id"]; ok {
		change.Key = fmt.Sprint(id)
	}
	if kind != database.ChangeDeleted && len(ev.FullDocument) > 0 {
		if msg, err := fromDocument(res, ev.FullDocument); err == nil {
			change.Message = msg
		}
	}
	return change, true
}

// encodeResume turns a resume token into something a caller can store and hand
// back. Extended JSON rather than the raw BSON, so it survives a round trip
// through a config file or a database column.
func encodeResume(token bson.M) string {
	if len(token) == 0 {
		return ""
	}
	b, err := bson.MarshalExtJSON(token, true, false)
	if err != nil {
		return ""
	}
	return string(b)
}

// decodeResume reverses [encodeResume].
func decodeResume(s string) (bson.M, error) {
	var token bson.M
	if err := bson.UnmarshalExtJSON([]byte(s), true, &token); err != nil {
		return nil, fmt.Errorf("mongodb: resume token is not one this driver produced: %w", err)
	}
	return token, nil
}
