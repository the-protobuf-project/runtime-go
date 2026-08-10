package core

import (
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// RowsToMessages decodes a slice of column maps into records.
//
// It is the loop every driver that reads rows would otherwise write, and the
// place a mistake in it would be a per-backend mistake rather than one.
func RowsToMessages(res *store.Resource, rows []map[string]any) ([]proto.Message, error) {
	out := make([]proto.Message, 0, len(rows))
	for _, row := range rows {
		msg, err := store.ColumnsToMessage(res, row)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}
