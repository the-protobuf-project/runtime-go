package evm

// revert.go maps a contract revert reason back to a store sentinel error. The
// generated Solidity uses require strings the adapter already understands —
// "<Struct>: not found" and "<Struct>: already exists" — so a reverted call
// surfaces as gRPC NotFound / AlreadyExists end to end, with no extra config.

import (
	"fmt"
	"strings"

	"github.com/the-protobuf-project/runtime-go/store"
)

// revertToError classifies a call/transaction error. When reason carries a
// recognized contract require string it returns the matching store sentinel;
// otherwise it wraps err unchanged.
func revertToError(err error, reason string) error {
	switch {
	case strings.HasSuffix(reason, ": not found"):
		return store.ErrNotFound
	case strings.HasSuffix(reason, ": already exists"):
		return store.ErrAlreadyExists
	case strings.Contains(reason, "not owner"), strings.Contains(reason, "not record owner"):
		return fmt.Errorf("%w: %s", store.ErrPermissionDenied, reason)
	case reason != "":
		return fmt.Errorf("evm: reverted: %s", reason)
	default:
		return err
	}
}
