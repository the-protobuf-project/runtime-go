package fabric

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/store"
)

// Driver is a not-yet-implemented Hyperledger Fabric store.Driver.
type Driver struct{}

// New returns a Fabric Driver stub.
func New() *Driver { return &Driver{} }

// compile-time proof the stub satisfies the backend-agnostic contract.
var _ store.Driver = (*Driver)(nil)

func (d *Driver) Create(context.Context, *store.Resource, proto.Message) (store.WriteResult, error) {
	return store.WriteResult{}, store.ErrUnimplemented
}

func (d *Driver) Get(context.Context, *store.Resource, string) (proto.Message, error) {
	return nil, store.ErrUnimplemented
}

func (d *Driver) Update(context.Context, *store.Resource, proto.Message) (store.WriteResult, error) {
	return store.WriteResult{}, store.ErrUnimplemented
}

func (d *Driver) Delete(context.Context, *store.Resource, string) error {
	return store.ErrUnimplemented
}

func (d *Driver) List(context.Context, *store.Resource, store.ListOptions) (store.ListResult, error) {
	return store.ListResult{}, store.ErrUnimplemented
}

func (d *Driver) Count(context.Context, *store.Resource, store.ListOptions) (int64, error) {
	return 0, store.ErrUnimplemented
}

func (d *Driver) Exists(context.Context, *store.Resource, string) (bool, error) {
	return false, store.ErrUnimplemented
}
