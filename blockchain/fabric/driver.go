package fabric

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Driver is a not-yet-implemented Hyperledger Fabric database.Driver.
type Driver struct{}

// New returns a Fabric Driver stub.
func New() *Driver { return &Driver{} }

// compile-time proof the stub satisfies the backend-agnostic contract.
var _ database.Driver = (*Driver)(nil)

func (d *Driver) Create(context.Context, *database.Resource, proto.Message) (database.WriteResult, error) {
	return database.WriteResult{}, database.ErrUnimplemented
}

func (d *Driver) Get(context.Context, *database.Resource, string) (proto.Message, error) {
	return nil, database.ErrUnimplemented
}

func (d *Driver) Update(context.Context, *database.Resource, proto.Message) (database.WriteResult, error) {
	return database.WriteResult{}, database.ErrUnimplemented
}

func (d *Driver) Delete(context.Context, *database.Resource, string) error {
	return database.ErrUnimplemented
}

func (d *Driver) List(context.Context, *database.Resource, database.ListOptions) (database.ListResult, error) {
	return database.ListResult{}, database.ErrUnimplemented
}

func (d *Driver) Count(context.Context, *database.Resource, database.ListOptions) (int64, error) {
	return 0, database.ErrUnimplemented
}

func (d *Driver) Exists(context.Context, *database.Resource, string) (bool, error) {
	return false, database.ErrUnimplemented
}
