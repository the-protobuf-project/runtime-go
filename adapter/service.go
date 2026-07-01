package adapter

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/store"
)

// Service routes AIP standard-method calls to a store.Driver, resolving each
// request's resource by name and mapping driver errors to gRPC status codes. It
// is backend-agnostic: the same Service drives a relational, EVM, or Fabric
// driver. Generated shims hold a *Service and call its helpers.
type Service struct {
	driver   store.Driver
	registry *store.Registry
}

// New returns a Service backed by driver, resolving resources through registry.
func New(driver store.Driver, registry *store.Registry) *Service {
	return &Service{driver: driver, registry: registry}
}

// Driver returns the underlying driver (handy for advanced shims, e.g. the LRO
// path that needs backend-specific operation handles).
func (s *Service) Driver() store.Driver { return s.driver }

// Get returns the record of the named resource with the given key.
func (s *Service) Get(ctx context.Context, resource, key string) (proto.Message, error) {
	res, err := s.resource(resource)
	if err != nil {
		return nil, err
	}
	msg, err := s.driver.Get(ctx, res, key)
	return msg, toStatus(err)
}

// Create stores msg as a new record of the named resource.
func (s *Service) Create(ctx context.Context, resource string, msg proto.Message) (store.WriteResult, error) {
	res, err := s.resource(resource)
	if err != nil {
		return store.WriteResult{}, err
	}
	wr, err := s.driver.Create(ctx, res, msg)
	return wr, toStatus(err)
}

// Update overwrites the record identified by msg's primary key.
func (s *Service) Update(ctx context.Context, resource string, msg proto.Message) (store.WriteResult, error) {
	res, err := s.resource(resource)
	if err != nil {
		return store.WriteResult{}, err
	}
	wr, err := s.driver.Update(ctx, res, msg)
	return wr, toStatus(err)
}

// Delete removes the record of the named resource with the given key.
func (s *Service) Delete(ctx context.Context, resource, key string) error {
	res, err := s.resource(resource)
	if err != nil {
		return err
	}
	return toStatus(s.driver.Delete(ctx, res, key))
}

// List returns a page of the named resource per opts.
func (s *Service) List(ctx context.Context, resource string, opts store.ListOptions) (store.ListResult, error) {
	res, err := s.resource(resource)
	if err != nil {
		return store.ListResult{}, err
	}
	lr, err := s.driver.List(ctx, res, opts)
	return lr, toStatus(err)
}

// resource resolves a registered descriptor, returning an Internal status (a
// generated shim referencing an unregistered resource is a wiring bug, not a
// client error).
func (s *Service) resource(name string) (*store.Resource, error) {
	res, err := s.registry.Resource(name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return res, nil
}

// toStatus maps a driver error to a gRPC status error. Sentinels map to their
// canonical codes; anything else is Internal. nil passes through.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrUnimplemented):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, store.ErrPermissionDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	default:
		// Already a status error? Preserve its code.
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, err.Error())
	}
}
