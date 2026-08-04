package store

import "fmt"

// Registry maps resource names to their descriptors. Generated descriptors are
// passed to NewRegistry once at startup; the adapter resolves a request's
// resource by name on each call. A Registry is read-only after construction and
// safe for concurrent use.
type Registry struct {
	byName map[string]*Resource
}

// NewRegistry indexes resources by Name. Each Resource is copied to the heap so
// the registry owns stable pointers (Driver methods take *Resource). A later
// duplicate Name overwrites an earlier one.
func NewRegistry(resources ...Resource) *Registry {
	r := &Registry{byName: make(map[string]*Resource, len(resources))}
	for i := range resources {
		res := resources[i]
		r.byName[res.Name] = &res
	}
	return r
}

// Resource returns the descriptor registered under name, or an error naming the
// missing resource (so the adapter can surface a precise NotFound/Internal).
func (r *Registry) Resource(name string) (*Resource, error) {
	res, ok := r.byName[name]
	if !ok {
		return nil, fmt.Errorf("store: no resource registered as %q", name)
	}
	return res, nil
}

// Names returns the registered resource names in no particular order.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.byName))
	for name := range r.byName {
		out = append(out, name)
	}
	return out
}
