package opcua

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// named is a subject that names its namespace by URI: `nsu=<uri>;s=Temp`.
//
// A namespace index is a position in the server's namespace table and can
// change when the server's configuration does; a URI is stable. So the
// identifier is kept apart from the namespace and joined to the index the
// server gives the URI at the moment of use.
type named struct {
	uri string
	// id is the identifier half, `s=Temp`, which ua.ParseNodeID reads in
	// namespace 0.
	id string
}

// node joins n's identifier to namespace index ns.
func (n named) node(ns uint16) (*ua.NodeID, error) {
	return ua.ParseNodeID(fmt.Sprintf("ns=%d;%s", ns, n.id))
}

// errNotNodeID is the refusal for a subject that is not a node id.
func errNotNodeID(subject string, err error) error {
	return fmt.Errorf("%w: opcua: subject %q is not an OPC UA node id; a subject is a node id in string form, such as \"ns=3;s=Temperature\" or \"nsu=urn:plant;i=1001\": %w",
		streams.ErrUnsupported, subject, err)
}

// parseSubject reads a subject in OPC UA's string form (Part 6, 5.3.1.10):
// `ns=3;s=Temp`, `i=2258`, `ns=2;g=<guid>`, `ns=2;b=<base64>`, or
// `nsu=<uri>;s=Temp`. Exactly one of the results is set.
func parseSubject(subject string) (*ua.NodeID, *named, error) {
	rest, ok := strings.CutPrefix(subject, "nsu=")
	if !ok {
		// gopcua reads a string with no identifier type as a string id in
		// namespace 0, so "orders.placed" would parse; OPC UA's string form
		// always names the type.
		if _, id, indexed := strings.Cut(subject, ";"); !identifier(id) && (indexed || !identifier(subject)) {
			return nil, nil, errNotNodeID(subject, errors.New("want [ns=<index>;]<type>=<value>"))
		}
		n, err := ua.ParseNodeID(subject)
		if err != nil {
			return nil, nil, errNotNodeID(subject, err)
		}
		return n, nil, nil
	}
	uri, id, ok := strings.Cut(rest, ";")
	if !ok || uri == "" || !identifier(id) {
		return nil, nil, errNotNodeID(subject, errors.New("want nsu=<uri>;<type>=<value>"))
	}
	// OPC UA escapes ';' and '%' in a namespace URI.
	uri = strings.NewReplacer("%3b", ";", "%3B", ";", "%25", "%").Replace(uri)
	if _, err := ua.ParseNodeID(id); err != nil {
		return nil, nil, errNotNodeID(subject, err)
	}
	return nil, &named{uri: uri, id: id}, nil
}

// identifier reports whether s starts with an identifier type: i=, s=, g= or b=.
func identifier(s string) bool {
	for _, prefix := range []string{"i=", "s=", "g=", "b="} {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

// namespaces caches the server's namespace table for one session.
//
// A restarted server may renumber its namespaces, so the table is read again
// whenever the client is on a session other than the one it was read on.
type namespaces struct {
	session *gopcua.Session
	table   []string
}

// namespace returns the server's index for uri on the current session.
func (h *hub) namespace(ctx context.Context, uri string) (uint16, error) {
	h.nsMu.Lock()
	defer h.nsMu.Unlock()

	session := h.s.client.Session()
	if session == nil {
		return 0, errors.New("opcua: the session is not connected")
	}
	if h.ns.session != session {
		table, err := h.s.client.NamespaceArray(ctx)
		if err != nil {
			return 0, fmt.Errorf("opcua: cannot read the namespace table: %w", err)
		}
		h.ns = namespaces{session: session, table: table}
	}
	i := slices.Index(h.ns.table, uri)
	if i < 0 {
		return 0, fmt.Errorf("%w: opcua: the server has no namespace %q", streams.ErrUnsupported, uri)
	}
	return uint16(i), nil
}

// resolve returns the node a declared subject names on the server now.
func (h *hub) resolve(ctx context.Context, subject string, n named) (*ua.NodeID, error) {
	ns, err := h.namespace(ctx, n.uri)
	if err != nil {
		return nil, fmt.Errorf("opcua: cannot resolve the namespace of %q: %w", subject, err)
	}
	return n.node(ns)
}
