package hints

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"unicode"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// DefaultGenericReasons are the reasons that mean nobody described the error
// further: the default reasons a store's bare sentinels map to.
var DefaultGenericReasons = []string{"NOT_FOUND", "ALREADY_EXISTS", "CONCURRENT_MODIFICATION"}

// Options configure a Hinter. The zero value of each field is its default.
type Options struct {
	// Product names the service in the apology for an internal error:
	// "<Product> failed unexpectedly". Default "The service".
	Product string
	// GenericReasons are the ErrorInfo reasons a rewrite may replace; an error
	// with no ErrorInfo, or an empty reason, is always generic. Default
	// DefaultGenericReasons.
	GenericReasons []string
	// Files are the protos read for resources and methods. Default
	// protoregistry.GlobalFiles.
	Files *protoregistry.Files
}

// Hinter rewrites generic errors. It reads the protos once, on first use.
type Hinter struct {
	opts Options

	once  sync.Once
	kinds []kind
	calls map[string]*call
}

// New returns a Hinter.
func New(opts Options) *Hinter {
	if opts.Product == "" {
		opts.Product = "The service"
	}
	if opts.GenericReasons == nil {
		opts.GenericReasons = DefaultGenericReasons
	}
	if opts.Files == nil {
		opts.Files = protoregistry.GlobalFiles
	}
	return &Hinter{opts: opts}
}

var (
	defaultOnce sync.Once
	defaultH    *Hinter
)

func global() *Hinter {
	defaultOnce.Do(func() { defaultH = New(Options{}) })
	return defaultH
}

// Interceptor rewrites the errors handlers return.
func (h *Hinter) Interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}
		m, _ := req.(proto.Message)
		return resp, h.Rewrite(ctx, info.FullMethod, m, err)
	}
}

// Rewrite returns err with a hint when it is one of the generic errors and
// the request says enough to give one, and err unchanged otherwise.
func (h *Hinter) Rewrite(ctx context.Context, fullMethod string, req proto.Message, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	if st.Code() == codes.Internal || st.Code() == codes.Unknown {
		return h.internal(ctx, fullMethod)
	}
	if req == nil || !h.generic(st) {
		return err
	}
	method := methodName(fullMethod)
	switch st.Code() {
	case codes.NotFound:
		// Only a bare "not found": a message that names something was
		// written by someone who knew what was missing, which need not be
		// the resource the request names.
		if bare(st.Message()) != "" {
			if name := named(req); name != "" {
				return h.notFound(st, name)
			}
		}
	case codes.AlreadyExists:
		if strings.HasPrefix(method, "Undelete") {
			if name := named(req); name != "" {
				return h.notDeleted(st, name)
			}
		}
		if name := created(req); name != "" {
			return h.alreadyExists(st, name)
		}
	case codes.Aborted:
		if name := updated(req); name != "" {
			return h.stale(st, name)
		}
	}
	return err
}

// generic reports a status whose ErrorInfo, if any, carries a generic reason.
func (h *Hinter) generic(st *status.Status) bool {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason() == "" || slices.Contains(h.opts.GenericReasons, info.GetReason())
		}
	}
	return true
}

// bare is msg when it is a bare sentinel with nothing said about what.
func bare(msg string) string {
	switch m := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(msg)), "."); m {
	case "not found", "already exists", "conflict":
		return m
	}
	return ""
}

func (h *Hinter) notDeleted(st *status.Status, name string) error {
	r := h.lookup(name)
	msg := fmt.Sprintf("%s %q is not deleted, so there is nothing to restore", r.label(), name)
	if g := r.getCall(name); g != "" {
		msg += "; " + g + " reads it as it is"
	}
	return with(st, msg+".", &errdetails.ResourceInfo{ResourceType: r.Type, ResourceName: name, Description: "not deleted"})
}

func (h *Hinter) notFound(st *status.Status, name string) error {
	// "shelves/x:archive" sent as GET reaches the Get of shelves/x, verb and
	// all: the caller meant the custom method, which is a POST.
	if base, verb, ok := strings.Cut(name, ":"); ok && verb != "" && !strings.Contains(verb, "/") {
		r := h.lookup(base)
		return with(st, fmt.Sprintf("%q is not a %s name: :%s is a custom method, and custom methods are called with POST, not GET.",
			name, strings.ToLower(r.label()), verb), &errdetails.ResourceInfo{ResourceType: r.Type, ResourceName: base, Description: "custom method called with GET"})
	}
	r := h.lookup(name)
	msg := fmt.Sprintf("%s %q does not exist. Check the name", r.label(), name)
	if l := r.listCall(parentOf(name)); l != "" {
		msg += ", or list what does exist with " + l
	}
	return with(st, msg+".", &errdetails.ResourceInfo{ResourceType: r.Type, ResourceName: name, Description: "does not exist"})
}

func (h *Hinter) alreadyExists(st *status.Status, name string) error {
	r := h.lookup(name)
	msg := fmt.Sprintf("%s %q already exists. Choose another id, or leave the id empty to have one assigned", r.label(), name)
	if g := r.getCall(name); g != "" {
		msg += "; " + g + " reads the existing one"
	}
	return with(st, msg+".", &errdetails.ResourceInfo{ResourceType: r.Type, ResourceName: name, Description: "already exists"})
}

func (h *Hinter) stale(st *status.Status, name string) error {
	r := h.lookup(name)
	msg := fmt.Sprintf("%s %q changed after you read it: the etag you sent is no longer current", r.label(), name)
	if g := r.getCall(name); g != "" {
		msg += ". Read it again with " + g + ", reapply your change and retry with the new etag"
	} else {
		msg += ". Read it again, reapply your change and retry with the new etag"
	}
	return with(st, msg+".", &errdetails.ResourceInfo{ResourceType: r.Type, ResourceName: name, Description: "etag is stale"})
}

func (h *Hinter) internal(ctx context.Context, fullMethod string) error {
	msg := h.opts.Product + " failed unexpectedly"
	if m := methodName(fullMethod); m != "" {
		msg += " while handling " + m
	}
	msg += ". This is a fault on our side, not in your request: retry it, and if it keeps failing, report"
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		msg += " trace " + sc.TraceID().String()
	} else {
		msg += " the time and the call"
	}
	return status.Error(codes.Internal, msg+".")
}

// with replaces st's message, keeping its details and adding info.
func with(st *status.Status, msg string, info *errdetails.ResourceInfo) error {
	p := st.Proto()
	p.Message = msg
	out := status.FromProto(p)
	if detailed, err := out.WithDetails(info); err == nil {
		return detailed.Err()
	}
	return out.Err()
}

// named is the resource a request is about: its name, or the name of the
// resource it carries (an update sends the resource itself).
func named(req proto.Message) string {
	if v := stringField(req.ProtoReflect(), "name"); v != "" {
		return v
	}
	return updated(req)
}

// updated is the name of the resource message a request carries.
func updated(req proto.Message) string {
	r := req.ProtoReflect()
	if v := stringField(r, "name"); v != "" {
		return v
	}
	var out string
	r.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap() && isResource(fd.Message()) {
			out = stringField(v.Message(), "name")
			return out == ""
		}
		return true
	})
	return out
}

// created is the name a create asked for: the resource's own name if it set
// one, else parent plus the caller-chosen id (AIP-133's <resource>_id).
func created(req proto.Message) string {
	r := req.ProtoReflect()
	if n := updated(req); n != "" {
		return n
	}
	parent := stringField(r, "parent")
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Kind() != protoreflect.MessageKind || !isResource(fd.Message()) {
			continue
		}
		id := stringField(r, string(fd.Name())+"_id")
		if id == "" {
			return ""
		}
		res := resourceOf(fd.Message())
		segs := strings.Split(res.GetPattern()[0], "/")
		if len(segs) < 2 {
			return ""
		}
		name := segs[len(segs)-2] + "/" + id
		if parent != "" {
			name = parent + "/" + name
		}
		return name
	}
	return ""
}

func stringField(r protoreflect.Message, name string) string {
	fd := r.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() {
		return ""
	}
	return r.Get(fd).String()
}

func isResource(md protoreflect.MessageDescriptor) bool { return resourceOf(md) != nil }

func resourceOf(md protoreflect.MessageDescriptor) *annotations.ResourceDescriptor {
	res, _ := proto.GetExtension(md.Options(), annotations.E_Resource).(*annotations.ResourceDescriptor)
	if res == nil || len(res.GetPattern()) == 0 {
		return nil
	}
	return res
}

// parentOf is the parent of a resource name, "" for a top-level one.
func parentOf(name string) string {
	segs := strings.Split(name, "/")
	if len(segs) < 4 {
		return ""
	}
	return strings.Join(segs[:len(segs)-2], "/")
}

func methodName(fullMethod string) string {
	if i := strings.LastIndex(fullMethod, "/"); i >= 0 {
		return fullMethod[i+1:]
	}
	return ""
}

// kind is what the protos say about one resource.
type kind struct {
	Type     string // "shop.example.com/Shelf"
	Singular string // "shelf"
	Plural   string // "shelves"
	pattern  []string
	list     *call // List<Plural>
	get      *call // Get<Singular>
}

// call is an RPC and its REST binding.
type call struct {
	method string // "ListShelves"
	verb   string // "GET"
	path   string // "/v1/{parent=shelves/*}/books"
}

// label is the kind in words: "Organizational unit".
func (k kind) label() string {
	if k.Singular == "" {
		return "resource"
	}
	var b strings.Builder
	for i, r := range k.Singular {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte(' ')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	s := b.String()
	return strings.ToUpper(s[:1]) + s[1:]
}

func (k kind) listCall(parent string) string {
	return k.list.render(map[string]string{"parent": parent})
}

func (k kind) getCall(name string) string {
	return k.get.render(map[string]string{"name": name})
}

var pathVar = regexp.MustCompile(`\{([a-z_.]+)(=[^}]*)?\}`)

// render is "ListBooks (GET /v1/shelves/x/books)", filling the path's
// variables from vars.
func (c *call) render(vars map[string]string) string {
	if c == nil {
		return ""
	}
	if c.path == "" {
		return c.method
	}
	path := pathVar.ReplaceAllStringFunc(c.path, func(m string) string {
		v := pathVar.FindStringSubmatch(m)[1]
		if val := vars[v]; val != "" {
			return val
		}
		return m
	})
	return fmt.Sprintf("%s (%s %s)", c.method, c.verb, path)
}

// lookup finds the kind whose pattern a name has.
func (h *Hinter) lookup(name string) kind {
	segs := strings.Split(name, "/")
	for _, k := range h.load() {
		if len(k.pattern) != len(segs) {
			continue
		}
		match := true
		for i, p := range k.pattern {
			if !strings.HasPrefix(p, "{") && p != segs[i] {
				match = false
				break
			}
		}
		if match {
			return k
		}
	}
	return kind{}
}

// TypeOf is the resource type a name has, as its proto declares it
// ("shop.example.com/Shelf"), or "" when none matches.
func (h *Hinter) TypeOf(name string) string { return h.lookup(name).Type }

// Call renders an RPC: its name and REST binding, with the path's variables
// filled from vars -- Call("UndeleteShelf", map[string]string{"name":
// "shelves/s"}) is "UndeleteShelf (POST /v1/shelves/s:undelete)". An unknown
// method renders as its name alone, so a hint never breaks over a rename.
func (h *Hinter) Call(method string, vars map[string]string) string {
	h.load()
	c := h.calls[method]
	if c == nil {
		return method
	}
	return c.render(vars)
}

// TypeOf is Hinter.TypeOf over protoregistry.GlobalFiles.
func TypeOf(name string) string { return global().TypeOf(name) }

// Call is Hinter.Call over protoregistry.GlobalFiles.
func Call(method string, vars map[string]string) string { return global().Call(method, vars) }

// load reads every registered resource and its List and Get methods.
func (h *Hinter) load() []kind {
	h.once.Do(func() {
		calls := map[string]*call{}
		var resources []*annotations.ResourceDescriptor
		h.opts.Files.RangeFiles(func(f protoreflect.FileDescriptor) bool {
			var walk func(protoreflect.MessageDescriptors)
			walk = func(ms protoreflect.MessageDescriptors) {
				for i := 0; i < ms.Len(); i++ {
					if res := resourceOf(ms.Get(i)); res != nil {
						resources = append(resources, res)
					}
					walk(ms.Get(i).Messages())
				}
			}
			walk(f.Messages())
			for i := 0; i < f.Services().Len(); i++ {
				ms := f.Services().Get(i).Methods()
				for j := 0; j < ms.Len(); j++ {
					m := ms.Get(j)
					c := &call{method: string(m.Name())}
					if rule, _ := proto.GetExtension(m.Options(), annotations.E_Http).(*annotations.HttpRule); rule != nil {
						switch {
						case rule.GetGet() != "":
							c.verb, c.path = "GET", rule.GetGet()
						case rule.GetPost() != "":
							c.verb, c.path = "POST", rule.GetPost()
						case rule.GetPatch() != "":
							c.verb, c.path = "PATCH", rule.GetPatch()
						case rule.GetDelete() != "":
							c.verb, c.path = "DELETE", rule.GetDelete()
						}
					}
					calls[c.method] = c
				}
			}
			return true
		})
		h.calls = calls
		for _, res := range resources {
			k := kind{Type: res.GetType(), Singular: res.GetSingular(), Plural: res.GetPlural(), pattern: strings.Split(res.GetPattern()[0], "/")}
			if k.Plural != "" {
				k.list = calls["List"+upperFirst(k.Plural)]
			}
			if k.Singular != "" {
				k.get = calls["Get"+upperFirst(k.Singular)]
			}
			h.kinds = append(h.kinds, k)
		}
	})
	return h.kinds
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
