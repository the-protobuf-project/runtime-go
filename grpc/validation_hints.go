package grpc

// validation_hints.go turns a protovalidate failure into an error a developer
// can act on.
//
// protovalidate reports what rule failed, in the rule's terms: a field "does
// not match regex pattern `^organizationalUnits/[^/]+$`". That is accurate
// and unhelpful -- the caller has to reverse-engineer a regex into the value
// it wanted. For the fields where that matters most, resource names, the
// proto already says what the value looks like: a google.api.resource
// pattern. So a violation is described by what the field must be, with the
// pattern as the example and the value that was sent beside it, and each one
// also travels as a google.rpc.BadRequest field violation a client can show
// next to the input it belongs to.

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"buf.build/go/protovalidate"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ValidationReason is the ErrorInfo reason of a request that failed its
// buf.validate rules.
const ValidationReason = "INVALID_REQUEST_FIELDS"

// validationStatus is the InvalidArgument status for a protovalidate error:
// one line per violated field in the message, and a BadRequest detail with
// the same, field by field.
func validationStatus(err error) error {
	var verr *protovalidate.ValidationError
	if !errors.As(err, &verr) || len(verr.Violations) == 0 {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	bad := &errdetails.BadRequest{}
	lines := make([]string, 0, len(verr.Violations))
	for _, v := range verr.Violations {
		field := protovalidate.FieldPathString(v.Proto.GetField())
		desc := describeViolation(v)
		bad.FieldViolations = append(bad.FieldViolations, &errdetails.BadRequest_FieldViolation{
			Field:       field,
			Description: joinField(field, desc),
			Reason:      v.Proto.GetRuleId(),
		})
		lines = append(lines, joinField(field, desc))
	}
	msg := lines[0]
	if !strings.HasSuffix(msg, ".") {
		msg += "."
	}
	if len(lines) > 1 {
		msg = fmt.Sprintf("%d fields need fixing:\n - %s", len(lines), strings.Join(lines, "\n - "))
	}
	st := status.New(codes.InvalidArgument, msg)
	detailed, derr := st.WithDetails(bad, &errdetails.ErrorInfo{Reason: ValidationReason})
	if derr != nil {
		return st.Err()
	}
	return detailed.Err()
}

// joinField reads a description after its field: "window is required",
// "parent must look like ...", and otherwise "page_size: <rule message>".
func joinField(field, desc string) string {
	if strings.HasPrefix(desc, "is ") || strings.HasPrefix(desc, "must ") {
		return field + " " + desc
	}
	return field + ": " + desc
}

// describeViolation says what the field must be.
func describeViolation(v *protovalidate.Violation) string {
	fd := v.FieldDescriptor
	rule := v.Proto.GetRuleId()
	switch {
	case rule == "required":
		if fd != nil {
			if p := namePattern(fd); p != "" {
				return "is required: the name of a resource, " + p
			}
		}
		return "is required"
	case strings.HasSuffix(rule, ".pattern") && fd != nil && fd.Kind() == protoreflect.StringKind:
		p := namePattern(fd)
		if p == "" {
			p = patternTemplate(v.RuleValue.String())
		}
		if p != "" {
			return fmt.Sprintf("must look like %s; got %s", p, sent(v))
		}
	}
	msg := v.Proto.GetMessage()
	if s := sent(v); s != "" && fd != nil && !fd.IsList() && !fd.IsMap() && fd.Kind() != protoreflect.MessageKind {
		return msg + "; got " + s
	}
	return msg
}

// sent renders the value a violation is about, for the message.
func sent(v *protovalidate.Violation) string {
	fd := v.FieldDescriptor
	if fd == nil || !v.FieldValue.IsValid() || fd.IsList() || fd.IsMap() {
		return ""
	}
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return ""
	case protoreflect.StringKind:
		if s := v.FieldValue.String(); s != "" {
			return fmt.Sprintf("%q", s)
		}
		return "nothing"
	}
	return v.FieldValue.String()
}

// namePattern is the resource-name pattern a string field holds, when the
// proto says: a resource_reference names the type (or, for a parent, the
// child type, whose pattern minus its last collection is the parent's), and
// an AIP-122 name field is its own message's resource.
func namePattern(fd protoreflect.FieldDescriptor) string {
	if fd.Kind() != protoreflect.StringKind {
		return ""
	}
	if ref, _ := proto.GetExtension(fd.Options(), annotations.E_ResourceReference).(*annotations.ResourceReference); ref != nil {
		if p := resourcePatterns()[ref.GetType()]; p != "" {
			return p
		}
		if segs := strings.Split(resourcePatterns()[ref.GetChildType()], "/"); len(segs) >= 4 {
			return strings.Join(segs[:len(segs)-2], "/")
		}
	}
	if fd.Name() == "name" {
		if res, _ := proto.GetExtension(fd.ContainingMessage().Options(), annotations.E_Resource).(*annotations.ResourceDescriptor); res != nil && len(res.GetPattern()) > 0 {
			return res.GetPattern()[0]
		}
	}
	return ""
}

var (
	patternsOnce sync.Once
	patterns     map[string]string
)

// resourcePatterns maps every registered resource type to its first pattern.
func resourcePatterns() map[string]string {
	patternsOnce.Do(func() {
		patterns = map[string]string{}
		var walk func(protoreflect.MessageDescriptors)
		walk = func(ms protoreflect.MessageDescriptors) {
			for i := 0; i < ms.Len(); i++ {
				m := ms.Get(i)
				if res, _ := proto.GetExtension(m.Options(), annotations.E_Resource).(*annotations.ResourceDescriptor); res != nil && len(res.GetPattern()) > 0 {
					patterns[res.GetType()] = res.GetPattern()[0]
				}
				walk(m.Messages())
			}
		}
		protoregistry.GlobalFiles.RangeFiles(func(f protoreflect.FileDescriptor) bool {
			walk(f.Messages())
			return true
		})
	})
	return patterns
}

// patternTemplate turns the common resource-name regex into the template it
// encodes -- `^organizationalUnits/[^/]+$` into organizationalUnits/{id} --
// and returns "" for any other regex, which is then reported as written.
func patternTemplate(re string) string {
	t := strings.TrimSuffix(strings.TrimPrefix(re, "^"), "$")
	t = strings.ReplaceAll(t, "[^/]+", "{id}")
	if !simpleTemplate.MatchString(t) {
		return ""
	}
	return t
}

var simpleTemplate = regexp.MustCompile(`^[A-Za-z0-9_\-]+(/(\{id\}|[A-Za-z0-9_\-]+))*$`)
