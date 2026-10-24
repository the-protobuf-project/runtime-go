package problem

import (
	"errors"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

// Problem is an error a caller can act on. It wraps one of the service's
// sentinels, Kind, which decides its status code.
type Problem struct {
	// Kind is the sentinel registered on the Translator.
	Kind error
	// Reason is the stable, machine-readable cause (ErrorInfo.reason).
	// Empty uses Kind's default reason.
	Reason string
	// What says what is wrong, in one sentence, naming the values involved.
	What string
	// Fix says what to do about it. Empty only when the caller can do
	// nothing.
	Fix string
	// Field is the request field at fault (a BadRequest field violation).
	Field string
	// ResourceType and ResourceName are the resource missing or in the way
	// (ResourceInfo, or PreconditionFailure on FAILED_PRECONDITION).
	ResourceType string
	ResourceName string
	// Metadata are extra facts for ErrorInfo.metadata.
	Metadata map[string]string
}

// Error is What, then Fix.
func (p *Problem) Error() string {
	if p.Fix == "" {
		return p.What
	}
	return strings.TrimSuffix(p.What, ".") + ". " + p.Fix
}

// Unwrap is Kind, so errors.Is(p, ErrNotFound) holds for a missing resource.
func (p *Problem) Unwrap() error { return p.Kind }

// Kind binds a sentinel error to the status code and default reason of every
// error wrapping it.
type Kind struct {
	Err    error
	Code   codes.Code
	Reason string
}

// Reason is a reason a service may return, declared so it can be documented
// and so a Problem with no Kind takes its code.
type Reason struct {
	Name        string
	Code        codes.Code
	Description string
}

// Translator maps a service's errors onto statuses. Build it once at start-up;
// it is safe for concurrent use after.
type Translator struct {
	domain   string
	kinds    []Kind
	strip    []error
	handlers []func(error) (error, bool)
	reasons  map[string]Reason
}

// New returns a Translator whose ErrorInfo details carry domain, mapping
// kinds in order: the first whose Err the error wraps decides.
func New(domain string, kinds ...Kind) *Translator {
	t := &Translator{domain: domain, reasons: map[string]Reason{}}
	for _, k := range kinds {
		t.kinds = append(t.kinds, k)
		t.strip = append(t.strip, k.Err)
	}
	return t
}

// Domain is the ErrorInfo domain.
func (t *Translator) Domain() string { return t.domain }

// Strip adds sentinels whose text "%w: ..." puts in front of a message, beyond
// the registered kinds', to drop from messages.
func (t *Translator) Strip(errs ...error) *Translator {
	t.strip = append(t.strip, errs...)
	return t
}

// Handle adds a translation tried, in order, after Problem and before the
// registered kinds: h returns the status error for err and true, or false to
// pass. Use it for typed errors of a library (a store's reference errors).
func (t *Translator) Handle(h func(err error) (error, bool)) *Translator {
	t.handlers = append(t.handlers, h)
	return t
}

// Declare records reasons the service may return.
func (t *Translator) Declare(reasons ...Reason) *Translator {
	for _, r := range reasons {
		t.reasons[r.Name] = r
	}
	return t
}

// Reasons are the declared reasons, by name.
func (t *Translator) Reasons() []Reason {
	out := make([]Reason, 0, len(t.reasons))
	for _, r := range t.reasons {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b Reason) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// ToStatus maps err onto a status a developer can act on.
//
// A Problem becomes its message -- what is wrong, then what to do -- with an
// ErrorInfo carrying its reason and the detail its kind calls for. Any other
// error wrapping a registered sentinel keeps its message, less the sentinel's
// own text ("not found: ..."), which only repeats the status code. Errors
// that are already gRPC statuses pass through; anything else is Internal.
func (t *Translator) ToStatus(err error) error {
	if err == nil {
		return nil
	}
	var p *Problem
	if errors.As(err, &p) {
		return t.problemStatus(p)
	}
	for _, h := range t.handlers {
		if out, ok := h(err); ok {
			return out
		}
	}
	code, reason, ok := t.Classify(err)
	if !ok {
		if _, isStatus := status.FromError(err); isStatus {
			return err
		}
		return status.Error(codes.Internal, err.Error())
	}
	return WithReason(code, Sentence(t.Plain(err)), reason, t.domain)
}

// Classify is the status code and default reason of err's sentinel.
func (t *Translator) Classify(err error) (codes.Code, string, bool) {
	if err == nil {
		return codes.Unknown, "", false
	}
	for _, k := range t.kinds {
		if errors.Is(err, k.Err) {
			return k.Code, k.Reason, true
		}
	}
	return codes.Unknown, "", false
}

// Plain is err's message without the sentinel texts in front of it.
func (t *Translator) Plain(err error) string {
	msg := err.Error()
	for again := true; again; {
		again = false
		for _, s := range t.strip {
			if rest, ok := strings.CutPrefix(msg, s.Error()+": "); ok {
				msg, again = rest, true
			}
		}
	}
	return msg
}

// problemStatus renders a Problem: its message, an ErrorInfo, and the detail
// its kind calls for.
func (t *Translator) problemStatus(p *Problem) error {
	code, reason, ok := t.Classify(p.Kind)
	if p.Reason != "" {
		reason = p.Reason
		if r, declared := t.reasons[reason]; !ok && declared {
			code = r.Code
		}
	}
	st := status.New(code, Sentence(p.Error()))
	details := []protoadapt.MessageV1{&errdetails.ErrorInfo{Reason: reason, Domain: t.domain, Metadata: p.Metadata}}
	switch {
	case p.Field != "":
		details = append(details, &errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{
			{Field: p.Field, Description: p.Error(), Reason: reason},
		}})
	case code == codes.FailedPrecondition && p.ResourceName != "":
		details = append(details, &errdetails.PreconditionFailure{Violations: []*errdetails.PreconditionFailure_Violation{
			{Type: reason, Subject: p.ResourceName, Description: p.Error()},
		}})
	case p.ResourceName != "":
		details = append(details, &errdetails.ResourceInfo{
			ResourceType: p.ResourceType, ResourceName: p.ResourceName, Description: p.Fix,
		})
	}
	return withDetails(st, details...)
}

// WithReason is a status of code and msg with an ErrorInfo of reason in
// domain, so clients can branch on a stable reason instead of parsing the
// message. If the detail cannot be attached the bare status still carries code
// and message, which is better than failing the call over its own error
// reporting.
func WithReason(code codes.Code, msg, reason, domain string) error {
	return withDetails(status.New(code, msg), &errdetails.ErrorInfo{Reason: reason, Domain: domain})
}

func withDetails(st *status.Status, details ...protoadapt.MessageV1) error {
	detailed, err := st.WithDetails(details...)
	if err != nil {
		return st.Err()
	}
	return detailed.Err()
}

// ReasonOf is the ErrorInfo reason err carries, or "".
func ReasonOf(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

// Sentence makes msg read as one: a capital first letter and a closing full
// stop, so every message an API returns is written the same way whoever wrote
// it. A first word that is not plain lower case -- a field name such as
// update_mask, a resource name -- is left as it is spelled.
func Sentence(msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return msg
	}
	first, _, _ := strings.Cut(msg, " ")
	if strings.IndexFunc(first, func(r rune) bool { return !unicode.IsLower(r) }) < 0 {
		r, n := utf8.DecodeRuneInString(msg)
		msg = string(unicode.ToUpper(r)) + msg[n:]
	}
	if !strings.HasSuffix(msg, ".") && !strings.HasSuffix(msg, "?") {
		msg += "."
	}
	return msg
}
