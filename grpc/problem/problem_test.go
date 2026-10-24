package problem

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errInvalidArgument = errors.New("invalid argument")
	errNotFound        = errors.New("not found")
	errInvalidState    = errors.New("invalid state for the requested transition")
	errForeignKey      = errors.New("foreign key violation")
)

// refError stands in for a store library's typed error.
type refError struct{ name string }

func (e *refError) Error() string { return errForeignKey.Error() + ": " + e.name + " is referenced" }

func translator() *Translator {
	return New("shop.example.com",
		Kind{Err: errNotFound, Code: codes.NotFound, Reason: "NOT_FOUND"},
		Kind{Err: errInvalidState, Code: codes.FailedPrecondition, Reason: "INVALID_STATE"},
		Kind{Err: errInvalidArgument, Code: codes.InvalidArgument, Reason: "INVALID_ARGUMENT"},
	).Strip(errForeignKey).Handle(func(err error) (error, bool) {
		var ref *refError
		if !errors.As(err, &ref) {
			return nil, false
		}
		return WithReason(codes.FailedPrecondition, "Referenced.", "RESOURCE_REFERENCED", "store"), true
	}).Declare(Reason{Name: "SHELF_EMPTY", Code: codes.FailedPrecondition, Description: "the shelf has no books"})
}

func details(st *status.Status) (info *errdetails.ErrorInfo, field, subject, resource string) {
	for _, d := range st.Details() {
		switch d := d.(type) {
		case *errdetails.ErrorInfo:
			info = d
		case *errdetails.BadRequest:
			field = d.GetFieldViolations()[0].GetField()
		case *errdetails.PreconditionFailure:
			subject = d.GetViolations()[0].GetSubject()
		case *errdetails.ResourceInfo:
			resource = d.GetResourceName()
		}
	}
	return info, field, subject, resource
}

func TestToStatusProblem(t *testing.T) {
	tr := translator()
	err := tr.ToStatus(fmt.Errorf("wrapped: %w", &Problem{Kind: errInvalidArgument, Reason: "ORGANIZATION_REQUIRED",
		Field: "organizational_unit.organization", What: "an organizational unit belongs to an organization", Fix: "Create one first"}))
	st := status.Convert(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("code = %v", st.Code())
	}
	if want := "An organizational unit belongs to an organization. Create one first."; st.Message() != want {
		t.Fatalf("message = %q, want %q", st.Message(), want)
	}
	info, field, _, _ := details(st)
	if info.GetReason() != "ORGANIZATION_REQUIRED" || info.GetDomain() != "shop.example.com" || field != "organizational_unit.organization" {
		t.Fatalf("details: %v, field %q", info, field)
	}
	if ReasonOf(err) != "ORGANIZATION_REQUIRED" {
		t.Fatalf("ReasonOf = %q", ReasonOf(err))
	}
}

func TestToStatusProblemDetails(t *testing.T) {
	tr := translator()
	// A blocked call names its resource in a PreconditionFailure.
	st := status.Convert(tr.ToStatus(&Problem{Kind: errInvalidState, Reason: "HOLD_NOT_ACTIVE", ResourceName: "holds/h1", What: "hold holds/h1 is expired"}))
	if info, _, subject, _ := details(st); st.Code() != codes.FailedPrecondition || info.GetReason() != "HOLD_NOT_ACTIVE" || subject != "holds/h1" {
		t.Fatalf("blocked: %v %v %q", st.Code(), info, subject)
	}
	// A missing one in a ResourceInfo; with no reason, the kind's default.
	st = status.Convert(tr.ToStatus(&Problem{Kind: errNotFound, ResourceType: "shop/Shelf", ResourceName: "shelves/s", What: "shelf shelves/s does not exist"}))
	if info, _, _, res := details(st); st.Code() != codes.NotFound || info.GetReason() != "NOT_FOUND" || res != "shelves/s" {
		t.Fatalf("missing: %v %v %q", st.Code(), info, res)
	}
	// A declared reason with no kind takes the declared code.
	st = status.Convert(tr.ToStatus(&Problem{Reason: "SHELF_EMPTY", What: "the shelf is empty"}))
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("declared reason: %v", st.Code())
	}
	if rs := tr.Reasons(); len(rs) != 1 || rs[0].Name != "SHELF_EMPTY" {
		t.Fatalf("Reasons = %v", rs)
	}
}

func TestToStatusPlain(t *testing.T) {
	tr := translator()
	st := status.Convert(tr.ToStatus(fmt.Errorf("%w: window needs both ends", errInvalidArgument)))
	if st.Code() != codes.InvalidArgument || st.Message() != "Window needs both ends." {
		t.Fatalf("got %v %q", st.Code(), st.Message())
	}
	if info, _, _, _ := details(st); info.GetReason() != "INVALID_ARGUMENT" {
		t.Fatalf("reason %v", info)
	}
	if got := tr.Plain(fmt.Errorf("%w: %w: x", errForeignKey, errNotFound)); got != "x" {
		t.Fatalf("Plain = %q", got)
	}
}

func TestToStatusPassThroughAndInternal(t *testing.T) {
	tr := translator()
	if tr.ToStatus(nil) != nil {
		t.Fatal("nil is not nil")
	}
	in := status.Error(codes.InvalidArgument, "bad request")
	if got := tr.ToStatus(in); got != in { //nolint:errorlint // identity: passed through untouched
		t.Fatalf("a status was rewritten: %v", got)
	}
	if st := status.Convert(tr.ToStatus(errors.New("boom"))); st.Code() != codes.Internal {
		t.Fatalf("unknown error: %v", st.Code())
	}
	if st := status.Convert(tr.ToStatus(fmt.Errorf("create: %w", &refError{name: "shelves/s"}))); st.Code() != codes.FailedPrecondition || ReasonOf(st.Err()) != "RESOURCE_REFERENCED" {
		t.Fatalf("handler: %v %q", st.Code(), ReasonOf(st.Err()))
	}
}

func TestInterceptor(t *testing.T) {
	ic := translator().Interceptor()
	_, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
		return nil, fmt.Errorf("%w: gone", errNotFound)
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("interceptor: %v", err)
	}
	resp, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return "ok", nil })
	if resp != "ok" || err != nil {
		t.Fatalf("success: %v %v", resp, err)
	}
}

func TestSentence(t *testing.T) {
	for in, want := range map[string]string{
		"organization does not exist":            "Organization does not exist.",
		"update_mask names nothing":              "update_mask names nothing.",
		"organization.display_names is required": "organization.display_names is required.",
		"Already a sentence.":                    "Already a sentence.",
		"  ":                                     "",
	} {
		if got := Sentence(in); got != want {
			t.Errorf("Sentence(%q) = %q, want %q", in, got, want)
		}
	}
}
