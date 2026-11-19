package repox

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-protobuf-project/runtime-go/events"
	"github.com/the-protobuf-project/runtime-go/grpc/problem"
)

// The generated servers translate these, so they must be problem's values.
func TestSentinelsAreProblems(t *testing.T) {
	for _, pair := range [][2]error{
		{ErrNotFound, problem.ErrNotFound},
		{ErrAlreadyExists, problem.ErrAlreadyExists},
		{ErrConflict, problem.ErrConflict},
		{ErrInvalidArgument, problem.ErrInvalidArgument},
	} {
		if !errors.Is(pair[0], pair[1]) {
			t.Errorf("repox %v is not problem's", pair[0])
		}
	}
}

func TestReferenceStatus(t *testing.T) {
	if _, ok := ReferenceStatus(errors.New("boom")); ok {
		t.Fatal("an unrelated error is not a reference error")
	}
	err := fmt.Errorf("delete: %w", &ReferenceError{
		Err: ErrReferenceNotFound, Field: "borrower", ResourceType: "vcard.example.com/Card", Name: "cards/2",
	})
	got, ok := ReferenceStatus(err)
	if !ok {
		t.Fatal("a wrapped reference error is handled")
	}
	st := status.Convert(got)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code %v, want FAILED_PRECONDITION", st.Code())
	}
	var info *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if i, isInfo := d.(*errdetails.ErrorInfo); isInfo {
			info = i
		}
	}
	if info == nil || info.GetReason() != ReasonReferenceNotFound || info.GetDomain() != ErrorDomain ||
		info.GetMetadata()["field"] != "borrower" || info.GetMetadata()["resource_name"] != "cards/2" {
		t.Fatalf("ErrorInfo = %v", info)
	}
}

func TestFeedInput(t *testing.T) {
	since := time.Date(2026, 11, 19, 7, 0, 0, 0, time.UTC)
	in := FeedInput(events.Query{
		PageSize:          50,
		Since:             since,
		SinceInclusive:    true,
		Names:             []string{"shelves/a", "shelves/b"},
		OrderByUpdateTime: true,
	})
	want := `update_time >= "2026-11-19T07:00:00Z" AND (name = "shelves/a" OR name = "shelves/b")`
	if in.Filter != want {
		t.Errorf("Filter = %s\n want %s", in.Filter, want)
	}
	if in.PageSize != 50 || in.OrderBy != "update_time" {
		t.Errorf("FeedInput = %+v", in)
	}
	if FeedInput(events.Query{}).Filter != "" {
		t.Error("an unbounded query filters nothing")
	}
}

func TestStoreErr(t *testing.T) {
	storeNotFound := errors.New("store: not found")
	err := StoreErr(fmt.Errorf("get: %w", ErrNotFound), storeNotFound, nil, nil)
	if !errors.Is(err, storeNotFound) || !errors.Is(err, ErrNotFound) {
		t.Errorf("StoreErr keeps both sentinels: %v", err)
	}
	if StoreErr(nil, storeNotFound, nil, nil) != nil {
		t.Error("nil stays nil")
	}
	other := errors.New("boom")
	if !errors.Is(StoreErr(other, storeNotFound, nil, nil), other) {
		t.Error("an unrelated error passes through")
	}
	if got := StoreErr(ErrConflict, storeNotFound, nil, nil); !errors.Is(got, ErrConflict) || errors.Is(got, storeNotFound) {
		t.Error("a sentinel with no store counterpart passes through")
	}
}

func TestScopeAndTime(t *testing.T) {
	if got := Scope("organizationalUnits/o/holds/h", "organizationalUnits"); got != "organizationalUnits/o" {
		t.Errorf("Scope = %q", got)
	}
	if got := Scope("holds/h", "organizationalUnits"); got != "" {
		t.Errorf("Scope with no such ancestor = %q", got)
	}
	if Timestamp(time.Time{}) != nil || !Time(nil).IsZero() {
		t.Error("zero time and nil timestamp map to each other")
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if !Time(Timestamp(now)).Equal(now) {
		t.Error("Time(Timestamp(t)) is t")
	}
	if l := Lifecycle(Timestamp(now), Timestamp(now), nil); strings.TrimSpace(l) == "" {
		t.Error("Lifecycle names a state")
	}
}
