package a2a

import (
	"errors"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The gRPC code is the part a caller can act on, so it survives into both the
// task state and the message a client reads.
func TestHandleError_MapsGRPCCodes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantState TaskState
		wantText  string
	}{
		{
			"plain error",
			errors.New("boom"),
			StateFailed,
			"boom",
		},
		{
			"not found stays a failure",
			status.Error(codes.NotFound, "no such record"),
			StateFailed,
			"NotFound: no such record",
		},
		{
			"canceled is not a failure",
			status.Error(codes.Canceled, "client went away"),
			StateCanceled,
			"Canceled: client went away",
		},
		{
			"unauthenticated is a refusal",
			status.Error(codes.Unauthenticated, "no token"),
			StateRejected,
			"Unauthenticated: no token",
		},
		{
			"permission denied is a refusal",
			status.Error(codes.PermissionDenied, "not yours"),
			StateRejected,
			"PermissionDenied: not yours",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, ok := HandleError(testContext(), tc.err).(*a2a.TaskStatusUpdateEvent)
			if !ok {
				t.Fatal("want a status update event")
			}
			if ev.Status.State != tc.wantState {
				t.Errorf("state = %q, want %q", ev.Status.State, tc.wantState)
			}
			if got := ev.Status.Message.Parts[0].Text(); got != tc.wantText {
				t.Errorf("text = %q, want %q", got, tc.wantText)
			}
			// Every state this produces has to end the execution, or a failed
			// task would sit open forever.
			if !ev.Status.State.Terminal() {
				t.Errorf("state %q is not terminal", ev.Status.State)
			}
		})
	}
}

func TestHandleError_NilIsCompletion(t *testing.T) {
	ev, ok := HandleError(testContext(), nil).(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatal("want a status update event")
	}
	if ev.Status.State != StateCompleted {
		t.Errorf("state = %q, want completed", ev.Status.State)
	}
}

func TestErrorText(t *testing.T) {
	if got := ErrorText(nil); got != "" {
		t.Errorf("nil: got %q", got)
	}
	if got := ErrorText(errors.New("boom")); got != "boom" {
		t.Errorf("plain: got %q", got)
	}
	if got := ErrorText(status.Error(codes.NotFound, "gone")); got != "NotFound: gone" {
		t.Errorf("status: got %q", got)
	}
}
