package a2a

import (
	"github.com/a2aproject/a2a-go/v2/a2a"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleError turns an error from an agent's own work into the event that
// reports it — a terminal status update carrying a message a client can read.
//
// A gRPC error keeps its code, because the code is the part a caller can act
// on: NotFound and Unavailable ask for different things from whoever sent the
// request, and flattening both to "failed" throws that away. The code arrives
// as the message's leading text rather than a separate field, since the
// protocol has no place for a transport's status.
//
// The state follows the code. Cancellation reports canceled rather than failed,
// because a task the client stopped did not fail; an unauthenticated or
// permission-denied call reports rejected, which is what the protocol says
// about work a server declined to do; everything else is failed.
//
// The event is terminal, so emitting it ends the execution:
//
//	resp, err := s.backend.DoWork(ctx, req)
//	if err != nil {
//	    yield(HandleError(execCtx, err), nil)
//	    return
//	}
func HandleError(execCtx *ExecutorContext, err error) Event {
	if err == nil {
		return StatusEvent(execCtx, StateCompleted, nil)
	}

	state, text := StateFailed, err.Error()
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		state, text = stateForCode(st.Code()), st.Code().String()+": "+st.Message()
	}

	// Every state this resolves to is terminal, which is what ends the
	// execution — the protocol reads finality off the state rather than a flag,
	// so there is nothing further to set here.
	return a2a.NewStatusUpdateEvent(execCtx, state, AgentMessage(TextPart(text)))
}

// stateForCode maps a gRPC status code onto the task state that describes what
// happened to the work.
func stateForCode(code codes.Code) TaskState {
	switch code {
	case codes.Canceled:
		return StateCanceled
	case codes.Unauthenticated, codes.PermissionDenied:
		return StateRejected
	default:
		return StateFailed
	}
}

// ErrorText renders err the way [HandleError] puts it on the wire. It is
// exported for agents that build their own failure event but want the message
// to read the same as every other one.
func ErrorText(err error) string {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return st.Code().String() + ": " + st.Message()
	}
	return err.Error()
}
