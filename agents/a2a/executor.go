package a2a

import (
	"context"
	"iter"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// The execution vocabulary, re-exported for the same reason as the card types:
// an agent implementation should need one import.
type (
	// Executor is what an agent implements. Execute runs a request and yields
	// the events it produces; Cancel stops one already running.
	Executor = a2asrv.AgentExecutor

	// ExecutorContext carries the request that triggered an execution — the
	// message, the task it belongs to, the user who sent it.
	ExecutorContext = a2asrv.ExecutorContext

	// Event is anything an execution can emit: a message, a task, a status
	// update, an artifact update.
	Event = a2a.Event

	// Events is the sequence an execution yields. Yielding a non-nil error
	// aborts it.
	Events = iter.Seq2[Event, error]

	// Message is one turn of the conversation.
	Message = a2a.Message

	// Part is a piece of a message or artifact — text, structured data, a file.
	Part = a2a.Part

	// TaskState is where a task has got to. Terminal states end the execution.
	TaskState = a2a.TaskState
)

// The task states an executor emits, re-exported.
const (
	StateSubmitted     = a2a.TaskStateSubmitted
	StateWorking       = a2a.TaskStateWorking
	StateInputRequired = a2a.TaskStateInputRequired
	StateAuthRequired  = a2a.TaskStateAuthRequired
	StateCompleted     = a2a.TaskStateCompleted
	StateCanceled      = a2a.TaskStateCanceled
	StateFailed        = a2a.TaskStateFailed
	StateRejected      = a2a.TaskStateRejected
)

// TextPart wraps text as a message part.
func TextPart(text string) *Part { return a2a.NewTextPart(text) }

// DataPart wraps structured data as a message part, for an agent answering
// with something a client will parse rather than display.
func DataPart(data any) *Part { return a2a.NewDataPart(data) }

// AgentMessage builds a message from this agent carrying parts.
func AgentMessage(parts ...*Part) *Message {
	return a2a.NewMessage(a2a.MessageRoleAgent, parts...)
}

// StatusEvent reports that a task has moved to state, with an optional message
// explaining why. It is the event an agent emits most.
func StatusEvent(execCtx *ExecutorContext, state TaskState, msg *Message) Event {
	return a2a.NewStatusUpdateEvent(execCtx, state, msg)
}

// ArtifactEvent emits a new artifact — a result the client keeps, as opposed to
// a message it reads.
func ArtifactEvent(execCtx *ExecutorContext, parts ...*Part) Event {
	return a2a.NewArtifactEvent(execCtx, parts...)
}

// RequestText is the text of the message that triggered this execution, with
// the parts joined by newlines.
//
// It is a convenience for the common case and a lossy one by construction: an
// agent handling files or structured input should read
// [ExecutorContext.Message] itself rather than the flattened text.
func RequestText(execCtx *ExecutorContext) string {
	if execCtx == nil || execCtx.Message == nil {
		return ""
	}
	var b strings.Builder
	for _, p := range execCtx.Message.Parts {
		if p == nil {
			continue
		}
		if text := p.Text(); text != "" {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(text)
		}
	}
	return b.String()
}

// ExecutorFunc adapts a plain function to [Executor] for an agent with nothing
// to cancel — a request that either completes within one call or not at all.
//
// Cancel yields a canceled status and stops, which is the honest answer for
// work that is not interruptible: the client learns the task will produce
// nothing more, rather than waiting on an execution that ignored the request.
// An agent that can genuinely abort mid-flight should implement [Executor]
// itself and act on the cancellation.
type ExecutorFunc func(ctx context.Context, execCtx *ExecutorContext) Events

// Execute runs f.
func (f ExecutorFunc) Execute(ctx context.Context, execCtx *ExecutorContext) Events {
	return f(ctx, execCtx)
}

// Cancel reports the task canceled without interrupting anything.
func (f ExecutorFunc) Cancel(_ context.Context, execCtx *ExecutorContext) Events {
	return func(yield func(Event, error) bool) {
		yield(StatusEvent(execCtx, StateCanceled, nil), nil)
	}
}

// TextAgent is the smallest useful agent: text in, text out, one reply per
// request.
//
// It exists because the shape of [Executor] is built for streaming — a sequence
// of events over a task's lifetime — and an agent that answers in one shot
// should not have to write that out. The reply is emitted as a message rather
// than a task, which is what the protocol prescribes for work that finished
// before there was anything to track.
//
// An error from fn becomes a failed task carrying its message; see
// [HandleError] for how a gRPC status is preserved through that.
func TextAgent(fn func(ctx context.Context, text string) (string, error)) Executor {
	return ExecutorFunc(func(ctx context.Context, execCtx *ExecutorContext) Events {
		return func(yield func(Event, error) bool) {
			reply, err := fn(ctx, RequestText(execCtx))
			if err != nil {
				yield(HandleError(execCtx, err), nil)
				return
			}
			yield(AgentMessage(TextPart(reply)), nil)
		}
	})
}
