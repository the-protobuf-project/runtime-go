package a2a

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func testContext(parts ...*Part) *ExecutorContext {
	return &ExecutorContext{
		TaskID:    a2a.TaskID("task-1"),
		ContextID: "ctx-1",
		Message:   a2a.NewMessage(a2a.MessageRoleUser, parts...),
	}
}

// collect drains an event sequence so a test can assert on what an agent
// actually emitted.
func collect(t *testing.T, events Events) []Event {
	t.Helper()
	var out []Event
	for ev, err := range events {
		if err != nil {
			t.Fatalf("execution yielded an error: %v", err)
		}
		out = append(out, ev)
	}
	return out
}

func TestRequestText(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  *ExecutorContext
		want string
	}{
		{"nil context", nil, ""},
		{"no message", &ExecutorContext{}, ""},
		{"one part", testContext(TextPart("hello")), "hello"},
		{"joined parts", testContext(TextPart("one"), TextPart("two")), "one\ntwo"},
		{"non-text parts skipped", testContext(DataPart(map[string]any{"k": "v"}), TextPart("hi")), "hi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RequestText(tc.ctx); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A one-shot agent answers with a message rather than a task, which is what the
// protocol prescribes for work that finished before there was anything to track.
func TestTextAgent_RepliesWithAMessage(t *testing.T) {
	agent := TextAgent(func(_ context.Context, text string) (string, error) {
		return "echo: " + text, nil
	})

	events := collect(t, agent.Execute(t.Context(), testContext(TextPart("hello"))))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	msg, ok := events[0].(*Message)
	if !ok {
		t.Fatalf("got %T, want a message", events[0])
	}
	if msg.Role != a2a.MessageRoleAgent {
		t.Errorf("role = %q, want agent", msg.Role)
	}
	if got := msg.Parts[0].Text(); got != "echo: hello" {
		t.Errorf("reply = %q", got)
	}
}

func TestTextAgent_ErrorBecomesAFailedTask(t *testing.T) {
	agent := TextAgent(func(context.Context, string) (string, error) {
		return "", errors.New("backend is down")
	})

	events := collect(t, agent.Execute(t.Context(), testContext(TextPart("hello"))))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev, ok := events[0].(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatalf("got %T, want a status update", events[0])
	}
	if ev.Status.State != StateFailed {
		t.Errorf("state = %q, want failed", ev.Status.State)
	}
	if got := ev.Status.Message.Parts[0].Text(); !strings.Contains(got, "backend is down") {
		t.Errorf("message = %q", got)
	}
}

// Cancellation on a one-shot agent is reported, not ignored: the client learns
// the task will produce nothing more instead of waiting on it.
func TestExecutorFunc_CancelReportsCanceled(t *testing.T) {
	agent := ExecutorFunc(func(context.Context, *ExecutorContext) Events {
		return func(func(Event, error) bool) {}
	})

	events := collect(t, agent.Cancel(t.Context(), testContext()))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev, ok := events[0].(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatalf("got %T, want a status update", events[0])
	}
	if ev.Status.State != StateCanceled {
		t.Errorf("state = %q, want canceled", ev.Status.State)
	}
}
