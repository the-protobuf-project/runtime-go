package a2a

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// fakeExecutor yields its events, then its error.
type fakeExecutor struct {
	events []a2a.Event
	err    error
}

func (f fakeExecutor) Execute(context.Context, *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		for _, ev := range f.events {
			if !yield(ev, nil) {
				return
			}
		}
		if f.err != nil {
			yield(nil, f.err)
		}
	}
}

func (f fakeExecutor) Cancel(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return f.Execute(ctx, ec)
}

func message() a2a.Event { return a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("hi")) }

func TestATaskExecutionIsOneOperationCountingItsEvents(t *testing.T) {
	rec := recorder.New()
	ex := observeExecutor(fakeExecutor{events: []a2a.Event{message(), message()}}, rec.Signals())
	for range ex.Execute(context.Background(), &a2asrv.ExecutorContext{TaskID: "t-1", ContextID: "c-1"}) {
	}
	s := rec.Span("agents.execute")
	if s == nil || s.Failed() || s.Attr("a2a.task.id") != "t-1" {
		t.Fatalf("span %+v", s)
	}
	if got := rec.Sum("runtime.records.read"); got != 2 {
		t.Errorf("events read %v, want 2", got)
	}
}

func TestAnExecutionErrorFailsTheOperation(t *testing.T) {
	rec := recorder.New()
	boom := errors.New("agent crashed")
	ex := observeExecutor(fakeExecutor{err: boom}, rec.Signals())
	for range ex.Execute(context.Background(), &a2asrv.ExecutorContext{TaskID: "t-2"}) {
	}
	if s := rec.Span("agents.execute"); s == nil || !errors.Is(s.Err(), boom) {
		t.Errorf("span %+v", s)
	}
}

func TestAReaderStoppingEarlyEndsTheOperationCancelled(t *testing.T) {
	rec := recorder.New()
	ex := observeExecutor(fakeExecutor{events: []a2a.Event{message(), message()}}, rec.Signals())
	for range ex.Cancel(context.Background(), &a2asrv.ExecutorContext{TaskID: "t-3"}) {
		break
	}
	if got := rec.Outcomes()["cancel"]; got != "cancelled" {
		t.Errorf("outcome %q, want cancelled", got)
	}
}
