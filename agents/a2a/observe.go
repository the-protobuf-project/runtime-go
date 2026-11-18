package a2a

import (
	"context"
	"iter"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// observeExecutor makes every task execution and cancellation one operation
// of component "agents", backend "a2a", named execute or cancel. The task and
// context ids are span attributes, never labels. The events the agent emits
// are its records read; an error it yields fails the operation, and a caller
// that stops reading early ends it as cancelled. Without signals it returns
// next.
func observeExecutor(next a2asrv.AgentExecutor, signals telemetry.Signals) a2asrv.AgentExecutor {
	if !signals.Enabled() {
		return next
	}
	return &observedExecutor{next: next, obs: signals.Observer("agents")}
}

type observedExecutor struct {
	next a2asrv.AgentExecutor
	obs  *telemetry.Observer
}

func (e *observedExecutor) Execute(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return e.observe(ctx, "execute", ec, e.next.Execute)
}

func (e *observedExecutor) Cancel(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return e.observe(ctx, "cancel", ec, e.next.Cancel)
}

// observe runs one call of the executor as an operation, for as long as its
// events are read.
func (e *observedExecutor) observe(ctx context.Context, operation string, ec *a2asrv.ExecutorContext,
	call func(context.Context, *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error]) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		attrs := map[string]any{"rpc.system": "a2a"}
		if ec != nil {
			attrs["a2a.task.id"] = string(ec.TaskID)
			attrs["a2a.context.id"] = ec.ContextID
		}
		opCtx, op := e.obs.Start(ctx, "a2a", operation, telemetry.SpanKindServer, telemetry.WithSpanAttributes(attrs))
		outcome := telemetry.OutcomeOK
		events := 0
		defer func() {
			op.ReadRecords(events)
			op.Finish(outcome)
		}()
		for ev, err := range call(opCtx, ec) {
			if err != nil {
				outcome = telemetry.OutcomeOf(err)
				if outcome.IsError() {
					op.Span().SetError(err)
				}
			} else {
				events++
			}
			if !yield(ev, err) {
				if err == nil {
					outcome = telemetry.OutcomeCancelled
				}
				return
			}
		}
	}
}
