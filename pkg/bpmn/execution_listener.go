package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// The run time of execution listeners. A listener's job is created and
// handed out as the job of a task (createInternalTask with a
// bpmn20.ListenerTask); what differs is when it is created and what its
// completion does, and both live here: an "end" listener is created when its
// element's own work is done and holds the token on the element
// (holdTokenAtEndListener); its completion ends the element and lets the
// token leave it (completeListenerElement). The "end" listener of a start
// event is the only one the engine runs so far (see
// bpmn20.TProcess.RunnableListenerEventTypes).

// runnableListenerOf answers the listener of the given event type which the
// engine runs on the element in this instance, or nil. Where a listener runs
// is decided by bpmn20.TProcess.ListenerEventTypesRunOn alone, which the
// deployment checks as well; this adds only that the instance runs its
// process's own listeners: the start of an embedded or multi-instance child
// runs none.
func runnableListenerOf(instance runtime.ProcessInstance, element bpmn20.FlowNode, eventType string) *bpmn20.ListenerTask {
	if instance.Type() != runtime.ProcessTypeDefault && instance.Type() != runtime.ProcessTypeCallActivity {
		return nil
	}
	listeners := instance.ProcessInstance().Definition.Definitions.Process.RunnableListenersOf(element, eventType)
	if len(listeners) == 0 {
		return nil
	}
	return bpmn20.NewListenerTask(element, listeners[0])
}

// holdTokenAtEndListener holds the token on the element behind the job of its
// "end" listener. The element's outputs, already evaluated by the caller, are
// written first to its history row, which processFlowNode saved before, so
// the row shows them while the job waits; the row is completed, and the token
// moves on, when the job is completed (completeListenerElement). The row
// copies no input variables: the job carries them.
func (engine *Engine) holdTokenAtEndListener(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	listener *bpmn20.ListenerTask,
	currentToken runtime.ExecutionToken,
	outputVariables map[string]any,
) ([]runtime.ExecutionToken, error) {
	if outputVariables != nil {
		if err := saveFlowElementOutputs(ctx, batch, instance, listener.Element(), currentToken, outputVariables, nil); err != nil {
			return nil, fmt.Errorf("failed to write the outputs of %s %s: %w", listener.GetType(), listener.GetId(), err)
		}
	}
	jobVariables := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	state, err := engine.createInternalTask(ctx, batch, instance, listener, currentToken, jobVariables)
	switch state {
	case runtime.ActivityStateActive:
		currentToken.State = runtime.TokenStateWaiting
		return []runtime.ExecutionToken{currentToken}, nil
	case runtime.ActivityStateFailed:
		currentToken.State = runtime.TokenStateFailed
		return []runtime.ExecutionToken{currentToken}, fmt.Errorf("failed to create the job of the %s listener of %s %s: %w",
			listener.EventType(), listener.GetType(), listener.GetId(), err)
	default:
		return nil, fmt.Errorf("unexpected state %s of the job of the %s listener of %s %s: %w",
			state, listener.EventType(), listener.GetType(), listener.GetId(), err)
	}
}

// completeListenerElement does what the completion of a listener's job does
// to the listener's element, and answers the element the token leaves by.
// The completion's variables change nothing: a listener has no output
// mappings.
func (engine *Engine) completeListenerElement(ctx context.Context, batch *EngineBatch, job runtime.Job, listener *bpmn20.ListenerTask) (bpmn20.FlowNode, error) {
	switch listener.EventType() {
	case extensions.ExecutionListenerEventTypeEnd:
		// the element's history row keeps the outputs written when its listener's job was created
		if err := batch.CompleteFlowElementInstance(ctx, job.Token.ElementInstanceKey, time.Now()); err != nil {
			return nil, fmt.Errorf("failed to complete %s %s behind its end listener's job %d: %w", listener.GetType(), listener.GetId(), job.Key, err)
		}
		return listener.Element(), nil
	default:
		return nil, fmt.Errorf("job %d belongs to an execution listener of event type %q on %s %s, which the engine does not run",
			job.Key, listener.EventType(), listener.GetType(), listener.GetId())
	}
}

// ensureStartEventIsUnambiguous refuses to run a start event whose id another
// flow node shares where one of them carries a listener the engine runs (see
// bpmn20.TProcess.ListenerIDIsAmbiguous). A deployment refuses such a model; a
// definition stored before is imported without that check, and its token
// could run the listener of the other element, or none. The refusal raises an
// incident instead.
func ensureStartEventIsUnambiguous(instance runtime.ProcessInstance, startEvent *bpmn20.TStartEvent) error {
	if instance.ProcessInstance().Definition.Definitions.Process.ListenerIDIsAmbiguous(startEvent.GetId()) {
		return fmt.Errorf("start event id=%q shares its id with another flow node, and one of them carries an execution listener, so the engine cannot tell which one the token is at; deploy a corrected version of the process",
			startEvent.GetId())
	}
	return nil
}

// taskOfJob answers the task a job belongs to: a listener of its element, or
// the element's own task.
func taskOfJob(instance runtime.ProcessInstance, job runtime.Job) bpmn20.InternalTask {
	return instance.ProcessInstance().Definition.Definitions.Process.TaskOfJob(job.ElementId, bpmn20.ListenerEventTypeOfJob(job.ElementType))
}

// isListenerJob reports whether the job is the job of an execution listener.
func isListenerJob(job runtime.Job) bool {
	return bpmn20.ListenerEventTypeOfJob(job.ElementType) != ""
}

// ensureListenerCanRun refuses to create a listener's job where a deployment
// refuses the listeners of its element (see bpmn20.TProcess.CheckListeners).
// A definition stored before is imported without those checks: a second
// listener would be dropped without a word; the token could not leave an
// element without an outgoing flow, so every completion of the job would
// fail; a job of type "" or of an expression would wait for a worker which
// never comes; and the literals-only rule promises that creating a listener's
// job never evaluates an expression. The refusal raises an incident instead.
func ensureListenerCanRun(instance runtime.ProcessInstance, listener *bpmn20.ListenerTask) error {
	if err := instance.ProcessInstance().Definition.Definitions.Process.CheckListeners(listener.Element()); err != nil {
		return fmt.Errorf("%w; deploy a corrected version of the process", err)
	}
	return nil
}
