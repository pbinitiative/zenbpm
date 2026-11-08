package bpmn

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// The run time of execution listeners. A listener's job is created and
// handed out as the job of a task (createInternalTask with a
// bpmn20.ListenerTask); what differs is when it is created and what its
// completion does, and both live here:
//
//   - an "end" listener is created when its element's own work is done, and
//     holds the token on the element (holdTokenAtEndListener); its completion
//     ends the element and lets the token leave it (completeListenerElement).
//
// The "end" listener of a start event is the only one the engine runs so far
// (see bpmn20.TProcess.RunnableListenerEventTypes). Another "end" listener of
// a flow node adds its position there and calls holdTokenAtEndListener where
// the element's work is done. Other listeners need more than another case:
// a "start" listener holds the token before the element's work, so its
// completion has to resume that work rather than leave the element, and a
// listener on the process has no flow node to own it or for a token to leave
// (an ExecutionListenerOwner is a flow node, and completeListenerElement
// answers one). Both, and a listener on an element which also creates task
// jobs, need the listener's event type stored with the job (see the TODO on
// bpmn20.ListenerEventTypeOfJob).

// runnableListenerOf answers the listener of the given event type which the
// engine runs on the owner in this instance, or nil. Where a listener runs is
// decided by bpmn20.TProcess.RunnableListenerEventTypes alone, which the
// deployment checks as well; this adds only that the instance runs its
// process's own listeners: the start of an embedded or multi-instance child
// runs none.
func runnableListenerOf(instance runtime.ProcessInstance, owner bpmn20.ExecutionListenerOwner, eventType string) *bpmn20.ListenerTask {
	if instance.Type() != runtime.ProcessTypeDefault && instance.Type() != runtime.ProcessTypeCallActivity {
		return nil
	}
	if !slices.Contains(instance.ProcessInstance().Definition.Definitions.Process.RunnableListenerEventTypes(owner), eventType) {
		return nil
	}
	return bpmn20.ListenerTaskOf(owner, eventType)
}

// holdTokenAtEndListener holds the token on the element behind the job of its
// "end" listener. The element's outputs, already evaluated by the caller, are
// written first to its history row, which processFlowNode saved before, so
// the row shows them while the job waits; the row is completed, and the token
// moves on, when the job is completed (completeListenerElement).
func (engine *Engine) holdTokenAtEndListener(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	listener *bpmn20.ListenerTask,
	currentToken runtime.ExecutionToken,
	outputVariables map[string]any,
) ([]runtime.ExecutionToken, error) {
	if err := batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
		ElementId:          listener.GetId(),
		ElementType:        string(listener.GetType()),
		ExecutionTokenKey:  currentToken.Key,
		OutputVariables:    outputVariables,
	}); err != nil {
		return nil, fmt.Errorf("failed to write the outputs of %s %s: %w", listener.GetType(), listener.GetId(), err)
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

// taskOfJob answers the task a job belongs to: a listener of its element, or
// the element's own task.
func taskOfJob(instance runtime.ProcessInstance, job runtime.Job) bpmn20.InternalTask {
	return instance.ProcessInstance().Definition.Definitions.Process.TaskOfJob(job.ElementId, bpmn20.ListenerEventTypeOfJob(job.ElementType))
}

// isListenerJob reports whether the job is the job of an execution listener.
func isListenerJob(job runtime.Job) bool {
	return bpmn20.ListenerEventTypeOfJob(job.ElementType) != ""
}

// ensureListenerTypeIsLiteral refuses to create a listener's job without a
// literal type. A deployment refuses such a listener; a definition stored
// before is imported without that check, and a job of type "" or of an
// expression would wait for a worker which never comes.
func ensureListenerTypeIsLiteral(listener *bpmn20.ListenerTask) error {
	taskType := listener.GetTaskType()
	if taskType == "" || extensions.IsExpression(taskType) {
		return fmt.Errorf("execution listener eventType=%q on %s id=%q has the type %q: a listener needs the job type as a literal; deploy a corrected version of the process",
			listener.EventType(), listener.GetType(), listener.GetId(), listener.GetTaskType())
	}
	return nil
}
