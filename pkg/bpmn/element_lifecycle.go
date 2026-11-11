package bpmn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// The lifecycle of an element which carries execution listeners. Such an
// element is not entered and left in one go: the token stays in it across
// several jobs, and between them the element instance keeps a scope
// (runtime.ElementScope) saying where it stands:
//
//	activating  the input mappings are evaluated; the start listeners run,
//	            one job after the other
//	active      the element's own work runs: the job of a task, the output
//	            mappings of a start event
//	completing  the own work is done and the output mappings are applied; the
//	            end listeners run, one job after the other
//
// and then the token leaves. A token is always moved by the run loop: the
// completion of a job only records what the job did in the scope and sets the
// token running at the same element; processFlowNode then finds the scope and
// goes on from there (runElementLifecycle). A failure on the way is so an
// incident of the token, as every failure of an element, and resolving it
// goes on from the scope as well: a listener whose job was completed never
// runs again for the same element instance.
//
// Which elements carry listeners the engine runs is decided by
// bpmn20.TProcess.ListenerEventTypesRunOn. An element without one never
// comes here, and no scope is stored for it.

// hasElementLifecycle reports whether the element is run through the
// lifecycle: one which carries a listener the engine runs, and one whose id
// such an element shares, so that the refusal of the ambiguous id is raised
// for every token at that id.
func hasElementLifecycle(instance runtime.ProcessInstance, element bpmn20.FlowNode) bool {
	process := processOf(instance)
	return process.RunsListenersOn(element) || process.ListenerIDIsAmbiguous(element.GetId())
}

// processOf answers the model of the process the instance runs.
func processOf(instance runtime.ProcessInstance) *bpmn20.TProcess {
	return &instance.ProcessInstance().Definition.Definitions.Process
}

// runElementLifecycle moves the token on in its element, from where the scope
// of the element instance says it stands, until it waits for a job or leaves
// the element.
func (engine *Engine) runElementLifecycle(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
	scope, err := engine.persistence.FindElementScope(ctx, token.ElementInstanceKey)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		if scope, err = engine.activateElement(ctx, batch, instance, element, token); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, fmt.Errorf("failed to read the scope of %s %s: %w", element.GetType(), element.GetId(), err)
	}
	process := processOf(instance)
	for {
		switch scope.Phase {
		case runtime.ElementPhaseActivating:
			startListeners := process.RunnableListenersOf(element, extensions.ExecutionListenerEventTypeStart)
			if scope.ListenerIndex < len(startListeners) {
				return engine.waitForListener(ctx, batch, instance, element, token, scope, startListeners[scope.ListenerIndex])
			}
			scope.Phase, scope.ListenerIndex = runtime.ElementPhaseActive, 0
		case runtime.ElementPhaseActive:
			waits, err := engine.executeElement(ctx, batch, instance, element, token, &scope)
			if err != nil {
				return nil, err
			}
			if waits {
				return engine.waitInElement(ctx, batch, token, scope)
			}
			scope.Phase, scope.ListenerIndex = runtime.ElementPhaseCompleting, 0
		case runtime.ElementPhaseCompleting:
			endListeners := process.RunnableListenersOf(element, extensions.ExecutionListenerEventTypeEnd)
			if scope.ListenerIndex < len(endListeners) {
				return engine.waitForListener(ctx, batch, instance, element, token, scope, endListeners[scope.ListenerIndex])
			}
			return engine.leaveElement(ctx, batch, instance, element, token)
		default:
			return nil, fmt.Errorf("the scope of %s %s is in the unknown phase %d", element.GetType(), element.GetId(), scope.Phase)
		}
	}
}

// activateElement enters the element: it refuses listeners a deployment
// refuses, which a definition stored without that check may carry, writes the
// element's history row, and evaluates the input mappings into a new scope.
// Nothing is stored for the scope here: it is saved when the token first
// waits in the element.
func (engine *Engine) activateElement(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
) (runtime.ElementScope, error) {
	process := processOf(instance)
	if process.ListenerIDIsAmbiguous(element.GetId()) {
		return runtime.ElementScope{}, fmt.Errorf("%s id=%q shares its id with another flow node, and one of them carries an execution listener, so the engine cannot tell which one the token is at; deploy a corrected version of the process",
			strings.ToLower(strings.ReplaceAll(string(element.GetType()), "_", " ")), element.GetId())
	}
	if err := process.CheckListeners(element); err != nil {
		return runtime.ElementScope{}, fmt.Errorf("%w; deploy a corrected version of the process", err)
	}
	scope := runtime.NewElementScope(token)
	parent := &instance.ProcessInstance().VariableHolder
	// the history row shows the variables the element was entered with; an
	// element without input mappings of its own, such as a start event,
	// copies none
	var historyInput map[string]any
	if mapped, ok := element.(interface {
		GetInputMapping() []extensions.TIoMapping
	}); ok {
		historyInput = parent.ExecutionScopeSnapshot()
		if err := scope.ApplyInputMappings(parent, mapped.GetInputMapping(), engine.evaluateExpression); err != nil {
			return runtime.ElementScope{}, fmt.Errorf("failed to evaluate input variables: %w", err)
		}
	}
	err := batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                token.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().Key,
		ElementId:          element.GetId(),
		ElementType:        string(element.GetType()),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  token.Key,
		InputVariables:     historyInput,
	})
	if err != nil {
		return runtime.ElementScope{}, fmt.Errorf("failed to save flow element instance of %s %s: %w", element.GetType(), element.GetId(), err)
	}
	return scope, nil
}

// executeElement does the element's own work, between its start and its end
// listeners, and reports whether the token waits for it. What the work adds to
// the scope is written into it.
func (engine *Engine) executeElement(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
	scope *runtime.ElementScope,
) (waits bool, err error) {
	switch element := element.(type) {
	case *bpmn20.TStartEvent:
		outputVariables, err := engine.applyStartEventOutputMappings(instance, element)
		if err != nil {
			return false, fmt.Errorf("failed to evaluate StartEvent output mappings %d: %w", token.ElementInstanceKey, err)
		}
		// the history row shows the outputs while the end listeners run
		if outputVariables != nil {
			if err := saveFlowElementOutputs(ctx, batch, instance, element, token, outputVariables, nil); err != nil {
				return false, fmt.Errorf("failed to write the outputs of %s %s: %w", element.GetType(), element.GetId(), err)
			}
		}
		return false, nil
	case bpmn20.InternalTask:
		// the job carries the variables of the element: its input mappings
		// were evaluated when it was entered, and its start listeners added theirs
		variables := scope.VariableHolder(&instance.ProcessInstance().VariableHolder)
		job, err := engine.createJob(ctx, batch, instance, element, token, variables, jobCreation{leavesHistoryOpen: true})
		if err != nil {
			return false, fmt.Errorf("failed to process %s %d: %w", element.GetType(), token.ElementInstanceKey, err)
		}
		switch job.State {
		case runtime.ActivityStateActive:
			// the boundary events of an activity watch its own work only,
			// neither its start nor its end listeners
			if _, isActivity := element.(bpmn20.Activity); isActivity {
				waiting := token
				waiting.State = runtime.TokenStateWaiting
				if err := engine.createBoundaryEventSubscriptions(ctx, batch, waiting, instance, element); err != nil {
					return false, fmt.Errorf("failed to process boundary events for %s %d: %w", element.GetType(), token.ElementInstanceKey, err)
				}
			}
			return true, nil
		case runtime.ActivityStateCompleted:
			// a handler in the engine completed the job at once
			scope.MergeLocalVariables(job.OutputVariables)
			return false, nil
		default:
			return false, fmt.Errorf("unexpected state %s of the job of %s %s", job.State, element.GetType(), element.GetId())
		}
	default:
		return false, fmt.Errorf("%s %s carries execution listeners, but the engine does not run the element through its lifecycle", element.GetType(), element.GetId())
	}
}

// waitForListener creates the job of the listener the scope stands at, and
// holds the token in the element behind it. The job carries the variables of
// the element (runtime.ElementScope.VariableHolder).
func (engine *Engine) waitForListener(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
	scope runtime.ElementScope,
	listener extensions.TExecutionListener,
) ([]runtime.ExecutionToken, error) {
	task := bpmn20.NewListenerTask(element, listener)
	variables := scope.VariableHolder(&instance.ProcessInstance().VariableHolder)
	if _, err := engine.createJob(ctx, batch, instance, task, token, variables, jobCreation{}); err != nil {
		return nil, fmt.Errorf("failed to create the job of the %s listener of %s %s: %w", listener.EventType, element.GetType(), element.GetId(), err)
	}
	return engine.waitInElement(ctx, batch, token, scope)
}

// waitInElement stores the scope and holds the token in its element, until
// the completion of a job sets it running again.
func (engine *Engine) waitInElement(
	ctx context.Context,
	batch *EngineBatch,
	token runtime.ExecutionToken,
	scope runtime.ElementScope,
) ([]runtime.ExecutionToken, error) {
	if err := batch.SaveElementScope(ctx, scope); err != nil {
		return nil, fmt.Errorf("failed to save the scope of element instance %d: %w", scope.ElementInstanceKey, err)
	}
	token.State = runtime.TokenStateWaiting
	return []runtime.ExecutionToken{token}, nil
}

// leaveElement ends the element instance and lets the token leave it by its
// outgoing sequence flows. It is the one place which deletes the scope: the
// scope of a token which is cancelled in its element stays until the history
// of its instance is cleaned up.
func (engine *Engine) leaveElement(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
	if err := batch.CompleteFlowElementInstance(ctx, token.ElementInstanceKey, time.Now()); err != nil {
		return nil, fmt.Errorf("failed to complete %s %s: %w", element.GetType(), element.GetId(), err)
	}
	if err := batch.DeleteElementScope(ctx, token.ElementInstanceKey); err != nil {
		return nil, fmt.Errorf("failed to delete the scope of %s %s: %w", element.GetType(), element.GetId(), err)
	}
	return engine.handleElementTransition(ctx, batch, instance, element, token)
}

// completeListenerJob records the completion of a listener's job in the scope
// of its element instance: the variables of the completion become variables
// of the element, which its later jobs see and no other scope does, and the
// scope moves on to the next listener. The token is answered running at its
// element, for the run loop to go on from the scope.
func (engine *Engine) completeListenerJob(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	job runtime.Job,
	variables map[string]any,
) (runtime.ExecutionToken, error) {
	scope, err := engine.persistence.FindElementScope(ctx, job.ElementInstanceKey)
	if err != nil {
		return runtime.ExecutionToken{}, fmt.Errorf("failed to read the scope of the element instance %d of listener job %d: %w", job.ElementInstanceKey, job.Key, err)
	}
	if waitedFor := engine.listenerTheScopeWaitsFor(instance, job.ElementId, scope); waitedFor == nil ||
		waitedFor.EventType != job.ListenerEventType || strings.TrimSpace(waitedFor.Type) != job.Type {
		return runtime.ExecutionToken{}, newEngineErrorf("job %d of the %s listener %q on %s is not the one its element instance %d waits for",
			job.Key, job.ListenerEventType, job.Type, job.ElementId, job.ElementInstanceKey)
	}
	scope.MergeLocalVariables(variables)
	scope.ListenerIndex++
	if err := batch.SaveElementScope(ctx, scope); err != nil {
		return runtime.ExecutionToken{}, fmt.Errorf("failed to save the scope of element instance %d: %w", scope.ElementInstanceKey, err)
	}
	token := job.Token
	token.State = runtime.TokenStateRunning
	return token, nil
}

// listenerTheScopeWaitsFor answers the listener whose job the element
// instance waits for, or nil where it waits for none.
func (engine *Engine) listenerTheScopeWaitsFor(instance runtime.ProcessInstance, elementID string, scope runtime.ElementScope) *extensions.TExecutionListener {
	process := processOf(instance)
	element := process.GetFlowNodeById(elementID)
	if element == nil {
		return nil
	}
	var listeners []extensions.TExecutionListener
	switch scope.Phase {
	case runtime.ElementPhaseActivating:
		listeners = process.RunnableListenersOf(element, extensions.ExecutionListenerEventTypeStart)
	case runtime.ElementPhaseCompleting:
		listeners = process.RunnableListenersOf(element, extensions.ExecutionListenerEventTypeEnd)
	}
	if scope.ListenerIndex >= len(listeners) {
		return nil
	}
	return &listeners[scope.ListenerIndex]
}

// scopeOfElement answers the scope the element instance keeps. The second
// answer is false for an element without listeners the engine runs, which is
// never asked for in the storage, and for an element instance which was
// entered before the engine ran its listeners.
func (engine *Engine) scopeOfElement(
	ctx context.Context,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	elementInstanceKey int64,
) (runtime.ElementScope, bool, error) {
	if element == nil || !processOf(instance).RunsListenersOn(element) {
		return runtime.ElementScope{}, false, nil
	}
	scope, err := engine.persistence.FindElementScope(ctx, elementInstanceKey)
	if errors.Is(err, storage.ErrNotFound) {
		return runtime.ElementScope{}, false, nil
	}
	if err != nil {
		return runtime.ElementScope{}, false, fmt.Errorf("failed to read the scope of element instance %d: %w", elementInstanceKey, err)
	}
	return scope, true, nil
}

// refreshElementScope evaluates the input mappings of the element again, as
// the resolution of an incident does for every element, and answers the scope
// with their new results; what the jobs of the element added stays. The
// second answer is false for an element instance which keeps no scope.
func (engine *Engine) refreshElementScope(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	elementID string,
	elementInstanceKey int64,
) (runtime.ElementScope, bool, error) {
	element := processOf(instance).GetFlowNodeById(elementID)
	scope, keepsScope, err := engine.scopeOfElement(ctx, instance, element, elementInstanceKey)
	if err != nil || !keepsScope {
		return runtime.ElementScope{}, false, err
	}
	if mapped, ok := element.(interface {
		GetInputMapping() []extensions.TIoMapping
	}); ok {
		if err := scope.ApplyInputMappings(&instance.ProcessInstance().VariableHolder, mapped.GetInputMapping(), engine.evaluateExpression); err != nil {
			return runtime.ElementScope{}, false, fmt.Errorf("failed to evaluate input variables of %s %s: %w", element.GetType(), element.GetId(), err)
		}
	}
	if err := batch.SaveElementScope(ctx, scope); err != nil {
		return runtime.ElementScope{}, false, fmt.Errorf("failed to save the scope of element instance %d: %w", elementInstanceKey, err)
	}
	return scope, true, nil
}

// taskOfJob answers the task a job belongs to: a listener of its element, or
// the element's own task. The answer is nil when the element no longer has
// what created the job.
func taskOfJob(instance runtime.ProcessInstance, job runtime.Job) bpmn20.InternalTask {
	if isListenerJob(job) {
		return processOf(instance).ListenerTaskOf(job.ElementId, job.ListenerEventType, job.Type)
	}
	return processOf(instance).GetInternalTaskById(job.ElementId)
}

// isListenerJob reports whether the job is the job of an execution listener.
func isListenerJob(job runtime.Job) bool {
	return job.Kind == runtime.JobKindExecutionListener
}
