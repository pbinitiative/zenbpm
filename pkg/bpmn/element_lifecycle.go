package bpmn

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// The lifecycle of an element. Every element whose own work the engine runs
// here goes through the same phases, with execution listeners or without:
//
//	activating  the input mappings are evaluated; the start listeners run,
//	            one job after the other
//	active      the element's own work runs: the job of a task, the output
//	            mappings of a start event
//	completing  the own work is done and the output mappings are applied; the
//	            end listeners run, one job after the other
//
// and then the token leaves. An element without listeners passes the first
// and the last phase at once: it is entered and left as if there were no
// phases, and nothing is stored for it beside its job.
//
// An element which carries listeners is not entered and left in one go: the
// token stays in it across several jobs, and between them the element instance
// keeps a scope (runtime.ElementScope) saying where it stands. Such a token is
// always moved by the run loop: the completion of a job only records what the
// job did in the scope and sets the token running at the same element;
// processFlowNode then finds the scope and goes on from there
// (runElementLifecycle). A failure on the way is so an incident of the token,
// as every failure of an element, and resolving it goes on from the scope as
// well: a listener whose job was completed never runs again for the same
// element instance.
//
// Which elements run through the lifecycle is decided by
// runsThroughElementLifecycle, and which of them carry listeners the engine
// runs by bpmn20.TProcess.ListenerEventTypesRunOn. An element type is added to
// both when its own work is moved into executeElement, and its completion
// then ends in finishWorkOfElement.

// runsThroughElementLifecycle reports whether the engine runs the element
// through the lifecycle: a start event, an element whose own work is one job,
// and any element whose id an element with listeners shares, so that the
// refusal of the ambiguous id is raised for every token at that id.
func runsThroughElementLifecycle(instance runtime.ProcessInstance, element bpmn20.FlowNode) bool {
	if _, isStartEvent := element.(*bpmn20.TStartEvent); isStartEvent || bpmn20.WorkIsOneJob(element) {
		return true
	}
	return processOf(instance).ListenerIDIsAmbiguous(element.GetId())
}

// processOf answers the model of the process the instance runs.
func processOf(instance runtime.ProcessInstance) *bpmn20.TProcess {
	return &instance.ProcessInstance().Definition.Definitions.Process
}

// runElementLifecycle moves the token on in its element until it waits for a
// job or leaves the element: from the beginning for a token which enters the
// element, and from where the scope of the element instance says it stands
// for a token which waited in it before.
func (engine *Engine) runElementLifecycle(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
	scope, waitedBefore, err := engine.scopeOfElement(ctx, instance, element, token.ElementInstanceKey)
	if err != nil {
		// the storage failed, not the element: whoever set the token running,
		// such as the completion of a listener's job, must not take the
		// incident this raises for the outcome of the process
		return nil, markTechnicalFailure(err)
	}
	if !waitedBefore {
		if scope, err = engine.activateElement(ctx, batch, instance, element, token); err != nil {
			return nil, err
		}
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
				return engine.waitInElement(ctx, batch, instance, element, token, scope)
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
// Nothing is stored for the scope here: that of an element with listeners is
// saved when the token first waits in the element.
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
			bpmn20.FlowNodeInWords(element), element.GetId())
	}
	// listeners the engine runs none of stay ignored, as in every definition
	// stored before the engine ran listeners
	if process.RunsListenersOn(element) {
		if err := process.CheckListeners(element); err != nil {
			return runtime.ElementScope{}, fmt.Errorf("%w; deploy a corrected version of the process", err)
		}
	}
	scope := runtime.NewElementScope(token)
	parent := &instance.ProcessInstance().VariableHolder
	// the history row shows the variables the element was entered with; an
	// element without input mappings of its own, such as a start event,
	// copies none
	var historyInput map[string]any
	if inputMappings, ok := bpmn20.InputMappingsOf(element); ok {
		historyInput = parent.ExecutionScopeSnapshot()
		if err := scope.ApplyInputMappings(parent, inputMappings, engine.evaluateExpression); err != nil {
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
		return false, fmt.Errorf("the engine does not run the own work of %s %s through the element lifecycle", element.GetType(), element.GetId())
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
	return engine.waitInElement(ctx, batch, instance, element, token, scope)
}

// waitInElement holds the token in its element, until the completion of a job
// sets it running again or lets it leave. The scope of an element with
// listeners is stored for that time; an element without any waits for its own
// job only, whose completion needs none (see finishWorkOfElement).
func (engine *Engine) waitInElement(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
	scope runtime.ElementScope,
) ([]runtime.ExecutionToken, error) {
	if keepsScope(instance, element) {
		if err := batch.SaveElementScope(ctx, scope); err != nil {
			return nil, fmt.Errorf("failed to save the scope of element instance %d: %w", scope.ElementInstanceKey, err)
		}
	}
	token.State = runtime.TokenStateWaiting
	return []runtime.ExecutionToken{token}, nil
}

// finishWorkOfElement ends the own work of an element, which a completion
// outside the run loop reports, such as that of the element's job: the history
// row gets the outputs the work wrote to the instance, and the element moves
// on to its end listeners, which see what the work was completed with. A token
// in an element with listeners is set running for that, and the run loop goes
// on from the scope; a token in an element without any leaves at once, and so
// does one which entered its element before the engine ran its listeners.
func (engine *Engine) finishWorkOfElement(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	token runtime.ExecutionToken,
	completedWith map[string]any,
	outputs map[string]any,
) ([]runtime.ExecutionToken, error) {
	if err := saveFlowElementOutputs(ctx, batch, instance, element, token, outputs, nil); err != nil {
		return nil, fmt.Errorf("failed to write the outputs of %s %s: %w", element.GetType(), element.GetId(), err)
	}
	scope, waitsInScope, err := engine.scopeOfElement(ctx, instance, element, token.ElementInstanceKey)
	if err != nil {
		return nil, err
	}
	if !waitsInScope {
		return engine.leaveElement(ctx, batch, instance, element, token)
	}
	scope.MergeLocalVariables(completedWith)
	scope.Phase, scope.ListenerIndex = runtime.ElementPhaseCompleting, 0
	if err := batch.SaveElementScope(ctx, scope); err != nil {
		return nil, fmt.Errorf("failed to save the scope of element instance %d: %w", scope.ElementInstanceKey, err)
	}
	token.State = runtime.TokenStateRunning
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
	if keepsScope(instance, element) {
		if err := batch.DeleteElementScope(ctx, token.ElementInstanceKey); err != nil {
			return nil, fmt.Errorf("failed to delete the scope of %s %s: %w", element.GetType(), element.GetId(), err)
		}
	}
	return engine.handleElementTransition(ctx, batch, instance, element, token)
}

// completeListenerJob records the completion of a listener's job in the scope
// of its element instance: the variables of the completion become variables
// of the element, which its later jobs see, and the scope moves on to the next
// listener. Those of a start listener stay there, for the element's output
// mappings to pick from; those of an end listener, which runs after the output
// mappings, are written to the instance as well (publishToInstance). The token
// is answered running at its element, for the run loop to go on from the scope.
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
	if scope.Phase == runtime.ElementPhaseCompleting {
		if err := engine.publishToInstance(ctx, batch, instance, job, variables); err != nil {
			return runtime.ExecutionToken{}, err
		}
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

// publishToInstance writes the variables an end listener's job was completed
// with to the instance its element runs in, every one of them: no output
// mapping comes after an end listener which could name them. The history row
// of the element shows them among its outputs, beside what its output mappings
// wrote. The caller saves the instance.
func (engine *Engine) publishToInstance(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	job runtime.Job,
	variables map[string]any,
) error {
	if len(variables) == 0 {
		return nil
	}
	history, err := engine.persistence.GetFlowElementInstanceByKey(ctx, job.ElementInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find flow element instance %d of listener job %d: %w", job.ElementInstanceKey, job.Key, err)
	}
	outputs := make(map[string]any, len(history.OutputVariables)+len(variables))
	maps.Copy(outputs, history.OutputVariables)
	maps.Copy(outputs, variables)
	history.OutputVariables = outputs
	if err := batch.UpdateOutputFlowElementInstance(ctx, history); err != nil {
		return fmt.Errorf("failed to write the outputs of %s %s: %w", history.ElementType, history.ElementId, err)
	}
	instance.ProcessInstance().VariableHolder.SetLocalVariables(variables)
	return nil
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

// keepsScope reports whether an instance of the element keeps a scope in the
// storage while its token waits in it: only an element which carries
// listeners the engine runs does, so that every other element costs no read
// and no write of one.
func keepsScope(instance runtime.ProcessInstance, element bpmn20.FlowNode) bool {
	return processOf(instance).RunsListenersOn(element)
}

// scopeOfElement answers the scope the element instance keeps. The second
// answer is false for an element which keeps none (keepsScope), which is
// never asked for in the storage, for an element instance a token only
// enters, and for one which was entered before the engine ran its listeners.
func (engine *Engine) scopeOfElement(
	ctx context.Context,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	elementInstanceKey int64,
) (runtime.ElementScope, bool, error) {
	if element == nil || !keepsScope(instance, element) {
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

// errInputMappingsDoNotEvaluate is wrapped into the error of
// refreshElementScope and of reevaluateJobInputVariables when an input
// mapping of the element fails, to tell it from a failure of the storage: the
// first one lasts until the variables are corrected, the second one may be
// gone when the call is repeated.
var errInputMappingsDoNotEvaluate = errors.New("failed to evaluate the input mappings")

// inputMappingsDoNotEvaluate is the error of an element whose input mappings
// fail when they are evaluated again.
func inputMappingsDoNotEvaluate(element bpmn20.FlowNode, err error) error {
	return fmt.Errorf("%w of %s %s: %w", errInputMappingsDoNotEvaluate, element.GetType(), element.GetId(), err)
}

// refreshElementScope evaluates the input mappings of the element again, as
// the resolution of an incident does for every element, and answers the scope
// with their new results; what the jobs of the element added stays. The
// history row of the element shows the variables its own work got its inputs
// from, so it follows them until that work is done: an element whose end
// listeners run keeps the row as its own job left it. The second answer is
// false for an element instance which keeps no scope.
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
	if inputMappings, ok := bpmn20.InputMappingsOf(element); ok {
		parent := &instance.ProcessInstance().VariableHolder
		if err := scope.ApplyInputMappings(parent, inputMappings, engine.evaluateExpression); err != nil {
			return runtime.ElementScope{}, false, inputMappingsDoNotEvaluate(element, err)
		}
		if scope.Phase != runtime.ElementPhaseCompleting {
			if err := engine.saveHistoryInput(ctx, batch, elementInstanceKey, parent.ExecutionScopeSnapshot()); err != nil {
				return runtime.ElementScope{}, false, err
			}
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
