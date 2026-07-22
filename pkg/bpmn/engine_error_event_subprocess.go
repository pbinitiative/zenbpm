package bpmn

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// errorEventSubprocessMatch describes an error event subprocess (a subProcess with
// triggeredByEvent=true whose single start event carries an errorEventDefinition) that
// is able to catch a thrown error within the scope of instance.
type errorEventSubprocessMatch struct {
	// instance is the scope (process instance) that owns the event subprocess and that
	// will be interrupted when the event subprocess is activated.
	instance   runtime.ProcessInstance
	subProcess *bpmn20.TSubProcess
	startEvent *bpmn20.TStartEvent
}

// errorCatchTarget is the result of walking the scope hierarchy looking for an error
// handler. Exactly one of its fields is non-nil.
type errorCatchTarget struct {
	boundary        *boundaryErrorMatch
	eventSubprocess *errorEventSubprocessMatch
}

// findErrorCatchTarget walks the scope hierarchy starting at the instance/token where the
// error was thrown and returns the first matching handler. Per BPMN 2.0, at each scope the
// error code is matched against the attached error boundary events and the error event
// subprocesses at that level; the error is caught by the first matching handler in the scope
// hierarchy. The single-scope precedence (boundary over event subprocess) is shared with the
// error-end-event path via matchErrorHandlerInScope. Returns (nil, nil) when no handler matches
// anywhere in the hierarchy.
func (engine *Engine) findErrorCatchTarget(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	token runtime.ExecutionToken,
	errorCode *string,
) (*errorCatchTarget, error) {
	current := &errorEventContext{
		instance:      instance,
		token:         token,
		attachedToRef: token.ElementId,
	}

	for current != nil {
		boundaryEvent, subprocessMatch := engine.matchErrorHandlerInScope(current.instance, current.attachedToRef, errorCode)
		if boundaryEvent != nil {
			return &errorCatchTarget{
				boundary: &boundaryErrorMatch{scope: current, event: boundaryEvent},
			}, nil
		}
		if subprocessMatch != nil {
			return &errorCatchTarget{eventSubprocess: subprocessMatch}, nil
		}

		parentScope, err := engine.loadParentErrorEventContext(ctx, batch, current.instance, false)
		if err != nil {
			return nil, err
		}
		current = parentScope
	}

	return nil, nil
}

// matchErrorHandlerInScope encapsulates the single-scope BPMN precedence rule used by every error
// propagation path (thrown job errors and error end events alike): within one scope an error boundary
// event attached to the throwing activity takes precedence over a scope-level error event subprocess.
// It returns the matching boundary event, or the matching error event subprocess, or neither (both nil).
// Centralizing this decision keeps the job and error-end-event walks from drifting apart.
func (engine *Engine) matchErrorHandlerInScope(
	instance runtime.ProcessInstance,
	attachedToRef string,
	errorCode *string,
) (*bpmn20.TBoundaryEvent, *errorEventSubprocessMatch) {
	if boundaryEvent := engine.findMatchingBoundaryErrorEventInCurrentProcessInstance(instance, attachedToRef, errorCode); boundaryEvent != nil {
		return boundaryEvent, nil
	}
	if match := engine.findMatchingErrorEventSubprocessInScope(instance, errorCode); match != nil {
		return nil, match
	}
	return nil, nil
}

// findMatchingErrorEventSubprocessInScope looks for an error event subprocess defined directly
// in the flow elements container of the given instance scope whose start event matches the
// thrown error code. An error event subprocess referencing a specific error code is prioritized
// over a catch-all error event subprocess (one whose start event has no errorRef) in the same scope.
func (engine *Engine) findMatchingErrorEventSubprocessInScope(instance runtime.ProcessInstance, errorCode *string) *errorEventSubprocessMatch {
	container := errorEventSubprocessScopeContainer(instance)
	if container == nil {
		return nil
	}

	definitions := &instance.ProcessInstance().Definition.Definitions
	var catchAll *errorEventSubprocessMatch

	for _, subProcess := range bpmn20.FindEventSubProcesses(container) {
		startEvent, isCatchAll, matches := classifyErrorEventSubprocess(definitions, subProcess, errorCode)
		if startEvent == nil {
			continue
		}
		if matches {
			return &errorEventSubprocessMatch{instance: instance, subProcess: subProcess, startEvent: startEvent}
		}
		if isCatchAll && catchAll == nil {
			catchAll = &errorEventSubprocessMatch{instance: instance, subProcess: subProcess, startEvent: startEvent}
		}
	}

	return catchAll
}

// classifyErrorEventSubprocess inspects a single event subprocess and reports its error start event
// (or nil when it is not a valid error event subprocess), whether it is a catch-all (no errorRef),
// and whether it matches the given error code.
func classifyErrorEventSubprocess(definitions *bpmn20.TDefinitions, subProcess *bpmn20.TSubProcess, errorCode *string) (startEvent *bpmn20.TStartEvent, isCatchAll bool, matches bool) {
	// An event subprocess must have exactly one start event per the BPMN 2.0 spec.
	if len(subProcess.StartEvents) != 1 {
		return nil, false, false
	}
	startEvent = &subProcess.StartEvents[0]
	errorDefinition, ok := errorStartEventDefinition(startEvent)
	if !ok {
		return nil, false, false
	}
	if errorDefinition.ErrorRef == nil {
		return startEvent, true, false
	}
	bpmnError, err := definitions.GetErrorByRef(*errorDefinition.ErrorRef)
	if err != nil {
		return nil, false, false
	}
	return startEvent, false, errorCode != nil && bpmnError.ErrorCode == *errorCode
}

// errorEventSubprocessScopeContainer returns the flow elements container that represents the
// scope of the given instance — i.e. the container in which scope-level error event subprocesses
// are declared. Multi-instance children delegate their boundary/event-subprocess scope to the
// parent and therefore return nil here.
//
// A nil return means "this instance owns no scope-level error event subprocess container" and is
// treated by the caller as "no matching handler in this scope". This also defensively covers a
// malformed model where a SubProcessInstance's parent element id does not resolve to a subProcess
// (which should be impossible for a well-formed, validated definition).
func errorEventSubprocessScopeContainer(instance runtime.ProcessInstance) *bpmn20.TFlowElementsContainer {
	switch inst := instance.(type) {
	case *runtime.DefaultProcessInstance, *runtime.CallActivityInstance:
		return &instance.ProcessInstance().Definition.Definitions.Process.TFlowElementsContainer
	case *runtime.SubProcessInstance:
		flowNode := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(inst.ParentProcessTargetElementId)
		subProcess, ok := flowNode.(*bpmn20.TSubProcess)
		if !ok {
			return nil
		}
		return &subProcess.TFlowElementsContainer
	default:
		return nil
	}
}

// errorStartEventDefinition extracts the TErrorEventDefinition from a start event, reporting
// whether the start event is an error start event.
func errorStartEventDefinition(startEvent *bpmn20.TStartEvent) (bpmn20.TErrorEventDefinition, bool) {
	for _, eventDefinition := range startEvent.EventDefinitions {
		if errorDefinition, ok := eventDefinition.(bpmn20.TErrorEventDefinition); ok {
			return errorDefinition, true
		}
	}
	return bpmn20.TErrorEventDefinition{}, false
}

// activateErrorEventSubprocess interrupts the scope owning the matched error event subprocess and
// builds the event subprocess instance. It performs all of its work on the supplied batch but does
// NOT flush it or run the new instance — callers are responsible for scheduling execution once the
// batch is flushed. It returns (nil, nil, nil) when the owning scope is no longer active.
//
// omitTokenKeys are forwarded to the interrupting cancellation so a caller can keep a specific token
// (e.g. the one that threw an error end event in the same scope) completing normally instead of being
// canceled.
func (engine *Engine) activateErrorEventSubprocess(
	ctx context.Context,
	batch *EngineBatch,
	throwingInstanceKey int64,
	match *errorEventSubprocessMatch,
	errorVariables map[string]any,
	omitTokenKeys ...int64,
) (runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	instance := match.instance

	if instance.ProcessInstance().Key != throwingInstanceKey {
		if err := batch.AddParentLockedInstance(ctx, instance); err != nil {
			return nil, nil, err
		}
		if instance.ProcessInstance().State == runtime.ActivityStateCompleted ||
			instance.ProcessInstance().State == runtime.ActivityStateTerminated ||
			instance.ProcessInstance().State == runtime.ActivityStateFailed {
			return nil, nil, nil
		}
	}

	variableHolder, tokens, err := engine.prepareParentForEventSubprocess(ctx, batch, instance, match.startEvent, errorVariables, omitTokenKeys...)
	if err != nil {
		return nil, nil, err
	}
	if len(tokens) == 0 { // Scope is not active anymore — nothing to start.
		return nil, nil, nil
	}

	subProcessInstance, subTokens, err := engine.buildEventSubprocessInstance(ctx, batch, instance, match.subProcess, match.startEvent, variableHolder, tokens)
	if err != nil {
		return nil, nil, err
	}
	return subProcessInstance, subTokens, nil
}

// processErrorEventSubprocessForJob handles a job error (JobFailByKey) that is caught by an error
// event subprocess. The failing job is terminated, the owning scope is interrupted and the event
// subprocess instance is started. It flushes the batch and runs the event subprocess instance.
// Reports whether the error was handled.
func (engine *Engine) processErrorEventSubprocessForJob(
	ctx context.Context,
	batch *EngineBatch,
	job runtime.Job,
	match *errorEventSubprocessMatch,
	variables map[string]interface{},
) (bool, error) {
	job.State = runtime.ActivityStateTerminated
	if err := batch.SaveJob(ctx, job); err != nil {
		return false, err
	}

	subProcessInstance, subTokens, err := engine.activateErrorEventSubprocess(ctx, batch, job.ProcessInstanceKey, match, variables)
	if err != nil {
		return false, err
	}
	if subProcessInstance == nil {
		// The owning scope was already terminated/completed by a concurrent activity — treat the
		// trigger as a no-op and let the caller fall back to incident handling for the job. The
		// job.State = Terminated written above is superseded by failJobWithIncident (which sets it to
		// Failed) on the same, not-yet-flushed batch, so the job ends up Failed with an incident.
		return false, nil
	}

	if err := batch.Flush(ctx); err != nil {
		return false, fmt.Errorf("failed to fail job %+v by error event subprocess handling: %w", job, err)
	}

	return true, engine.RunProcessInstance(ctx, subProcessInstance, subTokens)
}

// activateErrorEventSubprocessOnEndError activates an error event subprocess that caught an error
// thrown by an error end event in the same scope as the event subprocess. Unlike the job path it does
// NOT flush the batch — it schedules the event subprocess to run once the surrounding batch is flushed,
// matching the error boundary handler activation flow.
//
// omitTokenKeys are forwarded to the interrupting cancellation so the token that executed the error end
// event completes normally instead of being canceled and then re-written as completed by the caller.
func (engine *Engine) activateErrorEventSubprocessOnEndError(
	ctx context.Context,
	batch *EngineBatch,
	throwingInstanceKey int64,
	match *errorEventSubprocessMatch,
	errorVariables map[string]any,
	omitTokenKeys ...int64,
) error {
	subProcessInstance, subTokens, err := engine.activateErrorEventSubprocess(ctx, batch, throwingInstanceKey, match, errorVariables, omitTokenKeys...)
	if err != nil {
		return err
	}
	if subProcessInstance == nil {
		return nil
	}

	engine.scheduleErrorEventSubprocessRun(ctx, batch, subProcessInstance, subTokens)
	return nil
}

// activateErrorEventSubprocessInParentScope activates an error event subprocess defined in an ancestor
// scope of the instance that threw an error end event. The throwing scope and any intermediate propagating
// scopes must already have been terminated by the caller. This function interrupts the catching scope
// without cascading into the (already terminated) throwing chain — the token that links the catching scope
// to that chain is cancelled manually instead of through the normal child-instance cancellation, which would
// otherwise try to re-lock the still-running instance that threw the error and deadlock.
func (engine *Engine) activateErrorEventSubprocessInParentScope(
	ctx context.Context,
	batch *EngineBatch,
	parentScope *errorEventContext,
	match *errorEventSubprocessMatch,
) error {
	instance := match.instance

	if _, err := engine.handleProcessInstanceInnerCancel(ctx, instance, batch, parentScope.token.Key); err != nil {
		return fmt.Errorf("failed to interrupt scope %d for error event subprocess: %w", instance.ProcessInstance().Key, err)
	}
	parentScope.token.State = runtime.TokenStateCanceled
	if err := batch.SaveToken(ctx, parentScope.token); err != nil {
		return fmt.Errorf("failed to cancel catching scope token %d: %w", parentScope.token.Key, err)
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	propagatedVariables, err := variableHolder.PropagateMappedOutputsOrAll(match.startEvent.GetOutputMapping(), nil, engine.evaluateExpression)
	if err != nil {
		return fmt.Errorf("failed to propagate error variables to scope %d: %w", instance.ProcessInstance().Key, err)
	}
	for k, v := range propagatedVariables {
		variableHolder.SetLocalVariable(k, v)
	}
	if err := batch.SaveProcessInstance(ctx, instance); err != nil {
		return fmt.Errorf("failed to save scope %d after error event subprocess interruption: %w", instance.ProcessInstance().Key, err)
	}

	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed to get active tokens for scope %d: %w", instance.ProcessInstance().Key, err)
	}
	if len(tokens) == 0 {
		return nil
	}

	subProcessInstance, subTokens, err := engine.buildEventSubprocessInstance(ctx, batch, instance, match.subProcess, match.startEvent, variableHolder, tokens)
	if err != nil {
		return err
	}

	engine.scheduleErrorEventSubprocessRun(ctx, batch, subProcessInstance, subTokens)
	return nil
}

// scheduleErrorEventSubprocessRun registers a post-flush action that runs the freshly built error event
// subprocess instance once the surrounding batch has been flushed.
func (engine *Engine) scheduleErrorEventSubprocessRun(
	ctx context.Context,
	batch *EngineBatch,
	subProcessInstance runtime.ProcessInstance,
	subTokens []runtime.ExecutionToken,
) {
	batch.AddPostFlushAction(ctx, func() {
		safego.Go("event-subprocess-error", engine.logger, func() {
			if err := engine.RunProcessInstance(engine.context, subProcessInstance, subTokens); err != nil {
				engine.logger.Error(fmt.Sprintf("failed to run error event subprocess instance %d: %s", subProcessInstance.ProcessInstance().Key, err.Error()))
			}
		})
	})
}
