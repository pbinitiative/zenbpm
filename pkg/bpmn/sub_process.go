package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (engine *Engine) createCallActivity(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element *bpmn20.TCallActivity,
	currentToken runtime.ExecutionToken,
	callActivityVarHolder runtime.VariableHolder,
) (runtime.ActivityState, error) {
	processId := element.CalledElement.ProcessId
	if err := callActivityVarHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for call activity: %w", err)
	}
	batch.SaveFlowElementInstance(ctx,
		runtime.FlowElementInstance{
			Key:                currentToken.ElementInstanceKey,
			ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
			ElementId:          element.GetId(),
			CreatedAt:          time.Now(),
			ExecutionTokenKey:  currentToken.Key,
			InputVariables:     callActivityVarHolder.LocalVariables(),
			OutputVariables:    nil,
		},
	)

	processDefinition, err := engine.persistence.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("no process with id=%s was found (prior loaded into the engine)", processId), err)
	}

	calledProcessInstance, tokens, err := engine.createInstance(
		ctx,
		batch,
		&processDefinition,
		callActivityVarHolder,
		&runtime.CallActivityInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			ProcessInstanceData: runtime.ProcessInstanceData{
				HistoryTTLSec: instance.ProcessInstance().HistoryTTLSec,
			},
		})
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	batch.AddPostFlushAction(ctx, func() {
		safego.Go("call-activity", engine.logger, func() {
			err := engine.RunProcessInstance(engine.context, calledProcessInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to run call activity instance %d: %s", calledProcessInstance.ProcessInstance().Key, err.Error()))
			}
		})
	})

	return runtime.ActivityStateActive, nil
}

func (engine *Engine) createSubProcess(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element *bpmn20.TSubProcess,
	currentToken runtime.ExecutionToken,
	subProcessVariableHolder runtime.VariableHolder,
) (runtime.ActivityState, error) {
	if err := subProcessVariableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for sub process: %w", err)
	}

	batch.SaveFlowElementInstance(ctx,
		runtime.FlowElementInstance{
			Key:                currentToken.ElementInstanceKey,
			ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
			ElementId:          element.GetId(),
			CreatedAt:          time.Now(),
			ExecutionTokenKey:  currentToken.Key,
			InputVariables:     subProcessVariableHolder.LocalVariables(),
			OutputVariables:    nil,
		},
	)

	startingFlowNodes := make([]bpmn20.FlowNode, 0, len(element.StartEvents))
	for _, startEvent := range element.StartEvents {
		var flowNode bpmn20.FlowNode = &startEvent
		startingFlowNodes = append(startingFlowNodes, flowNode)
	}

	subProcessInstance, tokens, err := engine.createInstanceWithStartingElements(
		ctx,
		batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		subProcessVariableHolder,
		&runtime.SubProcessInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			ParentProcessTargetElementId:          element.Id,
			ProcessInstanceData: runtime.ProcessInstanceData{
				HistoryTTLSec: instance.ProcessInstance().HistoryTTLSec,
			},
		},
	)
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	// Create event sub process subscriptions for event subprocesses nested within this sub process
	err = engine.createEventSubProcessSubscriptions(ctx, batch, subProcessInstance, &element.TFlowElementsContainer)
	if err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to create event subprocess subscriptions in sub process %s: %w", element.Id, err)
	}
	batch.AddPostFlushAction(ctx, func() {
		safego.Go("sub-process", engine.logger, func() {
			err := engine.RunProcessInstance(engine.context, subProcessInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to sub process instance %d: %s", subProcessInstance.ProcessInstance().Key, err.Error()))
			}
		})
	})

	return runtime.ActivityStateActive, nil
}
func (engine *Engine) handleParentProcessContinuationForSubProcess(ctx context.Context, batch *EngineBatch, instance *runtime.SubProcessInstance) error {
	// Flush pending writes so that subsequent reads (tokens, process instances) see up-to-date persisted state
	err := batch.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}
	parentProcessTargetElementId := instance.ParentProcessTargetElementId
	parentProcessInstanceKey := instance.ParentProcessExecutionToken.ProcessInstanceKey
	parentTokenKey := instance.ParentProcessExecutionToken.Key
	parentProcessTargetElementInstanceKey := instance.ParentProcessTargetElementInstanceKey

	//setup
	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, parentProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find parent process instance %d: %w", parentProcessInstanceKey, err)
	}
	err = batch.AddParentLockedInstance(ctx, parentInstance)
	if err != nil {
		return err
	}
	if parentInstance.ProcessInstance().State == runtime.ActivityStateCompleted || parentInstance.ProcessInstance().State == runtime.ActivityStateTerminated {
		return nil
	}

	subProcessDef, ok := bpmn20.FindBaseElementById(&instance.ProcessInstanceData.GetProcessInfo().Definitions, parentProcessTargetElementId)
	if !ok {
		return fmt.Errorf("could not find sub process definition by id %s", parentProcessTargetElementId)
	}
	var isEventSubProcess bool
	completeParentAfterEventSubProcess := false
	subProcessDefTyped := subProcessDef.(*bpmn20.TSubProcess)
	if subProcessDefTyped.TriggeredByEvent {
		if len(subProcessDefTyped.TProcess.StartEvents) != 1 {
			return fmt.Errorf("event subprocess must have exactly 1 start event")
		}
		isEventSubProcess = true
		if subProcessDefTyped.TProcess.StartEvents[0].IsInterrupting {
			completeParentAfterEventSubProcess = true
		} else {
			tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, parentInstance.ProcessInstance().Key)
			if err != nil {
				return fmt.Errorf("failed to find active tokens for process %d: %w", parentInstance.ProcessInstance().Key, err)
			}
			if len(tokens) == 0 {
				completeParentAfterEventSubProcess = true
			}
		}
	}

	updatedParentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		return fmt.Errorf("failed to get token by key: %w", err)
	}
	// event subprocesses can be non-interrupting so child's parent token does not have to be waiting on the same target element
	// Can we even remove this condition or is it required for embedded (non-event) subprocesses?
	if !isEventSubProcess && updatedParentToken.ElementInstanceKey != parentProcessTargetElementInstanceKey {
		log.Infof(ctx, "failed to handleParentProcessContinuation for sub process instance %d: parent token is no longer waiting at target element instance key %d", instance.ProcessInstance().Key, parentProcessTargetElementInstanceKey)
		return nil
	}
	ctx, tokenSpan := engine.tracer.Start(ctx, fmt.Sprintf("token:%s", updatedParentToken.ElementId), trace.WithAttributes(
		attribute.String(otelPkg.AttributeElementId, updatedParentToken.ElementId),
		attribute.Int64(otelPkg.AttributeElementKey, updatedParentToken.ElementInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, updatedParentToken.Key),
	))
	defer func() {
		if err != nil {
			tokenSpan.RecordError(err)
			tokenSpan.SetStatus(codes.Error, err.Error())
		}
		tokenSpan.End()
	}()

	//process variables
	parentFlowNode := parentInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(parentProcessTargetElementId)
	if parentFlowNode == nil {
		return fmt.Errorf("failed to find flow node by id %s", parentProcessTargetElementId)
	}
	parentElement, ok := parentFlowNode.(*bpmn20.TSubProcess)
	if !ok {
		return fmt.Errorf("failed to find flow node by id %s", parentProcessTargetElementId)
	}

	variableHolder := runtime.NewVariableHolder(&parentInstance.ProcessInstance().VariableHolder, nil)
	output, err := variableHolder.PropagateOnlyMappedOutputs(parentElement.GetOutputMapping(), instance.ProcessInstance().VariableHolder.LocalVariables(), engine.evaluateExpression)
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
	}

	err = engine.cancelBoundarySubscriptions(ctx, batch, parentInstance.ProcessInstance().Key, updatedParentToken)
	if err != nil {
		batch.Clear(ctx)
		return fmt.Errorf("failed to cancel boundary subscriptions for parent process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	//handle the finalization, transition back to parent process if sub process is not an event sub process
	var tokens []runtime.ExecutionToken
	if !isEventSubProcess {
		isSubprocessChildrenActive := instance.ProcessInstance().State == runtime.ActivityStateActive
		if !isSubprocessChildrenActive {
			var hasActive bool
			hasActive, err = engine.hasActiveSubProcessInstance(ctx, instance.ProcessInstance().Key)
			if err != nil {
				return fmt.Errorf("failed to check if sub process children are still active: %w", err)
			}
			isSubprocessChildrenActive = hasActive
		}
		if !isSubprocessChildrenActive {
			tokens, err = engine.handleElementTransition(ctx, batch, parentInstance, parentElement, updatedParentToken)
			if err != nil {
				return fmt.Errorf("failed to handle simple transition for parent instance  %d: %w", parentInstance.ProcessInstance().Key, err)
			}
			for _, tok := range tokens {
				batch.SaveToken(ctx, tok)
			}
		}
	}
	if completeParentAfterEventSubProcess {
		parentInstance.ProcessInstance().State = runtime.ActivityStateCompleted
	}
	err = batch.SaveProcessInstance(ctx, parentInstance)
	if err != nil {
		return fmt.Errorf("failed to save process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	if err = batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:             updatedParentToken.ElementInstanceKey,
		OutputVariables: output,
		CompletedAt:     new(time.Now()),
	}); err != nil {
		return fmt.Errorf("failed to update flow element instance for sub process %s: %w", parentElement.GetId(), err)
	}

	// for nested event subprocesses: complete parent event subprocesses recursively going up
	if isEventSubProcess && parentInstance.ProcessInstance().State == runtime.ActivityStateCompleted && parentInstance.Type() == runtime.ProcessTypeSubProcess {
		err = engine.handleParentProcessContinuationForSubProcess(ctx, batch, parentInstance.(*runtime.SubProcessInstance))
		if err != nil {
			return fmt.Errorf("failed to handle parent process continuation for parent subprocess instance %d: %w", parentInstance.ProcessInstance().Key, err)
		}
	}

	batch.AddPostFlushAction(ctx, func() {
		safego.Go("parent-continuation-subprocess", engine.logger, func() {
			if parentInstance.ProcessInstance().State == runtime.ActivityStateActive {
				err := engine.RunProcessInstance(engine.context, parentInstance, tokens)
				if err != nil {
					engine.logger.Error("failed to continue with parent process instance for multi instance %d: %w", instance.Key, err)
				}
			}
		})
	})
	return nil
}

func (engine *Engine) handleParentProcessContinuationForCallActivity(ctx context.Context, batch *EngineBatch, instance *runtime.CallActivityInstance, flowNode bpmn20.FlowNode) error {
	parentProcessInstanceKey := instance.ParentProcessExecutionToken.ProcessInstanceKey
	parentTokenKey := instance.ParentProcessExecutionToken.Key
	parentProcessTargetElementInstanceKey := instance.ParentProcessTargetElementInstanceKey

	//setup
	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, parentProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find parent process instance %d: %w", parentProcessInstanceKey, err)
	}
	err = batch.AddParentLockedInstance(ctx, parentInstance)
	if err != nil {
		return err
	}
	if parentInstance.ProcessInstance().State == runtime.ActivityStateCompleted || parentInstance.ProcessInstance().State == runtime.ActivityStateTerminated {
		return nil
	}

	updatedParentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		return fmt.Errorf("failed to get token by key: %w", err)
	}
	if updatedParentToken.ElementInstanceKey != parentProcessTargetElementInstanceKey {
		log.Infof(ctx, "failed to handleParentProcessContinuation for call activity instance %d: parent token is no longer waiting at target element instance key %d", instance.ProcessInstance().Key, parentProcessTargetElementInstanceKey)
		return nil
	}
	ctx, tokenSpan := engine.tracer.Start(ctx, fmt.Sprintf("token:%s", updatedParentToken.ElementId), trace.WithAttributes(
		attribute.String(otelPkg.AttributeElementId, updatedParentToken.ElementId),
		attribute.Int64(otelPkg.AttributeElementKey, updatedParentToken.ElementInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, updatedParentToken.Key),
	))
	defer func() {
		if err != nil {
			tokenSpan.RecordError(err)
			tokenSpan.SetStatus(codes.Error, err.Error())
		}
		tokenSpan.End()
	}()

	//process variables
	parentFlowNode := parentInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(updatedParentToken.ElementId)
	if parentFlowNode == nil {
		return fmt.Errorf("failed to find flow node by id %s", updatedParentToken.ElementId)
	}
	parentElement, ok := parentFlowNode.(*bpmn20.TCallActivity)
	if !ok {
		return fmt.Errorf("failed to find flow node by id %s", updatedParentToken.ElementId)
	}

	variableHolder := runtime.NewVariableHolder(&parentInstance.ProcessInstance().VariableHolder, nil)
	output, err := variableHolder.PropagateOnlyMappedOutputs(parentElement.GetOutputMapping(), instance.ProcessInstance().VariableHolder.LocalVariables(), engine.evaluateExpression)
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
	}

	err = engine.cancelBoundarySubscriptions(ctx, batch, parentInstance.ProcessInstance().Key, updatedParentToken)
	if err != nil {
		batch.Clear(ctx)
		return fmt.Errorf("failed to cancel boundary subscriptions for parent process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	//handle the finalization
	tokens, err := engine.handleElementTransition(ctx, batch, parentInstance, parentElement, updatedParentToken)
	if err != nil {
		return fmt.Errorf("failed to handle simple trasition for parent instance  %d: %w", parentInstance.ProcessInstance().Key, err)
	}

	for _, tok := range tokens {
		batch.SaveToken(ctx, tok)
	}
	batch.SaveProcessInstance(ctx, parentInstance)
	if err := batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:             updatedParentToken.ElementInstanceKey,
		OutputVariables: output,
		CompletedAt:     new(time.Now()),
	}); err != nil {
		return fmt.Errorf("failed to update flow element instance for call activity %s: %w", parentElement.GetId(), err)
	}

	batch.AddPostFlushAction(ctx, func() {
		safego.Go("parent-continuation-call-activity", engine.logger, func() {
			err := engine.RunProcessInstance(engine.context, parentInstance, tokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance for call activity %d: %w", instance.Key, err)
			}
		})
	})
	return nil
}

func (engine *Engine) handleParentProcessContinuation(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, flowNode bpmn20.FlowNode) error {
	switch inst := instance.(type) {
	case *runtime.SubProcessInstance:
		err := engine.handleParentProcessContinuationForSubProcess(ctx, batch, inst)
		if err != nil {
			return err
		}
	case *runtime.CallActivityInstance:
		err := engine.handleParentProcessContinuationForCallActivity(ctx, batch, inst, flowNode)
		if err != nil {
			return err
		}
	case *runtime.MultiInstanceInstance:
		err := engine.handleParentProcessContinuationForMultiInstance(ctx, batch, inst, flowNode)
		if err != nil {
			return err
		}
	default:
		log.Infof(ctx, "wrong instance type %T regular instance can't have parent", instance)
	}
	return nil
}

func (engine *Engine) cancelSubProcessInstance(ctx context.Context, instance runtime.ProcessInstance, batch *EngineBatch) error {
	err := batch.AddLockedInstance(ctx, instance)
	if err != nil {
		return err
	}
	if instance.ProcessInstance().State == runtime.ActivityStateCompleted || instance.ProcessInstance().State == runtime.ActivityStateTerminated {
		return nil
	}

	return engine.cancelInstance(ctx, instance, batch)
}
