package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (engine *Engine) createCallActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	processId := element.CalledElement.ProcessId
	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for call activity: %w", err)
	}

	processDefinition, err := engine.persistence.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("no process with id=%s was found (prior loaded into the engine)", processId), err)
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			ctx, todoSpan := engine.tracer.Start(ctx, fmt.Sprintf("callActivity:%s", element.Id), trace.WithAttributes(
				attribute.Int64("parentProcessInstanceKey", instance.Key),
			))
			calledProcessInstance, err := engine.createInstance(ctx, &processDefinition, variableHolder, &runtime.SubProcessParentMetadata{
				ParentProcessExecutionToken:           currentToken,
				ParentProcessDefinitionKey:            instance.Definition.Key,
				ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
				ParentProcessTargetElementId:          element.Id,
			})
			if err != nil {
				engine.runningInstances.lockInstance(instance.Key)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance.Key)
				engine.logger.Error("failed to run call activity instance %d: %w", calledProcessInstance.Key, err)
				return
			}
			todoSpan.End()
		}()
	})
	return runtime.ActivityStateActive, nil
}

func (engine *Engine) createSubProcess(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TSubProcess, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		instance.State = runtime.ActivityStateFailed
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for sub process: %w", err)
	}

	startingFlowNodes := make([]bpmn20.FlowNode, 0, len(element.StartEvents))
	for _, startEvent := range element.StartEvents {
		var flowNode bpmn20.FlowNode = &startEvent
		startingFlowNodes = append(startingFlowNodes, flowNode)
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			ctx, todoSpan := engine.tracer.Start(ctx, fmt.Sprintf("subProcess:%s", element.Id), trace.WithAttributes(
				attribute.Int64("parentProcessInstanceKey", instance.Key),
				attribute.String("targetParentActivityID", element.Id),
			))

			_, err := engine.createInstanceWithStartingElements(
				ctx,
				instance.Definition,
				startingFlowNodes,
				variableHolder,
				&runtime.SubProcessParentMetadata{
					ParentProcessExecutionToken:           currentToken,
					ParentProcessDefinitionKey:            instance.Definition.Key,
					ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
					ParentProcessTargetElementId:          element.Id,
				},
			)
			if err != nil {
				engine.runningInstances.lockInstance(instance.Key)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance.Key)
				engine.logger.Error("failed to run subProcess %d: %w", instance.Key, err)
				return
			}

			todoSpan.End()
		}()
	})
	return runtime.ActivityStateActive, nil
}

func (engine *Engine) handleMultiInstanceActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element bpmn20.TActivity, activity runtime.Activity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	if instance.SubProcessParentMetadata == nil {
		if element.MultiInstance.IsSequential {
			tokens, err := engine.startSequentialMultiInstance(ctx, batch, instance, activity, element, currentToken)
			if err != nil {
				return nil, err
			}
			return tokens, nil
		}
		tokens, err := engine.startParallelMultiInstance(ctx, batch, instance, activity, &element, currentToken)
		if err != nil {
			return nil, err
		}
		return tokens, nil
	}

	parentElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, instance.SubProcessParentMetadata.ParentProcessTargetElementInstanceKey)
	if err != nil {
		return nil, err
	}

	inputCollection := parentElementInstance.InputVariables[element.MultiInstance.InputElementName].([]interface{})
	multiInstancesAlreadyStarted, err := engine.persistence.GetFlowElementInstanceCountByProcessInstanceKey(ctx, instance.Key)
	if err != nil {
		return nil, err
	}
	if multiInstancesAlreadyStarted > len(inputCollection) {
		return nil, fmt.Errorf("there is more instances started in MultiInstance than was supposed to, token key:  %d", instance.SubProcessParentMetadata.ParentProcessExecutionToken.Key)
	}
	instance.VariableHolder.SetLocalVariable(element.MultiInstance.InputElementName, inputCollection[multiInstancesAlreadyStarted+1])

	var activityResult runtime.ActivityState
	switch element := activity.Element().(type) {
	case *bpmn20.TServiceTask:
		activityResult, err = engine.createInternalTask(ctx, batch, instance, element, currentToken)
	case *bpmn20.TSendTask:
		activityResult, err = engine.createInternalTask(ctx, batch, instance, element, currentToken)
	case *bpmn20.TUserTask:
		activityResult, err = engine.createUserTask(ctx, batch, instance, element, currentToken)
	case *bpmn20.TCallActivity:
		activityResult, err = engine.createCallActivity(ctx, batch, instance, element, currentToken)
		// we created process instance and its running in separate goroutine
	case *bpmn20.TSubProcess:
		activityResult, err = engine.createSubProcess(ctx, batch, instance, element, currentToken)
		// we created process instance and its running in separate goroutine
	case *bpmn20.TBusinessRuleTask:
		activityResult, err = engine.createBusinessRuleTask(ctx, batch, instance, element, currentToken)
	default:
		return nil, fmt.Errorf("failed to process %s %d: %w", element.GetType(), activity.GetKey(), errors.New("unsupported activity"))
	}

	// Now check whether the activity ended right away and the process can move on or it needs to wait for external event
	switch activityResult {
	case runtime.ActivityStateActive:
		currentToken.State = runtime.TokenStateWaiting
		return []runtime.ExecutionToken{currentToken}, nil
	case runtime.ActivityStateCompleted:
		if element.MultiInstance.IsSequential {
			currentToken.State = runtime.TokenStateRunning
			currentToken.ElementInstanceKey = engine.generateKey()
		}
		currentToken.State = runtime.TokenStateCompleted
		return []runtime.ExecutionToken{currentToken}, nil
	}

	return []runtime.ExecutionToken{}, nil
}

func (engine *Engine) startParallelMultiInstance(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, activity runtime.Activity, element *bpmn20.TActivity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	tmpVariableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	evaluatedInputCollection, err := engine.evaluateExpression(element.MultiInstance.InputCollectionExpression, tmpVariableHolder.LocalVariables())
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate inputCollection expression %T: %w", element.MultiInstance.InputCollectionExpression, err)
	}
	inputCollection, ok := evaluatedInputCollection.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to evaluate result inputCollection %T: %w", evaluatedInputCollection, err)
	}
	inputVariables := runtime.NewVariableHolder(nil, map[string]interface{}{element.MultiInstance.InputElementName: inputCollection})

	startingFlowNodes := make([]bpmn20.FlowNode, 0, len(inputCollection))
	for _, _ = range inputCollection {
		startingFlowNodes = append(startingFlowNodes, activity.Element())
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			ctx, subProcessSpan := engine.tracer.Start(ctx, fmt.Sprintf("parallelMultiInstance:%s", element.Id), trace.WithAttributes(
				attribute.Int64("parentProcessInstanceKey", instance.Key),
				attribute.String("targetParentActivityID", element.Id),
			))

			_, err := engine.createInstanceWithStartingElements(
				ctx,
				instance.Definition,
				startingFlowNodes,
				inputVariables,
				&runtime.SubProcessParentMetadata{
					ParentProcessExecutionToken:           currentToken,
					ParentProcessDefinitionKey:            instance.Definition.Key,
					ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
					ParentProcessTargetElementId:          element.Id,
				},
			)
			if err != nil {
				engine.runningInstances.lockInstance(instance.Key)
				engine.handleIncident(ctx, currentToken, err, subProcessSpan)
				engine.runningInstances.unlockInstance(instance.Key)
				engine.logger.Error("failed to run parallelMultiInstance %d: %w", instance.Key, err)
				return
			}

			subProcessSpan.End()
		}()
	})

	currentToken.State = runtime.TokenStateWaiting
	return []runtime.ExecutionToken{currentToken}, nil
}

func (engine *Engine) startSequentialMultiInstance(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, activity runtime.Activity, element bpmn20.TActivity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	startingFlowNodes := []bpmn20.FlowNode{activity.Element()}

	tmpVariableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	evaluatedInputCollection, err := engine.evaluateExpression(element.MultiInstance.InputCollectionExpression, tmpVariableHolder.LocalVariables())
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate inputCollection expression %T: %w", element.MultiInstance.InputCollectionExpression, err)
	}
	inputCollection, ok := evaluatedInputCollection.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to evaluate result inputCollection %T: %w", evaluatedInputCollection, err)
	}
	inputVariables := runtime.NewVariableHolder(nil, map[string]interface{}{element.MultiInstance.InputElementName: inputCollection})

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			ctx, subProcessSpan := engine.tracer.Start(ctx, fmt.Sprintf("sequentialMultiInstance:%s", element.Id), trace.WithAttributes(
				attribute.Int64("parentProcessInstanceKey", instance.Key),
				attribute.String("targetParentActivityID", element.Id),
			))

			_, err := engine.createInstanceWithStartingElements(
				ctx,
				instance.Definition,
				startingFlowNodes,
				inputVariables,
				&runtime.SubProcessParentMetadata{
					ParentProcessExecutionToken:           currentToken,
					ParentProcessDefinitionKey:            instance.Definition.Key,
					ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
					ParentProcessTargetElementId:          element.Id,
				},
			)
			if err != nil {
				engine.runningInstances.lockInstance(instance.Key)
				engine.handleIncident(ctx, currentToken, err, subProcessSpan)
				engine.runningInstances.unlockInstance(instance.Key)
				engine.logger.Error("failed to run sequentialMultiInstance %d: %w", instance.Key, err)
				return
			}

			subProcessSpan.End()
		}()
	})

	currentToken.State = runtime.TokenStateWaiting
	return []runtime.ExecutionToken{currentToken}, nil
}

func (engine *Engine) handleParentProcessContinuation(ctx context.Context, instance runtime.ProcessInstance, flowNode bpmn20.FlowNode) {
	batch := engine.persistence.NewBatch()
	engine.runningInstances.lockInstance(instance.SubProcessParentMetadata.ParentProcessExecutionToken.ProcessInstanceKey)
	instanceUnlocked := false
	defer func() {
		if instanceUnlocked == false {
			engine.runningInstances.unlockInstance(instance.SubProcessParentMetadata.ParentProcessExecutionToken.ProcessInstanceKey)
		}
	}()

	parentToken, err := engine.persistence.GetTokenByKey(ctx, instance.SubProcessParentMetadata.ParentProcessExecutionToken.Key)
	if err != nil {
		err = fmt.Errorf("failed to get token by key: %w", err)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}

	ppi, err := engine.persistence.FindProcessInstanceByKey(ctx, instance.SubProcessParentMetadata.ParentProcessExecutionToken.ProcessInstanceKey)
	if err != nil {
		err = fmt.Errorf("failed to find parent process instance %d: %w", instance.SubProcessParentMetadata.ParentProcessExecutionToken.ProcessInstanceKey, err)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}
	parentInstance := &ppi

	if parentToken.ElementInstanceKey != instance.SubProcessParentMetadata.ParentProcessTargetElementInstanceKey {
		err = fmt.Errorf("failed to handleParentProcessContinuation for instance %d: parent token is no longer waiting at target element instance key %d", instance.Key, instance.SubProcessParentMetadata.ParentProcessTargetElementInstanceKey)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}

	parentElement := parentInstance.Definition.Definitions.Process.GetFlowNodeById(instance.SubProcessParentMetadata.ParentProcessTargetElementId).(*bpmn20.TActivity)

	if element, ok := flowNode.(*bpmn20.TActivity); ok && element.MultiInstance != nil {
		elementInstances, err := engine.persistence.GetFlowElementInstancesByProcessInstanceKey(ctx, instance.Key, true)
		if err != nil {
			engine.handleIncident(ctx, parentToken, err, tokenSpan)
			return
		}
		outputCollection := make([]interface{}, 0)
		for _, elementInstance := range elementInstances {
			evaluatedOutput, err := engine.feelRuntime.Evaluate(parentElement.MultiInstance.OutputElementExpression, elementInstance.OutputVariables)
			if err != nil {
				err = fmt.Errorf("failed to evaluate outputElementExpression on elementInstance %d for parent isntance %d: %w", elementInstance.Key, parentInstance.Key, err)
				engine.handleIncident(ctx, parentToken, err, tokenSpan)
			}
			outputCollection = append(outputCollection, evaluatedOutput)
		}

		parentInstance.VariableHolder.SetLocalVariable(parentElement.MultiInstance.OutputCollectionName, outputCollection)
	} else {
		variableHolder := runtime.NewVariableHolder(&parentInstance.VariableHolder, instance.VariableHolder.LocalVariables())
		if err := variableHolder.PropagateLocalVariablesToParent(parentElement.GetOutputMapping(), engine.evaluateExpression); err != nil {
			instance.State = runtime.ActivityStateFailed
			err = fmt.Errorf("failed to propagate variables back to parent: %w", err)
			engine.handleIncident(ctx, parentToken, err, tokenSpan)
			return
		}
	}

	tokens, err := engine.handleElementTransition(ctx, batch, parentInstance, parentElement, nil, parentToken)
	if err != nil {
		err = fmt.Errorf("failed to handle simple trasition for parent instance  %d: %w", parentInstance.Key, err)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}

	for _, tok := range tokens {
		batch.SaveToken(ctx, tok)
	}
	batch.SaveProcessInstance(ctx, instance)

	err = batch.Flush(ctx)
	if err != nil {
		err = fmt.Errorf("failed to flush updates for parent instance  %d: %w", parentInstance.Key, err)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}
	engine.runningInstances.unlockInstance(instance.SubProcessParentMetadata.ParentProcessExecutionToken.ProcessInstanceKey)
	instanceUnlocked = true

	go func() {
		err = engine.runProcessInstance(ctx, parentInstance, tokens)
		if err != nil {
			engine.logger.Error("failed to continue with parent process instance: %w", err)
		}
	}()
	return nil
}
