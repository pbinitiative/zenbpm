package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"reflect"
	"time"
)

func (engine *Engine) createCallActivity(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	processId := element.CalledElement.ProcessId
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, map[string]interface{}{})
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for call activity: %w", err)
	}
	batch.SaveFlowElementInstance(ctx,
		runtime.FlowElementInstance{
			Key:                currentToken.ElementInstanceKey,
			ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
			ElementId:          element.GetId(),
			CreatedAt:          time.Now(),
			ExecutionTokenKey:  currentToken.Key,
			InputVariables:     variableHolder.LocalVariables(),
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
		variableHolder,
		&runtime.CallActivityInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
		})
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err := engine.RunProcessInstance(ctx, calledProcessInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to run call activity instance %d: %s", calledProcessInstance.ProcessInstance().Key, err.Error()))
			}
		}()
	})

	return runtime.ActivityStateActive, nil
}

func (engine *Engine) createSubProcess(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TSubProcess, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, map[string]interface{}{})
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
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
			InputVariables:     variableHolder.LocalVariables(),
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
		variableHolder,
		&runtime.SubProcessInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			ParentProcessTargetElementId:          element.Id,
		},
	)
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err := engine.RunProcessInstance(ctx, subProcessInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to sub process instance %d: %s", subProcessInstance.ProcessInstance().Key, err.Error()))
			}
		}()
	})

	return runtime.ActivityStateActive, nil
}

func (engine *Engine) handleMultiInstanceActivity(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element bpmn20.Activity, activity runtime.Activity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	//Starts Multi Instance
	if instance.Type() != runtime.ProcessTypeMultiInstance {
		var activityState runtime.ActivityState
		var err error
		if element.GetMultiInstance().IsSequential {
			activityState, err = engine.startSequentialMultiInstance(ctx, batch, instance, activity, element, currentToken)
			if err != nil {
				return nil, err
			}
		} else {
			activityState, err = engine.startParallelMultiInstance(ctx, batch, instance, activity, element, currentToken)
			if err != nil {
				return nil, err
			}
		}

		switch activityState {
		case runtime.ActivityStateActive:
			currentToken.State = runtime.TokenStateWaiting
			return []runtime.ExecutionToken{currentToken}, nil
		case runtime.ActivityStateFailed:
			currentToken.State = runtime.TokenStateFailed
			return []runtime.ExecutionToken{currentToken}, nil
		default:
			return []runtime.ExecutionToken{}, fmt.Errorf("unsupported activity state: %s", activityState)
		}
	}

	multiInstance, ok := instance.(*runtime.MultiInstanceInstance)
	if !ok {
		return []runtime.ExecutionToken{}, fmt.Errorf("failed to handle multi instance activity activity instance")
	}

	//Handles Multi Instance
	parentElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, multiInstance.ParentProcessTargetElementInstanceKey)
	if err != nil {
		return []runtime.ExecutionToken{}, err
	}

	inputCollection := parentElementInstance.InputVariables[element.GetMultiInstance().LoopCharacteristics.InputElementName].([]interface{})
	//TODO: Optimize
	multiInstancesAlreadyStarted, err := engine.persistence.GetFlowElementInstanceCountByProcessInstanceKey(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return []runtime.ExecutionToken{}, err
	}
	if multiInstancesAlreadyStarted == int64(len(inputCollection)) {
		instance.ProcessInstance().State = runtime.ActivityStateCompleted
		currentToken.State = runtime.TokenStateCompleted
		return []runtime.ExecutionToken{currentToken}, err
	}
	instance.ProcessInstance().VariableHolder.SetLocalVariable(element.GetMultiInstance().LoopCharacteristics.InputElementName, inputCollection[multiInstancesAlreadyStarted])

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
		return []runtime.ExecutionToken{}, fmt.Errorf("failed to process %s %d: %w", element.GetType(), activity.GetKey(), errors.New("unsupported activity"))
	}

	// Now check whether the activity ended right away and the process can move on or it needs to wait for external event
	switch activityResult {
	case runtime.ActivityStateActive:
		currentToken.State = runtime.TokenStateWaiting
		return []runtime.ExecutionToken{currentToken}, nil
	case runtime.ActivityStateCompleted:
		tokens, err := engine.handleMultiInstanceElementTransition(ctx, batch, multiInstance, element, currentToken)
		if err != nil {
			return nil, err
		}
		return tokens, nil
	case runtime.ActivityStateFailed:
		currentToken.State = runtime.TokenStateFailed
		return []runtime.ExecutionToken{currentToken}, fmt.Errorf("failed to process %s %d: %w", element.GetType(), activity.GetKey(), err)
	default:
		return []runtime.ExecutionToken{}, fmt.Errorf("unsupported activity state: %s", activityResult)
	}
}

func (engine *Engine) startParallelMultiInstance(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, activity runtime.Activity, element bpmn20.Activity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	tmpVariableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	evaluatedInputCollection, err := engine.evaluateExpression(element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, tmpVariableHolder.LocalVariables())
	if err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection expression %T: %w", element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, err)
	}
	t := reflect.TypeOf(evaluatedInputCollection)
	if t != nil && (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection %T: %w", evaluatedInputCollection, err)
	}
	inputCollection := make([]interface{}, 0)
	if t != nil {
		rv := reflect.ValueOf(evaluatedInputCollection)
		inputCollection = make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			inputCollection[i] = rv.Index(i).Interface()
		}
	}
	batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().Key,
		ElementId:          element.GetId(),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  currentToken.Key,
		InputVariables:     map[string]interface{}{element.GetMultiInstance().LoopCharacteristics.InputElementName: inputCollection},
		OutputVariables:    nil,
	})

	startingFlowNodes := make([]bpmn20.FlowNode, 0, len(inputCollection))
	for _, _ = range inputCollection {
		startingFlowNodes = append(startingFlowNodes, activity.Element())
	}

	paralelMultiInstance, tokens, err := engine.createInstanceWithStartingElements(
		ctx,
		batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		runtime.NewVariableHolder(nil, map[string]interface{}{}),
		&runtime.MultiInstanceInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			ParentProcessTargetElementId:          element.GetId(),
		},
	)
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err := engine.RunProcessInstance(ctx, paralelMultiInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to sub parallel multiInstance instance %d: %s", paralelMultiInstance.ProcessInstance().Key, err.Error()))
			}
		}()
	})

	return runtime.ActivityStateActive, nil
}

func (engine *Engine) startSequentialMultiInstance(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, activity runtime.Activity, element bpmn20.Activity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	startingFlowNodes := []bpmn20.FlowNode{activity.Element()}

	tmpVariableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	evaluatedInputCollection, err := engine.evaluateExpression(element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, tmpVariableHolder.LocalVariables())
	if err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection expression %T: %w", element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, err)
	}
	t := reflect.TypeOf(evaluatedInputCollection)
	if t != nil && (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection %T: %w", evaluatedInputCollection, err)
	}
	inputCollection := make([]interface{}, 0)
	if t != nil {
		rv := reflect.ValueOf(evaluatedInputCollection)
		inputCollection = make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			inputCollection[i] = rv.Index(i).Interface()
		}
	}

	batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().Key,
		ElementId:          element.GetId(),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  currentToken.Key,
		InputVariables:     map[string]interface{}{element.GetMultiInstance().LoopCharacteristics.InputElementName: inputCollection},
		OutputVariables:    nil,
	})

	sequentialMultiInstance, tokens, err := engine.createInstanceWithStartingElements(
		ctx,
		batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		runtime.NewVariableHolder(nil, map[string]any{}),
		&runtime.MultiInstanceInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			ParentProcessTargetElementId:          element.GetId(),
		},
	)
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err := engine.RunProcessInstance(ctx, sequentialMultiInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to start sequential multiInstance instance %d: %s", sequentialMultiInstance.ProcessInstance().Key, err.Error()))
			}
		}()
	})

	return runtime.ActivityStateActive, nil
}

func (engine *Engine) handleParentProcessContinuationForSubProcess(ctx context.Context, batch *EngineBatch, instance *runtime.SubProcessInstance, flowNode bpmn20.FlowNode) error {
	parentProcessTargetElementId := instance.ParentProcessTargetElementId
	parentProcessInstanceKey := instance.ParentProcessExecutionToken.ProcessInstanceKey
	parentTokenKey := instance.ParentProcessExecutionToken.Key
	parentProcessTargetElementInstanceKey := instance.ParentProcessTargetElementInstanceKey

	//setup
	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, parentProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find parent process instance %d: %w", parentProcessInstanceKey, err)
	}
	err = batch.AddParentLockedInstance(ctx, instance, parentInstance)
	if err != nil {
		return err
	}

	updatedParentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		return fmt.Errorf("failed to get token by key: %w", err)
	}
	if updatedParentToken.ElementInstanceKey != parentProcessTargetElementInstanceKey {
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
	parentElement := parentInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(parentProcessTargetElementId).(*bpmn20.TSubProcess)
	variableHolder := runtime.NewVariableHolder(&parentInstance.ProcessInstance().VariableHolder, nil)
	output, err := variableHolder.PropagateOutputVariablesToParent(parentElement.GetOutputMapping(), instance.ProcessInstance().VariableHolder.LocalVariables(), engine.evaluateExpression)
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
	}

	err = engine.cancelBoundarySubscriptions(ctx, batch, parentInstance, &updatedParentToken)
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
	batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:             updatedParentToken.ElementInstanceKey,
		OutputVariables: output,
	})

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err = engine.RunProcessInstance(ctx, parentInstance, tokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance for multi instance %d: %w", instance.Key, err)
			}
		}()
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
	err = batch.AddParentLockedInstance(ctx, instance, parentInstance)
	if err != nil {
		return err
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
	parentElement := parentInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(updatedParentToken.ElementId).(*bpmn20.TCallActivity)
	variableHolder := runtime.NewVariableHolder(&parentInstance.ProcessInstance().VariableHolder, nil)
	output, err := variableHolder.PropagateOutputVariablesToParent(parentElement.GetOutputMapping(), instance.ProcessInstance().VariableHolder.LocalVariables(), engine.evaluateExpression)
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
	}

	err = engine.cancelBoundarySubscriptions(ctx, batch, parentInstance, &updatedParentToken)
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
	batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:             updatedParentToken.ElementInstanceKey,
		OutputVariables: output,
	})

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err = engine.RunProcessInstance(ctx, parentInstance, tokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance for call activity %d: %w", instance.Key, err)
			}
		}()
	})
	return nil
}

func (engine *Engine) handleParentProcessContinuationForMultiInstance(ctx context.Context, batch *EngineBatch, instance *runtime.MultiInstanceInstance, flowNode bpmn20.FlowNode) error {
	parentProcessTargetElementId := instance.ParentProcessTargetElementId
	parentProcessInstanceKey := instance.ParentProcessExecutionToken.ProcessInstanceKey
	parentTokenKey := instance.ParentProcessExecutionToken.Key
	parentProcessTargetElementInstanceKey := instance.ParentProcessTargetElementInstanceKey

	//setup
	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, parentProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find parent process instance %d: %w", parentProcessInstanceKey, err)
	}
	err = batch.AddParentLockedInstance(ctx, instance, parentInstance)
	if err != nil {
		return err
	}

	updatedParentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		return fmt.Errorf("failed to get token by key: %w", err)
	}
	if updatedParentToken.ElementInstanceKey != parentProcessTargetElementInstanceKey {
		log.Infof(ctx, "failed to handleParentProcessContinuation for multiInstance instance %d: parent token is no longer waiting at target element instance key %d", instance.ProcessInstance().Key, parentProcessTargetElementInstanceKey)
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
	parentElement := parentInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(parentProcessTargetElementId).(bpmn20.Activity)
	element, ok := flowNode.(bpmn20.Activity)
	if !ok && element.GetMultiInstance() == nil {
		log.Errorf(ctx, "cannot handle multiInstance Parent Process Continuation - MultiInstance missing data")
	}
	elementInstances, err := engine.persistence.GetFlowElementInstancesByProcessInstanceKey(ctx, instance.ProcessInstance().Key, true)
	if err != nil {
		return err
	}
	outputCollection := make([]interface{}, 0)
	for _, elementInstance := range elementInstances {
		evaluatedOutput, err := engine.evaluateExpression(parentElement.GetMultiInstance().LoopCharacteristics.OutputElementExpression, elementInstance.OutputVariables)
		if err != nil {
			return fmt.Errorf("failed to evaluate outputElementExpression on elementInstance %d for parent isntance %d: %w", elementInstance.Key, parentInstance.ProcessInstance().Key, err)
		}
		outputCollection = append(outputCollection, evaluatedOutput)
	}
	parentInstance.ProcessInstance().VariableHolder.SetLocalVariable(parentElement.GetMultiInstance().LoopCharacteristics.OutputCollectionName, outputCollection)

	err = engine.cancelBoundarySubscriptions(ctx, batch, parentInstance, &updatedParentToken)
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
	batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:             updatedParentToken.ElementInstanceKey,
		OutputVariables: map[string]any{parentElement.GetMultiInstance().LoopCharacteristics.OutputCollectionName: outputCollection},
	})

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			err = engine.RunProcessInstance(ctx, parentInstance, tokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance for multi instance %d: %w", instance.Key, err)
			}
		}()
	})

	return nil
}

func (engine *Engine) handleParentProcessContinuation(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, flowNode bpmn20.FlowNode) error {
	switch inst := instance.(type) {
	case *runtime.SubProcessInstance:
		err := engine.handleParentProcessContinuationForSubProcess(ctx, batch, inst, flowNode)
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

func (engine *Engine) handleMultiInstanceElementTransition(ctx context.Context,
	batch *EngineBatch,
	instance *runtime.MultiInstanceInstance,
	element bpmn20.FlowNode,
	currentToken runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
	multiInstanceElement, ok := element.(bpmn20.Activity)
	if !ok || multiInstanceElement.GetMultiInstance() == nil {
		return nil, fmt.Errorf("failed to handle multi instance transition")
	}

	multiInstanceElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, instance.ParentProcessTargetElementInstanceKey)
	if err != nil {
		currentToken.State = runtime.TokenStateFailed
		return []runtime.ExecutionToken{currentToken}, err
	}
	multiInstanceInputCollectionLength := len(multiInstanceElementInstance.InputVariables[multiInstanceElement.GetMultiInstance().LoopCharacteristics.InputElementName].([]interface{}))

	if multiInstanceElement.GetMultiInstance().IsSequential == true {
		currentToken.State = runtime.TokenStateRunning
		currentToken.ElementInstanceKey = engine.generateKey()
		return []runtime.ExecutionToken{currentToken}, nil
	} else {
		tokens, err := engine.persistence.GetCompletedTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
		if err != nil {
			currentToken.State = runtime.TokenStateFailed
			return []runtime.ExecutionToken{currentToken}, err
		}
		if len(tokens) == multiInstanceInputCollectionLength-1 {
			currentToken.State = runtime.TokenStateRunning
			currentToken.ElementInstanceKey = engine.generateKey()
			return []runtime.ExecutionToken{currentToken}, nil
		} else {
			currentToken.State = runtime.TokenStateCompleted
			return []runtime.ExecutionToken{currentToken}, nil
		}
	}
}
