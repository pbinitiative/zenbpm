package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (engine *Engine) createCallActivity(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	processId := element.CalledElement.ProcessId
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
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
				attribute.Int64("parentProcessInstanceKey", instance.ProcessInstance().Key),
			))

			calledProcessInstance, err := engine.createInstance(ctx, &processDefinition, variableHolder, &runtime.CallActivityInstance{
				ParentProcessExecutionToken:           currentToken,
				ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			})
			if err != nil {
				engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
				engine.logger.Error("failed to run call activity instance %d: %w", calledProcessInstance.ProcessInstance().Key, err)
				return
			}
			todoSpan.End()
		}()
	})
	return runtime.ActivityStateActive, nil
}

func (engine *Engine) createSubProcess(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, element *bpmn20.TSubProcess, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
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
				attribute.Int64("parentProcessInstanceKey", instance.ProcessInstance().Key),
				attribute.String("targetParentActivityID", element.Id),
			))

			_, err := engine.createInstanceWithStartingElements(
				ctx,
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
				engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
				engine.logger.Error("failed to run subProcess %d: %w", instance.ProcessInstance().Key, err)
				return
			}

			todoSpan.End()
		}()
	})
	return runtime.ActivityStateActive, nil
}

func (engine *Engine) handleMultiInstanceActivity(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, element bpmn20.TActivity, activity runtime.Activity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	//Starts Multi Instance
	if instance.Type() != runtime.ProcessTypeMultiInstance {
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

	multiInstance, ok := instance.(*runtime.MultiInstanceInstance)
	if !ok {
		return nil, fmt.Errorf("failed to handle multi instance activity activity instance")
	}

	//Handles Multi Instance
	parentElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, multiInstance.ParentProcessTargetElementInstanceKey)
	if err != nil {
		return nil, err
	}

	inputCollection := parentElementInstance.InputVariables[element.MultiInstance.InputElementName].([]interface{})
	multiInstancesAlreadyStarted, err := engine.persistence.GetFlowElementInstanceCountByProcessInstanceKey(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return nil, err
	}
	if multiInstancesAlreadyStarted > len(inputCollection) {
		return nil, fmt.Errorf("there is more instances started in MultiInstance than was supposed to, token key:  %d", multiInstance.ParentProcessExecutionToken.Key)
	}
	instance.ProcessInstance().VariableHolder.SetLocalVariable(element.MultiInstance.InputElementName, inputCollection[multiInstancesAlreadyStarted+1])

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
			return []runtime.ExecutionToken{currentToken}, nil
		}
		currentToken.State = runtime.TokenStateCompleted
		return []runtime.ExecutionToken{currentToken}, nil
	}

	return []runtime.ExecutionToken{}, nil
}

func (engine *Engine) startParallelMultiInstance(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, activity runtime.Activity, element *bpmn20.TActivity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	tmpVariableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
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
				attribute.Int64("parentProcessInstanceKey", instance.ProcessInstance().Key),
				attribute.String("targetParentActivityID", element.Id),
			))

			_, err := engine.createInstanceWithStartingElements(
				ctx,
				instance.ProcessInstance().Definition,
				startingFlowNodes,
				inputVariables,
				&runtime.MultiInstanceInstance{
					ParentProcessExecutionToken:           currentToken,
					ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
					ParentProcessTargetElementId:          element.Id,
				},
			)
			if err != nil {
				engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
				engine.handleIncident(ctx, currentToken, err, subProcessSpan)
				engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
				engine.logger.Error("failed to run parallelMultiInstance %d: %w", instance.ProcessInstance().Key, err)
				return
			}

			subProcessSpan.End()
		}()
	})

	currentToken.State = runtime.TokenStateWaiting
	return []runtime.ExecutionToken{currentToken}, nil
}

func (engine *Engine) startSequentialMultiInstance(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, activity runtime.Activity, element bpmn20.TActivity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	startingFlowNodes := []bpmn20.FlowNode{activity.Element()}

	tmpVariableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
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
				attribute.Int64("parentProcessInstanceKey", instance.ProcessInstance().Key),
				attribute.String("targetParentActivityID", element.Id),
			))

			_, err := engine.createInstanceWithStartingElements(
				ctx,
				instance.ProcessInstance().Definition,
				startingFlowNodes,
				inputVariables,
				&runtime.MultiInstanceInstance{
					ParentProcessExecutionToken:           currentToken,
					ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
					ParentProcessTargetElementId:          element.Id,
				},
			)
			if err != nil {
				engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
				engine.handleIncident(ctx, currentToken, err, subProcessSpan)
				engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)
				engine.logger.Error("failed to run sequentialMultiInstance %d: %w", instance.ProcessInstance().Key, err)
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

	var parentProcessTargetElementId string
	var parentProcessTargetElementInstanceKey int64
	var parentProcessInstanceKey int64
	var parentTokenKey int64
	switch instance.(type) {
	case *runtime.SubProcessInstance:
		subProcess := instance.(*runtime.SubProcessInstance)
		parentProcessTargetElementId = subProcess.ParentProcessTargetElementId
		parentProcessInstanceKey = subProcess.ParentProcessExecutionToken.ProcessInstanceKey
		parentTokenKey = subProcess.ParentProcessExecutionToken.Key
		parentProcessTargetElementInstanceKey = subProcess.ParentProcessTargetElementInstanceKey
	case *runtime.CallActivityInstance:
		callActivity := instance.(*runtime.CallActivityInstance)
		parentProcessTargetElementId = callActivity.ParentProcessTargetElementId
		parentProcessInstanceKey = callActivity.ParentProcessExecutionToken.ProcessInstanceKey
		parentTokenKey = callActivity.ParentProcessExecutionToken.Key
		parentProcessTargetElementInstanceKey = callActivity.ParentProcessTargetElementInstanceKey
	case *runtime.MultiInstanceInstance:
		subProcess := instance.(*runtime.MultiInstanceInstance)
		parentProcessTargetElementId = subProcess.ParentProcessTargetElementId
		parentProcessInstanceKey = subProcess.ParentProcessExecutionToken.ProcessInstanceKey
		parentTokenKey = subProcess.ParentProcessExecutionToken.Key
		parentProcessTargetElementInstanceKey = subProcess.ParentProcessTargetElementInstanceKey
	default:
		log.Infof(ctx, "wrong instance type %T regular instance cant be a subprocess", instance)
	}

	engine.runningInstances.lockInstance(parentProcessInstanceKey)
	instanceUnlocked := false
	defer func() {
		if instanceUnlocked == false {
			engine.runningInstances.unlockInstance(parentProcessInstanceKey)
		}
	}()

	parentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		err = fmt.Errorf("failed to get token by key: %w", err)
		return
	}
	ctx, tokenSpan := engine.tracer.Start(ctx, fmt.Sprintf("token:%s", parentToken.ElementId), trace.WithAttributes(
		attribute.String(otelPkg.AttributeElementId, parentToken.ElementId),
		attribute.Int64(otelPkg.AttributeElementKey, parentToken.ElementInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, parentToken.Key),
	))
	defer func() {
		if err != nil {
			tokenSpan.RecordError(err)
			tokenSpan.SetStatus(codes.Error, err.Error())
		}
		tokenSpan.End()
	}()

	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, parentProcessInstanceKey)
	if err != nil {
		err = fmt.Errorf("failed to find parent process instance %d: %w", parentProcessInstanceKey, err)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}

	if parentToken.ElementInstanceKey != parentProcessTargetElementInstanceKey {
		err = fmt.Errorf("failed to handleParentProcessContinuation for instance %d: parent token is no longer waiting at target element instance key %d", instance.ProcessInstance().Key, parentProcessTargetElementInstanceKey)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}

	parentElement := parentInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(parentProcessTargetElementId).(*bpmn20.TActivity)

	switch instance.(type) {
	case *runtime.SubProcessInstance, *runtime.CallActivityInstance:
		variableHolder := runtime.NewVariableHolder(&parentInstance.ProcessInstance().VariableHolder, instance.ProcessInstance().VariableHolder.LocalVariables())
		if err := variableHolder.PropagateLocalVariablesToParent(parentElement.GetOutputMapping(), engine.evaluateExpression); err != nil {
			instance.ProcessInstance().State = runtime.ActivityStateFailed
			err = fmt.Errorf("failed to propagate variables back to parent: %w", err)
			engine.handleIncident(ctx, parentToken, err, tokenSpan)
			return
		}
	case *runtime.MultiInstanceInstance:
		element, ok := flowNode.(*bpmn20.TActivity)
		if !ok && element.MultiInstance == nil {
			log.Errorf(ctx, "cannot handle multiInstance Parent Process Continuation - MultiInstance missing data")
		}
		elementInstances, err := engine.persistence.GetFlowElementInstancesByProcessInstanceKey(ctx, instance.ProcessInstance().Key, true)
		if err != nil {
			engine.handleIncident(ctx, parentToken, err, tokenSpan)
			return
		}
		outputCollection := make([]interface{}, 0)
		for _, elementInstance := range elementInstances {
			evaluatedOutput, err := engine.feelRuntime.Evaluate(parentElement.MultiInstance.OutputElementExpression, elementInstance.OutputVariables)
			if err != nil {
				err = fmt.Errorf("failed to evaluate outputElementExpression on elementInstance %d for parent isntance %d: %w", elementInstance.Key, parentInstance.ProcessInstance().Key, err)
				engine.handleIncident(ctx, parentToken, err, tokenSpan)
			}
			outputCollection = append(outputCollection, evaluatedOutput)
		}

		parentInstance.ProcessInstance().VariableHolder.SetLocalVariable(parentElement.MultiInstance.OutputCollectionName, outputCollection)
	default:
		log.Infof(ctx, "wrong instance type %T regular instance cant be a subprocess", instance)
	}

	tokens, err := engine.handleElementTransition(ctx, batch, parentInstance, parentElement, nil, parentToken)
	if err != nil {
		err = fmt.Errorf("failed to handle simple trasition for parent instance  %d: %w", parentInstance.ProcessInstance().Key, err)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}

	for _, tok := range tokens {
		batch.SaveToken(ctx, tok)
	}
	batch.SaveProcessInstance(ctx, instance)

	err = batch.Flush(ctx)
	if err != nil {
		err = fmt.Errorf("failed to flush updates for parent instance  %d: %w", parentInstance.ProcessInstance().Key, err)
		engine.handleIncident(ctx, parentToken, err, tokenSpan)
		return
	}
	engine.runningInstances.unlockInstance(parentProcessInstanceKey)
	instanceUnlocked = true

	go func() {
		err = engine.runProcessInstance(ctx, parentInstance, tokens)
		if err != nil {
			engine.logger.Error("failed to continue with parent process instance: %w", err)
		}
	}()
}
