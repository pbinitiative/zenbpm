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
	"time"
)

func (engine *Engine) createCallActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	processId := element.CalledElement.ProcessId
	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if err := variableHolder.EvaluateAndSetInputMappings(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		instance.State = runtime.ActivityStateFailed
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
			calledProcessInstance, err := engine.createInstance(ctx, &processDefinition, variableHolder, &currentToken)
			if err != nil {
				engine.runningInstances.lockInstance(instance)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance)
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
	if err := variableHolder.EvaluateAndSetInputMappings(element.GetInputMapping(), engine.evaluateExpression); err != nil {
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

			_, err := engine.createInstanceWithStartingElements(ctx, instance.Definition, startingFlowNodes, variableHolder, &currentToken, &element.Id)
			if err != nil {
				engine.runningInstances.lockInstance(instance)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance)
				engine.logger.Error("failed to run subProcess %d: %w", instance.Key, err)
				return
			}

			todoSpan.End()
		}()
	})
	return runtime.ActivityStateActive, nil
}

func (engine *Engine) handleMultiInstanceActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element bpmn20.TActivity, activity runtime.Activity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	if instance.ParentProcessExecutionToken == nil {
		if element.MultiInstance.IsSequential {
			tokens, err := engine.startParallelMultiInstance(ctx, batch, instance, activity, &element, currentToken)
			if err != nil {
				return nil, err
			}
			return tokens, nil
		}
		tokens, err := engine.startSequentialMultiInstance(ctx, batch, instance, activity, element, currentToken)
		if err != nil {
			return nil, err
		}
		return tokens, nil
	}

	elementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, instance.ParentProcessExecutionToken.ElementInstanceKey)
	if err != nil {
		return nil, err
	}
	inputCollection := elementInstance.InputVariables[element.MultiInstance.InputElementName].([]interface{})

	multiInstancesAlreadyStarted, err := engine.persistence.GetFlowElementInstanceCountByProcessInstanceKey(ctx, instance.Key)
	if err != nil {
		return nil, err
	}
	if multiInstancesAlreadyStarted > len(inputCollection) {
		return nil, fmt.Errorf("there is more instances started in MultiInstance than was supposed to, token key:  %d", instance.ParentProcessExecutionToken.Key)
	}
	instance.VariableHolder.SetLocalVariable(element.MultiInstance.InputElementName, inputCollection[multiInstancesAlreadyStarted+1])

	//TODO: move to each method
	newFlowElementInstance := runtime.FlowElementInstanceItem{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.Key,
		ElementId:          element.GetId(),
		CreatedAt:          time.Now(),
		ExecutionToken:     currentToken,
		InputVariables:     instance.VariableHolder.LocalVariables(),
		OutputVariables:    nil,
	}
	err = engine.persistence.SaveFlowElementInstance(ctx, newFlowElementInstance)
	if err != nil {
		return nil, err
	}

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
		//TODO: move to each method
		newFlowElementInstance.OutputVariables = output
		err = engine.persistence.SaveFlowElementInstance(ctx, newFlowElementInstance)
		if err != nil {
			return nil, err
		}
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

			_, err := engine.createInstanceWithStartingElements(ctx, instance.Definition, startingFlowNodes, inputVariables, &currentToken, &element.Id)
			if err != nil {
				engine.runningInstances.lockInstance(instance)
				engine.handleIncident(ctx, currentToken, err, subProcessSpan)
				engine.runningInstances.unlockInstance(instance)
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

			_, err := engine.createInstanceWithStartingElements(ctx, instance.Definition, startingFlowNodes, inputVariables, &currentToken, &element.Id)
			if err != nil {
				engine.runningInstances.lockInstance(instance)
				engine.handleIncident(ctx, currentToken, err, subProcessSpan)
				engine.runningInstances.unlockInstance(instance)
				engine.logger.Error("failed to run sequentialMultiInstance %d: %w", instance.Key, err)
				return
			}

			subProcessSpan.End()
		}()
	})

	currentToken.State = runtime.TokenStateWaiting
	return []runtime.ExecutionToken{currentToken}, nil
}

func (engine *Engine) handleParentProcessContinuation(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, token runtime.ExecutionToken) error {
	ppi, err := engine.persistence.FindProcessInstanceByKey(ctx, instance.ParentProcessExecutionToken.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find parent process instance %d", instance.ParentProcessExecutionToken.ProcessInstanceKey), err)
	}
	parentInstance := &ppi

	engine.runningInstances.lockInstance(parentInstance)

	element := ppi.Definition.Definitions.Process.GetFlowNodeById(token.ElementId)

	variableHolder := runtime.NewVariableHolder(&parentInstance.VariableHolder, instance.VariableHolder.LocalVariables())
	// map the variables back to the parent
	switch element.(type) {
	case *bpmn20.TSubProcess:
		if err := variableHolder.PropagateLocalVariables(element.(*bpmn20.TSubProcess).GetOutputMapping(), engine.evaluateExpression); err != nil {
			instance.State = runtime.ActivityStateFailed
			return fmt.Errorf("failed to propagate variables back to parent: %w", err)
		}
	case *bpmn20.TCallActivity:
		if err := variableHolder.PropagateLocalVariables(element.(*bpmn20.TCallActivity).GetOutputMapping(), engine.evaluateExpression); err != nil {
			instance.State = runtime.ActivityStateFailed
			return fmt.Errorf("failed to propagate variables back to parent: %w", err)
		}
	}

	// unblock token of the parent
	ppi, err = engine.persistence.FindProcessInstanceByKey(ctx, instance.ParentProcessExecutionToken.ProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find parent process instance %d", instance.ParentProcessExecutionToken.ProcessInstanceKey)
	}

	element = ppi.Definition.Definitions.Process.GetFlowNodeById(instance.ParentProcessExecutionToken.ElementId)

	tokens, err := engine.handleSimpleTransition(ctx, batch, parentInstance, element, *instance.ParentProcessExecutionToken)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to handle simple transition for call activity: %s", instance.ParentProcessExecutionToken.ElementId), err)
	}

	for _, tok := range tokens {
		batch.SaveToken(ctx, tok)
	}

	err = batch.SaveProcessInstance(ctx, *parentInstance)
	if err != nil {
		return fmt.Errorf("failed to save updated parent process instance: %w", err)
	}
	batch.AddPostFlushAction(ctx, func() {
		go func() {
			engine.runningInstances.unlockInstance(parentInstance)
			err = engine.runProcessInstance(ctx, parentInstance, tokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance: %w", err)
			}
		}()
	})

	return nil
}
