package bpmn

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (engine *Engine) handleMultiInstanceActivity(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element bpmn20.Activity, activity runtime.Activity, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {

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
			err := engine.createBoundaryEventSubscriptions(ctx, batch, currentToken, instance, activity.Element())
			if err != nil {
				return nil, fmt.Errorf("failed to process boundary events for %s %d: %w", element.GetType(), activity.GetKey(), err)
			}
			currentToken.State = runtime.TokenStateWaiting
			return []runtime.ExecutionToken{currentToken}, nil
		case runtime.ActivityStateFailed:
			currentToken.State = runtime.TokenStateFailed
			return []runtime.ExecutionToken{currentToken}, nil
		case runtime.ActivityStateCompleted:
			tokens, err := engine.handleElementTransition(ctx, batch, instance, activity.Element(), currentToken)
			if err != nil {
				return nil, fmt.Errorf("failed to process %s flow transition %d: %w", element.GetType(), activity.GetKey(), err)
			}
			return tokens, nil
		default:
			return []runtime.ExecutionToken{}, fmt.Errorf("unsupported activity state: %s", activityState)
		}
	}

	multiInstance, ok := instance.(*runtime.MultiInstanceInstance)
	if !ok {
		return []runtime.ExecutionToken{}, fmt.Errorf("expected MultiInstanceInstance, got %T", instance)
	}

	//Handles Multi Instance
	parentElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, multiInstance.ParentProcessTargetElementInstanceKey)
	if err != nil {
		return []runtime.ExecutionToken{}, err
	}

	inputElementName := element.GetMultiInstance().LoopCharacteristics.InputElementName
	inputCollectionVariable, ok := parentElementInstance.InputVariables[inputElementName]
	if !ok {
		return []runtime.ExecutionToken{}, fmt.Errorf("multi instance parent element instance %d is missing input collection under key %q", parentElementInstance.Key, inputElementName)
	}
	inputCollection, ok := inputCollectionVariable.([]interface{})
	if !ok {
		return []runtime.ExecutionToken{}, fmt.Errorf("multi instance input collection under key %q has unexpected type %T", inputElementName, inputCollectionVariable)
	}

	//TODO (n^2) — replaces with iteration index tracked on MultiInstanceInstance
	multiInstancesAlreadyStarted, err := engine.persistence.GetFlowElementInstanceCountByProcessInstanceKey(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return []runtime.ExecutionToken{}, err
	}
	if multiInstancesAlreadyStarted == int64(len(inputCollection)) {
		instance.ProcessInstance().State = runtime.ActivityStateCompleted
		currentToken.State = runtime.TokenStateCompleted
		return []runtime.ExecutionToken{currentToken}, nil
	}
	var activityResult runtime.ActivityState
	multiInstanceVariableContext := map[string]interface{}{
		inputElementName: inputCollection[multiInstancesAlreadyStarted],
	}
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, multiInstanceVariableContext)
	switch element := activity.Element().(type) {
	case *bpmn20.TServiceTask:
		activityResult, err = engine.createInternalTask(ctx, batch, instance, element, currentToken, variableHolder)
	case *bpmn20.TSendTask:
		activityResult, err = engine.createInternalTask(ctx, batch, instance, element, currentToken, variableHolder)
	case *bpmn20.TReceiveTask:
		activityResult, err = engine.createMultiInstanceReceiveTask(ctx, batch, instance, element, currentToken, variableHolder)
	case *bpmn20.TUserTask:
		activityResult, err = engine.createUserTask(ctx, batch, instance, element, currentToken, variableHolder)
	case *bpmn20.TCallActivity:
		activityResult, err = engine.createCallActivity(ctx, batch, instance, element, currentToken, variableHolder)
		// we created process instance and it's running in separate goroutine
	case *bpmn20.TSubProcess:
		activityResult, err = engine.createSubProcess(ctx, batch, instance, element, currentToken, variableHolder)
		// we created process instance and it's running in separate goroutine
	case *bpmn20.TBusinessRuleTask:
		activityResult, err = engine.createBusinessRuleTask(ctx, batch, instance, element, currentToken, variableHolder)
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

func (engine *Engine) startParallelMultiInstance(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	activity runtime.Activity,
	element bpmn20.Activity,
	currentToken runtime.ExecutionToken,
) (runtime.ActivityState, error) {

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	evaluatedInputCollection, err := engine.evaluateExpression(element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, variableHolder.LocalVariables())
	if err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection expression %T: %w", element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, err)
	}
	t := reflect.TypeOf(evaluatedInputCollection)
	if t != nil && (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection %T", evaluatedInputCollection)
	}
	inputCollection := make([]interface{}, 0)
	if t != nil {
		rv := reflect.ValueOf(evaluatedInputCollection)
		inputCollection = make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			inputCollection[i] = rv.Index(i).Interface()
		}
	}
	flowElementInput := variableHolder.ExecutionScopeSnapshot()
	flowElementInput[element.GetMultiInstance().LoopCharacteristics.InputElementName] = inputCollection
	err = batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().Key,
		ElementId:          element.GetId(),
		ElementType:        string(element.GetType()),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  currentToken.Key,
		InputVariables:     flowElementInput,
		OutputVariables:    nil,
	})
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	if len(inputCollection) == 0 {
		instance.ProcessInstance().VariableHolder.SetLocalVariable(element.GetMultiInstance().LoopCharacteristics.OutputCollectionName, []interface{}{})
		return runtime.ActivityStateCompleted, nil
	}

	startingFlowNodes := make([]bpmn20.FlowNode, 0, len(inputCollection))
	for range inputCollection {
		startingFlowNodes = append(startingFlowNodes, activity.Element())
	}

	parallelMultiInstance, tokens, err := engine.createInstanceWithStartingElements(
		ctx,
		batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		variableHolder,
		&runtime.MultiInstanceInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			ParentProcessTargetElementId:          element.GetId(),
			ProcessInstanceData: runtime.ProcessInstanceData{
				BusinessKey:   instance.ProcessInstance().BusinessKey,
				HistoryTTLSec: instance.ProcessInstance().HistoryTTLSec,
			},
		},
	)
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	batch.AddPostFlushAction(ctx, func() {
		safego.Go("parallel-multi-instance", engine.logger, func() {
			err := engine.RunProcessInstance(engine.context, parallelMultiInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to sub parallel multiInstance instance %d: %s", parallelMultiInstance.ProcessInstance().Key, err.Error()))
			}
		})
	})

	return runtime.ActivityStateActive, nil
}

func (engine *Engine) startSequentialMultiInstance(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, activity runtime.Activity, element bpmn20.Activity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	startingFlowNodes := []bpmn20.FlowNode{activity.Element()}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	evaluatedInputCollection, err := engine.evaluateExpression(element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, variableHolder.LocalVariables())
	if err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection expression %T: %w", element.GetMultiInstance().LoopCharacteristics.InputCollectionExpression, err)
	}
	t := reflect.TypeOf(evaluatedInputCollection)
	if t != nil && (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate inputCollection %T", evaluatedInputCollection)
	}
	inputCollection := make([]interface{}, 0)
	if t != nil {
		rv := reflect.ValueOf(evaluatedInputCollection)
		inputCollection = make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			inputCollection[i] = rv.Index(i).Interface()
		}
	}

	flowElementInput := variableHolder.ExecutionScopeSnapshot()
	flowElementInput[element.GetMultiInstance().LoopCharacteristics.InputElementName] = inputCollection
	batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().Key,
		ElementId:          element.GetId(),
		ElementType:        string(element.GetType()),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  currentToken.Key,
		InputVariables:     flowElementInput,
		OutputVariables:    nil,
	})

	if len(inputCollection) == 0 {
		instance.ProcessInstance().VariableHolder.SetLocalVariable(element.GetMultiInstance().LoopCharacteristics.OutputCollectionName, []interface{}{})
		return runtime.ActivityStateCompleted, nil
	}

	sequentialMultiInstance, tokens, err := engine.createInstanceWithStartingElements(
		ctx,
		batch,
		instance.ProcessInstance().Definition,
		startingFlowNodes,
		variableHolder,
		&runtime.MultiInstanceInstance{
			ParentProcessExecutionToken:           currentToken,
			ParentProcessTargetElementInstanceKey: currentToken.ElementInstanceKey,
			ParentProcessTargetElementId:          element.GetId(),
			ProcessInstanceData: runtime.ProcessInstanceData{
				BusinessKey:   instance.ProcessInstance().BusinessKey,
				HistoryTTLSec: instance.ProcessInstance().HistoryTTLSec,
			},
		},
	)
	if err != nil {
		return runtime.ActivityStateFailed, err
	}

	batch.AddPostFlushAction(ctx, func() {
		safego.Go("sequential-multi-instance", engine.logger, func() {
			err := engine.RunProcessInstance(engine.context, sequentialMultiInstance, tokens)
			if err != nil {
				engine.logger.Error(fmt.Sprintf("failed to start sequential multiInstance instance %d: %s", sequentialMultiInstance.ProcessInstance().Key, err.Error()))
			}
		})
	})

	return runtime.ActivityStateActive, nil
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

	parentFlowNode := parentInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(parentProcessTargetElementId)
	if parentFlowNode == nil {
		return fmt.Errorf("failed to find flow node by id %s", parentProcessTargetElementId)
	}
	parentElement, ok := parentFlowNode.(bpmn20.Activity)
	if !ok || parentElement.GetMultiInstance() == nil {
		return fmt.Errorf("failed to find flow node by id %s", parentProcessTargetElementId)
	}

	outputCollection, err := engine.collectMultiInstanceOutputCollection(ctx, instance.ProcessInstance().Key, parentElement, parentInstance.ProcessInstance().Key)
	if err != nil {
		return err
	}
	parentInstance.ProcessInstance().VariableHolder.SetLocalVariable(parentElement.GetMultiInstance().LoopCharacteristics.OutputCollectionName, outputCollection)

	err = engine.cancelBoundarySubscriptions(ctx, batch, parentInstance.ProcessInstance().Key, updatedParentToken)
	if err != nil {
		batch.Clear(ctx)
		return fmt.Errorf("failed to cancel boundary subscriptions for parent process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	tokens, err := engine.handleElementTransition(ctx, batch, parentInstance, parentElement, updatedParentToken)
	if err != nil {
		return fmt.Errorf("failed to handle simple trasition for parent instance  %d: %w", parentInstance.ProcessInstance().Key, err)
	}

	for _, tok := range tokens {
		batch.SaveToken(ctx, tok)
	}
	err = batch.SaveProcessInstance(ctx, parentInstance)
	if err != nil {
		return err
	}
	err = batch.UpdateOutputFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                updatedParentToken.ElementInstanceKey,
		ProcessInstanceKey: parentInstance.ProcessInstance().GetInstanceKey(),
		ElementId:          parentElement.GetId(),
		ElementType:        string(parentElement.GetType()),
		ExecutionTokenKey:  updatedParentToken.Key,
		OutputVariables:    map[string]any{parentElement.GetMultiInstance().LoopCharacteristics.OutputCollectionName: outputCollection},
		CompletedAt:        new(time.Now()),
	})
	if err != nil {
		return err
	}

	batch.AddPostFlushAction(ctx, func() {
		safego.Go("parent-continuation-multi-instance", engine.logger, func() {
			err := engine.RunProcessInstance(engine.context, parentInstance, tokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance for multi instance %d: %w", instance.Key, err)
			}
		})
	})

	return nil
}

func (engine *Engine) collectMultiInstanceOutputCollection(
	ctx context.Context,
	multiInstanceProcessKey int64,
	parentElement bpmn20.Activity,
	parentProcessInstanceKey int64,
) ([]interface{}, error) {
	elementInstances, err := engine.persistence.GetFlowElementInstancesByProcessInstanceKey(ctx, multiInstanceProcessKey, false)

	if err != nil {
		return nil, err
	}

	flowElementInstancesSortByCreatedAtAndKey := sortFlowElementInstancesSortByCreatedAtAndKey(elementInstances, parentElement.GetId())

	outputCollection := make([]interface{}, 0, len(flowElementInstancesSortByCreatedAtAndKey))
	for _, elementInstance := range flowElementInstancesSortByCreatedAtAndKey {
		inputAndOutputVariables, err := engine.buildActivityOutputEvaluationScope(elementInstance, parentElement)
		if err != nil {
			return nil, err
		}
		evaluatedOutput, err := engine.evaluateExpression(parentElement.GetMultiInstance().LoopCharacteristics.OutputElementExpression, inputAndOutputVariables)

		if err != nil {
			return nil, fmt.Errorf("failed to evaluate outputElementExpression on elementInstance %d for parent instance %d: %w", elementInstance.Key, parentProcessInstanceKey, err)
		}
		outputCollection = append(outputCollection, evaluatedOutput)
	}

	return outputCollection, nil
}

func (engine *Engine) buildActivityOutputEvaluationScope(elementInstance runtime.FlowElementInstance, element bpmn20.Activity) (map[string]any, error) {
	scope := make(map[string]any, len(elementInstance.InputVariables)+len(elementInstance.OutputVariables))
	for key, value := range elementInstance.InputVariables {
		scope[key] = value
	}

	inputMappings, ok := element.(interface {
		GetInputMapping() []extensions.TIoMapping
	})
	if ok {
		if err := engine.evaluateInputMappingsAgainstScope(scope, inputMappings.GetInputMapping()); err != nil {
			return nil, err
		}
	}

	for key, value := range elementInstance.OutputVariables {
		scope[key] = value
	}
	return scope, nil
}

func (engine *Engine) handleMultiInstanceElementTransition(ctx context.Context,
	batch *EngineBatch,
	instance *runtime.MultiInstanceInstance,
	element bpmn20.FlowNode,
	currentToken runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
	multiInstanceElement, ok := element.(bpmn20.Activity)
	if !ok || multiInstanceElement.GetMultiInstance() == nil {
		return nil, fmt.Errorf("element %s is not a multi instance activity", element.GetId())
	}

	multiInstanceElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, instance.ParentProcessTargetElementInstanceKey)
	if err != nil {
		currentToken.State = runtime.TokenStateFailed
		return []runtime.ExecutionToken{currentToken}, err
	}
	inputElementName := multiInstanceElement.GetMultiInstance().LoopCharacteristics.InputElementName
	multiInstanceCollectionVariable, ok := multiInstanceElementInstance.InputVariables[inputElementName]
	if !ok {
		return nil, fmt.Errorf("multi instance parent element instance %d is missing input collection under key %q", multiInstanceElementInstance.Key, inputElementName)
	}
	multiInstanceCollection, ok := multiInstanceCollectionVariable.([]interface{})
	if !ok {
		return nil, fmt.Errorf("multi instance input collection under key %q has unexpected type %T", inputElementName, multiInstanceCollectionVariable)
	}

	multiInstanceInputCollectionLength := len(multiInstanceCollection)

	if multiInstanceElement.GetMultiInstance().IsSequential {
		currentToken.State = runtime.TokenStateRunning
		currentToken.ElementInstanceKey = engine.generateKey()
		return []runtime.ExecutionToken{currentToken}, nil
	}

	completedTokens, err := engine.persistence.GetCompletedTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		currentToken.State = runtime.TokenStateFailed
		return []runtime.ExecutionToken{currentToken}, err
	}

	isLastIteration := len(completedTokens) == multiInstanceInputCollectionLength-1
	if isLastIteration {
		currentToken.State = runtime.TokenStateRunning
		currentToken.ElementInstanceKey = engine.generateKey()
		return []runtime.ExecutionToken{currentToken}, nil
	}

	currentToken.State = runtime.TokenStateCompleted
	return []runtime.ExecutionToken{currentToken}, nil
}

func sortFlowElementInstancesSortByCreatedAtAndKey(elementInstances []runtime.FlowElementInstance, parentElementId string) []runtime.FlowElementInstance {
	elementInstancesForParent := make([]runtime.FlowElementInstance, 0, len(elementInstances))

	for _, elementInstance := range elementInstances {
		if elementInstance.ElementId != parentElementId {
			continue
		}
		elementInstancesForParent = append(elementInstancesForParent, elementInstance)
	}

	sort.Slice(elementInstancesForParent, func(i, j int) bool {
		if elementInstancesForParent[i].CreatedAt.Equal(elementInstancesForParent[j].CreatedAt) {
			return elementInstancesForParent[i].Key < elementInstancesForParent[j].Key
		}
		return elementInstancesForParent[i].CreatedAt.Before(elementInstancesForParent[j].CreatedAt)
	})

	return elementInstancesForParent
}
