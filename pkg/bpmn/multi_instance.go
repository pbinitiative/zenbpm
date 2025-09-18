package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// TODO: rename
func (engine *Engine) prepareParallelMultiInstance(instance *runtime.ProcessInstance, element bpmn20.TActivity, mi bpmn20.TMultiInstanceLoopCharacteristics) ([]runtime.VariableHolder, error) {

	inColExpr := mi.LoopCharacteristics.InputCollection
	inputCollectionObject, err := evaluateExpression(inColExpr, instance.VariableHolder.Variables())
	if err != nil {
		return []runtime.VariableHolder{}, errors.Join(newEngineErrorf("failed to evaluate inputCollection expression: %s", inColExpr), err)
	}
	inputCollection, ok := inputCollectionObject.([]interface{})
	if !ok {
		instance.State = runtime.ActivityStateFailed
		return []runtime.VariableHolder{}, errors.Join(newEngineErrorf("inputCollection is not a collection"), err)
	}

	total := len(inputCollection)
	if total == 0 {
		// do nothing
		return []runtime.VariableHolder{}, nil
	}
	vh := make([]runtime.VariableHolder, total)

	inputElementName := mi.LoopCharacteristics.InputElement
	for i, input := range inputCollection {
		variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
		if inputElementName != "" {
			variableHolder.SetVariable(inputElementName, input)
		}
		//vh = append(vh, variableHolder)
		vh[i] = variableHolder
	}
	instance.VariableHolder.SetVariable(mi.GetOutCollectionName(element.TBaseElement), make([]interface{}, 0, total))
	return vh, nil
}

func (engine *Engine) activityElementExecutor(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, currentToken runtime.ExecutionToken, activity runtime.Activity) (runtime.ActivityState, error) {
	var activityResult runtime.ActivityState
	var err error
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
		// TODO: check why should be present and implement in another way
		//if activityResult == runtime.ActivityStateActive {
		//	currentToken.State = runtime.TokenStateWaiting
		//	return []runtime.ExecutionToken{currentToken}, nil
		//}
	case *bpmn20.TBusinessRuleTask:
		activityResult, err = engine.createBusinessRuleTask(ctx, batch, instance, element, currentToken)
	default:
		return runtime.ActivityStateFailed, fmt.Errorf("failed to process %s %d: %w", element.GetType(), activity.GetKey(), errors.New("unsupported activity"))
	}
	return activityResult, err
}

func (engine *Engine) handleMultiInstanceCompletion(
	ctx context.Context,
	batch storage.Batch,
	instance *runtime.ProcessInstance,
	element bpmn20.FlowNode,
) (finished bool, err error) {
	activityElement := engine.castToTActivity(element)
	if activityElement != nil {
		mi := activityElement.MultiInstanceLoopCharacteristics
		isMultiInstance := mi.LoopCharacteristics.InputCollection != ""
		if isMultiInstance {
			outColName := mi.GetOutCollectionName(activityElement.TBaseElement)
			originalInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, instance.Key)
			if err != nil {
				return false, fmt.Errorf("failed to find process instance with key: %d", instance.Key)
			}
			outputCollection, ok := originalInstance.GetVariable(outColName).([]interface{})
			if !ok {
				return false, errors.New("outputCollection is not a collection on multi-instance flow")
			}

			if mi.LoopCharacteristics.OutputElement != "" {
				outElExpr := mi.LoopCharacteristics.OutputElement
				outVal, err := evaluateExpression(outElExpr, instance.VariableHolder.Variables())
				if err != nil {
					return false, fmt.Errorf("failed to evaluate outputElement expression: %w", err)
				}
				outputCollection = append(outputCollection, outVal)
			} else {
				outputCollection = append(outputCollection, true)
			}

			originalInstance.VariableHolder.SetVariable(outColName, outputCollection)

			// for call activity we need to update outputCollection in the working process instance
			instance.VariableHolder.SetVariable(outColName, outputCollection)

			inColExpr := mi.LoopCharacteristics.InputCollection
			inputCollectionObject, err := evaluateExpression(inColExpr, instance.VariableHolder.Variables())
			if err != nil {
				return false, fmt.Errorf("failed to evaluate inputCollection expression: %w", err)

			}
			inputCollection, ok := inputCollectionObject.([]interface{})
			if !ok {
				return false, errors.New("inputCollection is not a collection")

			}

			// persistently store the outputCollection
			err = batch.SaveProcessInstance(ctx, originalInstance)
			if err != nil {
				return false, fmt.Errorf("failed to save updated parent process instance: %w", err)
			}

			// TODO: implement completion condition
			if len(outputCollection) != len(inputCollection) {
				return false, nil
			}

			// clear an output collection if it was not defined and a default name was used instead
			if mi.LoopCharacteristics.OutputCollection == "" {
				originalInstance.VariableHolder.SetVariable(mi.GetOutCollectionName(activityElement.TBaseElement), nil)
				if err := batch.SaveProcessInstance(ctx, originalInstance); err != nil {
					return false, fmt.Errorf("failed to save process instance after clearing default outputCollection: %w", err)
				}
			}
		}
	}
	return true, nil
}

func (engine *Engine) castToTActivity(element bpmn20.FlowNode) *bpmn20.TActivity {
	var activityElement *bpmn20.TActivity
	switch e := element.(type) {
	case *bpmn20.TServiceTask:
		activityElement = &e.TExternallyProcessedTask.TTask.TActivity
	case *bpmn20.TSendTask:
		activityElement = &e.TExternallyProcessedTask.TTask.TActivity
	case *bpmn20.TUserTask:
		activityElement = &e.TTask.TActivity
	case *bpmn20.TBusinessRuleTask:
		activityElement = &e.TTask.TActivity
	case *bpmn20.TCallActivity:
		activityElement = &e.TActivity
	}
	return activityElement
}
