package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

func (engine *Engine) initMultiInstances(
	ctx context.Context,
	instance *runtime.ProcessInstance,
	activityElement *bpmn20.TActivity,
	currentToken *runtime.ExecutionToken,
	batch storage.Batch,
	activity runtime.Activity,
) error {
	var activityResult runtime.ActivityState
	mi := activityElement.MultiInstanceLoopCharacteristics
	vhs, err := engine.prepareVariableHoldersForParallelMultiInstance(instance, activityElement)
	if err != nil {
		currentToken.State = runtime.TokenStateFailed
		return fmt.Errorf("failed to prepare variable holders for multi-instance: %w", err)
	}
	if mi.IsSequential {
		// TODO: serial
	}

	var lastTransitionToken runtime.ExecutionToken

	for _, variableHolder := range vhs {
		instanceCopy := *instance
		instanceCopy.VariableHolder = runtime.NewVariableHolderForPropagation(&instance.VariableHolder, variableHolder.Variables())
		activityResult, err = engine.activityElementExecutor(ctx, batch, &instanceCopy, *currentToken, activity)
		if err != nil {
			return err
		}
		if activityResult == runtime.ActivityStateCompleted {
			// internal task is completed synchronously => trigger multi-instance completion
			tokens, err := engine.handleActivityCompletion(ctx, batch, &instanceCopy, activity.Element(), *currentToken, true)
			if err != nil {
				return err
			}
			if len(tokens) > 0 && tokens[0].State == runtime.TokenStateRunning && tokens[0].ElementId != activity.Element().GetId() {
				lastTransitionToken = tokens[0]
			}
		}
	}

	currentToken.State = runtime.TokenStateWaiting
	if lastTransitionToken.Key != 0 {
		*currentToken = lastTransitionToken
	}
	return nil
}

func (engine *Engine) prepareVariableHoldersForParallelMultiInstance(instance *runtime.ProcessInstance, element *bpmn20.TActivity) ([]runtime.VariableHolder, error) {

	mi := element.MultiInstanceLoopCharacteristics
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

	instance.VariableHolder.SetVariable(mi.GetInputCollectionName(element.TBaseElement), inputCollection)
	vh := make([]runtime.VariableHolder, total)

	inputElementName := mi.LoopCharacteristics.InputElement
	for i, input := range inputCollection {
		variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
		if inputElementName != "" {
			variableHolder.SetVariable(inputElementName, input)
		}
		vh[i] = variableHolder
	}
	instance.VariableHolder.SetVariable(mi.GetOutCollectionName(element.TBaseElement), make([]interface{}, 0, total))
	return vh, nil
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

			// TODO: filter variables which are not outputCollection

			var outputCollection []interface{}
			var ok bool
			var isNested bool
			if parent := instance.VariableHolder.GetParent(); parent != nil {
				isNested = true
				outputCollection, ok = parent.GetVariable(outColName).([]interface{})
			} else {
				outputCollection, ok = instance.VariableHolder.GetVariable(outColName).([]interface{})
			}
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

			if isNested {
				instance.VariableHolder.GetParent().SetVariable(outColName, outputCollection)
			} else {
				instance.VariableHolder.SetVariable(outColName, outputCollection)
			}

			inputCollectionObject := instance.VariableHolder.GetVariable(mi.GetInputCollectionName(activityElement.TBaseElement))
			inputCollection, ok := inputCollectionObject.([]interface{})
			if !ok {
				return false, errors.New("inputCollection is not a collection")
			}

			// persistently store the outputCollection
			err = batch.SaveProcessInstance(ctx, *instance)
			if err != nil {
				return false, fmt.Errorf("failed to save updated parent process instance: %w", err)
			}

			// check if any boundary events have been triggered
			boundaryEvents := bpmn20.FindBoundaryEventsForActivity(&instance.Definition.Definitions, element)
			if len(boundaryEvents) > 0 {
				// check for active message subscriptions
				subscriptions, err := engine.persistence.FindProcessInstanceMessageSubscriptions(ctx, instance.Key, runtime.ActivityStateActive)
				if err != nil {
					return false, fmt.Errorf("failed to find message subscriptions for instance %d: %w", instance.Key, err)
				}

				// if there are no active subscriptions for this activity, it means a boundary event was triggered
				hasActiveSubscription := false
				for _, sub := range subscriptions {
					if sub.Token.ElementId == element.GetId() {
						hasActiveSubscription = true
						break
					}
				}

				// if a boundary event was triggered, and it was interrupting, don't complete the multi-instance activity
				if !hasActiveSubscription {
					for _, be := range boundaryEvents {
						if be.CancellActivity {
							return false, nil
						}
					}
				}
			}

			// TODO: implement completion condition
			if len(outputCollection) != len(inputCollection) {
				return false, nil
			}

			batch.AddPostFlushAction(ctx, func() {
				if mi.LoopCharacteristics.OutputCollection == "" {
					// clear an output collection if it was not defined and a default name was used instead
					instance.VariableHolder.SetVariable(mi.GetOutCollectionName(activityElement.TBaseElement), nil)
				}
				instance.VariableHolder.SetVariable(mi.GetInputCollectionName(activityElement.TBaseElement), nil)
			})
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

func (engine *Engine) isBoundaryEventMultiInstance(instance *runtime.ProcessInstance, listener *bpmn20.TBoundaryEvent) bool {
	element := instance.Definition.Definitions.Process.GetFlowNodeById(listener.AttachedToRef)
	activityElement := engine.castToTActivity(element)
	return activityElement != nil && activityElement.MultiInstanceLoopCharacteristics.LoopCharacteristics.InputCollection != ""
}

func (engine *Engine) terminateMultiInstanceJobs(
	ctx context.Context,
	instance *runtime.ProcessInstance,
	token runtime.ExecutionToken,
	batch storage.Batch,
) error {
	// find and cancel all jobs
	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(ctx, instance.Key)
	if err != nil {
		return fmt.Errorf("failed to find jobs for multi-instance jobs termination %s: %w", token.ElementId, err)
	}
	// filter jobs by element ID
	for _, job := range jobs {
		if job.ElementId == token.ElementId {
			job.State = runtime.ActivityStateTerminated
			err = batch.SaveJob(ctx, job)
			if err != nil {
				return fmt.Errorf("failed to terminate multi-instance job %d: %w", job.Key, err)
			}
		}
	}
	return nil
}

func (engine *Engine) shouldCallActivityContinue(instance *runtime.ProcessInstance, element bpmn20.FlowNode) (bool, error) {
	activityElement := engine.castToTActivity(element)
	mi := activityElement.MultiInstanceLoopCharacteristics
	if mi.LoopCharacteristics.InputCollection == "" {
		return true, nil
	}

	outColName := mi.GetOutCollectionName(activityElement.TBaseElement)
	outputCollection, ok := instance.GetVariable(outColName).([]interface{})
	if !ok {
		return false, errors.New("outputCollection is not a collection")
	}

	// TODO: implement completion condition
	inputCollectionObject := instance.VariableHolder.GetVariable(mi.GetInputCollectionName(activityElement.TBaseElement))
	inputCollection, ok := inputCollectionObject.([]interface{})
	if !ok {
		return false, errors.New("inputCollection is not a collection")
	}
	return len(outputCollection) == len(inputCollection), nil
}
