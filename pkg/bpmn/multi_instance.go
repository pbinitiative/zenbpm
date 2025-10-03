// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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
		return fmt.Errorf("failed to prepare variable holders fr multi-instance: %w", err)
	}
	if mi.IsSequential {
		// TODO: serial
	}

	var lastTransitionToken runtime.ExecutionToken
	var lastTokenSet bool

	for _, variableHolder := range vhs {
		instanceCopy := *instance
		instanceCopy.VariableHolder = variableHolder
		activityResult, err = engine.activityElementExecutor(ctx, batch, &instanceCopy, *currentToken, activity)
		if err != nil {
			return err
		}
		if activityResult == runtime.ActivityStateActive {
			// an external worker will complete the job later
			continue
		}
		if activityResult == runtime.ActivityStateCompleted {
			// Internal handler completed synchronously => force multi-instance completion (usefully for tests)
			tokens, err := engine.handleSimpleTransition(ctx, batch, &instanceCopy, activity.Element(), *currentToken)
			if err != nil {
				return err
			}
			if len(tokens) > 0 {
				lastTransitionToken = tokens[0]
				lastTokenSet = true
			}
		}
	}

	currentToken.State = runtime.TokenStateWaiting

	// initialize and persist the output collection for the multi-instance
	instance.VariableHolder.SetVariable(mi.GetOutCollectionName(activityElement.TBaseElement), make([]interface{}, 0, len(vhs)))
	if err := batch.SaveProcessInstance(ctx, *instance); err != nil {
		return fmt.Errorf("failed to persist initialized output collection for multi-instance: %w", err)
	}

	// In case of internal completions, propagate the updated token back
	if lastTokenSet && lastTransitionToken.State == runtime.TokenStateRunning && lastTransitionToken.ElementId != activity.Element().GetId() {
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

			// heck if any boundary events have been triggered
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
				return fmt.Errorf("failed to terminate muli-instance job %d: %w", job.Key, err)
			}
		}
	}
	return nil
}
