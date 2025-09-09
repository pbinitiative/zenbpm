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

// createCallActivity creates a call activity, handling multi-instance or simple executions.
func (engine *Engine) createCallActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	processId := element.CalledElement.ProcessId

	processDefinition, err := engine.persistence.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("no process with id=%s was found (prior loaded into the engine)", processId), err)
	}

	mi := element.MultiInstanceLoopCharacteristics
	if mi.LoopCharacteristics.InputCollection != "" {
		if mi.IsSequential {
			// TODO: support serial multi-instance call activities
			// for now it is executed as parallel:
			return engine.createMultiParallelCallActivity(ctx, batch, instance, element, currentToken, processDefinition, mi)
		}
		return engine.createMultiParallelCallActivity(ctx, batch, instance, element, currentToken, processDefinition, mi)
	}
	return engine.createSimpleCallActivity(ctx, batch, instance, element, currentToken, processDefinition)
}

func (engine *Engine) createSimpleCallActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken, processDefinition runtime.ProcessDefinition) (runtime.ActivityState, error) {
	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	return engine.executeCreateCallActivity(ctx, batch, instance, element, currentToken, processDefinition, variableHolder)
}

func (engine *Engine) createMultiParallelCallActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken, processDefinition runtime.ProcessDefinition, mi bpmn20.TMultiInstanceLoopCharacteristics) (runtime.ActivityState, error) {
	inColExpr := mi.LoopCharacteristics.InputCollection
	inputCollection, err := evaluateExpression(inColExpr, instance.VariableHolder.Variables())
	if err != nil {
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("failed to evaluate inputCollection expression: %s", inColExpr), err)
	}
	collection, ok := inputCollection.([]interface{})
	if !ok {
		instance.State = runtime.ActivityStateFailed
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("inputCollection is not a collection"), err)
	}

	total := len(collection)
	if total == 0 {
		return runtime.ActivityStateCompleted, nil
	}

	inputElementName := mi.LoopCharacteristics.InputElement
	for _, input := range collection {
		variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
		if inputElementName != "" {
			variableHolder.SetVariable(inputElementName, input)
		}

		status, err := engine.executeCreateCallActivity(ctx, batch, instance, element, currentToken, processDefinition, variableHolder)
		if err != nil {
			return runtime.ActivityStateFailed, err
		}
		if status == runtime.ActivityStateFailed {
			return runtime.ActivityStateFailed, nil
		}
	}
	instance.VariableHolder.SetVariable(mi.GetOutCollectionName(element.TBaseElement), make([]interface{}, 0, total))
	return runtime.ActivityStateActive, nil
}

func (engine *Engine) executeCreateCallActivity(
	ctx context.Context,
	batch storage.Batch,
	instance *runtime.ProcessInstance,
	element *bpmn20.TCallActivity,
	currentToken runtime.ExecutionToken,
	processDefinition runtime.ProcessDefinition,
	variableHolder runtime.VariableHolder) (runtime.ActivityState, error) {
	if err := evaluateLocalVariables(&variableHolder, element.GetInputMapping()); err != nil {
		instance.State = runtime.ActivityStateFailed
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for call activity: %w", err)
	}

	batch.AddFlushSuccessAction(ctx, func() {
		go func() {
			calledProcessInstance, err := engine.createInstance(ctx, &processDefinition, variableHolder, &currentToken)
			if err != nil {
				// TODO: update parent instance/token with fail
				engine.logger.Error("failed to run activity instance %d: %w", calledProcessInstance.Key, err)
			}
		}()
	})
	return runtime.ActivityStateActive, nil
}

// handleCallActivityParentContinuation handles the continuation of a call activity parent.
func (engine *Engine) handleCallActivityParentContinuation(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, token runtime.ExecutionToken) error {

	ppi, err := engine.persistence.FindProcessInstanceByKey(ctx, instance.ParentProcessExecutionToken.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find parent process instance %d", instance.ParentProcessExecutionToken.ProcessInstanceKey), err)
	}
	parentInstance := &ppi

	engine.runningInstances.lockInstance(parentInstance)
	defer engine.runningInstances.unlockInstance(parentInstance)

	element := ppi.Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
	// map the variables back to the parent
	callActivity, ok := element.(*bpmn20.TCallActivity)
	if !ok {
		// handle the case where the element is not a *bpmn20.TCallActivity
		return errors.New("element is not a *bpmn20.TCallActivity")
	}

	mi := callActivity.MultiInstanceLoopCharacteristics
	if mi.LoopCharacteristics.InputCollection != "" && !mi.IsSequential {
		if mi.IsSequential {
			// TODO: support serial multi-instance call activities
			// for now it is executed as parallel:
			return engine.handleMultiParallelCallActivityParentContinuation(ctx, batch, instance, ppi, callActivity, mi)
		}
		return engine.handleMultiParallelCallActivityParentContinuation(ctx, batch, instance, ppi, callActivity, mi)
	}
	return engine.handleSimpleCallActivityParentContinuation(ctx, batch, instance, ppi, callActivity)
}

func (engine *Engine) handleSimpleCallActivityParentContinuation(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, parentInstance runtime.ProcessInstance, callActivity *bpmn20.TCallActivity) error {
	err := propagateVariablesBackToParent(&parentInstance, instance, callActivity)
	if err != nil {
		return fmt.Errorf("failed to propagate variables to parent: %w", err)
	}

	// unblock token of the parent
	ppi, err := engine.persistence.FindProcessInstanceByKey(ctx, instance.ParentProcessExecutionToken.ProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find parent process instance %d", instance.ParentProcessExecutionToken.ProcessInstanceKey)
	}
	return engine.executeHandleCallActivityParentContinuation(ctx, batch, instance, ppi)
}

func (engine *Engine) handleMultiParallelCallActivityParentContinuation(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, parentInstance runtime.ProcessInstance, callActivity *bpmn20.TCallActivity, mi bpmn20.TMultiInstanceLoopCharacteristics) error {
	outputCollection, ok := parentInstance.GetVariable(mi.GetOutCollectionName(callActivity.TBaseElement)).([]interface{})
	if !ok {
		return errors.New("outputCollection is not a collection")
	}
	if mi.LoopCharacteristics.OutputElement != "" {
		outElExpr := mi.LoopCharacteristics.OutputElement
		outVal, err := evaluateExpression(outElExpr, instance.VariableHolder.Variables())
		if err != nil {
			return fmt.Errorf("failed to evaluate outputElement expression: %w", err)
		}
		outputCollection = append(outputCollection, outVal)
	} else {
		outputCollection = append(outputCollection, true)
	}

	parentInstance.VariableHolder.SetVariable(mi.GetOutCollectionName(callActivity.TBaseElement), outputCollection)

	inColExpr := mi.LoopCharacteristics.InputCollection
	inputCollectionRaw, err := evaluateExpression(inColExpr, instance.VariableHolder.Variables())
	if err != nil {
		return fmt.Errorf("failed to evaluate inputCollection expression: %w", err)
	}
	inputCollection, ok := inputCollectionRaw.([]interface{})
	if !ok {
		return errors.New("inputCollection is not a collection")
	}

	// persistently store the outputCollection
	err = batch.SaveProcessInstance(ctx, parentInstance)
	if err != nil {
		return fmt.Errorf("failed to save updated parent process instance: %w", err)
	}

	// TODO: implement completion condition
	if len(outputCollection) != len(inputCollection) {
		return nil
	}

	// clear an output collection if it was not defined and a default name was used instead
	if mi.LoopCharacteristics.OutputCollection == "" {
		parentInstance.VariableHolder.SetVariable(mi.GetOutCollectionName(callActivity.TBaseElement), nil)
	}

	return engine.executeHandleCallActivityParentContinuation(ctx, batch, instance, parentInstance)
}

func propagateVariablesBackToParent(instance *runtime.ProcessInstance, calledProcessInstance runtime.ProcessInstance, element *bpmn20.TCallActivity) error {
	// map the variables back to the parent
	variableHolder := runtime.NewVariableHolderForPropagation(&instance.VariableHolder, calledProcessInstance.VariableHolder.Variables())

	if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
		instance.State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
	}
	return nil
}

func (engine *Engine) executeHandleCallActivityParentContinuation(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, parentInstance runtime.ProcessInstance) error {
	element := parentInstance.Definition.Definitions.Process.GetFlowNodeById(instance.ParentProcessExecutionToken.ElementId)
	ppi := &parentInstance
	tokens, err := engine.handleSimpleTransition(ctx, batch, ppi, element, *instance.ParentProcessExecutionToken)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to handle simple transition for call activity: %s", instance.ParentProcessExecutionToken.ElementId), err)
	}

	for _, tok := range tokens {
		err = batch.SaveToken(ctx, tok)
		if err != nil {
			return fmt.Errorf("failed to save token: %w", err)
		}
	}

	err = batch.SaveProcessInstance(ctx, parentInstance)
	if err != nil {
		return fmt.Errorf("failed to save updated parent process instance: %w", err)
	}
	batch.AddFlushSuccessAction(ctx, func() {
		go func() {
			err = engine.runProcessInstance(ctx, ppi, tokens)
			if err != nil {
				engine.logger.Error("failed to continue with parent process instance: %w", err)
			}
		}()
	})

	return nil
}
