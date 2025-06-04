package bpmn

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

func (engine *Engine) createCallActivity(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TCallActivity, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	// 1. create a new process instance while mapping input variables or find already running
	processId := element.CalledElement.ProcessId
	variableHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if err := evaluateLocalVariables(&variableHolder, element.GetInputMapping()); err != nil {
		instance.State = runtime.ActivityStateFailed
		if err != nil {
			return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for call activity: %w", err)
		}
		return runtime.ActivityStateFailed, nil
	}

	processDefinition, err := engine.persistence.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("no process with id=%s was found (prior loaded into the engine)", processId), err)
	}

	calledProcessInstance, err := engine.createInstance(ctx, &processDefinition, variableHolder, &currentToken)
	if err != nil {
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("failed to create process instance: %s", processId), err)
	}

	if calledProcessInstance.State == runtime.ActivityStateActive {
		// Called process still running parent needs to wait
		return runtime.ActivityStateActive, nil
	} else if calledProcessInstance.State == runtime.ActivityStateCompleted {

		// Called process completed. Propagate variables back to the parent
		err := propagateVariablesBackToParent(*instance, *calledProcessInstance, element)
		if err != nil {
			return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("failed to propagate variables back to parent: %s", processId), err)
		}

		// And complete right away
		return runtime.ActivityStateCompleted, nil
	} else {
		return runtime.ActivityStateFailed, errors.Join(newEngineErrorf("unknown error when receiving result from call activity: %s", processId), err)
	}

	// 2. create a new execution token
	// 3. start the process instance
}

func propagateVariablesBackToParent(instance runtime.ProcessInstance, calledProcessInstance runtime.ProcessInstance, element *bpmn20.TCallActivity) error {
	// map the variables back to the parent
	variableHolder := runtime.NewVariableHolderForPropagation(&instance.VariableHolder, calledProcessInstance.VariableHolder.Variables())

	if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
		instance.State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
	}
	return nil
}

func (engine *Engine) handleCallActivityParentContinuation(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, parentInstance runtime.ProcessInstance, token runtime.ExecutionToken) error {

	element := parentInstance.Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
	// map the variables back to the parent
	callActivity, ok := element.(*bpmn20.TCallActivity)
	if !ok {
		// handle the case where element is not a *bpmn20.TCallActivity
		return errors.New("element is not a *bpmn20.TCallActivity")
	}
	err := propagateVariablesBackToParent(parentInstance, instance, callActivity)

	if err != nil {
		return err
	}
	// unblock token of the parent
	// instance.ParentProcessExecutionToken.State = runtime.TokenStateActive

	ppi, err := engine.persistence.FindProcessInstanceByKey(ctx, instance.ParentProcessExecutionToken.ProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find parent process instance %d", instance.ParentProcessExecutionToken.ProcessInstanceKey)
	}

	element = ppi.Definition.Definitions.Process.GetFlowNodeById(instance.ParentProcessExecutionToken.ElementId)

	tokens, err := engine.handleSimpleTransition(ctx, &ppi, element, *instance.ParentProcessExecutionToken)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to handle simple transition for call activity: %s", instance.ParentProcessExecutionToken.ElementId), err)
	}

	engine.runProcessInstance(ctx, &ppi, tokens)

	return nil
}
