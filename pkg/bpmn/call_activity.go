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

	batch.AddPostFlushAction(ctx, func() {
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

func propagateVariablesBackToParent(instance *runtime.ProcessInstance, calledProcessInstance runtime.ProcessInstance, element *bpmn20.TCallActivity) error {
	// map the variables back to the parent
	variableHolder := runtime.NewVariableHolderForPropagation(&instance.VariableHolder, calledProcessInstance.VariableHolder.Variables())

	if err := propagateProcessInstanceVariables(&variableHolder, element.GetOutputMapping()); err != nil {
		instance.State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
	}
	return nil
}

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
		// handle the case where element is not a *bpmn20.TCallActivity
		return errors.New("element is not a *bpmn20.TCallActivity")
	}
	err = propagateVariablesBackToParent(parentInstance, instance, callActivity)
	if err != nil {
		return fmt.Errorf("failed to propagate variables to parent: %w", err)
	}

	element = ppi.Definition.Definitions.Process.GetFlowNodeById(instance.ParentProcessExecutionToken.ElementId)

	tokens, err := engine.handleActivityCompletion(ctx, batch, parentInstance, element, *instance.ParentProcessExecutionToken, true)
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

	shouldContinue, err := engine.shouldCallActivityContinue(parentInstance, element)
	if err != nil {
		return err
	}
	if shouldContinue {
		batch.AddPostFlushAction(ctx, func() {
			go func() {
				err = engine.runProcessInstance(ctx, parentInstance, tokens)
				if err != nil {
					engine.logger.Error("failed to continue with parent process instance: %w", err)
				}
			}()
		})
	}

	return nil
}
