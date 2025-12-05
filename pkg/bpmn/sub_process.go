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
			//TODO: We need tokenSpan from when the parent token started in runProcessInstance() to properly fail the token and span
			ctx, todoSpan := engine.tracer.Start(ctx, fmt.Sprintf("callActivity:%s", element.Id), trace.WithAttributes(
				attribute.Int64("parentProcessInstanceKey", instance.Key),
			))
			calledProcessInstance, err := engine.createInstance(ctx, &processDefinition, variableHolder, &currentToken, nil)
			if err != nil {
				engine.runningInstances.lockInstance(instance)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance)
				engine.logger.Error("failed to run activity instance %d: %w", calledProcessInstance.Key, err)
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
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for call activity: %w", err)
	}

	batch.AddPostFlushAction(ctx, func() {
		go func() {
			//TODO: We need tokenSpan from when the parent token started in runProcessInstance() to properly fail the token and span
			ctx, todoSpan := engine.tracer.Start(ctx, fmt.Sprintf("subProcess:%s", element.Id), trace.WithAttributes(
				attribute.Int64("parentProcessInstanceKey", instance.Key),
			))
			calledProcessInstance, err := engine.createInstance(ctx, instance.Definition, variableHolder, &currentToken, &element.Id)
			if err != nil {
				engine.runningInstances.lockInstance(instance)
				engine.handleIncident(ctx, currentToken, err, todoSpan)
				engine.runningInstances.unlockInstance(instance)
				engine.logger.Error("failed to run activity instance %d: %w", calledProcessInstance.Key, err)
				return
			}
			todoSpan.End()
		}()
	})
	return runtime.ActivityStateActive, nil
}

func (engine *Engine) handleParentProcessContinuation(ctx context.Context, batch storage.Batch, instance runtime.ProcessInstance, token runtime.ExecutionToken) error {

	ppi, err := engine.persistence.FindProcessInstanceByKey(ctx, instance.ParentProcessExecutionToken.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find parent process instance %d", instance.ParentProcessExecutionToken.ProcessInstanceKey), err)
	}
	parentInstance := &ppi

	engine.runningInstances.lockInstance(parentInstance)

	element := ppi.Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
	// map the variables back to the parent
	callActivity, ok := element.(*bpmn20.TCallActivity)
	if !ok {
		// handle the case where element is not a *bpmn20.TCallActivity
		return errors.New("element is not a *bpmn20.TCallActivity")
	}

	variableHolder := runtime.NewVariableHolder(&parentInstance.VariableHolder, instance.VariableHolder.LocalVariables())

	if err := variableHolder.PropagateLocalVariables(callActivity.GetOutputMapping(), engine.evaluateExpression); err != nil {
		instance.State = runtime.ActivityStateFailed
		return fmt.Errorf("failed to propagate variables back to parent: %w", err)
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
