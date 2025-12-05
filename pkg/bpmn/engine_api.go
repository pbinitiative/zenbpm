package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// CreateInstanceById creates a new instance for a process with given process ID and uses latest version (if available)
// Might return BpmnEngineError, when no process with given ID was found
func (engine *Engine) CreateInstanceById(ctx context.Context, processId string, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	processDefinition, err := engine.persistence.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("no process with id=%s was found (prior loaded into the engine)", processId), err)
	}

	instance, err := engine.CreateInstance(ctx, &processDefinition, variableContext)
	if err != nil {
		return instance, errors.Join(newEngineErrorf("failed to create process instance: %s", processId), err)
	}

	return instance, nil
}

// CreateInstanceByKey creates a new instance for a process with given process definition key
// Might return BpmnEngineError, when no process with given ID was found
func (engine *Engine) CreateInstanceByKey(ctx context.Context, definitionKey int64, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, definitionKey)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("no process definition with key %d was found (prior loaded into the engine)", definitionKey), err)
	}

	instance, err := engine.CreateInstance(ctx, &processDefinition, variableContext)
	if err != nil {
		return instance, errors.Join(newEngineErrorf("failed to create process instance with definition key: %d", definitionKey), err)
	}

	return instance, nil
}

// CreateInstance creates a new instance for a process with given processKey
// Might return BpmnEngineError, if process key was not found
func (engine *Engine) CreateInstance(ctx context.Context, process *runtime.ProcessDefinition, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	return engine.createInstance(ctx, process, runtime.NewVariableHolder(nil, variableContext), nil, nil)
}

func (engine *Engine) CancelInstanceByKey(ctx context.Context, instanceKey int64) error {

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, instanceKey)
	if err != nil {
		return fmt.Errorf("failed to find process instance %d: %w", instanceKey, err)
	}
	// Check if process is root
	if instance.ParentProcessExecutionToken != nil {
		// Cancel all child process instances
		return fmt.Errorf("cannot cancel process instance %d, it is not a root process", instance.Key)
	}

	batch := engine.persistence.NewBatch()
	err = engine.cancelInstance(ctx, instance, batch)

	if err != nil {
		return err
	}

	return batch.Flush(ctx)

}

func (engine *Engine) ModifyInstance(ctx context.Context, processInstanceKey int64, elementInstanceIdsToTerminate []int64, elementIdsToStartInstance []string, variableContext map[string]interface{}) (*runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	processInstance := runtime.ProcessInstance{
		Key: processInstanceKey,
	}
	engine.runningInstances.lockInstance(&processInstance)
	instanceUnlocked := false
	defer func() {
		if instanceUnlocked == false {
			engine.runningInstances.unlockInstance(&processInstance)
		}
	}()

	processInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, processInstanceKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find process instance %d: %w", processInstance.Key, err)
	}

	ctx, createSpan := engine.tracer.Start(ctx, fmt.Sprintf("modify-instance:%s", processInstance.Definition.BpmnProcessId), trace.WithAttributes(
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, processInstance.Key),
		attribute.String(otelPkg.AttributeProcessId, processInstance.Definition.BpmnProcessId),
		attribute.Int64(otelPkg.AttributeProcessDefinitionKey, processInstance.Definition.Key),
	))
	defer createSpan.End()

	batch := engine.persistence.NewBatch()

	var activeTokensLeft []runtime.ExecutionToken
	if len(elementInstanceIdsToTerminate) > 0 {
		activeTokensLeft, err = engine.terminateExecutionTokens(ctx, batch, elementInstanceIdsToTerminate, processInstanceKey)
		if err != nil {
			createSpan.RecordError(err)
			createSpan.SetStatus(codes.Error, err.Error())
			return &processInstance, nil, err
		}
	}

	startedTokens, err := engine.startExecutionTokens(ctx, batch, elementIdsToStartInstance, &processInstance)
	if err != nil {
		createSpan.RecordError(err)
		createSpan.SetStatus(codes.Error, err.Error())
		return &processInstance, nil, err
	}

	activeTokens := append(activeTokensLeft, startedTokens...)

	for key, value := range variableContext {
		processInstance.VariableHolder.SetLocalVariable(key, value)
	}
	err = batch.SaveProcessInstance(ctx, processInstance)
	if err != nil {
		createSpan.RecordError(err)
		createSpan.SetStatus(codes.Error, err.Error())
		return &processInstance, nil, err
	}

	err = batch.Flush(ctx)
	if err != nil {
		return &processInstance, activeTokens, fmt.Errorf("failed to modify process instance %d: %w", processInstance.Key, err)
	}

	engine.runningInstances.unlockInstance(&processInstance)
	instanceUnlocked = true
	err = engine.runProcessInstance(ctx, &processInstance, activeTokens)
	if err != nil {
		return &processInstance, activeTokens, err
	}

	return &processInstance, activeTokens, nil
}

// FindProcessInstance searches for a given processInstanceKey
// and returns the corresponding processInstanceInfo, or otherwise nil
func (engine *Engine) FindProcessInstance(processInstanceKey int64) (runtime.ProcessInstance, error) {
	return engine.persistence.FindProcessInstanceByKey(context.TODO(), processInstanceKey)
}

// FindProcessesById returns all registered processes with given ID
// result array is ordered by version number, from 1 (first) and largest version (last)
func (engine *Engine) FindProcessesById(id string) ([]runtime.ProcessDefinition, error) {
	return engine.persistence.FindProcessDefinitionsById(context.TODO(), id)
}

func (engine *Engine) StartInstanceOnElementsByKey(ctx context.Context, processDefinitionKey int64, startingElementIds []string, variableContext map[string]interface{}, parentToken *runtime.ExecutionToken) (*runtime.ProcessInstance, error) {
	processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("no process definition with key %d was found (prior loaded into the engine)", processDefinitionKey), err)
	}

	return engine.startInstanceOnElements(ctx, &processDefinition, startingElementIds, runtime.NewVariableHolder(nil, variableContext), nil)
}
