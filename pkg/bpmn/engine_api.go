package bpmn

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"time"
)

// Start will start the process engine instance.ProcessInstance().
// Engine will start to pull process instances with execution tokens that need to be processed
func (engine *Engine) Start() error {
	ctx := context.Background()
	if engine.timerManager != nil {
		engine.timerManager.stop()
	}
	engine.timerManager = newTimerManager(engine.processTimer, engine.persistence.FindTimersTo, 10*time.Second)
	engine.timerManager.start()
	tokens, err := engine.persistence.GetRunningTokens(ctx)
	if err != nil {
		return fmt.Errorf("failed to load running tokens: %w", err)
	}
	type instanceToStart struct {
		instance runtime.ProcessInstance
		tokens   []runtime.ExecutionToken
	}
	instancesToStart := make(map[int64]instanceToStart)
	for _, token := range tokens {
		if val, ok := instancesToStart[token.ProcessInstanceKey]; ok {
			val.tokens = append(val.tokens, token)
			instancesToStart[token.ProcessInstanceKey] = val
		} else {
			instance, err := engine.persistence.FindProcessInstanceByKey(ctx, token.ProcessInstanceKey)
			if err != nil {
				return fmt.Errorf("failed to load instance %d for token %d: %w", token.ProcessInstanceKey, token.Key, err)
			}
			instancesToStart[token.ProcessInstanceKey] = instanceToStart{
				instance: instance,
				tokens:   []runtime.ExecutionToken{token},
			}
		}
	}
	for _, instance := range instancesToStart {
		err := engine.runProcessInstance(ctx, instance.instance, instance.tokens)
		if err != nil {
			engine.logger.Error(fmt.Sprintf("failed to run process instance %d: %s", instance.instance.ProcessInstance().Key, err.Error()))
		}
	}

	return nil
}

func (engine *Engine) Stop() {
	engine.timerManager.stop()
}

// CreateInstanceById creates a new instance for a process with given process ID and uses latest version (if available)
// Might return BpmnEngineError, when no process with given ID was found
func (engine *Engine) CreateInstanceById(ctx context.Context, processId string, variableContext map[string]interface{}) (runtime.ProcessInstance, error) {
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
func (engine *Engine) CreateInstanceByKey(ctx context.Context, definitionKey int64, variableContext map[string]interface{}) (runtime.ProcessInstance, error) {
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
func (engine *Engine) CreateInstance(ctx context.Context, process *runtime.ProcessDefinition, variableContext map[string]interface{}) (runtime.ProcessInstance, error) {
	return engine.createInstance(
		ctx,
		process,
		runtime.NewVariableHolder(nil, variableContext),
		nil,
	)
}

func (engine *Engine) CancelInstanceByKey(ctx context.Context, instanceKey int64) error {

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, instanceKey)
	if err != nil {
		return fmt.Errorf("failed to find process instance %d: %w", instanceKey, err)
	}
	// Check if process is root
	if instance.Type() != runtime.ProcessTypeDefault {
		// Cancel all child process instances
		return fmt.Errorf("cannot cancel process instance %d, it is not a root process", instance.ProcessInstance().Key)
	}

	batch := engine.persistence.NewBatch()
	err = engine.cancelInstance(ctx, instance, batch)

	if err != nil {
		return err
	}

	return batch.Flush(ctx)

}

func (engine *Engine) ModifyInstance(ctx context.Context, processInstanceKey int64, elementInstanceIdsToTerminate []int64, elementIdsToStartInstance []string, variableContext map[string]interface{}) (runtime.ProcessInstance, []runtime.ExecutionToken, error) {
	engine.runningInstances.lockInstance(processInstanceKey)
	instanceUnlocked := false
	defer func() {
		if instanceUnlocked == false {
			engine.runningInstances.unlockInstance(processInstanceKey)
		}
	}()

	processInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, processInstanceKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find process instance %d: %w", processInstance.ProcessInstance().Key, err)
	}

	ctx, createSpan := engine.tracer.Start(ctx, fmt.Sprintf("modify-instance:%s", processInstance.ProcessInstance().Definition.BpmnProcessId), trace.WithAttributes(
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, processInstance.ProcessInstance().Key),
		attribute.String(otelPkg.AttributeProcessId, processInstance.ProcessInstance().Definition.BpmnProcessId),
		attribute.Int64(otelPkg.AttributeProcessDefinitionKey, processInstance.ProcessInstance().Definition.Key),
	))
	defer createSpan.End()

	batch := engine.persistence.NewBatch()

	var activeTokensLeft []runtime.ExecutionToken
	if len(elementInstanceIdsToTerminate) > 0 {
		activeTokensLeft, err = engine.terminateExecutionTokens(ctx, batch, elementInstanceIdsToTerminate, processInstanceKey)
		if err != nil {
			createSpan.RecordError(err)
			createSpan.SetStatus(codes.Error, err.Error())
			return processInstance, nil, err
		}
	}

	startedTokens, err := engine.startExecutionTokens(ctx, batch, elementIdsToStartInstance, processInstance)
	if err != nil {
		createSpan.RecordError(err)
		createSpan.SetStatus(codes.Error, err.Error())
		return processInstance, nil, err
	}

	activeTokens := append(activeTokensLeft, startedTokens...)

	for key, value := range variableContext {
		processInstance.ProcessInstance().VariableHolder.SetLocalVariable(key, value)
	}
	err = batch.SaveProcessInstance(ctx, processInstance)
	if err != nil {
		createSpan.RecordError(err)
		createSpan.SetStatus(codes.Error, err.Error())
		return processInstance, nil, err
	}

	err = batch.Flush(ctx)
	if err != nil {
		return processInstance, activeTokens, fmt.Errorf("failed to modify process instance %d: %w", processInstance.ProcessInstance().Key, err)
	}

	engine.runningInstances.unlockInstance(processInstance.ProcessInstance().Key)
	instanceUnlocked = true
	err = engine.runProcessInstance(ctx, processInstance, activeTokens)
	if err != nil {
		return processInstance, activeTokens, err
	}

	return processInstance, activeTokens, nil
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

func (engine *Engine) StartInstanceOnElementsByKey(ctx context.Context, processDefinitionKey int64, startingElementIds []string, variableContext map[string]interface{}, parentToken *runtime.ExecutionToken) (runtime.ProcessInstance, error) {
	processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("no process definition with key %d was found (prior loaded into the engine)", processDefinitionKey), err)
	}

	startingFlowNodes := make([]bpmn20.FlowNode, 0, len(startingElementIds))
	for _, startingFlowNodeId := range startingElementIds {
		startNode := processDefinition.Definitions.Process.GetFlowNodeById(startingFlowNodeId)
		if startNode == nil {
			return nil, fmt.Errorf("could not find starting flow node with id %s in process definition %d", startingFlowNodeId, processDefinition.Key)
		}
		startingFlowNodes = append(startingFlowNodes, startNode)
	}

	return engine.createInstanceWithStartingElements(ctx, &processDefinition, startingFlowNodes, runtime.NewVariableHolder(nil, variableContext), nil)
}
