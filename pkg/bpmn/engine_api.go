package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Start will start the process engine instance.ProcessInstance().
// Engine will start to pull process instances with execution tokens that need to be processed
func (engine *Engine) Start() error {
	ctx := context.Background()
	if engine.timerManager != nil {
		engine.timerManager.stop()
	}
	engine.timerManager = newTimerManager(engine.ProcessTimer, engine.persistence.FindTimersTo, 10*time.Second)
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
		err := engine.RunProcessInstance(ctx, instance.instance, instance.tokens)
		if err != nil {
			engine.logger.Error(fmt.Sprintf("failed to run process instance %d: %s", instance.instance.ProcessInstance().Key, err.Error()))
		}
	}

	return nil
}

func (engine *Engine) Stop() {
	engine.timerManager.stop()
}

// RunProcessInstance will run the process instance with supplied tokens.
// As a first thing it will try to acquire a lock on the process instance key to prevent parallel runs of the same process instance by multiple goroutines.
// Lock will be released once the RunProcessInstance function finishes the processing.
// Processing is finished when all the tokens are consumed (TokenStateCompleted, TokenStateCanceled) or they reached waiting (TokenStateWaiting) state
func (engine *Engine) RunProcessInstance(ctx context.Context, instance runtime.ProcessInstance, executionTokens []runtime.ExecutionToken) (err error) {
	engine.metrics.ProcessesRunning.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bpmn_process_id", instance.ProcessInstance().Definition.BpmnProcessId),
	))
	defer func() {
		engine.metrics.ProcessesRunning.Add(ctx, -1, metric.WithAttributes(
			attribute.String("bpmn_process_id", instance.ProcessInstance().Definition.BpmnProcessId),
		))
	}()
	engine.runningInstances.lockInstance(instance.ProcessInstance().Key)
	defer engine.runningInstances.unlockInstance(instance.ProcessInstance().Key)

	process := instance.ProcessInstance().Definition
	runningExecutionTokens := executionTokens
	var runErr error

	ctx, instanceSpan := engine.tracer.Start(ctx, fmt.Sprintf("run-instance:%s", instance.ProcessInstance().Definition.BpmnProcessId), trace.WithAttributes(
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, instance.ProcessInstance().Key),
		attribute.String(otelPkg.AttributeProcessId, instance.ProcessInstance().Definition.BpmnProcessId),
		attribute.Int64(otelPkg.AttributeProcessDefinitionKey, instance.ProcessInstance().Definition.Key),
	))

	instance.ProcessInstance().State = runtime.ActivityStateActive
	err = engine.persistence.SaveProcessInstance(ctx, instance)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to save process instance %d status update", instance.ProcessInstance().Key), err)
	}
	defer func() {
		if err != nil {
			instanceSpan.RecordError(err)
			instanceSpan.SetStatus(codes.Error, err.Error())
		}
		instanceSpan.End()
	}()

	// *** MAIN LOOP ***
mainLoop:
	for len(runningExecutionTokens) > 0 {
		batch, err := engine.NewEngineBatchClean()
		if err != nil {
			return fmt.Errorf("failed to create engine batch: %w", err)
		}
		currentToken := runningExecutionTokens[0]
		runningExecutionTokens = runningExecutionTokens[1:]
		if currentToken.State != runtime.TokenStateRunning {
			continue
		}
		ctx, tokenSpan := engine.tracer.Start(ctx, fmt.Sprintf("token:%s", currentToken.ElementId), trace.WithAttributes(
			attribute.String(otelPkg.AttributeElementId, currentToken.ElementId),
			attribute.Int64(otelPkg.AttributeElementKey, currentToken.ElementInstanceKey),
			attribute.Int64(otelPkg.AttributeToken, currentToken.Key),
		))

		activity, err := engine.getExecutionTokenActivity(ctx, instance, currentToken)
		if err != nil {
			runErr = errors.Join(runErr, err)
			engine.logger.Warn("failed to get execution activity", "token", currentToken.Key, "processInstance", instance.ProcessInstance().Key, "err", err)
			batch.WriteTokenIncident(ctx, currentToken, instance, err)
			incidentError := batch.Flush(ctx)
			if incidentError != nil {
				err = errors.Join(err, incidentError)
				runErr = errors.Join(runErr, incidentError)
			}
			endErrorSpan(tokenSpan, err)
			continue
		}

		updatedTokens, err := engine.processFlowNode(ctx, &batch, instance, activity, currentToken)
		if err != nil {
			runErr = errors.Join(runErr, err)
			engine.logger.Warn("failed to process token", "token", currentToken.Key, "processInstance", instance.ProcessInstance().Key, "err", err)
			batch.WriteTokenIncident(ctx, currentToken, instance, err)
			incidentError := batch.Flush(ctx)
			if incidentError != nil {
				err = errors.Join(err, incidentError)
				runErr = errors.Join(runErr, incidentError)
			}
			endErrorSpan(tokenSpan, err)
			continue
		}

		for _, tok := range updatedTokens {
			switch tok.State {
			case runtime.TokenStateRunning:
				batch.SaveToken(ctx, tok)
				runningExecutionTokens = append(runningExecutionTokens, tok)
			default:
				batch.SaveToken(ctx, tok)
			}
		}

		// if we encounter any error we switch the instance to failed state
		if runErr != nil {
			instance.ProcessInstance().State = runtime.ActivityStateFailed
		}
		batch.SaveProcessInstance(ctx, instance)

		if instance.ProcessInstance().State == runtime.ActivityStateCompleted {
			if instance.Type() != runtime.ProcessTypeDefault {
				err := engine.handleParentProcessContinuation(ctx, &batch, instance, activity.Element())
				if err != nil {
					runErr = errors.Join(runErr, err)
					engine.logger.Warn("failed to flush after processing the tokens parent", "token", currentToken.Key, "processInstance", instance.ProcessInstance().Key, "err", err)
					batch.WriteTokenIncident(ctx, currentToken, instance, err)
					incidentError := batch.Flush(ctx)
					if incidentError != nil {
						err = errors.Join(err, incidentError)
						runErr = errors.Join(runErr, incidentError)
					}
					endErrorSpan(tokenSpan, err)
					break
				}
			}
			err = batch.Flush(ctx)
			if err != nil {
				runErr = errors.Join(runErr, err)
				engine.logger.Warn("failed to flush after processing the token", "token", currentToken.Key, "processInstance", instance.ProcessInstance().Key, "err", err)
				batch.WriteTokenIncident(ctx, currentToken, instance, err)
				incidentError := batch.Flush(ctx)
				if incidentError != nil {
					err = errors.Join(err, incidentError)
					runErr = errors.Join(runErr, incidentError)
				}
				endErrorSpan(tokenSpan, err)
				break
			}
			tokenSpan.End()
			break
		}

		err = batch.Flush(ctx)
		if err != nil {
			runErr = errors.Join(runErr, err)
			engine.logger.Warn("failed to flush after processing the token", "token", currentToken.Key, "processInstance", instance.ProcessInstance().Key, "err", err)
			batch.WriteTokenIncident(ctx, currentToken, instance, err)
			incidentError := batch.Flush(ctx)
			if incidentError != nil {
				err = errors.Join(err, incidentError)
				runErr = errors.Join(runErr, incidentError)
			}
			endErrorSpan(tokenSpan, err)
			continue
		}
		tokenSpan.End()

		switch element := activity.Element().(type) {
		case *bpmn20.TEndEvent:
			if element.IsTerminate() {
				break mainLoop
			}
		}
	}

	if instance.ProcessInstance().State == runtime.ActivityStateCompleted || instance.ProcessInstance().State == runtime.ActivityStateFailed {
		engine.exportEndProcessEvent(*process, instance)
		engine.metrics.ProcessesEnded.Add(ctx, 1, metric.WithAttributes(
			attribute.String("bpmn_process_id", instance.ProcessInstance().Definition.BpmnProcessId),
		))
	}
	if runErr != nil {
		return errors.Join(newEngineErrorf("failed to run process instance %d", instance.ProcessInstance().Key), runErr)
	}

	return nil
}

func endErrorSpan(tokenSpan trace.Span, err error) {
	tokenSpan.RecordError(err)
	tokenSpan.SetStatus(codes.Error, err.Error())
	tokenSpan.End()
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
	batch := engine.persistence.NewBatch()
	instance, executionTokens, err := engine.createInstance(
		ctx,
		batch,
		process,
		runtime.NewVariableHolder(nil, variableContext),
		&runtime.DefaultProcessInstance{},
	)
	err = batch.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start process instance: %w", err)
	}

	engine.metrics.ProcessesStarted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bpmn_process_id", instance.ProcessInstance().Definition.BpmnProcessId),
	))
	err = engine.RunProcessInstance(ctx, instance, executionTokens)
	if err != nil {
		return instance, fmt.Errorf("failed to run process instance %d: %w", instance.ProcessInstance().Key, err)
	}
	return instance, nil
}

func (engine *Engine) CreateInstanceWithStartingElements(ctx context.Context, processDefinitionKey int64, startingElementIds []string, variableContext map[string]interface{}, parentToken *runtime.ExecutionToken) (runtime.ProcessInstance, error) {
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

	batch := engine.persistence.NewBatch()

	instance, executionTokens, err := engine.createInstanceWithStartingElements(ctx, batch, &processDefinition, startingFlowNodes, runtime.NewVariableHolder(nil, variableContext), &runtime.DefaultProcessInstance{})
	if err != nil {
		return nil, fmt.Errorf("failed to create instance on elements: %w", err)
	}

	err = batch.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start process instance: %w", err)
	}

	engine.metrics.ProcessesStarted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bpmn_process_id", instance.ProcessInstance().Definition.BpmnProcessId),
	))

	err = engine.RunProcessInstance(ctx, instance, executionTokens)
	if err != nil {
		return instance, fmt.Errorf("failed to run process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	return instance, nil
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

	batch, err := engine.NewEngineBatchClean()
	if err != nil {
		return fmt.Errorf("failed to create engine batch: %w", err)
	}
	err = engine.cancelInstance(ctx, instance, &batch)
	if err != nil {
		batch.Clear(ctx)
		return err
	}

	return batch.Flush(ctx)
}

func (engine *Engine) ModifyInstance(ctx context.Context, processInstanceKey int64, elementInstanceIdsToTerminate []int64, elementIdsToStartInstance []string, variableContext map[string]interface{}) (runtime.ProcessInstance, []runtime.ExecutionToken, error) {
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

	batch, err := engine.NewEngineBatch(ctx, processInstance)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create engine batch: %w", err)
	}

	var activeTokensLeft []runtime.ExecutionToken
	if len(elementInstanceIdsToTerminate) > 0 {
		activeTokensLeft, err = engine.terminateExecutionTokens(ctx, &batch, elementInstanceIdsToTerminate, processInstanceKey)
		if err != nil {
			batch.Clear(ctx)
			createSpan.RecordError(err)
			createSpan.SetStatus(codes.Error, err.Error())
			return processInstance, nil, err
		}
	}

	startedTokens, err := engine.startExecutionTokens(ctx, &batch, elementIdsToStartInstance, processInstance)
	if err != nil {
		batch.Clear(ctx)
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
		batch.Clear(ctx)
		createSpan.RecordError(err)
		createSpan.SetStatus(codes.Error, err.Error())
		return processInstance, nil, err
	}

	err = batch.Flush(ctx)
	if err != nil {
		return processInstance, activeTokens, fmt.Errorf("failed to modify process instance %d: %w", processInstance.ProcessInstance().Key, err)
	}

	err = engine.RunProcessInstance(ctx, processInstance, activeTokens)
	if err != nil {
		return processInstance, activeTokens, err
	}

	return processInstance, activeTokens, nil
}

func (engine *Engine) DeleteInstanceVariable(ctx context.Context, processInstanceKey int64, variable string) (runtime.ProcessInstance, error) {
	engine.runningInstances.lockInstance(processInstanceKey)
	instanceUnlocked := false
	defer func() {
		if instanceUnlocked == false {
			engine.runningInstances.unlockInstance(processInstanceKey)
		}
	}()

	processInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, processInstanceKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find process instance %d: %w", processInstance.ProcessInstance().Key, err)
	}

	ctx, createSpan := engine.tracer.Start(ctx, fmt.Sprintf("delete-instance-variable:%s", processInstance.ProcessInstance().Definition.BpmnProcessId), trace.WithAttributes(
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, processInstance.ProcessInstance().Key),
		attribute.String(otelPkg.AttributeProcessId, processInstance.ProcessInstance().Definition.BpmnProcessId),
		attribute.Int64(otelPkg.AttributeProcessDefinitionKey, processInstance.ProcessInstance().Definition.Key),
	))
	defer createSpan.End()

	batch := engine.persistence.NewBatch()

	processInstance.ProcessInstance().VariableHolder.DeleteLocalVariable(variable)
	err = batch.SaveProcessInstance(ctx, processInstance)
	if err != nil {
		createSpan.RecordError(err)
		createSpan.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	err = batch.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to delete variable for process instance %d: %w", processInstance.ProcessInstance().Key, err)
	}
	engine.runningInstances.unlockInstance(processInstanceKey)
	instanceUnlocked = true

	return processInstance, nil
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
