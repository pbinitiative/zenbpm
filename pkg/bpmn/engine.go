package bpmn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// Engine holds the state of the bpmn engine.
// It interacts with the outside world using persistence storage interface and outside world interacts with it using public methods (message correlations, job updates, ...).
type Engine struct {
	taskhandlersMu *sync.RWMutex // we can probably remove this once we fix tests reuse same handler matchers
	taskHandlers   []*taskHandler
	exporters      []exporter.EventExporter
	persistence    storage.Storage
	logger         hclog.Logger

	// cache that holds process instances being processed by the engine
	runningInstances *RunningInstancesCache
}

type EngineOption = func(*Engine)

// NewEngine creates a new instance of the BPMN Engine;
func NewEngine(options ...EngineOption) Engine {
	engine := Engine{
		taskhandlersMu: &sync.RWMutex{},
		taskHandlers:   []*taskHandler{},
		exporters:      []exporter.EventExporter{},
		persistence:    nil,
		logger:         hclog.Default(),
		runningInstances: &RunningInstancesCache{
			processInstances: map[int64]*runtime.ProcessInstance{},
			mu:               sync.RWMutex{},
		},
	}

	for _, option := range options {
		option(&engine)
	}

	return engine
}

func EngineWithExporter(exporter exporter.EventExporter) EngineOption {
	return func(engine *Engine) { engine.AddEventExporter(exporter) }
}

func EngineWithStorage(persistence storage.Storage) EngineOption {
	return func(engine *Engine) {
		engine.persistence = persistence
	}
}

func EngineWithLogger(logger hclog.Logger) EngineOption {
	return func(engine *Engine) {
		engine.logger = logger
	}
}

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
func (engine *Engine) CreateInstanceByKey(ctx context.Context, processKey int64, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	processDefinition, err := engine.persistence.FindProcessDefinitionByKey(ctx, processKey)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("no process with key=%d was found (prior loaded into the engine)", processKey), err)
	}

	instance, err := engine.CreateInstance(ctx, &processDefinition, variableContext)
	if err != nil {
		return instance, errors.Join(newEngineErrorf("failed to create process instance with definition key: %d", processKey), err)
	}

	return instance, nil
}

// CreateInstance creates a new instance for a process with given processKey
// Might return BpmnEngineError, if process key was not found
func (engine *Engine) CreateInstance(ctx context.Context, process *runtime.ProcessDefinition, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	processInstance := runtime.ProcessInstance{
		Definition:     process,
		Key:            engine.generateKey(),
		VariableHolder: runtime.NewVariableHolder(nil, variableContext),
		CreatedAt:      time.Now(),
		State:          runtime.ActivityStateReady,
	}
	batch := engine.persistence.NewBatch()
	err := batch.SaveProcessInstance(ctx, processInstance)
	if err != nil {
		return nil, err
	}
	executionTokens := make([]runtime.ExecutionToken, 0, 1)
	for _, startEvent := range process.Definitions.Process.StartEvents {
		var be bpmn20.FlowNode = &startEvent
		executionTokens = append(executionTokens, runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          be.GetId(),
			ProcessInstanceKey: processInstance.Key,
			State:              runtime.TokenStateRunning,
		})
	}
	// add to runningInstances to prevent loading tokens from storage for running instance
	engine.runningInstances.addInstance(&processInstance)
	err = batch.Flush(ctx)
	if err != nil {
		engine.runningInstances.removeInstance(&processInstance)
		return nil, fmt.Errorf("failed to start process instance: %w", err)
	}
	err = engine.runProcessInstance(ctx, &processInstance, executionTokens)
	if err != nil {
		return &processInstance, fmt.Errorf("failed to run process instance %d: %w", processInstance.Key, err)
	}
	return &processInstance, nil
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

func (b *Engine) Stop() {
}

// Start will start the process engine instance.
// Engine will start to pull process instances with execution tokens that need to be processed
func (b *Engine) Start() {
}

// runProcessInstance will
func (engine *Engine) runProcessInstance(ctx context.Context, instance *runtime.ProcessInstance, executionTokens []runtime.ExecutionToken) (err error) {
	process := instance.Definition
	runningExecutionTokens := executionTokens
	var runErr error

	instance.State = runtime.ActivityStateActive
	err = engine.persistence.SaveProcessInstance(ctx, *instance)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to save process instance %d status update", instance.Key), err)
	}

	engine.runningInstances.addInstance(instance)
	defer engine.runningInstances.removeInstance(instance)

	// *** MAIN LOOP ***
	for len(runningExecutionTokens) > 0 {
		batch := engine.persistence.NewBatch()
		currentToken := runningExecutionTokens[0]
		runningExecutionTokens = runningExecutionTokens[1:]
		if currentToken.State != runtime.TokenStateRunning {
			continue
		}

		activity, err := engine.getExecutionTokenActivity(ctx, batch, instance, currentToken)
		if err != nil {
			engine.logger.Warn("failed to get execution activity", "token", currentToken.Key, "processInstance", instance.Key, "err", err)
			runErr = errors.Join(runErr, err)
			currentToken.State = runtime.TokenStateFailed
			batch.SaveToken(ctx, currentToken)
			// TODO: create incident here?
			continue
		}

		updatedTokens, err := engine.processFlowNode(ctx, batch, instance, activity, currentToken)
		if err != nil {
			engine.logger.Warn("failed to process token", "token", currentToken.Key, "processInstance", instance.Key, "err", err)
			runErr = errors.Join(runErr, err)
			currentToken.State = runtime.TokenStateFailed
			// TODO: create incident here?
			err := batch.SaveToken(ctx, currentToken)
			if err != nil {
				engine.logger.Error("failed to save ExecutionToken [%v]: %w", currentToken, err)
			}
			continue
		}

		for _, tok := range updatedTokens {
			err := batch.SaveToken(ctx, tok)
			if err != nil {
				engine.logger.Error("failed to save ExecutionToken [%v]: %w", tok, err)
				continue
			}
			if tok.State == runtime.TokenStateRunning {
				runningExecutionTokens = append(runningExecutionTokens, tok)
			}
		}

		err = batch.Flush(ctx)
		if err != nil {
			return errors.Join(newEngineErrorf("failed to close batch for token %d", currentToken.Key), err)
		}
	}

	if instance.State == runtime.ActivityStateCompleted || instance.State == runtime.ActivityStateFailed {
		// TODO need to send failed State
		engine.exportEndProcessEvent(*process, *instance)
	}
	// if we encounter any error we switch the instance to failed state
	if runErr != nil {
		instance.State = runtime.ActivityStateFailed
	}
	err = engine.persistence.SaveProcessInstance(ctx, *instance)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to save process instance %d", instance.Key), err)
	}
	if runErr != nil {
		return errors.Join(newEngineErrorf("failed to run process instance %d", instance.Key), runErr)
	}
	return nil
}

func (engine *Engine) getExecutionTokenActivity(
	ctx context.Context,
	batch storage.Batch,
	instance *runtime.ProcessInstance,
	token runtime.ExecutionToken,
) (*elementActivity, error) {
	currentFlowNode := instance.Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
	if currentFlowNode == nil {
		return nil, fmt.Errorf("failed to find flow node %s for execution token in process definition", token.ElementId)
	}
	activity := &elementActivity{
		key:     engine.generateKey(),
		state:   runtime.ActivityStateReady,
		element: currentFlowNode,
	}
	return activity, nil
}

// processFlowNode handles the activation of the activity.
// If the activity is waiting for external input it returns token updated in waiting state.
// If the activity is completed returns an array of tokens that need to be processed by the engine in next stage.
func (engine *Engine) processFlowNode(
	ctx context.Context,
	batch storage.Batch,
	instance *runtime.ProcessInstance,
	activity *elementActivity,
	currentToken runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
	switch element := activity.Element().(type) {
	case *bpmn20.TStartEvent:
		tokens, err := engine.handleSimpleTransition(ctx, instance, activity.element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process StartEvent flow transition %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	case *bpmn20.TEndEvent:
		err := engine.handleEndEvent(instance)
		if err != nil {
			return nil, fmt.Errorf("failed to process EndEvent %d: %w", activity.GetKey(), err)
		}
	case *bpmn20.TServiceTask:
		activityResult, err := engine.createInternalTask(ctx, batch, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process ServiceTask %d: %w", activity.GetKey(), err)
		}
		switch activityResult {
		case runtime.ActivityStateActive:
			currentToken.State = runtime.TokenStateWaiting
			return []runtime.ExecutionToken{currentToken}, nil
		case runtime.ActivityStateCompleted:
			tokens, err := engine.handleSimpleTransition(ctx, instance, activity.element, currentToken)
			if err != nil {
				return nil, fmt.Errorf("failed to process ServiceTask flow transition %d: %w", activity.GetKey(), err)
			}
			return tokens, nil
		}
	case *bpmn20.TUserTask:
		activityResult, err := engine.createUserTask(ctx, batch, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process UserTask %d: %w", activity.GetKey(), err)
		}
		switch activityResult {
		case runtime.ActivityStateActive:
			currentToken.State = runtime.TokenStateWaiting
			return []runtime.ExecutionToken{currentToken}, nil
		case runtime.ActivityStateCompleted:
			tokens, err := engine.handleSimpleTransition(ctx, instance, activity.element, currentToken)
			if err != nil {
				return nil, fmt.Errorf("failed to process UserTask flow transition %d: %w", activity.GetKey(), err)
			}
			return tokens, nil
		}
	case *bpmn20.TIntermediateCatchEvent:
		// intermediate catch events following event based gateway are handled in event based gateway
		tokens, err := engine.createIntermediateCatchEvent(ctx, batch, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process IntermediateCatchEvent %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	case *bpmn20.TIntermediateThrowEvent:
		tokens, err := engine.handleIntermediateThrowEvent(ctx, batch, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process IntermediateThrowEvent %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	case *bpmn20.TExclusiveGateway:
		tokens, err := engine.handleExclusiveGateway(ctx, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process ExclusiveGateway %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	case *bpmn20.TInclusiveGateway:
		tokens, err := engine.handleInclusiveGateway(ctx, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process InclusiveGateway %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	case *bpmn20.TEventBasedGateway:
		// handle subscriptions in handle func
		tokens, err := engine.handleEventBasedGateway(ctx, batch, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process EventBasedGateway %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	case *bpmn20.TParallelGateway:
		tokens, err := engine.handleParallelGateway(ctx, batch, instance, element, currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process EventBasedGateway %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	default:
		panic(fmt.Sprintf("[invariant check] unsupported element: id=%s, type=%s", activity.Element().GetId(), activity.Element().GetType()))
	}
	return nil, nil
}

func (engine *Engine) handleParallelGateway(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TParallelGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	// TODO: this implementation is wrong does not count with multiple gateways activated at the same time
	incoming := element.GetIncomingAssociation()
	instanceTokens, err := engine.persistence.GetTokensForProcessInstance(ctx, instance.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to get current tokens for process instance: %w", err)
	}
	gatewayTokens := []runtime.ExecutionToken{}
	for _, token := range instanceTokens {
		if token.ElementId == currentToken.ElementId {
			gatewayTokens = append(gatewayTokens, token)
		}
	}
	completedGatewayTokens := []runtime.ExecutionToken{}
	for _, token := range gatewayTokens {
		if token.State == runtime.TokenStateCompleted {
			completedGatewayTokens = append(completedGatewayTokens, token)
		}
	}
	currentToken.State = runtime.TokenStateCompleted
	if len(completedGatewayTokens) != len(incoming)-1 {
		// we are still waiting for additional tokens to arrive
		return []runtime.ExecutionToken{currentToken}, nil
	}

	outgoing := element.GetOutgoingAssociation()
	resTokens := make([]runtime.ExecutionToken, len(outgoing)+1)
	currentToken.State = runtime.TokenStateCompleted
	resTokens[0] = currentToken
	for i, flow := range outgoing {
		resTokens[i+1] = runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          flow.GetTargetRef().GetId(),
			ProcessInstanceKey: instance.Key,
			State:              runtime.TokenStateRunning,
		}
	}
	return resTokens, nil
}

func (engine *Engine) handleEventBasedGateway(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element *bpmn20.TEventBasedGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	outgoing := element.GetOutgoingAssociation()
	resTokens := make([]runtime.ExecutionToken, 0, 2)
	// complete token that activated gateway
	currentToken.State = runtime.TokenStateCompleted
	resTokens = append(resTokens, currentToken)
	// generate new gateway token
	gatewayToken := runtime.ExecutionToken{
		Key:                engine.generateKey(),
		ElementInstanceKey: engine.generateKey(),
		ElementId:          element.GetId(),
		ProcessInstanceKey: instance.Key,
		State:              runtime.TokenStateWaiting,
	}
	for _, flow := range outgoing {
		switch targetElem := flow.GetTargetRef().(type) {
		case *bpmn20.TIntermediateCatchEvent:
			tokens, err := engine.createIntermediateCatchEvent(ctx, batch, instance, targetElem, gatewayToken)
			resTokens = append(resTokens, tokens...)
			if err != nil {
				return resTokens, fmt.Errorf("failed to handle IntermediateCatchEvent: %w", err)
			}
		default:
			panic(fmt.Sprintf("[invariant check] unsupported element: id=%s, type=%s", flow.GetTargetRef().GetId(), flow.GetTargetRef().GetType()))
		}
	}
	return resTokens, nil
}

func (engine *Engine) handleExclusiveGateway(ctx context.Context, instance *runtime.ProcessInstance, element *bpmn20.TExclusiveGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	// TODO: handle incoming mapping
	outgoing := element.GetOutgoingAssociation()
	activatedFlows, err := exclusivelyFilterByConditionExpression(outgoing, element.GetDefaultFlow(), instance.VariableHolder.Variables())
	if err != nil {
		instance.State = runtime.ActivityStateFailed
		return nil, fmt.Errorf("failed to filter outgoing associations from ExclusiveGateway: %w", err)
	}
	resTokens := make([]runtime.ExecutionToken, len(activatedFlows)+1)
	currentToken.State = runtime.TokenStateCompleted
	resTokens[0] = currentToken
	for i, flow := range activatedFlows {
		resTokens[i+1] = runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          flow.GetTargetRef().GetId(),
			ProcessInstanceKey: instance.Key,
			State:              runtime.TokenStateRunning,
		}
	}
	return resTokens, nil
}

func (engine *Engine) handleInclusiveGateway(ctx context.Context, instance *runtime.ProcessInstance, element *bpmn20.TInclusiveGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	// TODO: handle incoming mapping
	outgoing := element.GetOutgoingAssociation()
	activatedFlows, err := inclusivelyFilterByConditionExpression(outgoing, element.GetDefaultFlow(), instance.VariableHolder.Variables())
	if err != nil {
		instance.State = runtime.ActivityStateFailed
		return nil, fmt.Errorf("failed to filter outgoing associations from InclusiveGateway: %w", err)
	}
	resTokens := make([]runtime.ExecutionToken, len(activatedFlows)+1)
	currentToken.State = runtime.TokenStateCompleted
	resTokens[0] = currentToken
	for i, flow := range activatedFlows {
		resTokens[i+1] = runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          flow.GetTargetRef().GetId(),
			ProcessInstanceKey: instance.Key,
			State:              runtime.TokenStateRunning,
		}
	}
	return resTokens, nil
}

func (engine *Engine) handleSimpleTransition(ctx context.Context, instance *runtime.ProcessInstance, element bpmn20.FlowNode, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	var resTokens = []runtime.ExecutionToken{currentToken}
	// TODO: handle no outgoing associations
	for i, flow := range element.GetOutgoingAssociation() {
		// TODO: handle condition expressions
		if i == 0 {
			resTokens[0].ElementId = flow.GetTargetRef().GetId()
			resTokens[0].ElementInstanceKey = engine.generateKey()
			resTokens[0].State = runtime.TokenStateRunning
		} else {
			resTokens = append(resTokens, runtime.ExecutionToken{
				Key:                engine.generateKey(),
				ElementInstanceKey: engine.generateKey(),
				ElementId:          flow.GetTargetRef().GetId(),
				ProcessInstanceKey: instance.Key,
				State:              runtime.TokenStateRunning,
			})
		}
	}
	return resTokens, nil
}

func (engine *Engine) createIntermediateCatchEvent(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, ice *bpmn20.TIntermediateCatchEvent, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	switch ice.EventDefinition.(type) {
	case bpmn20.TMessageEventDefinition:
		token, err := engine.createIntermediateMessageCatchEvent(ctx, batch, instance, ice, currentToken)
		return []runtime.ExecutionToken{token}, err
	case bpmn20.TTimerEventDefinition:
		token, err := engine.createIntermediateTimerCatchEvent(ctx, batch, instance, ice, currentToken)
		return []runtime.ExecutionToken{token}, err
	case bpmn20.TLinkEventDefinition:
		tokens, err := engine.handleSimpleTransition(ctx, instance, ice, currentToken)
		return tokens, err
	default:
		panic(fmt.Sprintf("unsupported IntermediateCatchEvent %+v", ice))
	}
}

func (engine *Engine) handleEndEvent(instance *runtime.ProcessInstance) error {
	activeSubscriptions := false
	// FIXME: check if this is correct to seems wrong i need to check if there are any tokens in this process not only messages subscriptions but elements also
	activeSubs, err := engine.persistence.FindProcessInstanceMessageSubscriptions(context.TODO(), instance.Key, runtime.ActivityStateActive)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load active subscriptions"), err)
	}
	if len(activeSubs) > 0 {
		activeSubscriptions = true
	}
	readySubs, err := engine.persistence.FindProcessInstanceMessageSubscriptions(context.TODO(), instance.Key, runtime.ActivityStateReady)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load ready subscriptions"), err)
	}
	if len(readySubs) > 0 {
		activeSubscriptions = true
	}

	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(context.TODO(), instance.Key)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load pending process instance jobs for key: %d", instance.Key), err)
	}
	if len(jobs) > 0 {
		activeSubscriptions = true
	}

	if !activeSubscriptions {
		instance.State = runtime.ActivityStateCompleted
	}
	return nil
}
