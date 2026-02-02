package bpmn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/pkg/script"
	"github.com/pbinitiative/zenbpm/pkg/script/feel"
	"github.com/pbinitiative/zenbpm/pkg/script/js"

	"github.com/pbinitiative/zenbpm/pkg/dmn"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

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
	tracer         trace.Tracer
	meter          metric.Meter
	metrics        *otelPkg.EngineMetrics
	timerManager   *timerManager
	dmnEngine      *dmn.ZenDmnEngine
	feelRuntime    script.FeelRuntime
	jsRuntime      script.JsRuntime

	// cache that holds process instances being processed by the engine
	runningInstances *RunningInstancesCache
}

type EngineOption = func(*Engine)

// NewEngine creates a new instance of the BPMN Engine;
func NewEngine(options ...EngineOption) Engine {
	logger := hclog.Default()
	meter := otel.GetMeterProvider().Meter("bpmn-engine")
	tracer := otel.GetTracerProvider().Tracer("bpmn-engine")
	metrics, err := otelPkg.NewMetrics(meter)
	if err != nil {
		logger.Error("Failed to initialize metrics for the engine", "err", err)
	}
	persistence := inmemory.NewStorage()
	feelRuntime := feel.NewFeelinRuntime(context.TODO(), 1, 1)
	jsRuntime := js.NewJsRuntime(context.TODO(), 1, 1)
	engine := Engine{
		taskhandlersMu:   &sync.RWMutex{},
		taskHandlers:     []*taskHandler{},
		exporters:        []exporter.EventExporter{},
		persistence:      persistence,
		logger:           logger,
		runningInstances: newRunningInstanceCache(),
		tracer:           tracer,
		meter:            meter,
		metrics:          metrics,
		feelRuntime:      feelRuntime,
		jsRuntime:        jsRuntime,
		dmnEngine:        dmn.NewEngine(dmn.EngineWithStorage(persistence), dmn.EngineWithFeel(feelRuntime)),
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
		engine.dmnEngine = dmn.NewEngine(dmn.EngineWithStorage(persistence))
	}
}

func EngineWithStorageAndFeel(persistence storage.Storage, feelRuntime script.FeelRuntime) EngineOption {
	return func(engine *Engine) {
		engine.persistence = persistence
		engine.feelRuntime = feelRuntime
		engine.dmnEngine = dmn.NewEngine(dmn.EngineWithStorage(persistence), dmn.EngineWithFeel(feelRuntime))
	}
}

func EngineWithJs(jsRuntime script.JsRuntime) EngineOption {
	return func(engine *Engine) {
		engine.jsRuntime = jsRuntime
	}
}

func EngineWithLogger(logger hclog.Logger) EngineOption {
	return func(engine *Engine) {
		engine.logger = logger
	}
}

func (engine *Engine) GetDmnEngine() *dmn.ZenDmnEngine {
	return engine.dmnEngine
}

func (engine *Engine) cancelInstance(ctx context.Context, instance runtime.ProcessInstance, batch *EngineBatch) error {
	err := batch.AddLockedInstance(ctx, instance)
	if err != nil {
		return err
	}

	// Cancel all message subscriptions
	subscriptions, err := engine.persistence.FindProcessInstanceMessageSubscriptions(ctx, instance.ProcessInstance().GetInstanceKey(), runtime.ActivityStateActive)
	if err != nil {
		return fmt.Errorf("failed to find message subscriptions for instance %d: %w", instance.ProcessInstance().Key, err)
	}

	for _, sub := range subscriptions {
		sub.State = runtime.ActivityStateTerminated
		err = batch.SaveMessageSubscription(ctx, sub)
		if err != nil {
			return fmt.Errorf("failed to save changes to message subscription %d: %w", sub.GetKey(), err)
		}
	}

	// Cancel all timer subscriptions
	timers, err := engine.persistence.FindProcessInstanceTimers(ctx, instance.ProcessInstance().Key, runtime.TimerStateCreated)
	if err != nil {
		return fmt.Errorf("failed to find timers for instance %d: %w", instance.ProcessInstance().Key, err)
	}
	for _, timer := range timers {
		timer.TimerState = runtime.TimerStateCancelled
		err = batch.SaveTimer(ctx, timer)
		if err != nil {
			return fmt.Errorf("failed to save changes to timer %d: %w", timer.Key, err)
		}
	}

	// Cancel all jobs
	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed to find jobs for instance %d: %w", instance.ProcessInstance().Key, err)
	}

	for _, job := range jobs {
		job.State = runtime.ActivityStateTerminated
		err = batch.SaveJob(ctx, job)
		if err != nil {
			return fmt.Errorf("failed to save changes to job %d: %w", job.Key, err)
		}
	}

	// Cancel all incidents

	incidents, err := engine.persistence.FindIncidentsByProcessInstanceKey(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed to find incidents for instance %d: %w", instance.ProcessInstance().Key, err)
	}

	for _, incident := range incidents {
		incident.ResolvedAt = ptr.To(time.Now())
		err = batch.SaveIncident(ctx, incident)
		if err != nil {
			return fmt.Errorf("failed to save changes to incident %d: %w", incident.Key, err)
		}
	}

	// Cancel all called processes

	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return fmt.Errorf("failed to find tokens for instance %d: %w", instance.ProcessInstance().Key, err)
	}

	for _, token := range tokens {
		calledProcesses, err := engine.persistence.FindProcessInstanceByParentExecutionTokenKey(ctx, token.Key)
		if err != nil {
			return fmt.Errorf("failed to find called process for token %d: %w", token.Key, err)
		}

		for _, calledProcess := range calledProcesses {
			err = engine.cancelInstance(ctx, calledProcess, batch)
			if err != nil {
				return fmt.Errorf("failed to cancel called process for token %d: %w", token.Key, err)
			}
		}
		token.State = runtime.TokenStateCanceled
		err = batch.SaveToken(ctx, token)
		if err != nil {
			return fmt.Errorf("failed to save changes to token %d: %w", token.Key, err)
		}
	}

	// Cancel process instance
	instance.ProcessInstance().State = runtime.ActivityStateTerminated
	err = batch.SaveProcessInstance(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	return nil
}

func (engine *Engine) terminateExecutionTokens(
	ctx context.Context,
	batch *EngineBatch,
	elementInstanceKeysToTerminate []int64,
	processInstanceKey int64,
) ([]runtime.ExecutionToken, error) {
	activeTokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, processInstanceKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find tokens for instance %d: %w", processInstanceKey, err)
	}

	activeTokensLeft := make([]runtime.ExecutionToken, 0, len(activeTokens))
	for _, activeToken := range activeTokens {
		for _, elementInstanceKey := range elementInstanceKeysToTerminate {
			if activeToken.ElementInstanceKey == elementInstanceKey {
				// Cancel all message subscriptions
				subscriptions, err := engine.persistence.FindTokenMessageSubscriptions(ctx, activeToken.Key, runtime.ActivityStateActive)
				if err != nil {
					return nil, fmt.Errorf("failed to find message subscriptions for execution token %d: %w", activeToken.Key, err)
				}
				for _, sub := range subscriptions {
					sub.State = runtime.ActivityStateTerminated
					err = batch.SaveMessageSubscription(ctx, sub)
					if err != nil {
						return nil, fmt.Errorf("failed to save changes to message subscription %d: %w", sub.GetKey(), err)
					}
				}

				// Cancel all timer subscriptions
				timers, err := engine.persistence.FindTokenActiveTimerSubscriptions(ctx, activeToken.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to find timers for execution token %d: %w", activeToken.Key, err)
				}
				for _, timer := range timers {
					timer.TimerState = runtime.TimerStateCancelled
					err = batch.SaveTimer(ctx, timer)
					if err != nil {
						return nil, fmt.Errorf("failed to save changes to timer %d: %w", timer.Key, err)
					}
				}

				// Cancel all jobs
				jobs, err := engine.persistence.FindTokenJobsInState(ctx, activeToken.Key, []runtime.ActivityState{runtime.ActivityStateActive, runtime.ActivityStateCompleting, runtime.ActivityStateFailed})
				if err != nil {
					return nil, fmt.Errorf("failed to find jobs for execution token %d: %w", activeToken.Key, err)
				}
				for _, job := range jobs {
					job.State = runtime.ActivityStateTerminated
					err = batch.SaveJob(ctx, job)
					if err != nil {
						return nil, fmt.Errorf("failed to save changes to job %d: %w", job.Key, err)
					}
				}

				// Cancel all incidents
				incidents, err := engine.persistence.FindIncidentsByExecutionTokenKey(ctx, activeToken.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to find incidents for execution token %d: %w", activeToken.Key, err)
				}
				for _, incident := range incidents {
					incident.ResolvedAt = ptr.To(time.Now())
					err = batch.SaveIncident(ctx, incident)
					if err != nil {
						return nil, fmt.Errorf("failed to save changes to incident %d: %w", incident.Key, err)
					}
				}

				// Cancel called processes
				// TODO: This can cause a deadlock
				// TODO: Fix THIS
				calledProcesses, err := engine.persistence.FindProcessInstanceByParentExecutionTokenKey(ctx, activeToken.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to find called process for token %d: %w", activeToken.Key, err)
				}
				for _, calledProcess := range calledProcesses {
					err = engine.cancelInstance(ctx, calledProcess, batch)
					if err != nil {
						return nil, fmt.Errorf("failed to cancel called process for token %d: %w", activeToken.Key, err)
					}
				}

				activeToken.State = runtime.TokenStateCanceled
				err = batch.SaveToken(ctx, activeToken)
				if err != nil {
					return nil, fmt.Errorf("failed to terminate execution activeToken %d: %w", activeToken.Key, err)
				}
			} else {
				activeTokensLeft = append(activeTokensLeft, activeToken)
			}
		}
	}

	return activeTokensLeft, nil
}

func (engine *Engine) startExecutionTokens(ctx context.Context, batch *EngineBatch, startingElementIds []string, processInstance runtime.ProcessInstance) ([]runtime.ExecutionToken, error) {
	executionTokens := make([]runtime.ExecutionToken, 0, 1)
	for _, elementId := range startingElementIds {
		var flowNode = processInstance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(elementId)
		executionToken := runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          flowNode.GetId(),
			ProcessInstanceKey: processInstance.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
			CreatedAt:          time.Now(),
		}
		executionTokens = append(executionTokens, executionToken)
		err := batch.SaveToken(ctx, executionToken)
		if err != nil {
			return nil, fmt.Errorf("failed to start execution token starting at %s for process instance %d: %w", flowNode.GetId(), processInstance.ProcessInstance().Key, err)
		}
	}

	return executionTokens, nil
}

func (engine *Engine) createInstance(
	ctx context.Context,
	batch storage.Batch,
	process *runtime.ProcessDefinition,
	variableHolder runtime.VariableHolder,
	instance runtime.ProcessInstance,
) (output runtime.ProcessInstance, outputTokens []runtime.ExecutionToken, err error) {
	instance.ProcessInstance().Definition = process
	instance.ProcessInstance().Key = engine.generateKey()
	instance.ProcessInstance().VariableHolder = variableHolder
	instance.ProcessInstance().CreatedAt = time.Now()
	instance.ProcessInstance().State = runtime.ActivityStateReady

	ctx, createSpan := engine.tracer.Start(ctx, fmt.Sprintf("create-instance:%s", instance.ProcessInstance().Definition.BpmnProcessId), trace.WithAttributes(
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, instance.ProcessInstance().Key),
		attribute.String(otelPkg.AttributeProcessId, instance.ProcessInstance().Definition.BpmnProcessId),
		attribute.Int64(otelPkg.AttributeProcessDefinitionKey, instance.ProcessInstance().Definition.Key),
	))
	defer func() {
		if err != nil {
			createSpan.RecordError(err)
			createSpan.SetStatus(codes.Error, err.Error())
		}
		createSpan.End()
	}()

	batch.SaveProcessInstance(ctx, instance)

	executionTokens := make([]runtime.ExecutionToken, 0, 1)
	for _, startEvent := range process.Definitions.Process.StartEvents {
		var be bpmn20.FlowNode = &startEvent
		token := runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          be.GetId(),
			ProcessInstanceKey: instance.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
		}
		executionTokens = append(executionTokens, token)
		batch.SaveToken(ctx, token)
	}
	return instance, executionTokens, nil
}

func (engine *Engine) createInstanceWithStartingElements(
	ctx context.Context,
	batch storage.Batch,
	processDefinition *runtime.ProcessDefinition,
	startingFlowNodes []bpmn20.FlowNode,
	variableHolder runtime.VariableHolder,
	instance runtime.ProcessInstance,
) (output runtime.ProcessInstance, outputTokens []runtime.ExecutionToken, err error) {
	instance.ProcessInstance().Definition = processDefinition
	instance.ProcessInstance().Key = engine.generateKey()
	instance.ProcessInstance().VariableHolder = variableHolder
	instance.ProcessInstance().CreatedAt = time.Now()
	instance.ProcessInstance().State = runtime.ActivityStateReady

	startNodeIds := make([]string, 0, len(startingFlowNodes))
	for _, startNode := range startingFlowNodes {
		startNodeIds = append(startNodeIds, startNode.GetId())
	}
	ctx, createSpan := engine.tracer.Start(ctx, fmt.Sprintf("start-instance-on-elements: %s %s", instance.ProcessInstance().Definition.BpmnProcessId, startNodeIds), trace.WithAttributes(
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, instance.ProcessInstance().Key),
		attribute.String(otelPkg.AttributeProcessId, instance.ProcessInstance().Definition.BpmnProcessId),
		attribute.Int64(otelPkg.AttributeProcessDefinitionKey, instance.ProcessInstance().Definition.Key),
	))
	defer func() {
		if err != nil {
			createSpan.RecordError(err)
			createSpan.SetStatus(codes.Error, err.Error())
		}
		createSpan.End()
	}()

	batch.SaveProcessInstance(ctx, instance)

	executionTokens := make([]runtime.ExecutionToken, 0, len(startingFlowNodes))
	for _, startNode := range startingFlowNodes {
		token := runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          startNode.GetId(),
			ProcessInstanceKey: instance.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
		}
		executionTokens = append(executionTokens, token)
		batch.SaveToken(ctx, token)
	}

	return instance, executionTokens, nil
}

func (engine *Engine) getExecutionTokenActivity(
	ctx context.Context,
	instance runtime.ProcessInstance,
	token runtime.ExecutionToken,
) (*elementActivity, error) {
	var currentFlowNode bpmn20.FlowNode
	switch instance.(type) {
	case *runtime.DefaultProcessInstance, *runtime.CallActivityInstance, *runtime.MultiInstanceInstance:
		currentFlowNode = instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(token.ElementId)
	case *runtime.SubProcessInstance:
		parentActivityDefinition := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(instance.(*runtime.SubProcessInstance).ParentProcessTargetElementId)
		currentFlowNode = parentActivityDefinition.(*bpmn20.TSubProcess).GetFlowNodeById(token.ElementId)
	default:
		return nil, errors.New("invalid instance type")
	}
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
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	activity *elementActivity,
	currentToken runtime.ExecutionToken,
) (tokens []runtime.ExecutionToken, err error) {
	ctx, flowNodeSpan := engine.tracer.Start(ctx, fmt.Sprintf("flow-node:%s", activity.element.GetId()), trace.WithAttributes(
		attribute.Int64(otelPkg.AttributeElementKey, activity.GetKey()),
		attribute.String(otelPkg.AttributeElementName, activity.Element().GetName()),
		attribute.String(otelPkg.AttributeElementType, string(activity.Element().GetType())),
	))
	defer func() {
		if err != nil {
			flowNodeSpan.RecordError(err)
			flowNodeSpan.SetStatus(codes.Error, err.Error())
		}
		flowNodeSpan.End()
	}()

	//TODO: move this into each element handler
	batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().Key,
		ElementId:          activity.Element().GetId(),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  currentToken.Key,
		InputVariables:     nil,
		OutputVariables:    nil,
	})

	switch element := activity.Element().(type) {
	case *bpmn20.TStartEvent:
		//TODO: input output Variables
		tokens, err := engine.handleElementTransition(ctx, batch, instance, activity.element, currentToken)
		if err != nil {
			flowNodeSpan.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("failed to process StartEvent flow transition %d: %w", activity.GetKey(), err)
		}
		return tokens, nil
	case *bpmn20.TEndEvent:
		err := engine.handleEndEvent(ctx, instance)
		if err != nil {
			return nil, fmt.Errorf("failed to process EndEvent %d: %w", activity.GetKey(), err)
		}
		currentToken.State = runtime.TokenStateCompleted
		return []runtime.ExecutionToken{currentToken}, nil
	case *bpmn20.TServiceTask, *bpmn20.TUserTask, *bpmn20.TCallActivity, *bpmn20.TBusinessRuleTask, *bpmn20.TSendTask, *bpmn20.TSubProcess:
		if element := element.(bpmn20.Activity); element.GetMultiInstance() != nil {
			tokens, err := engine.handleMultiInstanceActivity(ctx, batch, instance, element, activity, currentToken)
			if err != nil {
				return nil, fmt.Errorf("failed to process MultiInstance%d: %w", activity.GetKey(), err)
			}
			return tokens, nil
		}
		return engine.handleActivity(ctx, batch, instance, activity, currentToken, activity.Element())
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
}

func (engine *Engine) handleActivity(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, activity runtime.Activity, currentToken runtime.ExecutionToken, element bpmn20.FlowNode) ([]runtime.ExecutionToken, error) {
	var activityResult runtime.ActivityState
	var err error

	switch element := activity.Element().(type) {
	case *bpmn20.TServiceTask:
		activityResult, err = engine.createInternalTask(ctx, batch, instance, element, currentToken)
	case *bpmn20.TSendTask:
		activityResult, err = engine.createInternalTask(ctx, batch, instance, element, currentToken)
	case *bpmn20.TUserTask:
		activityResult, err = engine.createUserTask(ctx, batch, instance, element, currentToken)
	case *bpmn20.TCallActivity:
		activityResult, err = engine.createCallActivity(ctx, batch, instance, element, currentToken)
		// we created process instance and its running in separate goroutine
	case *bpmn20.TSubProcess:
		activityResult, err = engine.createSubProcess(ctx, batch, instance, element, currentToken)
		// we created process instance and its running in separate goroutine
	case *bpmn20.TBusinessRuleTask:
		activityResult, err = engine.createBusinessRuleTask(ctx, batch, instance, element, currentToken)
	default:
		return nil, fmt.Errorf("failed to process %s %d: %w", element.GetType(), activity.GetKey(), errors.New("unsupported activity"))
	}

	// Now check whether the activity ended right away and the process can move on or it needs to wait for external event
	switch activityResult {
	case runtime.ActivityStateActive:
		currentToken.State = runtime.TokenStateWaiting
		// TODO: we are in waiting state so we instantiate boundary event subscriptions
		err := createBoundaryEventSubscriptions(ctx, engine, batch, currentToken, instance, activity.Element())
		if err != nil {
			return nil, fmt.Errorf("failed to process boundary events for %s %d: %w", element.GetType(), activity.GetKey(), err)
		}
		return []runtime.ExecutionToken{currentToken}, nil
	case runtime.ActivityStateCompleted:
		tokens, err := engine.handleElementTransition(ctx, batch, instance, activity.Element(), currentToken)
		if err != nil {
			return nil, fmt.Errorf("failed to process %s flow transition %d: %w", element.GetType(), activity.GetKey(), err)
		}
		return tokens, nil
	case runtime.ActivityStateFailed:
		currentToken.State = runtime.TokenStateFailed
		return []runtime.ExecutionToken{currentToken}, fmt.Errorf("failed to process %s %d: %w", element.GetType(), activity.GetKey(), err)
	default:
		return []runtime.ExecutionToken{}, fmt.Errorf("unsupported activity state: %s", activityResult)
	}
}

func createBoundaryEventSubscriptions(ctx context.Context, engine *Engine, batch *EngineBatch, currentToken runtime.ExecutionToken, instance runtime.ProcessInstance, element bpmn20.FlowNode) error {
	bes := bpmn20.FindBoundaryEventsForActivity(&instance.ProcessInstance().Definition.Definitions, element)
	for _, be := range bes {
		switch be.EventDefinition.(type) {
		case bpmn20.TMessageEventDefinition:
			_, err := engine.createMessageCatchEvent(ctx, batch, instance, be.EventDefinition.(bpmn20.TMessageEventDefinition), element, currentToken)
			if err != nil {
				return err
			}
		case bpmn20.TTimerEventDefinition:
			_, err := engine.createTimerCatchEvent(ctx, batch, instance, be.EventDefinition.(bpmn20.TTimerEventDefinition), element, currentToken)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unsupported BoundaryEvent %+v", be))
		}
	}

	return nil
}

func (engine *Engine) createBusinessRuleTask(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element *bpmn20.TBusinessRuleTask,
	currentToken runtime.ExecutionToken,
) (runtime.ActivityState, error) {
	var activityResult runtime.ActivityState
	var err error

	switch element.Implementation.(type) {
	case *bpmn20.TBusinessRuleTaskLocal:
		activityResult, err = engine.handleLocalBusinessRuleTask(ctx, batch, instance, element, element.Implementation.(*bpmn20.TBusinessRuleTaskLocal), currentToken)
	case *bpmn20.TBusinessRuleTaskExternal:
		activityResult, err = engine.createExternalBusinessRuleTask(ctx, batch, instance, element, element.Implementation.(*bpmn20.TBusinessRuleTaskExternal), currentToken)
	default:
		panic(fmt.Sprintf("unsupported BusinessRuleTask Implementation %s", element.Implementation))
	}

	if err != nil {
		return activityResult, err
	}

	return activityResult, nil
}

func (engine *Engine) handleLocalBusinessRuleTask(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element *bpmn20.TBusinessRuleTask,
	implementation *bpmn20.TBusinessRuleTaskLocal,
	currentToken runtime.ExecutionToken,
) (runtime.ActivityState, error) {
	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate local variables for business rule %s: %w", element.TTask.Id, err)
	}

	batch.SaveFlowElementInstance(ctx,
		runtime.FlowElementInstance{
			Key:                currentToken.ElementInstanceKey,
			ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
			ElementId:          element.GetId(),
			CreatedAt:          time.Now(),
			ExecutionTokenKey:  currentToken.Key,
			InputVariables:     variableHolder.LocalVariables(),
			OutputVariables:    nil,
		},
	)

	ctx = appcontext.WithProcessInstanceKey(ctx, instance.Key)
	ctx = appcontext.WithElementInstanceKey(ctx, token.ElementInstanceKey)

	result, err := engine.dmnEngine.FindAndEvaluateDRD(
		ctx,
		implementation.CalledDecision.BindingType,
		implementation.CalledDecision.DecisionId,
		implementation.CalledDecision.VersionTag,
		variableHolder.LocalVariables(),
	)
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate business rule %s: %w", element.TTask.Id, err)
	}

	// TODO persist relation between result.DecisionInstanceKey and flow_element_instance

	if len(element.GetOutputMapping()) > 0 {
		outputVariables, err := variableHolder.PropagateOutputVariablesToParent(element.GetOutputMapping(), map[string]any{implementation.CalledDecision.ResultVariable: result.DecisionOutput}, engine.evaluateExpression)
		if err != nil {
			instance.ProcessInstance().State = runtime.ActivityStateFailed
			return runtime.ActivityStateFailed, fmt.Errorf("failed to propagate variables back to parent for business rule %s : %w", element.TTask.Id, err)
		}
		batch.UpdateOutputFlowElementInstance(ctx,
			runtime.FlowElementInstance{
				Key:             currentToken.ElementInstanceKey,
				OutputVariables: outputVariables,
			},
		)
		return runtime.ActivityStateCompleted, nil
	}

	variableHolder.PropagateVariable(implementation.CalledDecision.ResultVariable, result.DecisionOutput)
	batch.UpdateOutputFlowElementInstance(ctx,
		runtime.FlowElementInstance{
			Key:             currentToken.ElementInstanceKey,
			OutputVariables: map[string]any{implementation.CalledDecision.ResultVariable: result.DecisionOutput},
		},
	)

	return runtime.ActivityStateCompleted, nil
}

// TODO: Implement Headers as worker parameters and Retries
func (engine *Engine) createExternalBusinessRuleTask(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element *bpmn20.TBusinessRuleTask,
	implementation *bpmn20.TBusinessRuleTaskExternal,
	currentToken runtime.ExecutionToken,
) (runtime.ActivityState, error) {
	activityState, err := engine.createInternalTask(ctx, batch, instance, element, currentToken)
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return runtime.ActivityStateFailed, fmt.Errorf("failed to create internal task for business rule %s : %w", element.TTask.Id, err)
	}
	return activityState, nil
}

func (engine *Engine) handleParallelGateway(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TParallelGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	// TODO: this implementation is wrong does not count with multiple gateways activated at the same time
	incoming := element.GetIncomingAssociation()
	instanceTokens, err := engine.persistence.GetAllTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
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
		//TODO: should probably be WAITING not COMPLETED
		if token.State == runtime.TokenStateCompleted {
			completedGatewayTokens = append(completedGatewayTokens, token)
		}
	}
	//TODO: should probably be WAITING not COMPLETED
	currentToken.State = runtime.TokenStateCompleted
	if len(completedGatewayTokens) != len(incoming)-1 {
		// we are still waiting for additional tokens to arrive
		return []runtime.ExecutionToken{currentToken}, nil
	}

	outgoing := element.GetOutgoingAssociation()
	resTokens := make([]runtime.ExecutionToken, len(outgoing)+1)
	//TODO: should probably be WAITING not COMPLETED
	currentToken.State = runtime.TokenStateCompleted
	resTokens[0] = currentToken
	for i, flow := range outgoing {
		resTokens[i+1] = runtime.ExecutionToken{
			Key:                engine.generateKey(),
			ElementInstanceKey: engine.generateKey(),
			ElementId:          flow.GetTargetRef().GetId(),
			ProcessInstanceKey: instance.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
		}
	}
	return resTokens, nil
}

func (engine *Engine) handleEventBasedGateway(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element *bpmn20.TEventBasedGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
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
		ProcessInstanceKey: instance.ProcessInstance().Key,
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

// handleExclusiveGateway handles Exclusive gateway behaviour
// A diverging Exclusive Gateway (Decision) is used to create alternative paths within a Process flow. This is basically
// the “diversion point in the road” for a Process. For a given instance of the Process, only one of the paths can be taken.
func (engine *Engine) handleExclusiveGateway(ctx context.Context, instance runtime.ProcessInstance, element *bpmn20.TExclusiveGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	// TODO: handle incoming mapping
	outgoing := element.GetOutgoingAssociation()
	activatedFlows, err := engine.exclusivelyFilterByConditionExpression(outgoing, element.GetDefaultFlow(), instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
		return nil, fmt.Errorf("failed to filter outgoing associations from ExclusiveGateway: %w", err)
	}
	if len(activatedFlows) != 0 {
		currentToken.ElementId = activatedFlows[0].GetTargetRef().GetId()
		currentToken.ElementInstanceKey = engine.generateKey()
	}
	return []runtime.ExecutionToken{currentToken}, nil
}

func (engine *Engine) handleInclusiveGateway(ctx context.Context, instance runtime.ProcessInstance, element *bpmn20.TInclusiveGateway, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	// TODO: handle incoming mapping
	outgoing := element.GetOutgoingAssociation()
	activatedFlows, err := engine.inclusivelyFilterByConditionExpression(outgoing, element.GetDefaultFlow(), instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		instance.ProcessInstance().State = runtime.ActivityStateFailed
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
			ProcessInstanceKey: instance.ProcessInstance().Key,
			State:              runtime.TokenStateRunning,
		}
	}
	return resTokens, nil
}

// TODO: check each use of this method and add change to how variables are added to process instance
func (engine *Engine) handleElementTransition(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	currentToken runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
	switch inst := instance.(type) {
	case *runtime.DefaultProcessInstance, *runtime.SubProcessInstance, *runtime.CallActivityInstance:
		return engine.handleDefaultElementTransition(ctx, batch, inst, element, currentToken)
	case *runtime.MultiInstanceInstance:
		return engine.handleMultiInstanceElementTransition(ctx, batch, inst, element, currentToken)
	default:
		return nil, fmt.Errorf("unsupported instance type: %T", instance)
	}
}

func (engine *Engine) handleDefaultElementTransition(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.FlowNode,
	currentToken runtime.ExecutionToken,
) ([]runtime.ExecutionToken, error) {
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
				ProcessInstanceKey: instance.ProcessInstance().Key,
				State:              runtime.TokenStateRunning,
			})
		}

		//this saves only transitions between nodes
		batch.SaveFlowElementInstance(ctx,
			runtime.FlowElementInstance{
				Key:                engine.generateKey(),
				ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
				ElementId:          flow.GetId(),
				CreatedAt:          time.Now(),
				ExecutionTokenKey:  resTokens[i].Key,
				InputVariables:     nil,
				OutputVariables:    nil,
			},
		)
	}
	return resTokens, nil
}

func (engine *Engine) createIntermediateCatchEvent(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, ice *bpmn20.TIntermediateCatchEvent, currentToken runtime.ExecutionToken) ([]runtime.ExecutionToken, error) {
	switch ice.EventDefinition.(type) {
	case bpmn20.TMessageEventDefinition:
		token, err := engine.createMessageCatchEvent(ctx, batch, instance, ice.EventDefinition.(bpmn20.TMessageEventDefinition), ice, currentToken)
		return []runtime.ExecutionToken{token}, err
	case bpmn20.TTimerEventDefinition:
		token, err := engine.createTimerCatchEvent(ctx, batch, instance, ice.EventDefinition.(bpmn20.TTimerEventDefinition), ice, currentToken)
		return []runtime.ExecutionToken{token}, err
	case bpmn20.TLinkEventDefinition:
		tokens, err := engine.handleElementTransition(ctx, batch, instance, ice, currentToken)
		return tokens, err
	default:
		panic(fmt.Sprintf("unsupported IntermediateCatchEvent %+v", ice))
	}
}

func (engine *Engine) handleEndEvent(ctx context.Context, instance runtime.ProcessInstance) error {
	activeSubscriptions := false

	activeSubs, err := engine.persistence.FindProcessInstanceMessageSubscriptions(ctx, instance.ProcessInstance().Key, runtime.ActivityStateActive)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load active subscriptions"), err)
	}
	if len(activeSubs) > 0 {
		activeSubscriptions = true
	}

	readySubs, err := engine.persistence.FindProcessInstanceMessageSubscriptions(ctx, instance.ProcessInstance().Key, runtime.ActivityStateReady)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load ready subscriptions"), err)
	}
	if len(readySubs) > 0 {
		activeSubscriptions = true
	}

	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load pending process instance jobs for key: %d", instance.ProcessInstance().Key), err)
	}
	if len(jobs) > 0 {
		activeSubscriptions = true
	}

	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(ctx, instance.ProcessInstance().Key)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load active tokens for key: %d", instance.ProcessInstance().Key), err)
	}
	if len(tokens) > 1 {
		activeSubscriptions = true
	}

	if !activeSubscriptions {
		instance.ProcessInstance().State = runtime.ActivityStateCompleted
	}
	return nil
}
