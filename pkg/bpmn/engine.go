package bpmn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type Engine struct {
	name string
	// _processes            []*ProcessInfo
	_processInstances []*runtime.ProcessInstance
	// _messageSubscriptions []*MessageSubscription
	_jobs []*runtime.Job
	// _timers               []*Timer
	taskHandlers []*taskHandler
	exporters    []exporter.EventExporter
	snowflake    *snowflake.Node
	persistence  storage.PersistentStorageNew
}

// CreateInstanceById creates a new instance for a process with given process ID and uses latest version (if available)
// Might return BpmnEngineError, when no process with given ID was found
func (engine *Engine) CreateInstanceById(processId string, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	processDefinition, err := engine.persistence.FindProcessDefinitionById(context.TODO(), processId)
	if err != nil {
		return nil, newEngineErrorf("no process with id=%s was found (prior loaded into the engine): %w", processId, err)
	}

	if err != nil {
		return engine.CreateInstance(&processDefinition, variableContext)
	}

	return nil, newEngineErrorf("no process with id=%s was found (prior loaded into the engine)", processId)
}

// CreateInstance creates a new instance for a process with given processKey
// Might return BpmnEngineError, if process key was not found
func (engine *Engine) CreateInstance(process *runtime.ProcessDefinition, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	processInstance := runtime.ProcessInstance{
		Definition:     process,
		InstanceKey:    engine.generateKey(),
		VariableHolder: runtime.New(nil, variableContext),
		CreatedAt:      time.Now(),
		State:          runtime.Ready,
		CaughtEvents:   []runtime.CatchEvent{},
		Activities:     []runtime.Activity{},
	}
	err := engine.persistence.SaveProcessInstance(context.Background(), processInstance)
	if err != nil {
		return nil, err
	}
	engine.exportProcessInstanceEvent(*process, processInstance)
	return &processInstance, nil
}

// CreateAndRunInstanceById creates a new instance by process ID (and uses latest process version), and executes it immediately.
// The provided variableContext can be nil or refers to a variable map,
// which is provided to every service task handler function.
// Might return BpmnEngineError or ExpressionEvaluationError.
func (engine *Engine) CreateAndRunInstanceById(processId string, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	instance, err := engine.CreateInstanceById(processId, variableContext)
	if err != nil {
		return nil, err
	}
	return instance, engine.run(instance)
}

// CreateAndRunInstance creates a new instance and executes it immediately.
// The provided variableContext can be nil or refers to a variable map,
// which is provided to every service task handler function.
// Might return BpmnEngineError or ExpressionEvaluationError.
func (engine *Engine) CreateAndRunInstance(processKey int64, variableContext map[string]interface{}) (*runtime.ProcessInstance, error) {
	process, err := engine.persistence.FindProcessDefinitionByKey(context.TODO(), processKey)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("failed to load process definition with key: %d", processKey), err)
	}

	instance, err := engine.CreateInstance(&process, variableContext)
	if err != nil {
		return nil, err
	}
	return instance, engine.run(instance)
}

// RunOrContinueInstance runs or continues a process instance by a given processInstanceKey.
// returns the process instances, when found;
// does nothing, if process is already in ProcessInstanceCompleted State;
// returns nil, nil when no process instance was found;
// might return BpmnEngineError or ExpressionEvaluationError.
func (engine *Engine) RunOrContinueInstance(processInstanceKey int64) (*runtime.ProcessInstance, error) {
	pi, err := engine.persistence.FindProcessInstanceByKey(context.TODO(), processInstanceKey)
	if err != nil {
		return nil, newEngineErrorf("failed to find process instance with key: %d", processInstanceKey)
	}
	if pi.GetState() == runtime.Completed {
		return nil, nil
	}
	err = engine.run(&pi)
	if err != nil {
		return nil, errors.Join(newEngineErrorf("failed to RunOrContinueInstance"), err)
	}
	return &pi, nil
}

// FindProcessInstance searches for a given processInstanceKey
// and returns the corresponding processInstanceInfo, or otherwise nil
func (engine *Engine) FindProcessInstance(processInstanceKey int64) (runtime.ProcessInstance, error) {
	return engine.persistence.FindProcessInstanceByKey(context.TODO(), processInstanceKey)
}

// Name returns the name of the engine, only useful in case you control multiple ones
func (engine *Engine) Name() string {
	return engine.name
}

// FindProcessesById returns all registered processes with given ID
// result array is ordered by version number, from 1 (first) and largest version (last)
func (engine *Engine) FindProcessesById(id string) ([]runtime.ProcessDefinition, error) {
	return engine.persistence.FindProcessDefinitionsById(context.TODO(), id)
}

func (engine *Engine) checkExclusiveGatewayDone(activity eventBasedGatewayActivity) error {
	if !activity.OutboundCompleted() {
		return nil
	}

	// cancel other activities started by this one
	msgSubs, err := engine.persistence.FindActivityMessageSubscription(context.TODO(), activity.Key(), runtime.Active)
	if err != nil {
		return fmt.Errorf("failed to find process instance message subscriptions by activity key: %d: %w", activity.Key(), err)
	}
	for _, ms := range msgSubs {
		ms.MessageState = runtime.Withdrawn
	}
	timers, err := engine.persistence.FindActivityTimers(context.TODO(), activity.Key(), runtime.TimerCreated)
	if err != nil {
		return fmt.Errorf("failed to find process instance timers by activity key: %d: %w", activity.Key(), err)
	}
	for _, t := range timers {
		t.TimerState = runtime.TimerCancelled
	}
	return nil
}

func (b *Engine) Stop() {
}

func (engine *Engine) run(instance *runtime.ProcessInstance) (err error) {
	ctx := context.TODO()
	executionKey := engine.snowflake.Generate().Int64()
	ctx = context.WithValue(ctx, appcontext.ExecutionKey, executionKey)
	process := instance.Definition
	var commandQueue []command

	switch instance.State {
	case runtime.Ready:
		// use start events to start the instance
		for _, startEvent := range process.Definitions.Process.StartEvents {
			var be bpmn20.FlowNode = startEvent
			commandQueue = append(commandQueue, activityCommand{
				element: be,
			})
		}
		instance.State = runtime.Active
		// TODO: check? export process EVENT
	case runtime.Active:
		jobs, err := engine.persistence.FindPendingProcessInstanceJobs(context.TODO(), instance.InstanceKey)
		if err != nil {
			return errors.Join(newEngineErrorf("failed to find pending instance jobs for key: %d", instance.InstanceKey), err)
		}
		for _, j := range jobs {
			commandQueue = append(commandQueue, continueActivityCommand{
				activity: j,
			})
		}
		activeSubscriptions, err := engine.findActiveSubscriptions(instance)
		if err != nil {
			return errors.Join(newEngineErrorf("failed to find active subscriptions for key: %d", instance.InstanceKey), err)
		}
		for _, subscr := range activeSubscriptions {
			commandQueue = append(commandQueue, continueActivityCommand{
				activity:       subscr,
				originActivity: subscr.OriginActivity,
			})
		}
		createdTimers, err := engine.findCreatedTimers(instance)
		if err != nil {
			return errors.Join(newEngineErrorf("failed to find active subscriptions for key: %d", instance.InstanceKey), err)
		}
		for _, timer := range createdTimers {
			commandQueue = append(commandQueue, continueActivityCommand{
				activity:       timer,
				originActivity: timer.OriginActivity,
			})
		}
	}

	// *** MAIN LOOP ***
	for len(commandQueue) > 0 {
		cmd := commandQueue[0]
		commandQueue = commandQueue[1:]

		switch cmd.Type() {
		case flowTransitionType:
			sourceActivity := cmd.(flowTransitionCommand).sourceActivity
			flowId := cmd.(flowTransitionCommand).sequenceFlowId
			nextFlows := bpmn20.FindSequenceFlows(&process.Definitions.Process.SequenceFlows, []string{flowId})
			if bpmn20.ExclusiveGateway == sourceActivity.Element().GetType() {
				nextFlows, err = exclusivelyFilterByConditionExpression(nextFlows, instance.VariableHolder.Variables())
				if err != nil {
					instance.State = runtime.Failed
					return err
				}
			}
			for _, flow := range nextFlows {
				engine.exportSequenceFlowEvent(*process, *instance, flow)
				baseElements := bpmn20.FindFlowNodesById(&process.Definitions, flow.TargetRef)
				targetBaseElement := baseElements[0]
				aCmd := activityCommand{
					sourceId:       flowId,
					originActivity: sourceActivity,
					element:        targetBaseElement,
				}
				commandQueue = append(commandQueue, aCmd)
			}
		case activityType:
			element := cmd.(activityCommand).element
			originActivity := cmd.(activityCommand).originActivity
			nextCommands, err := engine.handleElement(ctx, process, instance, element, originActivity)
			if err != nil {
				return errors.Join(newEngineErrorf("failed to handle activity type element (%+v)", element), err)
			}
			commandQueue = append(commandQueue, nextCommands...)
		case continueActivityType:
			element := cmd.(continueActivityCommand).activity.Element()
			originActivity := cmd.(continueActivityCommand).originActivity
			nextCommands, err := engine.handleElement(ctx, process, instance, element, originActivity)
			if err != nil {
				return errors.Join(newEngineErrorf("failed to handle continue activity type element (%+v)", element), err)
			}
			commandQueue = append(commandQueue, nextCommands...)
		case errorType:
			err = cmd.(errorCommand).err
			instance.State = runtime.Failed
			break
		case checkExclusiveGatewayDoneType:
			activity := cmd.(checkExclusiveGatewayDoneCommand).gatewayActivity
			err := engine.checkExclusiveGatewayDone(activity)
			if err != nil {
				return errors.Join(newEngineErrorf("failed to check exclusive gateway"), err)
			}
		default:
			panic("[invariant check] command type check not fully implemented")
		}
	}

	if instance.State == runtime.Completed || instance.State == runtime.Failed {
		// TODO need to send failed State
		engine.exportEndProcessEvent(*process, *instance)
	}
	engine.persistence.SaveProcessInstance(ctx, *instance)

	// TODO: create a way to control batches of sql statements
	// engine.persistence.GetPersistence().FlushTransaction(ctx)

	return err
}

func (engine *Engine) handleElement(
	ctx context.Context,
	process *runtime.ProcessDefinition,
	instance *runtime.ProcessInstance,
	element bpmn20.FlowNode,
	originActivity runtime.Activity,
) ([]command, error) {
	engine.exportElementEvent(*process, *instance, element, exporter.ElementActivated) // FIXME: don't create event on continuation ?!?!
	createFlowTransitions := true
	var activity runtime.Activity
	var nextCommands []command
	var err error
	switch element.GetType() {
	case bpmn20.StartEvent:
		createFlowTransitions = true
		activity = &elementActivity{
			key:     engine.generateKey(),
			state:   runtime.Completed,
			element: element,
		}
	case bpmn20.EndEvent:
		engine.handleEndEvent(process, instance)
		engine.exportElementEvent(*process, *instance, element, exporter.ElementCompleted) // special case here, to end the instance
		createFlowTransitions = false
		activity = &elementActivity{
			key:     engine.generateKey(),
			state:   runtime.Completed,
			element: element,
		}
	case bpmn20.ServiceTask:
		taskElement := element.(bpmn20.TaskElement)
		activity, err = engine.handleServiceTask(ctx, process, instance, taskElement)
		if err != nil {
			return nil, fmt.Errorf("failed to handle service task: %w", err)
		}
		createFlowTransitions = activity.State() == runtime.Completed
	case bpmn20.UserTask:
		taskElement := element.(bpmn20.TaskElement)
		activity, err = engine.handleUserTask(ctx, process, instance, taskElement)
		if err != nil {
			return nil, fmt.Errorf("failed to handle user task: %w", err)
		}
		createFlowTransitions = activity.State() == runtime.Completed
	case bpmn20.IntermediateCatchEvent:
		ice := element.(bpmn20.TIntermediateCatchEvent)
		createFlowTransitions, activity, err = engine.handleIntermediateCatchEvent(ctx, process, instance, ice, originActivity)
		if err != nil {
			nextCommands = append(nextCommands, errorCommand{
				err:         err,
				elementId:   element.GetId(),
				elementName: element.GetName(),
			})
		} else {
			nextCommands = append(nextCommands, createCheckExclusiveGatewayDoneCommand(originActivity)...)
		}

		if ms, ok := activity.(*runtime.MessageSubscription); ok {
			engine.persistence.SaveMessageSubscription(ctx, *ms)
			// TODO: this is needed because endevent checks subscriptions and if transaction is not flushed yet it will lock process in active state
			// engine.persistence.GetPersistence().FlushTransaction(ctx)
		} else {
			// Handle the case when activity is not a MessageSubscription
			// For example, you can return an error or log a message
			log.Panicf("Unexpected Activity type: %T", activity)
		}
	case bpmn20.IntermediateThrowEvent:
		activity = &elementActivity{
			key:     engine.generateKey(),
			state:   runtime.Active, // FIXME: should be Completed?
			element: element,
		}
		cmds := engine.handleIntermediateThrowEvent(process, instance, element.(bpmn20.TIntermediateThrowEvent), activity)
		nextCommands = append(nextCommands, cmds...)
		createFlowTransitions = false
	case bpmn20.ParallelGateway:
		createFlowTransitions, activity = engine.handleParallelGateway(process, instance, element.(bpmn20.TParallelGateway), originActivity)
	case bpmn20.ExclusiveGateway:
		activity = elementActivity{
			key:     engine.generateKey(),
			state:   runtime.Active,
			element: element,
		}
		createFlowTransitions = true
	case bpmn20.EventBasedGateway:
		activity = &eventBasedGatewayActivity{
			key:     engine.generateKey(),
			state:   runtime.Completed,
			element: element,
		}
		instance.AppendActivity(activity)
		createFlowTransitions = true
	case bpmn20.InclusiveGateway:
		activity = elementActivity{
			key:     engine.generateKey(),
			state:   runtime.Active,
			element: element,
		}
		createFlowTransitions = true
	default:
		panic(fmt.Sprintf("[invariant check] unsupported element: id=%s, type=%s", element.GetId(), element.GetType()))
	}
	if createFlowTransitions && err == nil {
		engine.exportElementEvent(*process, *instance, element, exporter.ElementCompleted)
		nextCommands = append(nextCommands, createNextCommands(process, instance, element, activity)...)
	}
	return nextCommands, nil
}

func createCheckExclusiveGatewayDoneCommand(originActivity runtime.Activity) (cmds []command) {
	if originActivity.Element().GetType() == bpmn20.EventBasedGateway {
		evtBasedGatewayActivity := originActivity.(*eventBasedGatewayActivity)
		cmds = append(cmds, checkExclusiveGatewayDoneCommand{
			gatewayActivity: *evtBasedGatewayActivity,
		})
	}
	return cmds
}

func createNextCommands(process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, element bpmn20.FlowNode, activity runtime.Activity) (cmds []command) {
	nextFlows := bpmn20.FindSequenceFlows(&process.Definitions.Process.SequenceFlows, element.GetOutgoingAssociation())
	var err error
	switch element.GetType() {
	case bpmn20.ExclusiveGateway:
		nextFlows, err = exclusivelyFilterByConditionExpression(nextFlows, instance.VariableHolder.Variables())
		if err != nil {
			instance.State = runtime.Failed
			cmds = append(cmds, errorCommand{
				err:         err,
				elementId:   element.GetId(),
				elementName: element.GetName(),
			})
			return cmds
		}
	case bpmn20.InclusiveGateway:
		nextFlows, err = inclusivelyFilterByConditionExpression(nextFlows, instance.VariableHolder.Variables())
		if err != nil {
			instance.State = runtime.Failed
			return []command{
				errorCommand{
					elementId:   element.GetId(),
					elementName: element.GetName(),
					err:         err,
				},
			}
		}
	}
	for _, flow := range nextFlows {
		cmds = append(cmds, flowTransitionCommand{
			sourceId:       element.GetId(),
			sourceActivity: activity,
			sequenceFlowId: flow.Id,
		})
	}
	return cmds
}

func (engine *Engine) handleIntermediateCatchEvent(ctx context.Context, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, ice bpmn20.TIntermediateCatchEvent, originActivity runtime.Activity) (continueFlow bool, activity runtime.Activity, err error) {
	continueFlow = false
	if ice.MessageEventDefinition.Id != "" {
		continueFlow, activity, err = engine.handleIntermediateMessageCatchEvent(ctx, process, instance, ice, originActivity)
	} else if ice.TimerEventDefinition.Id != "" {
		continueFlow, activity, err = engine.handleIntermediateTimerCatchEvent(ctx, instance, ice, originActivity)
	} else if ice.LinkEventDefinition.Id != "" {
		var be bpmn20.FlowNode = ice
		activity = &elementActivity{
			key:     engine.generateKey(),
			state:   runtime.Active, // FIXME: should be Completed?
			element: be,
		}
		throwLinkName := originActivity.Element().(bpmn20.TIntermediateThrowEvent).LinkEventDefinition.Name
		catchLinkName := ice.LinkEventDefinition.Name
		elementVarHolder := runtime.New(&instance.VariableHolder, nil)
		if err := propagateProcessInstanceVariables(&elementVarHolder, ice.Output); err != nil {
			msg := fmt.Sprintf("Can't evaluate expression in element id=%s name=%s", ice.Id, ice.Name)
			err = &ExpressionEvaluationError{Msg: msg, Err: err}
		} else {
			continueFlow = throwLinkName == catchLinkName // just stating the obvious
		}
	}
	return continueFlow, activity, err
}

func (engine *Engine) handleEndEvent(process *runtime.ProcessDefinition, instance *runtime.ProcessInstance) error {
	activeSubscriptions := false
	// FIXME: check if this is correct to seems wrong i need to check if there are any tokens in this process not only messages subscriptions but elements also
	activeSubs, err := engine.persistence.FindProcessInstanceMessageSubscription(context.TODO(), instance.InstanceKey, runtime.Active)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load active subscriptions"), err)
	}
	if len(activeSubs) > 0 {
		activeSubscriptions = true
	}
	readySubs, err := engine.persistence.FindProcessInstanceMessageSubscription(context.TODO(), instance.InstanceKey, runtime.Ready)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load ready subscriptions"), err)
	}
	if len(readySubs) > 0 {
		activeSubscriptions = true
	}

	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(context.TODO(), instance.InstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to load pending process instance jobs for key: %d", instance.InstanceKey), err)
	}
	if len(jobs) > 0 {
		activeSubscriptions = true
	}

	if !activeSubscriptions {
		instance.State = runtime.Completed
	}
	return nil
}

func (engine *Engine) handleParallelGateway(process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, element bpmn20.TParallelGateway, originActivity runtime.Activity) (continueFlow bool, resultActivity runtime.Activity) {
	resultActivity = instance.FindActiveActivityByElementId(element.Id)
	if resultActivity == nil {
		var be bpmn20.FlowNode = element
		resultActivity = &gatewayActivity{
			key:      engine.generateKey(),
			state:    runtime.Active,
			element:  be,
			parallel: true,
		}
		instance.AppendActivity(resultActivity)
	}
	sourceFlow := bpmn20.FindFirstSequenceFlow(&process.Definitions.Process.SequenceFlows, originActivity.Element().GetId(), element.GetId())
	resultActivity.(*gatewayActivity).SetInboundFlowCompleted(sourceFlow.Id)
	continueFlow = resultActivity.(*gatewayActivity).parallel && resultActivity.(*gatewayActivity).AreInboundFlowsCompleted()
	if continueFlow {
		resultActivity.(*gatewayActivity).SetState(runtime.Completed)
	}
	return continueFlow, resultActivity
}

// findActiveSubscriptions returns active subscriptions;
// if ids are provided, the result gets filtered;
// if no ids are provided, all active subscriptions are returned
func (engine *Engine) findActiveSubscriptions(instance *runtime.ProcessInstance) ([]runtime.MessageSubscription, error) {
	subs, err := engine.persistence.FindProcessInstanceMessageSubscription(context.TODO(), instance.InstanceKey, runtime.Active)
	if err != nil {
		return nil, fmt.Errorf("failed to load process instance message subscriptions for key %d: %w", instance.InstanceKey, err)
	}
	result := make([]runtime.MessageSubscription, 0, len(subs))
	for _, ms := range subs {
		bes := bpmn20.FindFlowNodesById(&instance.Definition.Definitions, ms.ElementId)
		if len(bes) == 0 {
			continue
		}
		ms.BaseElement = bes[0]
		// FIXME: rewrite this hack
		// instance.findActivity(ms.originActivity.Key())
		result = append(result, ms)
	}
	return result, nil
}

// findCreatedTimers the list of all scheduled/creates timers in the engine, not yet completed
func (engine *Engine) findCreatedTimers(instance *runtime.ProcessInstance) ([]runtime.Timer, error) {
	timers, err := engine.persistence.FindTimersByState(context.TODO(), instance.InstanceKey, runtime.TimerCreated)
	if err != nil {
		return nil, fmt.Errorf("failed to load process instance timers for key %d: %w", instance.InstanceKey, err)
	}
	result := make([]runtime.Timer, 0, len(timers))
	for _, t := range timers {
		bes := bpmn20.FindFlowNodesById(&instance.Definition.Definitions, t.ElementId)
		if len(bes) == 0 {
			continue
		}
		t.BaseElement = bes[0]
		result = append(result, t)
	}
	return result, nil
}
