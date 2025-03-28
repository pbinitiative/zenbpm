package bpmn

import (
	"context"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"log"
	"time"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	rqlite "github.com/pbinitiative/zenbpm/internal/rqlite"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
)

func (state *Engine) GetPersistence() *rqlite.PersistenceRqlite {
	return state.persistence.GetPersistence()
}

func (state *Engine) GetPersistenceService() BpmnEnginePersistenceService {
	return state.persistence
}

// CreateInstanceById creates a new instance for a process with given process ID and uses latest version (if available)
// Might return BpmnEngineError, when no process with given ID was found
func (state *Engine) CreateInstanceById(processId string, variableContext map[string]interface{}) (*processInstanceInfo, error) {
	process := state.persistence.FindProcessById(processId)

	if process != nil {
		return state.CreateInstance(process, variableContext)
	}

	return nil, newEngineErrorf("no process with id=%s was found (prior loaded into the engine)", processId)
}

// CreateInstance creates a new instance for a process with given processKey
// Might return BpmnEngineError, if process key was not found
func (state *Engine) CreateInstance(process *runtime.ProcessDefinition, variableContext map[string]interface{}) (*processInstanceInfo, error) {
	processInstanceInfo := processInstanceInfo{
		ProcessInfo:    process,
		InstanceKey:    state.generateKey(),
		VariableHolder: runtime.New(nil, variableContext),
		CreatedAt:      time.Now(),
		State:          runtime.Ready,
		CaughtEvents:   []catchEvent{},
		activities:     []runtime.Activity{},
	}
	err := state.persistence.PersistProcessInstance(context.Background(), &processInstanceInfo)
	if err != nil {
		return nil, err
	}
	state.exportProcessInstanceEvent(*process, processInstanceInfo)
	return &processInstanceInfo, nil
}

// CreateAndRunInstanceById creates a new instance by process ID (and uses latest process version), and executes it immediately.
// The provided variableContext can be nil or refers to a variable map,
// which is provided to every service task handler function.
// Might return BpmnEngineError or ExpressionEvaluationError.
func (state *Engine) CreateAndRunInstanceById(processId string, variableContext map[string]interface{}) (*processInstanceInfo, error) {
	instance, err := state.CreateInstanceById(processId, variableContext)
	if err != nil {
		return nil, err
	}
	return instance, state.run(instance)
}

// CreateAndRunInstance creates a new instance and executes it immediately.
// The provided variableContext can be nil or refers to a variable map,
// which is provided to every service task handler function.
// Might return BpmnEngineError or ExpressionEvaluationError.
func (state *Engine) CreateAndRunInstance(processKey int64, variableContext map[string]interface{}) (*processInstanceInfo, error) {
	process := state.persistence.FindProcessByKey(processKey)

	instance, err := state.CreateInstance(process, variableContext)
	if err != nil {
		return nil, err
	}
	return instance, state.run(instance)
}

// RunOrContinueInstance runs or continues a process instance by a given processInstanceKey.
// returns the process instances, when found;
// does nothing, if process is already in ProcessInstanceCompleted State;
// returns nil, nil when no process instance was found;
// might return BpmnEngineError or ExpressionEvaluationError.
func (state *Engine) RunOrContinueInstance(processInstanceKey int64) (*processInstanceInfo, error) {
	pi := state.persistence.FindProcessInstanceByKey(processInstanceKey)
	if pi == nil {
		return nil, nil
	}
	return pi, state.run(pi)
}

func (state *Engine) run(instance *processInstanceInfo) (err error) {
	ctx := context.TODO()
	executionKey := state.snowflake.Generate().Int64()
	ctx = context.WithValue(ctx, appcontext.ExecutionKey, executionKey)
	process := instance.ProcessInfo
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
		jobs := state.findActiveJobsForContinuation(instance)
		for _, j := range jobs {
			commandQueue = append(commandQueue, continueActivityCommand{
				activity: j,
			})
		}
		activeSubscriptions := state.findActiveSubscriptions(instance)
		for _, subscr := range activeSubscriptions {
			commandQueue = append(commandQueue, continueActivityCommand{
				activity:       subscr,
				originActivity: subscr.OriginActivity,
			})
		}
		createdTimers := state.findCreatedTimers(instance)
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
				state.exportSequenceFlowEvent(*process, *instance, flow)
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
			nextCommands := state.handleElement(ctx, process, instance, element, originActivity)
			commandQueue = append(commandQueue, nextCommands...)
		case continueActivityType:
			element := cmd.(continueActivityCommand).activity.Element()
			originActivity := cmd.(continueActivityCommand).originActivity
			nextCommands := state.handleElement(ctx, process, instance, element, originActivity)
			commandQueue = append(commandQueue, nextCommands...)
		case errorType:
			err = cmd.(errorCommand).err
			instance.State = runtime.Failed
			break
		case checkExclusiveGatewayDoneType:
			activity := cmd.(checkExclusiveGatewayDoneCommand).gatewayActivity
			state.checkExclusiveGatewayDone(activity)
		default:
			panic("[invariant check] command type check not fully implemented")
		}
	}

	if instance.State == runtime.Completed || instance.State == runtime.Failed {
		// TODO need to send failed State
		state.exportEndProcessEvent(*process, *instance)
	}
	// TODO: persistently update state
	state.persistence.PersistProcessInstance(ctx, instance)
	state.persistence.GetPersistence().FlushTransaction(ctx)

	return err
}

func (state *Engine) handleElement(ctx context.Context, process *runtime.ProcessDefinition, instance *processInstanceInfo, element bpmn20.FlowNode, originActivity runtime.Activity) []command {
	state.exportElementEvent(*process, *instance, element, exporter.ElementActivated) // FIXME: don't create event on continuation ?!?!
	createFlowTransitions := true
	var activity runtime.Activity
	var nextCommands []command
	var err error
	switch element.GetType() {
	case bpmn20.StartEvent:
		createFlowTransitions = true
		activity = &elementActivity{
			key:     state.generateKey(),
			state:   runtime.Completed,
			element: element,
		}
	case bpmn20.EndEvent:
		state.handleEndEvent(process, instance)
		state.exportElementEvent(*process, *instance, element, exporter.ElementCompleted) // special case here, to end the instance
		createFlowTransitions = false
		activity = &elementActivity{
			key:     state.generateKey(),
			state:   runtime.Completed,
			element: element,
		}
	case bpmn20.ServiceTask:
		taskElement := element.(bpmn20.TaskElement)
		_, activity = state.handleServiceTask(ctx, process, instance, taskElement)
		createFlowTransitions = activity.State() == runtime.Completed
	case bpmn20.UserTask:
		taskElement := element.(bpmn20.TaskElement)
		activity = state.handleUserTask(ctx, process, instance, taskElement)
		createFlowTransitions = activity.State() == runtime.Completed
	case bpmn20.IntermediateCatchEvent:
		ice := element.(bpmn20.TIntermediateCatchEvent)
		createFlowTransitions, activity, err = state.handleIntermediateCatchEvent(ctx, process, instance, ice, originActivity)
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
			state.persistence.PersistNewMessageSubscription(ctx, ms)
			// TODO: this is needed because endevent checks subscriptions and if transaction is not flushed yet it will lock process in active state
			state.persistence.GetPersistence().FlushTransaction(ctx)
		} else {
			// Handle the case when activity is not a MessageSubscription
			// For example, you can return an error or log a message
			log.Panicf("Unexpected Activity type: %T", activity)
		}
	case bpmn20.IntermediateThrowEvent:
		activity = &elementActivity{
			key:     state.generateKey(),
			state:   runtime.Active, // FIXME: should be Completed?
			element: element,
		}
		cmds := state.handleIntermediateThrowEvent(process, instance, element.(bpmn20.TIntermediateThrowEvent), activity)
		nextCommands = append(nextCommands, cmds...)
		createFlowTransitions = false
	case bpmn20.ParallelGateway:
		createFlowTransitions, activity = state.handleParallelGateway(process, instance, element.(bpmn20.TParallelGateway), originActivity)
	case bpmn20.ExclusiveGateway:
		activity = elementActivity{
			key:     state.generateKey(),
			state:   runtime.Active,
			element: element,
		}
		createFlowTransitions = true
	case bpmn20.EventBasedGateway:
		activity = &eventBasedGatewayActivity{
			key:     state.generateKey(),
			state:   runtime.Completed,
			element: element,
		}
		instance.appendActivity(activity)
		createFlowTransitions = true
	case bpmn20.InclusiveGateway:
		activity = elementActivity{
			key:     state.generateKey(),
			state:   runtime.Active,
			element: element,
		}
		createFlowTransitions = true
	default:
		panic(fmt.Sprintf("[invariant check] unsupported element: id=%s, type=%s", element.GetId(), element.GetType()))
	}
	if createFlowTransitions && err == nil {
		state.exportElementEvent(*process, *instance, element, exporter.ElementCompleted)
		nextCommands = append(nextCommands, createNextCommands(process, instance, element, activity)...)
	}
	return nextCommands
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

func createNextCommands(process *runtime.ProcessDefinition, instance *processInstanceInfo, element bpmn20.FlowNode, activity runtime.Activity) (cmds []command) {
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

func (state *Engine) handleIntermediateCatchEvent(ctx context.Context, process *runtime.ProcessDefinition, instance *processInstanceInfo, ice bpmn20.TIntermediateCatchEvent, originActivity runtime.Activity) (continueFlow bool, activity runtime.Activity, err error) {
	continueFlow = false
	if ice.MessageEventDefinition.Id != "" {
		continueFlow, activity, err = state.handleIntermediateMessageCatchEvent(ctx, process, instance, ice, originActivity)
	} else if ice.TimerEventDefinition.Id != "" {
		continueFlow, activity, err = state.handleIntermediateTimerCatchEvent(ctx, instance, ice, originActivity)
	} else if ice.LinkEventDefinition.Id != "" {
		var be bpmn20.FlowNode = ice
		activity = &elementActivity{
			key:     state.generateKey(),
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

func (state *Engine) handleEndEvent(process *runtime.ProcessDefinition, instance *processInstanceInfo) {
	activeSubscriptions := false
	// FIXME: check if this is correct to seems wrong i need to check if there are any tokens in this process not only messages subscriptions but elements also
	if len(state.persistence.FindMessageSubscription(nil, instance, nil, runtime.Active)) > 0 {
		activeSubscriptions = true
	}
	if len(state.persistence.FindMessageSubscription(nil, instance, nil, runtime.Ready)) > 0 {
		activeSubscriptions = true
	}

	jobs := state.persistence.FindJobs(nil, nil, instance, nil, runtime.Active, runtime.Completing)
	if len(jobs) > 0 {
		activeSubscriptions = true
	}

	if !activeSubscriptions {
		instance.State = runtime.Completed
	}
}

func (state *Engine) handleParallelGateway(process *runtime.ProcessDefinition, instance *processInstanceInfo, element bpmn20.TParallelGateway, originActivity runtime.Activity) (continueFlow bool, resultActivity runtime.Activity) {
	resultActivity = instance.findActiveActivityByElementId(element.Id)
	if resultActivity == nil {
		var be bpmn20.FlowNode = element
		resultActivity = &gatewayActivity{
			key:      state.generateKey(),
			state:    runtime.Active,
			element:  be,
			parallel: true,
		}
		instance.appendActivity(resultActivity)
	}
	sourceFlow := bpmn20.FindFirstSequenceFlow(&process.Definitions.Process.SequenceFlows, originActivity.Element().GetId(), element.GetId())
	resultActivity.(*gatewayActivity).SetInboundFlowCompleted(sourceFlow.Id)
	continueFlow = resultActivity.(*gatewayActivity).parallel && resultActivity.(*gatewayActivity).AreInboundFlowsCompleted()
	if continueFlow {
		resultActivity.(*gatewayActivity).SetState(runtime.Completed)
	}
	return continueFlow, resultActivity
}

func (state *Engine) findActiveJobsForContinuation(instance *processInstanceInfo) (ret []*runtime.Job) {
	return state.persistence.FindJobs(nil, nil, instance, nil, runtime.Active, runtime.Completing)
}

// findActiveSubscriptions returns active subscriptions;
// if ids are provided, the result gets filtered;
// if no ids are provided, all active subscriptions are returned
func (state *Engine) findActiveSubscriptions(instance *processInstanceInfo) (result []*runtime.MessageSubscription) {
	for _, ms := range state.persistence.FindMessageSubscription(nil, instance, nil, runtime.Active) {
		bes := bpmn20.FindFlowNodesById(&instance.ProcessInfo.Definitions, ms.ElementId)
		if len(bes) == 0 {
			continue
		}
		ms.BaseElement = bes[0]
		// FIXME: rewrite this hack
		// instance.findActivity(ms.originActivity.Key())
		result = append(result, ms)
	}
	return result
}

// findCreatedTimers the list of all scheduled/creates timers in the engine, not yet completed
func (state *Engine) findCreatedTimers(instance *processInstanceInfo) (result []*runtime.Timer) {
	for _, t := range state.persistence.FindTimers(nil, ptr.To(instance.InstanceKey), runtime.TimerCreated) {
		bes := bpmn20.FindFlowNodesById(&instance.ProcessInfo.Definitions, t.ElementId)
		if len(bes) == 0 {
			continue
		}
		t.BaseElement = bes[0]
		result = append(result, t)
	}
	return result
}
