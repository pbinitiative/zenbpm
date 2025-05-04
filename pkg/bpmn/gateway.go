package bpmn

import (
	"context"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

func createCheckExclusiveGatewayDoneCommand(originActivity runtime.Activity) (cmds []command) {
	if originActivity.Element().GetType() == bpmn20.ElementTypeEventBasedGateway {
		evtBasedGatewayActivity := originActivity.(*eventBasedGatewayActivity)
		cmds = append(cmds, checkExclusiveGatewayDoneCommand{
			gatewayActivity: *evtBasedGatewayActivity,
		})
	}
	return cmds
}

// PARALLEL_GATEWAY_EXECUTOR ==============================================

type ParallelGatewayExecutor struct {
	FlowNodeExecutor
}

func (e *ParallelGatewayExecutor) Execute(ctx context.Context, batch storage.Batch, engine *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, originActivity runtime.Activity) (createFlowTransitions bool, activityResult runtime.Activity, nextCommands []command, err error) {
	e.FlowNodeExecutor.Execute(ctx, batch, engine, process, instance)

	var activity runtime.Activity
	nextCommands = []command{}
	createFlowTransitions, activity = engine.handleParallelGateway(process, instance, e.flowNode.element.(bpmn20.TParallelGateway), originActivity)

	return createFlowTransitions, activity, nextCommands, err
}

func (engine *Engine) handleParallelGateway(process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, element bpmn20.TParallelGateway, originActivity runtime.Activity) (continueFlow bool, resultActivity runtime.Activity) {
	resultActivity = instance.FindActiveActivityByElementId(element.Id)
	if resultActivity == nil {
		var be bpmn20.FlowNode = element
		resultActivity = &gatewayActivity{
			key:      engine.generateKey(),
			state:    runtime.ActivityStateActive,
			element:  be,
			parallel: true,
		}
		instance.AppendActivity(resultActivity)
	}
	sourceFlow := bpmn20.FindFirstSequenceFlow(&process.Definitions.Process.SequenceFlows, originActivity.Element().GetId(), element.GetId())
	resultActivity.(*gatewayActivity).SetInboundFlowCompleted(sourceFlow.Id)
	continueFlow = resultActivity.(*gatewayActivity).parallel && resultActivity.(*gatewayActivity).AreInboundFlowsCompleted()
	if continueFlow {
		resultActivity.(*gatewayActivity).SetState(runtime.ActivityStateCompleted)
	}
	return continueFlow, resultActivity
}

// EXCLUSIVE_GATEWAY_EXECUTOR ==============================================

type ExclusiveGatewayExecutor struct {
	FlowNodeExecutor
}

func (e *ExclusiveGatewayExecutor) Execute(ctx context.Context, batch storage.Batch, engine *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, originActivity runtime.Activity) (createFlowTransitions bool, activityResult runtime.Activity, nextCommands []command, err error) {
	var activity runtime.Activity
	nextCommands = []command{}
	activity = elementActivity{
		key:     engine.generateKey(),
		state:   runtime.ActivityStateActive,
		element: e.flowNode.element,
	}
	createFlowTransitions = true
	return createFlowTransitions, activity, nextCommands, err
}

// INCLUSIVE_GATEWAY_EXECUTOR ==============================================
type InclusiveGatewayExecutor struct {
	FlowNodeExecutor
}

func (e *InclusiveGatewayExecutor) Execute(ctx context.Context, batch storage.Batch, engine *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, originActivity runtime.Activity) (createFlowTransitions bool, activityResult runtime.Activity, nextCommands []command, err error) {
	var activity runtime.Activity
	nextCommands = []command{}
	activity = elementActivity{
		key:     engine.generateKey(),
		state:   runtime.ActivityStateActive,
		element: e.flowNode.element,
	}
	createFlowTransitions = true
	return createFlowTransitions, activity, nextCommands, err
}

// EVENT_BASED_GATEWAY_EXECUTOR ==============================================
type EventBasedGatewayExecutor struct {
	FlowNodeExecutor
}

func (e *EventBasedGatewayExecutor) Execute(ctx context.Context, batch storage.Batch, engine *Engine, process *runtime.ProcessDefinition, instance *runtime.ProcessInstance, originActivity runtime.Activity) (createFlowTransitions bool, activityResult runtime.Activity, nextCommands []command, err error) {
	var activity runtime.Activity
	nextCommands = []command{}
	activity = &eventBasedGatewayActivity{
		key:     engine.generateKey(),
		state:   runtime.ActivityStateCompleted,
		element: e.flowNode.element,
	}
	instance.AppendActivity(activity)
	createFlowTransitions = true
	return createFlowTransitions, activity, nextCommands, err
}
