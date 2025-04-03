package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
)

// // ProcessInstances returns the list of process instances
// // Hint: completed instances are prone to be removed from the list,
// // which means typically you only see currently active process instances
// func (state *BpmnEngineState) ProcessInstances() []*processInstanceInfo {
// 	return state.processInstances
// }

// FindProcessInstance searches for a given processInstanceKey
// and returns the corresponding processInstanceInfo, or otherwise nil
func (state *Engine) FindProcessInstance(processInstanceKey int64) *processInstanceInfo {
	return state.persistence.FindProcessInstanceByKey(processInstanceKey)
}

// Name returns the name of the engine, only useful in case you control multiple ones
func (state *Engine) Name() string {
	return state.name
}

// FindProcessesById returns all registered processes with given ID
// result array is ordered by version number, from 1 (first) and largest version (last)
func (state *Engine) FindProcessesById(id string) (infos []*runtime.ProcessDefinition) {
	processes := state.persistence.FindProcessesById(id)
	return processes
}

func (state *Engine) checkExclusiveGatewayDone(activity eventBasedGatewayActivity) {
	if !activity.OutboundCompleted() {
		return
	}

	// cancel other activities started by this one
	for _, ms := range state.persistence.FindMessageSubscription(ptr.To(activity.Key()), nil, nil, runtime.Active) {
		ms.MessageState = runtime.Withdrawn
	}
	for _, t := range state.persistence.FindTimers(ptr.To(activity.Key()), nil, runtime.TimerCreated) {
		t.TimerState = runtime.TimerCancelled
	}
}

func (b *Engine) Stop() {
}
