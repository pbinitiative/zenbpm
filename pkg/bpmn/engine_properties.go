package bpmn

import (
	"github.com/bwmarrin/snowflake"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
)

type Engine struct {
	name string
	// _processes            []*ProcessInfo
	_processInstances []*processInstanceInfo
	// _messageSubscriptions []*MessageSubscription
	_jobs []*job
	// _timers               []*Timer
	taskHandlers []*taskHandler
	exporters    []exporter.EventExporter
	snowflake    *snowflake.Node
	persistence  BpmnEnginePersistenceService
}

type ProcessInfo struct {
	BpmnProcessId    string              // The ID as defined in the BPMN file
	Version          int32               // A version of the process, default=1, incremented, when another process with the same ID is loaded
	ProcessKey       int64               // The engines key for this given process with version
	definitions      bpmn20.TDefinitions // parsed file content
	bpmnData         string              // the raw source data, compressed and encoded via ascii85
	bpmnResourceName string              // some name for the resource
	bpmnChecksum     [16]byte            // internal checksum to identify different versions
}

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
func (state *Engine) FindProcessesById(id string) (infos []*ProcessInfo) {
	processes := state.persistence.FindProcessesById(id)
	return processes
}

func (state *Engine) checkExclusiveGatewayDone(activity eventBasedGatewayActivity) {
	if !activity.OutboundCompleted() {
		return
	}

	// cancel other activities started by this one
	for _, ms := range state.persistence.FindMessageSubscription(ptr.To(activity.Key()), nil, nil, Active) {
		ms.MessageState = Withdrawn
	}
	for _, t := range state.persistence.FindTimers(ptr.To(activity.Key()), nil, TimerCreated) {
		t.TimerState = TimerCancelled
	}
}

func (b *Engine) Stop() {
}
