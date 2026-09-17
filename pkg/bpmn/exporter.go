package bpmn

import (
	"encoding/hex"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// AddEventExporter registers an EventExporter instance
func (engine *Engine) AddEventExporter(exporter exporter.EventExporter) {
	engine.exporters = append(engine.exporters, exporter)
}

// exportNewProcessEvent reports a stored definition to the exporters. It must
// not be called with an engine mutex held: an exporter may call back into the engine.
func (engine *Engine) exportNewProcessEvent(processInfo runtime.ProcessDefinition) {
	event := exporter.ProcessEvent{
		ProcessId:  processInfo.BpmnProcessId,
		ProcessKey: processInfo.Key,
		Version:    processInfo.Version,
		XmlData:    []byte(processInfo.BpmnData),
		Checksum:   hex.EncodeToString(processInfo.BpmnChecksum[:]),
	}
	for _, exp := range engine.exporters {
		exp.NewProcessEvent(&event)
	}
}

func (engine *Engine) exportEndProcessEvent(process runtime.ProcessDefinition, processInstance runtime.ProcessInstance) {
	event := exporter.ProcessInstanceEvent{
		ProcessId:          process.BpmnProcessId,
		ProcessKey:         process.Key,
		Version:            process.Version,
		ProcessInstanceKey: processInstance.ProcessInstance().Key,
	}
	for _, exp := range engine.exporters {
		exp.EndProcessEvent(&event)
	}
}

func (engine *Engine) exportProcessInstanceEvent(process runtime.ProcessDefinition, processInstance runtime.ProcessInstance) {
	event := exporter.ProcessInstanceEvent{
		ProcessId:          process.BpmnProcessId,
		ProcessKey:         process.Key,
		Version:            process.Version,
		ProcessInstanceKey: processInstance.ProcessInstance().Key,
	}
	for _, exp := range engine.exporters {
		exp.NewProcessInstanceEvent(&event)
	}
}

func (engine *Engine) exportElementEvent(process runtime.ProcessDefinition, processInstance runtime.ProcessInstance, element bpmn20.FlowNode, intent exporter.Intent) {
	event := exporter.ProcessInstanceEvent{
		ProcessId:          process.BpmnProcessId,
		ProcessKey:         process.Key,
		Version:            process.Version,
		ProcessInstanceKey: processInstance.ProcessInstance().Key,
	}
	info := exporter.ElementInfo{
		BpmnElementType: string(element.GetType()),
		ElementId:       element.GetId(),
		Intent:          string(intent),
	}
	for _, exp := range engine.exporters {
		exp.NewElementEvent(&event, &info)
	}
}

func (engine *Engine) exportSequenceFlowEvent(process runtime.ProcessDefinition, processInstance runtime.ProcessInstance, flow bpmn20.SequenceFlow) {
	event := exporter.ProcessInstanceEvent{
		ProcessId:          process.BpmnProcessId,
		ProcessKey:         process.Key,
		Version:            process.Version,
		ProcessInstanceKey: processInstance.ProcessInstance().Key,
	}
	info := exporter.ElementInfo{
		BpmnElementType: string(bpmn20.ElementTypeSequenceFlow),
		ElementId:       flow.GetId(),
		Intent:          string(exporter.SequenceFlowTaken),
	}
	for _, exp := range engine.exporters {
		exp.NewElementEvent(&event, &info)
	}
}
