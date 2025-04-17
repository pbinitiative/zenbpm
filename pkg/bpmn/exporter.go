package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

// AddEventExporter registers an EventExporter instance
func (engine *Engine) AddEventExporter(exporter exporter.EventExporter) {
	engine.exporters = append(engine.exporters, exporter)
}

func (engine *Engine) exportNewProcessEvent(processInfo runtime.ProcessDefinition, xmlData []byte, resourceName string, checksum string) {
	event := exporter.ProcessEvent{
		ProcessId:    processInfo.BpmnProcessId,
		ProcessKey:   processInfo.Key,
		Version:      processInfo.Version,
		XmlData:      xmlData,
		ResourceName: resourceName,
		Checksum:     checksum,
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
		ProcessInstanceKey: processInstance.Key,
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
		ProcessInstanceKey: processInstance.Key,
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
		ProcessInstanceKey: processInstance.Key,
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

func (engine *Engine) exportSequenceFlowEvent(process runtime.ProcessDefinition, processInstance runtime.ProcessInstance, flow bpmn20.TSequenceFlow) {
	event := exporter.ProcessInstanceEvent{
		ProcessId:          process.BpmnProcessId,
		ProcessKey:         process.Key,
		Version:            process.Version,
		ProcessInstanceKey: processInstance.Key,
	}
	info := exporter.ElementInfo{
		BpmnElementType: string(bpmn20.ElementTypeSequenceFlow),
		ElementId:       flow.Id,
		Intent:          string(exporter.SequenceFlowTaken),
	}
	for _, exp := range engine.exporters {
		exp.NewElementEvent(&event, &info)
	}
}
