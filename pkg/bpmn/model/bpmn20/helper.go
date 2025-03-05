package bpmn20

import (
	"html"
	"strings"
)

func FindSequenceFlows(sequenceFlows *[]TSequenceFlow, ids []string) (ret []TSequenceFlow) {
	for _, flow := range *sequenceFlows {
		for _, id := range ids {
			if id == flow.Id {
				ret = append(ret, flow)
			}
		}
	}
	return ret
}

// FindFirstSequenceFlow returns the first flow definition for any given source and target element ID
func FindFirstSequenceFlow(sequenceFlows *[]TSequenceFlow, sourceId string, targetId string) (result *TSequenceFlow) {
	for _, flow := range *sequenceFlows {
		if flow.SourceRef == sourceId && flow.TargetRef == targetId {
			result = &flow
			break
		}
	}
	return result
}

func FindFlowNodesById(definitions *TDefinitions, id string) (elements []*FlowNode) {
	appender := func(element *FlowNode) {
		if (*element).GetId() == id {
			elements = append(elements, element)
		}
	}
	for _, startEvent := range definitions.RootElements.Process().StartEvents {
		var be FlowNode = startEvent
		appender(&be)
	}
	for _, endEvent := range definitions.RootElements.Process().EndEvents {
		var be FlowNode = endEvent
		appender(&be)
	}
	for _, task := range definitions.RootElements.Process().ServiceTasks {
		var be FlowNode = task
		appender(&be)
	}
	for _, task := range definitions.RootElements.Process().UserTasks {
		var be FlowNode = task
		appender(&be)
	}
	for _, parallelGateway := range definitions.RootElements.Process().ParallelGateway {
		var be FlowNode = parallelGateway
		appender(&be)
	}
	for _, exclusiveGateway := range definitions.RootElements.Process().ExclusiveGateway {
		var be FlowNode = exclusiveGateway
		appender(&be)
	}
	for _, eventBasedGateway := range definitions.RootElements.Process().EventBasedGateway {
		var be FlowNode = eventBasedGateway
		appender(&be)
	}
	for _, intermediateCatchEvent := range definitions.RootElements.Process().IntermediateCatchEvent {
		var be FlowNode = intermediateCatchEvent
		appender(&be)
	}
	for _, intermediateCatchEvent := range definitions.RootElements.Process().IntermediateThrowEvent {
		var be FlowNode = intermediateCatchEvent
		appender(&be)
	}
	for _, inclusiveGateway := range definitions.RootElements.Process().InclusiveGateway {
		var be FlowNode = inclusiveGateway
		appender(&be)
	}
	return elements
}

// HasConditionExpression returns true, if there's exactly 1 expression present (as by the spec)
// and there's some non-whitespace-characters available
func (flow TSequenceFlow) HasConditionExpression() bool {
	return len(flow.ConditionExpression) == 1 && len(strings.TrimSpace(flow.GetConditionExpression())) > 0
}

// GetConditionExpression returns the embedded expression. There will be a panic thrown, in case none exists!
func (flow TSequenceFlow) GetConditionExpression() string {
	return html.UnescapeString(flow.ConditionExpression[0].Text)
}
