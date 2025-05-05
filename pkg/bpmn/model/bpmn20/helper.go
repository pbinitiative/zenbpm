package bpmn20

import (
	"encoding/xml"
	"fmt"
)

func (definitions *TDefinitions) ResolveReferences() error {
	// Map to store FlowNodes by their IDs
	baseElementMap := make(map[string]BaseElement)
	err := collectBaseElements(definitions, &baseElementMap)
	if err != nil {
		return fmt.Errorf("failed to collect references: %w", err)
	}
	definitions.baseElements = baseElementMap
	// Try to resolve references for each base element implementing ResolvableReferences
	for _, baseElement := range baseElementMap {
		// Check if the baseElement implements ResolvableReferences
		resolvable, ok := baseElement.(ResolvableReferences)
		if !ok {
			continue // Skip if it doesn't implement the interface
		}
		err := resolvable.resolveReferences(&baseElementMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (definitions *TDefinitions) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Create an alias to avoid recursion
	type Alias TDefinitions
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(definitions),
	}

	// Unmarshal into the alias
	if err := d.DecodeElement(aux, &start); err != nil {
		return fmt.Errorf("failed to unmarshal TDefinitions: %w", err)
	}

	// Resolve references after unmarshalling
	if err := definitions.ResolveReferences(); err != nil {
		return fmt.Errorf("failed to resolve references: %w", err)
	}
	return nil
}

func (flowNode *TFlowNode) resolveReferences(refs *map[string]BaseElement) error {

	var incomingRefs, errIn = resolveSequenceFlows(&(flowNode.IncomingAssociation), refs)
	if errIn != nil {
		return fmt.Errorf("failed to resolve incoming references for FlowNode with ID [%s]", flowNode.GetId())
	}

	flowNode.incomingRefs = incomingRefs
	var outgoingRefs, errOut = resolveSequenceFlows(&(flowNode.OutgoingAssociation), refs)
	if errOut != nil {
		return fmt.Errorf("failed to resolve outgoing references for FlowNode with ID [%s]", flowNode.GetId())
	}
	flowNode.outgoingRefs = outgoingRefs

	return nil
}
func (sequenceFlow *TSequenceFlow) resolveReferences(refs *map[string]BaseElement) error {
	var srcRef, err1 = (*refs)[sequenceFlow.SourceRefId]
	if !err1 {
		return fmt.Errorf("failed to resolve incoming references for FlowNode with ID [%s]", sequenceFlow.GetId())
	}
	var srcRefFlowNode, err2 = srcRef.(FlowNode)
	if !err2 {
		return fmt.Errorf("resolved reference with ID [%s] is expected to be of FLowNode type", srcRef.GetId())
	}
	sequenceFlow.sourceRef = srcRefFlowNode

	var targetRef, err3 = (*refs)[sequenceFlow.TargetRefId]
	if !err3 {
		return fmt.Errorf("failed to resolve outgoing references for FlowNode with ID [%s]", sequenceFlow.GetId())
	}
	var targetRefFlowNode, err4 = targetRef.(FlowNode)
	if !err4 {
		return fmt.Errorf("resolved reference with ID [%s] is expected to be of FLowNode type", targetRef.GetId())
	}
	sequenceFlow.targetRef = targetRefFlowNode
	return nil
}

func resolveSequenceFlows(ids *[]string, refs *map[string]BaseElement) ([]SequenceFlow, error) {
	var flows []SequenceFlow
	for _, value := range *ids {
		ref, ok := (*refs)[value]
		if !ok {
			return nil, fmt.Errorf("no registered reference with ID [%s]", value)
		}
		sf, ok := ref.(SequenceFlow)
		if !ok {
			return nil, fmt.Errorf("found reference with ID [%s] is not of SequenceFlow type", value)
		}

		flows = append(flows, sf)
	}
	return flows, nil
}

func (dfe *TDefaultFlowExtension) resolveReferences(refs *map[string]BaseElement) error {
	if dfe.DefaultFlow != "" {
		var dflRef, err1 = (*refs)[dfe.DefaultFlow]
		if !err1 {
			return fmt.Errorf("failed to resolve default sequence flow references for Gateway with ID [%s]", dfe.DefaultFlow)
		}

		var dfltRef, err2 = dflRef.(SequenceFlow)
		if !err2 {
			return fmt.Errorf("resolved reference with ID [%s] is expected to be of SequenceFlow type", dfltRef.GetId())
		}
		dfe.defaultFlowRef = dfltRef
	}
	return nil
}
func (exclusiveGateway *TExclusiveGateway) resolveReferences(refs *map[string]BaseElement) error {
	if err := (*exclusiveGateway).TDefaultFlowExtension.resolveReferences(refs); err != nil {
		return err
	}
	return (*exclusiveGateway).TFlowNode.resolveReferences(refs)
}

func (inclusiveGateway *TInclusiveGateway) resolveReferences(refs *map[string]BaseElement) error {
	if err := (*inclusiveGateway).TDefaultFlowExtension.resolveReferences(refs); err != nil {
		return err
	}
	return (*inclusiveGateway).TFlowNode.resolveReferences(refs)
}

// FindFirstSequenceFlow returns the first flow definition for any given source and target element ID
func FindFirstSequenceFlow(source FlowNode, target FlowNode) (result SequenceFlow) {
	for _, flow := range source.GetOutgoingAssociation() {
		if flow.GetTargetRef().GetId() == target.GetId() {
			result = flow
			break
		}
	}
	return result
}

func FindFlowNodesById(definitions *TDefinitions, id string) (element FlowNode) {
	if baseElement, ok := definitions.baseElements[id]; ok {
		if flowNode, ok := baseElement.(FlowNode); ok {
			element = flowNode
		}
	}
	return element
}
