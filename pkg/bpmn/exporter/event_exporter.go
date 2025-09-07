// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package exporter

type EventExporter interface {
	NewProcessEvent(event *ProcessEvent)
	EndProcessEvent(event *ProcessInstanceEvent)
	NewProcessInstanceEvent(event *ProcessInstanceEvent)
	NewElementEvent(event *ProcessInstanceEvent, elementInfo *ElementInfo)
}

type Intent string

const (
	ElementActivating Intent = "ELEMENT_ACTIVATING"
	ElementActivated  Intent = "ELEMENT_ACTIVATED"
	ElementCompleting Intent = "ELEMENT_COMPLETING"
	ElementCompleted  Intent = "ELEMENT_COMPLETED"
	SequenceFlowTaken Intent = "SEQUENCE_FLOW_TAKEN"
	Created           Intent = "CREATED"
)

type ProcessEvent struct {
	ProcessId    string
	ProcessKey   int64
	Version      int32
	XmlData      []byte
	ResourceName string
	Checksum     string
}

type ProcessInstanceEvent struct {
	ProcessId          string
	ProcessKey         int64
	Version            int32
	ProcessInstanceKey int64
	ActivityKey        int64
}

type ElementInfo struct {
	BpmnElementType string
	ElementId       string
	ElementKey      int64
	Intent          string // ELEMENT_ACTIVATING || ELEMENT_ACTIVATED || ELEMENT_COMPLETING || ELEMENT_COMPLETED
}
