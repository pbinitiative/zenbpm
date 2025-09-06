// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package otel

const (
	Prefix                      = "bpmn-"
	AttributeProcessInstanceKey = Prefix + "instance-key"
	AttributeProcessId          = Prefix + "process-id"

	AttributeProcessDefinitionKey = Prefix + "definition-key"

	AttributeToken = Prefix + "token-key"

	AttributeElementId   = Prefix + "element-id"
	AttributeElementKey  = Prefix + "element-key"
	AttributeElementName = Prefix + "element-name"
	AttributeElementType = Prefix + "element-type"

	AttributeJobKey      = Prefix + "job-key"
	AttributeIncidentKey = Prefix + "incident-key"

	SpanStatusToken = Prefix + "token-status"
)
