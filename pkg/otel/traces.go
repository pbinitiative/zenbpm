package otel

const (
	Prefix                        = "bpmn-"
	AttributeProcessInstanceKey   = Prefix + "instance-key"
	AttributeProcessId            = Prefix + "process-id"
	AttributeProcessDefinitionKey = Prefix + "definition-key"
	AttributeElementId            = Prefix + "element-id"
	AttributeElementKey           = Prefix + "element-key"
	AttributeElementName          = Prefix + "element-name"
	AttributeElementType          = Prefix + "element-type"

	SpanStatusToken = Prefix + "token-status"
)
