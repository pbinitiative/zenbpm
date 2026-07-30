package otel

const (
	// Prefix namespaces all ZenBPM specific span attributes following the
	// OpenTelemetry attribute naming conventions (dot separated namespaces).
	Prefix                      = "zenbpm."
	AttributeProcessInstanceKey = Prefix + "process.instance_key"
	AttributeProcessId          = Prefix + "process.id"

	AttributeProcessDefinitionKey = Prefix + "process.definition_key"

	AttributeToken = Prefix + "token.key"

	AttributeElementId   = Prefix + "element.id"
	AttributeElementKey  = Prefix + "element.key"
	AttributeElementName = Prefix + "element.name"
	AttributeElementType = Prefix + "element.type"

	AttributeJobKey      = Prefix + "job.key"
	AttributeIncidentKey = Prefix + "incident.key"

	AttributeDecisionId          = Prefix + "decision.id"
	AttributeDecisionKey         = Prefix + "decision.key"
	AttributeDecisionInstanceKey = Prefix + "decision.instance_key"
	AttributeDrdId               = Prefix + "decision.drd_id"

	SpanStatusToken = Prefix + "token.status"
)
