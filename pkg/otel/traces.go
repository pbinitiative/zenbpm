package otel

// ZenBPM specific span attribute names shared by the BPMN and DMN engines.
const (
	// Prefix namespaces all ZenBPM specific span attributes following the
	// OpenTelemetry attribute naming conventions (dot separated namespaces).
	Prefix                      = "zenbpm."
	AttributeProcessInstanceKey = Prefix + "process.instance_key"
	AttributeProcessID          = Prefix + "process.id"

	AttributeProcessDefinitionKey = Prefix + "process.definition_key"

	AttributeToken = Prefix + "token.key"

	AttributeElementID   = Prefix + "element.id"
	AttributeElementKey  = Prefix + "element.key"
	AttributeElementName = Prefix + "element.name"
	AttributeElementType = Prefix + "element.type"

	AttributeJobKey      = Prefix + "job.key"
	AttributeIncidentKey = Prefix + "incident.key"

	AttributeDecisionID          = Prefix + "decision.id"
	AttributeDecisionKey         = Prefix + "decision.key"
	AttributeDecisionInstanceKey = Prefix + "decision.instance_key"
	AttributeDrdID               = Prefix + "decision.drd_id"

	SpanStatusToken = Prefix + "token.status"
)
