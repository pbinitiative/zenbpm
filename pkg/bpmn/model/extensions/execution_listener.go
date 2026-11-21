package extensions

// The event types of an execution listener. Where the engine runs which of
// them is decided by bpmn20.TProcess.ListenerEventTypesRunOn.
const (
	// ExecutionListenerEventTypeStart is the event type of a listener which
	// runs when its element is entered, after the element's input mappings
	// and before the element's own work.
	ExecutionListenerEventTypeStart = "start"
	// ExecutionListenerEventTypeEnd is the event type of a listener which runs
	// when its element's own work is done, after the element's output
	// mappings and before the token leaves the element.
	ExecutionListenerEventTypeEnd = "end"
)

// TExecutionListener is one entry of an `executionListeners` extension
// element. Its shape is the one of `zeebe:executionListener`, so a model drawn
// for Camunda 8 keeps its listeners. Like every extension element it is read
// whatever its namespace prefix is.
type TExecutionListener struct {
	// EventType is when the listener runs, for example "end".
	EventType string `xml:"eventType,attr"`
	// Type is the job type of the listener's job, a literal.
	Type string `xml:"type,attr"`
	// Retries is read as the retries of a zenbpm:taskDefinition, but only a
	// literal is accepted.
	Retries string `xml:"retries,attr"`
	// RetryBackoff is read as the retryBackoff of a zenbpm:taskDefinition, but
	// only a literal is accepted.
	RetryBackoff string `xml:"retryBackoff,attr"`
}
