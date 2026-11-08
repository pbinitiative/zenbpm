package extensions

// ExecutionListenerEventTypeEnd is the event type of a listener which runs
// when its element's own work is done, before the token leaves the element.
// It is the one event type the engine runs so far, on a start event of a
// process (see bpmn20.TProcess.RunnableListenerEventTypes).
const ExecutionListenerEventTypeEnd = "end"

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

// FirstExecutionListenerOf answers the first listener of the given event type,
// or nil when there is none.
func FirstExecutionListenerOf(listeners []TExecutionListener, eventType string) *TExecutionListener {
	for i := range listeners {
		if listeners[i].EventType == eventType {
			return &listeners[i]
		}
	}
	return nil
}
