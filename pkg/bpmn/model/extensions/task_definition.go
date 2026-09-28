package extensions

type TTaskDefinition struct {
	TypeName string `xml:"type,attr"`
	// Retries is how many failures without an error code a job of the task
	// survives: a non-negative integer, or a FEEL expression starting with '='.
	Retries string `xml:"retries,attr"`
	// RetryBackoff is how long a job waits after a failure before it is handed
	// out again: an ISO-8601 duration, a comma-separated list of them (one per
	// failure, the last repeating), or a FEEL expression starting with '='.
	RetryBackoff string `xml:"retryBackoff,attr"`
}

type THeader struct {
	Key   string `xml:"key,attr"`
	Value string `xml:"value,attr"`
}

type TCalledDecision struct {
	DecisionId     string `xml:"decisionId,attr"`
	ResultVariable string `xml:"resultVariable,attr"`
	BindingType    string `xml:"bindingType,attr"`
	VersionTag     string `xml:"versionTag,attr"`
}
