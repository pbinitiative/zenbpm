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

// THeader is a single entry of a `taskHeaders` extension element. It mirrors
// the Zeebe task header shape (`zeebe:header`) so BPMN authored in Camunda
// Modeler round-trips unchanged.
type THeader struct {
	Id    string `xml:"id,attr"`
	Key   string `xml:"key,attr"`
	Value string `xml:"value,attr"`
}

// HeadersToMap converts the ordered header list into the key/value map exposed
// to job workers. Returns nil when there are no headers.
func HeadersToMap(headers []THeader) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	res := make(map[string]string, len(headers))
	for _, header := range headers {
		res[header.Key] = header.Value
	}
	return res
}

type TCalledDecision struct {
	DecisionId     string `xml:"decisionId,attr"`
	ResultVariable string `xml:"resultVariable,attr"`
	BindingType    string `xml:"bindingType,attr"`
	VersionTag     string `xml:"versionTag,attr"`
}
