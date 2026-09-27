package extensions

type TTaskDefinition struct {
	TypeName string `xml:"type,attr"`
	Retries  string `xml:"retries,attr"`
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
