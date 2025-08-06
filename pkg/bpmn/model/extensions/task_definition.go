package extensions

type TTaskDefinition struct {
	TypeName string `xml:"type,attr"`
	Retries  string `xml:"retries,attr"`
}

type THeader struct {
	Key   string `xml:"key,attr"`
	Value string `xml:"value,attr"`
}

type TCalledDecision struct {
	DecisionId     string `xml:"decisionId,attr"`
	ResultVariable string `xml:"resultVariable,attr"`
	BindingType    string `xml:"bindingType"`
	VersionTag     string `xml:"versionTag"`
}
