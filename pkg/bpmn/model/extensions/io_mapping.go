package extensions

//TODO: This needs to be revised licensewise

type TIoMapping struct {
	Source string `xml:"source,attr"`
	Target string `xml:"target,attr"`
}

type TCalledElement struct {
	ProcessId   string  `xml:"processId,attr"`
	BindingType *string `xml:"bindingType,attr,omitempty"`
	VersionTag  *string `xml:"versionTag,attr,omitempty"`
}
