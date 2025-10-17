package extensions

type TLoopCharacteristics struct {
	InputCollection  string `xml:"inputCollection,attr,omitempty"`
	InputElement     string `xml:"inputElement,attr"`
	OutputCollection string `xml:"outputCollection,attr"`
	OutputElement    string `xml:"outputElement,attr"`
}
