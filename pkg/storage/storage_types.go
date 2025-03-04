package storage

type ProcessDefinition struct {
	BpmnProcessId    string // The ID as defined in the BPMN file
	Version          int32  // A version of the process, default=1, incremented, when another process with the same ID is loaded
	ProcessKey       int64  // The engines key for this given BpmnProcessId with Version
	BpmnData         string // the raw source data, compressed and encoded via ascii85
	BpmnChecksum     string // internal checksum to identify different versions; using sha1 as string, all lower case; similar as git-hashes
	BpmnResourceName string // some name for the resource; optional, can be empty
}
